// arrow_io.c — Apache Arrow IPC reader/writer/streamer for L.                 //
// Shared library loaded via 2: — no libarrow dependency.                      //
// Reader: hand-rolled flatbuf parser (arrow_flatbuf.h) + AVX-512 kernels.     //
// Writer: flatcc-generated builders (generated/) + one-write file assembly.   //
// Terse macro vocabulary (Z/V/R/SW/CS/P/U): legend in arrow_flatbuf.h.        //
//                                                                             //
// Exports (bind via `2:`):                                                    //
//   arrow_read(path)          — Arrow IPC file → L table                      //
//   arrow_stream(src;dst)     — Arrow IPC → L splayed table on disk           //
//   arrow_write(table;path)   — L table → Arrow IPC file                      //
#include "l.h"                                                                  // L K object API
#include <stdio.h>                                                              // snprintf
#include <stdlib.h>                                                             // malloc, calloc, free
#include <pthread.h>                                                            // parallel multi-batch read workers
#include <sys/mman.h>                                                           // mmap, munmap, madvise
#include <sys/stat.h>                                                           // fstat, struct stat, mkdir
#include <fcntl.h>                                                              // open, O_RDONLY, O_CREAT
#include <unistd.h>                                                             // close, write, read
#include <string.h>                                                             // memcpy, memset, memcmp
#ifdef __AVX512F__                                                              // AVX-512 SIMD path
#include <immintrin.h>                                                          // _mm512_* intrinsics
#endif                                                                          // end AVX-512 guard
#include "generated/Schema_builder.h"                                           // flatcc-generated Schema builder
#include "generated/Message_builder.h"                                          // flatcc-generated Message builder
#include "generated/File_builder.h"                                             // flatcc-generated Footer builder
#include "flatcc/flatcc_builder.h"                                              // flatcc runtime builder API
#include "arrow_types.h"                                                        // ARROW_* type enum, constants
#include "arrow_flatbuf.h"                                                      // flatbuf reader + macro legend (LAST)

// base types (G/I/J/F/H/E) and NJ come from l.h; NI/NH are arrow-local        //
#define NI ((I)0x80000000)                                                      // L int null sentinel
#define NH ((H)0x8000)                                                          // L short null sentinel
Z F _nf(V){F x=0;R x/x;}                                                        // _nf.  NaN (the float null).

typedef struct{const char*name;I name_len,arrow_type,l_type,                    // Arrow schema field descriptor
    elem_size,dict_id,type_detail;                                              // byte width, dict id, unit/bitwidth
    char**dict_syms;I dict_count;                                               // for dictionary-encoded cols
    struct SymCacheS*uc;                                                        // shared utf8 dedup cache (or 0)
    I child_type,child_elem_size;                                               // for List: child element info
    I unsup;}ArrowField;                                                        // 1 = no L mapping (clean error)

// ── map_arrow_type ─────────────────────────────────────────────────────── //
// map_arrow_type.  Arrow type enum → L type + elem size.                      //
Z V map_arrow_type(const G*b,I tid,I tt,ArrowField*f){                          // b=flatbuf, tid=type id, tt=type table
    f->arrow_type=tid;f->dict_id=-1;f->type_detail=0;f->uc=0;f->unsup=0;        // init: no dict/detail/cache
    f->dict_syms=0;f->dict_count=0;f->child_type=0;f->child_elem_size=0;        // clear dict + child fields
    SW(tid){                                                                    // dispatch on Arrow type enum
    case ARROW_BOOL:f->l_type=KB;f->elem_size=1;break;                          // Bool → KB (1 byte per bool)
    case ARROW_INT:{I bw=fb_fieldi32(b,tt,FB_INT_BITWIDTH);                     // Int: read bitWidth from flatbuf
        f->type_detail=bw;                                                      // stash bitWidth for later use
        if(bw==8){f->l_type=KG;f->elem_size=1;}                                 // 8-bit → KG (byte)
        else if(bw==16){f->l_type=KH;f->elem_size=2;}                           // 16-bit → KH (short)
        else if(bw==32){f->l_type=KI;f->elem_size=4;}                           // 32-bit → KI (int)
        else{f->l_type=KJ;f->elem_size=8;}break;}                               // 64-bit → KJ (long)
    case ARROW_FLOAT:{I p=fb_fieldi16(b,tt,FB_FLOAT_PRECISION);                 // Float: read precision enum
        f->type_detail=p;                                                       // stash precision for later
        if(p==ARROW_SINGLE){f->l_type=KE;f->elem_size=4;}                       // SINGLE → KE (float32)
        else{f->l_type=KF;f->elem_size=8;}break;}                               // DOUBLE → KF (float64)
    case ARROW_UTF8:case ARROW_LARGEUTF8:                                       // Utf8/LargeUtf8: variable-length strings
        f->l_type=KC;f->elem_size=0;break;                                      // KC placeholder, offset-based
    case ARROW_DATE:f->type_detail=fb_fieldi16(b,tt,FB_DATE_UNIT);              // Date: read unit (day/ms)
        f->l_type=KD;f->elem_size=4;break;                                      // → KD (L date, int32 days)
    case ARROW_TIME:f->type_detail=fb_field(b,tt,FB_TIME_UNIT)                  // Time unit; ABSENT means the
        ?fb_fieldi16(b,tt,FB_TIME_UNIT):ARROW_MS;                               // flatbuf default: MILLISECOND
        f->l_type=KT;f->elem_size=4;break;                                      // → KT (L time, int32 ms)
    case ARROW_TIMESTAMP:f->type_detail=fb_fieldi16(b,tt,FB_TS_UNIT);           // Timestamp unit (default s=0)
        f->l_type=KP;f->elem_size=8;break;                                      // → KP timestamp (i64 ns since 2000)
    case ARROW_DURATION:f->type_detail=fb_field(b,tt,FB_DUR_UNIT)               // Duration unit; ABSENT means
        ?fb_fieldi16(b,tt,FB_DUR_UNIT):ARROW_MS;                                // the flatbuf default: ms
        f->l_type=KN;f->elem_size=8;break;                                      // → KN timespan (i64 ns)
    case ARROW_LIST:f->l_type=0;f->elem_size=0;break;                           // List: generic, child typed later
    default:f->l_type=0;f->elem_size=0;}}                                       // unknown: skip
// col_is_sym.  Does this column decode to an L symbol vector?                 //
Z I col_is_sym(ArrowField*f){R f->l_type==KS||                                  // dict-encoded reads back as KS
    f->arrow_type==ARROW_UTF8||f->arrow_type==ARROW_LARGEUTF8;}                 // utf8 reads back as KS

// ═══════════════════════════════════════════════════════════════════════════
// AVX-512 / scalar null expansion
// ═══════════════════════════════════════════════════════════════════════════
#ifdef __AVX512F__                                                              // AVX-512 SIMD null expansion path
// DEF_EXPAND.  One masked-copy kernel per width: valid→source, null→NV.       //
// T=elem type, NL=lanes/iter, MT=mask type, LD/SET/MLD = per-width intrinsics.//
#define DEF_EXPAND(NM,T,NL,MT,LD,SET,MLD)                                     \
Z V NM(T*__restrict__ d,const T*__restrict__ s,const G*bm,J n,T nv){          \
    __m512i vnull=SET(nv);J i;                                                \
    for(i=0;i+NL<=n;i+=NL){MT k=LD((MT*)(bm+(i>>3)));                         \
        _mm512_storeu_si512(d+i,MLD(vnull,k,s+i));}                           \
    for(;i<n;i++)d[i]=(bm[i>>3]&(1<<(i&7)))?s[i]:nv;}                           // scalar tail for remainder
DEF_EXPAND(expand_i32,I,16,__mmask16,_load_mask16,                              // 16 ints per 512-bit iter
    _mm512_set1_epi32,_mm512_mask_loadu_epi32)                                  //
DEF_EXPAND(expand_i64,J, 8,__mmask8 ,_load_mask8 ,                              // 8 longs per 512-bit iter
    _mm512_set1_epi64,_mm512_mask_loadu_epi64)                                  //
// shift_i32.  Subtract offset from every int32 (epoch shift, 16/iter).        //
Z V shift_i32(I*d,J n,I off){__m512i vo=_mm512_set1_epi32(off);                 // broadcast offset to 16 lanes
    J i;for(i=0;i+16<=n;i+=16)                                                  // 16 ints per 512-bit iteration
        _mm512_storeu_si512(d+i,_mm512_sub_epi32(                               // store d[i..i+15] = d[i..] - off
            _mm512_loadu_si512(d+i),vo));                                       // load 16 ints, subtract offset
    for(;i<n;i++)d[i]-=off;}                                                    // scalar tail
// shift_fuse_i32.  Masked null-expand + epoch shift in one pass.              //
Z V shift_fuse_i32(I*__restrict__ d,const I*__restrict__ s,                     // d=dest, s=source
    const G*bm,J n,I nv,I off){                                                 // nv=null, off=epoch offset
    __m512i vnull=_mm512_set1_epi32(nv),vo=_mm512_set1_epi32(off);J i;          // broadcast null + offset to lanes
    for(i=0;i+16<=n;i+=16){                                                     // 16 ints per iteration
        __mmask16 k=_load_mask16((__mmask16*)(bm+(i>>3)));                      // load 16 validity bits
        __m512i v=_mm512_mask_loadu_epi32(vnull,k,s+i);                         // masked load: valid→src, null→NI
        _mm512_storeu_si512(d+i,_mm512_mask_sub_epi32(v,k,v,vo));}              // valid lanes: subtract offset
    for(;i<n;i++)d[i]=(bm[i>>3]&(1<<(i&7)))?s[i]-off:nv;}                       // scalar tail
#else                                                                           // scalar fallback path
// DEF_EXPAND.  Scalar null expansion, one bitmap byte (8 elems) at a time:    //
// all-valid bytes bulk-copy, all-null bytes bulk-fill, mixed test each bit.   //
#define DEF_EXPAND(NM,T)                                                      \
Z V NM(T*d,const T*s,const G*bm,J n,T nv){                                    \
    J i;for(i=0;i<(n&~7);i+=8){G m=bm[i>>3];                                  \
        if(m==0xFF)memcpy(d+i,s+i,8*sizeof(T));                               \
        else if(!m)for(I j=0;j<8;j++)d[i+j]=nv;                               \
        else for(I j=0;j<8;j++)d[i+j]=(m&(1<<j))?s[i+j]:nv;}                  \
    for(;i<n;i++)d[i]=(bm[i>>3]&(1<<(i&7)))?s[i]:nv;}                           // scalar tail for remainder
DEF_EXPAND(expand_i32,I)                                                        // int32 null expansion
DEF_EXPAND(expand_i64,J)                                                        // int64 null expansion
// shift_i32.  Scalar subtract offset (epoch shift, Arrow→L).                  //
Z V shift_i32(I*d,J n,I off){for(J i=0;i<n;i++)d[i]-=off;}                      // Arrow epoch (1970) → L (2000)
// shift_fuse_i32.  Scalar null-expand + shift in one pass.                    //
Z V shift_fuse_i32(I*d,const I*s,const G*bm,J n,I nv,I off){                    // d=dest, s=src, off=epoch delta
    for(J i=0;i<n;i++)d[i]=(bm[i>>3]&(1<<(i&7)))?s[i]-off:nv;}                  // valid: copy-shift; null: NI
#endif                                                                          // end AVX-512/scalar guard
// expand_f64.  Doubles ride the i64 kernel: NaN is just a 64-bit pattern.     //
Z V expand_f64(F*d,const F*s,const G*bm,J n){J nb;F nv=_nf();                   // NaN as float null sentinel
    memcpy(&nb,&nv,8);expand_i64((J*)d,(const J*)s,bm,n,nb);}                   // bit-identical masked copy
// expand_f32.  Reals ride an i32-width copy: NaN is just a 32-bit pattern.    //
Z V expand_f32(E*d,const E*s,const G*bm,J n){I nb;E nv=(E)_nf();                // NaN as real null sentinel
    memcpy(&nb,&nv,4);expand_i32((I*)d,(const I*)s,bm,n,nb);}                   // bit-identical masked copy
// expand_i16.  Scalar null expansion for shorts (rare; SIMD not worth it).    //
Z V expand_i16(H*d,const H*s,const G*bm,J n,H nv){                              // d=dest, s=src, nv=0Nh
    for(J i=0;i<n;i++)d[i]=(bm[i>>3]&(1<<(i&7)))?s[i]:nv;}                      // valid: copy; null: sentinel
// expand_bool.  Unpack Arrow bitpacked bools → 1 byte per bool.               //
Z V expand_bool(G*d,const G*s,J n){                                             // d=dest bytes, s=packed bits
    for(J i=0;i<n;i++)d[i]=(s[i>>3]>>(i&7))&1;}                                 // extract bit i → byte 0 or 1

// ts_ns_mul.  Arrow TimeUnit (s/ms/us/ns) → integer nanosecond multiplier.    //
Z J ts_ns_mul(I u){R u==ARROW_NS?1LL:u==ARROW_US?1000LL                         // ns:×1 us:×1e3
    :u==ARROW_MS?1000000LL:1000000000LL;}                                       // ms:×1e6 s:×1e9

// ═══════════════════════════════════════════════════════════════════════════
// utf8 dedup intern — content-addressed local cache for plain Utf8 columns.
// Each DISTINCT string is interned via sn() exactly once; repeats resolve
// via a local open-addressing table with linear probing.  The table
// doubles at 75% load — a rehash only re-places the stored pointers, it
// never re-interns or re-compares strings.  At the growth cap new
// distinct strings fall through to sn() uncached, so correctness never
// depends on the cache.
// ═══════════════════════════════════════════════════════════════════════════
typedef struct{const G*p;I l;unsigned hg;S s;}SymCE;                            // str ptr, len, hash tag, sym
typedef struct SymCacheS{SymCE*e;J sz,cnt;}SymCache;                            // slot array, slot count, used count
#define SYM_CACHE_INIT (1LL<<16)                                                // 64K slots to start (1.5MB)
#define SYM_CACHE_CAP  (1LL<<23)                                                // 8M-slot growth ceiling
// sym_hash.  FNV-1a over the full string bytes (strings are short).           //
Z unsigned sym_hash(const G*p,I l){unsigned h=2166136261u;                      // FNV offset basis
    for(I j=0;j<l;j++)h=(h^p[j])*16777619u;R h;}                                // fold every byte (exact hash)
// sym_grow.  Double the table; re-place live entries.  1 ok, 0 OOM.           //
Z I sym_grow(SymCache*c){J ns=c->sz*2,m=ns-1;                                   // new size + probe mask
    SymCE*ne=(SymCE*)calloc(ns,sizeof(SymCE));P(!ne,0)                          // OOM: keep old (still correct)
    for(J i=0;i<c->sz;i++)if(c->e[i].s){                                        // re-place each live entry
        J h=c->e[i].hg&m;                                                       // fresh home from stored tag
        while(ne[h].s)h=(h+1)&m;ne[h]=c->e[i];}                                 // linear probe to first empty
    free(c->e);c->e=ne;c->sz=ns;R 1;}                                           // swap in the doubled table
// sym_intern.  Dedup lookup → interned symbol; miss interns via sn once.      //
// hr = precomputed sym_hash(p,l) (callers hash early so the home slot can     //
// be PREFETCHED a few strings ahead — the probe is DRAM-latency-bound).       //
Z S sym_intern(SymCache*c,const G*p,I l,unsigned hr){                           // p,l = string bytes in mmap
    J m=c->sz-1,h=hr&m;                                                         // probe mask + home slot
    while(c->e[h].s){SymCE*e=c->e+h;                                            // occupied: compare content
        if(e->hg==hr&&e->l==l&&!memcmp(e->p,p,l))R e->s;                        // tag gate, then exact compare
        h=(h+1)&m;}                                                             // collision: next slot
    S s=sn((char*)p,l);                                                         // miss: intern via sn (once)
    if(4*(c->cnt+1)>3*c->sz){                                                   // insert would pass 75% load
        if(c->sz>=SYM_CACHE_CAP||!sym_grow(c))R s;                              // capped/OOM: serve uncached
        m=c->sz-1;h=hr&m;                                                       // re-derive slot in new table
        while(c->e[h].s)h=(h+1)&m;}                                             // probe to first empty
    c->e[h].p=p;c->e[h].l=l;c->e[h].hg=hr;c->e[h].s=s;c->cnt++;R s;}            // record + return interned sym
// sym_intern_mt.  Shared-cache variant for parallel batch decode — fully      //
// LOCK-FREE.  Probes acquire-load s; a non-zero s guarantees l/hg/p are       //
// visible (they are stored before the release-store of s).  A miss           //
// interns via sn (idempotent: every caller gets the same pointer), then       //
// claims the empty slot by CAS on p; the loser of a claim race waits for      //
// the winner's publish (3 plain stores away) and re-compares.  Same           //
// string ⇒ same home slot + probe order ⇒ no duplicate entries.  The          //
// shared table is pre-sized and never grows (growth would swap the slot       //
// array under concurrent readers); at saturation misses serve uncached.      //
Z S sym_intern_mt(SymCache*c,const G*p,I l,unsigned hr){                        // p,l = string bytes in mmap
    J m=c->sz-1,h=hr&m;                                                         // probe mask + home slot
    for(;;){S s=__atomic_load_n(&c->e[h].s,__ATOMIC_ACQUIRE);                   // lock-free probe
        if(s){if(c->e[h].hg==hr&&c->e[h].l==l&&!memcmp(c->e[h].p,p,l))R s;      // published: tag+exact compare
            h=(h+1)&m;continue;}                                                // other string: next slot
        const G*q=__atomic_load_n(&c->e[h].p,__ATOMIC_ACQUIRE);                 // s empty: claimed mid-publish?
        if(q){do s=__atomic_load_n(&c->e[h].s,__ATOMIC_ACQUIRE);while(!s);      // yes: spin for the publish
            if(c->e[h].hg==hr&&c->e[h].l==l&&!memcmp(c->e[h].p,p,l))R s;        // (bounded: 3 stores away)
            h=(h+1)&m;continue;}                                                // other string: next slot
        S sv=sn((char*)p,l);                                                    // miss: global intern — sn is
        J n=__atomic_load_n(&c->cnt,__ATOMIC_RELAXED);                          // idempotent, so claim races
        if(4*(n+1)>3*c->sz)R sv;                                                // saturated: serve uncached
        const G*ex=0;                                                           // claim the slot: CAS 0 → p
        if(__atomic_compare_exchange_n(&c->e[h].p,&ex,p,0,                      //
            __ATOMIC_ACQ_REL,__ATOMIC_ACQUIRE)){                                //
            c->e[h].l=l;c->e[h].hg=hr;                                          // fill entry fields first,
            __atomic_store_n(&c->e[h].s,sv,__ATOMIC_RELEASE);                   // then publish via s (release)
            __atomic_fetch_add(&c->cnt,1,__ATOMIC_RELAXED);R sv;}}}             // count; lost race: re-read h
// sym_cache_new.  Pre-sized shared cache: 2× headroom, but never sized        //
// past rows/2 slots — dedup only pays when strings repeat, and a smaller      //
// table keeps the latency-bound probes in fewer, warmer pages.                //
Z SymCache*sym_cache_new(J want){                                               // want = expected row count
    J sz=SYM_CACHE_INIT;while(2*sz<want&&sz<SYM_CACHE_CAP)sz*=2;                // grow toward rows/2, capped
    SymCache*c=(SymCache*)malloc(sizeof(SymCache));U(c)                         //
    c->e=(SymCE*)calloc(sz,sizeof(SymCE));                                      // zero slots = empty
    if(!c->e){free(c);R 0;}                                                     // OOM: caller falls back
    c->sz=sz;c->cnt=0;R c;}                                                     //
// sym_cache_free.  Destroy a shared cache (symbols stay interned).            //
Z V sym_cache_free(SymCache*c){if(!c)R;free(c->e);free(c);}                     //
// sym_cache_attach.  Give every utf8 field one shared cache; 0 if none.       //
Z SymCache*sym_cache_attach(ArrowField*flds,I nc,J total){                      // total = expected row count
    SymCache*uc=0;                                                              //
    for(I c=0;c<nc;c++)if(flds[c].arrow_type==ARROW_UTF8||                      // every plain-utf8 column
        flds[c].arrow_type==ARROW_LARGEUTF8){                                   // shares the ONE cache
        if(!uc)uc=sym_cache_new(total);                                         // lazily create on first
        flds[c].uc=uc;}                                                         // attach (0 on OOM = local)
    R uc;}                                                                      //

// ═══════════════════════════════════════════════════════════════════════════
// read_column — convert one column from mmap'd Arrow body into ktn
// ═══════════════════════════════════════════════════════════════════════════
// read_column.  Convert one Arrow column buffer → L K vector.                 //
Z K read_column(const G*body,J vo,J vl,J do2,J dl,                              // vo=validity off, do2=data off
    J oo,J ol,J nc,J nr,ArrowField*f){                                          // oo=offset off, nr=row count
    I et=f->l_type;const G*src=body+do2;                                        // target L type; data pointer
    const G*bm=(vl>0&&nc>0)?(body+vo):0;                                        // validity bitmap (null if none)
    if(f->arrow_type==ARROW_BOOL){K r=ktn(KB,(I)nr);                            // Bool: alloc byte vector
        expand_bool(kG(r),src,nr);                                              // unpack bits → 1 byte per bool
        if(bm)for(J i=0;i<nr;i++)                                               // apply null bitmap if present
            if(!(bm[i>>3]&(1<<(i&7))))kG(r)[i]=0;                               // null bools → 0
        R r;}                                                                   // done: bool column
    if(f->elem_size>0&&et!=KC&&et!=KS&&et!=KB){K r=ktn(et,(I)nr);               // fixed-width numeric: alloc typed vec
        if(et==KD&&!bm){memcpy(kI(r),src,(size_t)nr*4);                         // Date no-null: bulk copy int32
            shift_i32(kI(r),nr,ARROW_L_DATE_OFFSET);R r;}                       // epoch shift 1970→2000
        if(et==KD&&bm){shift_fuse_i32(kI(r),(const I*)src,bm,                   // Date with nulls: fused expand+shift
            nr,NI,ARROW_L_DATE_OFFSET);R r;}                                    // null dates → NI
                                                                                // ── Timestamp → KP (i64 ns since 2000) / Duration → KN (i64 ns) ──  //
        if(et==KP||et==KN){const J*s=(const J*)src;J*d=kJ(r);                   // i64 Arrow source → i64 L dest
            J mul=ts_ns_mul(f->type_detail);                                    // unit (s/ms/us/ns) → ns multiplier
            J off=et==KP?ARROW_L_TS_OFFSET_NS:0;                                // KP shifts 1970→2000; KN has no epoch
            for(J i=0;i<nr;i++){                                                // convert each value
                if(bm&&!(bm[i>>3]&(1<<(i&7))))d[i]=NJ;                          // null → NJ
                else d[i]=s[i]*mul-off;}                                        // unit→ns, epoch shift
            R r;}                                                               // done: timestamp/duration column
        if(!bm)memcpy(kG(r),src,(size_t)nr*f->elem_size);                       // no nulls: bulk memcpy
        else if(et==KI||et==KT)expand_i32(kI(r),(const I*)src,bm,nr,NI);        // int32/time with nulls → NI
        else if(et==KJ)expand_i64(kJ(r),(const J*)src,bm,nr,NJ);                // int64 with nulls → NJ
        else if(et==KF)expand_f64(kF(r),(const F*)src,bm,nr);                   // float64 with nulls → NaN
        else if(et==KE)expand_f32((E*)kG(r),(const E*)src,bm,nr);               // float32 with nulls → NaN
        else if(et==KH)expand_i16((H*)kG(r),(const H*)src,bm,nr,NH);            // int16 with nulls → 0Nh
        else memcpy(kG(r),src,(size_t)nr*f->elem_size);                         // other fixed: raw copy (KG)
        if(et==KT&&f->type_detail==ARROW_SEC){I*d=kI(r);                        // Time32[s]: L time is ms —
            for(J i=0;i<nr;i++)if(d[i]!=NI)d[i]*=1000;}                         // scale valid values ×1000
        R r;}                                                                   // done: fixed-width column
                                                                                // ── Dictionary-encoded → symbol vector (indices in data buf) ──         //
    if(f->dict_id>=0&&f->l_type==KS){                                           // dict-encoded column
        const I*idx=(const I*)src;                                              // int32 dictionary indices
        K r=ktn(KS,(I)nr);S*d=kS(r);                                            // alloc symbol vector + raw dest
        S es=ss((char*)"");S*syms=f->dict_syms;I dc=f->dict_count;              // hoist empty sym + dict fields
        if(!bm&&syms)for(J i=0;i<nr;i++){I x=idx[i];                            // no-null fast path: pure gather
            d[i]=(x>=0&&x<dc)?syms[x]:es;}                                      // bounds-checked symbol lookup
        else for(J i=0;i<nr;i++){                                               // nulls present (or no dict)
            if(bm&&!(bm[i>>3]&(1<<(i&7))))d[i]=es;                              // null → empty string
            else if(syms&&idx[i]>=0&&idx[i]<dc)d[i]=syms[idx[i]];               // valid index → interned symbol
            else d[i]=es;}                                                      // out-of-range → empty string
        R r;}                                                                   // done: dict column
                                                                                // ── List → generic K list of typed vectors ──                            //
    if(f->arrow_type==ARROW_LIST&&oo>0){                                        // Arrow List column
        const I*off=(const I*)(body+oo);                                        // int32 offsets array
        const G*child=body+do2;                                                 // child data (flattened)
        I ct=f->child_type;I cs=f->child_elem_size;                             // child L type + byte width
        K r=ktn(0,(I)nr);                                                       // alloc generic K list
        for(J i=0;i<nr;i++){                                                    // build each sub-vector
            if(bm&&!(bm[i>>3]&(1<<(i&7)))){kK(r)[i]=ktn(ct,0);}                 // null → empty typed vector
            else{I s=off[i],l=off[i+1]-s;                                       // s=start offset, l=length
                K v=ktn(ct,l);if(cs>0)memcpy(kG(v),child+s*cs,l*cs);            // alloc child vec, copy data
                kK(r)[i]=v;}}                                                   // store sub-vector in list
        R r;}                                                                   // done: list column
                                                                                // ── Plain Utf8 → symbol vector ──                                      //
    if(f->arrow_type==ARROW_UTF8||f->arrow_type==ARROW_LARGEUTF8){              // variable-length string column
        const I*o32=(const I*)(body+oo);                                        // Utf8: int32 offsets
        const J*o64=(const J*)(body+oo);                                        // LargeUtf8: int64 offsets
        I lg=f->arrow_type==ARROW_LARGEUTF8;                                    // which offset width applies
#define OFF_AT(IX) (lg?o64[IX]:(J)o32[IX])                                      // width-correct offset fetch
        K r=ktn(KS,(I)nr);                                                      // alloc symbol vec
        S*d=kS(r);S es=ss((char*)"");                                           // raw dest + hoisted empty sym
        SymCache lc,*c=f->uc;                                                   // shared (cross-batch) cache,
        if(!c){lc.sz=SYM_CACHE_INIT;lc.cnt=0;                                   // else growable local one
            lc.e=(SymCE*)calloc(lc.sz,sizeof(SymCE));c=&lc;}                    // (legend above sym_intern)
        enum{UBK=16};unsigned hh[UBK];                                          // hash-ahead block: the probe
        for(J i0=0;i0<nr;i0+=UBK){J nb=nr-i0<UBK?nr-i0:UBK;                     // is DRAM-bound, so hash and
            J m=c->sz-1;                                                        // PREFETCH 16 slots ahead of
            for(J k=0;k<nb;k++){J i=i0+k;                                       // the dependent probe chain
                if(bm&&!(bm[i>>3]&(1<<(i&7)))){hh[k]=0;d[i]=es;continue;}       // null → empty, no probe
                J s=OFF_AT(i);hh[k]=sym_hash(src+s,(I)(OFF_AT(i+1)-s));         // hash now …
                __builtin_prefetch(c->e+(hh[k]&m),0,1);}                        // … pull home slot into cache
            for(J k=0;k<nb;k++){J i=i0+k;                                       // probe pass: slots now warm
                if(bm&&!(bm[i>>3]&(1<<(i&7))))continue;                         // (nulls already assigned)
                J s=OFF_AT(i);I l=(I)(OFF_AT(i+1)-s);                           //
                d[i]=f->uc?sym_intern_mt(c,src+s,l,hh[k])                       // shared: lock-free probes,
                          :sym_intern(c,src+s,l,hh[k]);}}                       // local: single-thread caller
#undef OFF_AT                                                                   // offset fetch no longer needed
        if(!f->uc)free(lc.e);                                                   // local cache dies with column
        R r;}                                                                   // done: utf8 column
    R krr((char*)"arrow: unsupported type");}                                   // unhandled Arrow type

// ═══════════════════════════════════════════════════════════════════════════
// parse_batch — parse one RecordBatch, return columns as K array
// body=mmap'd body, meta=flatbuf metadata, nc=column count
// ═══════════════════════════════════════════════════════════════════════════
// BUF_O/BUF_L.  Buffer struct i in a Buffer vector: body offset / length.     //
#define BUF_O(BD,IX) (*(const J*)((BD)+(IX)*ARROW_BUFDESC_SIZE))                // Buffer[i].offset (i64)
#define BUF_L(BD,IX) (*(const J*)((BD)+(IX)*ARROW_BUFDESC_SIZE+8))              // Buffer[i].length (i64)
// ND_NC.  FieldNode struct i: null_count (second i64 of the 16B struct).      //
#define ND_NC(ND,IX) (*(const J*)((ND)+(IX)*ARROW_FIELDNODE_SIZE+8))            // FieldNode[i].null_count
// parse_batch.  Decode one RecordBatch flatbuf → K column array.              //
Z J parse_batch(const G*meta,const G*body,I nc,ArrowField*flds,                 // meta=flatbuf, body=data region
    K*out_cols,J*out_nrows){                                                    // out: column K objects + row count
    I mr=fb_deref(meta,0);                                                      // deref Message root table
    I rb=fb_union_val(meta,mr,FB_MSG_HEADER_TYPE);                              // resolve RecordBatch union
    J nr=fb_fieldi64(meta,rb,FB_RB_LENGTH);                                     // row count for this batch
    *out_nrows=nr;                                                              // return row count to caller
    I nvo=fb_vecoff(meta,rb,FB_RB_NODES);                                       // offset to FieldNode vector
    I buo=fb_vecoff(meta,rb,FB_RB_BUFFERS);                                     // offset to Buffer vector
    const G*nd=fb_vecdat(meta,nvo);                                             // raw FieldNode structs (16B each)
    const G*bd=fb_vecdat(meta,buo);                                             // raw Buffer structs (16B each)
    I bi=0,ni=0;                                                                // buffer index, node index
    for(I c=0;c<nc;c++){                                                        // iterate over columns
        J ncnt=ND_NC(nd,ni);ni++;                                               // null_count from FieldNode[ni]
        J b0o=BUF_O(bd,bi),b0l=BUF_L(bd,bi);bi++;                               // buf0: validity bitmap (off,len)
        J b1o=BUF_O(bd,bi),b1l=BUF_L(bd,bi);bi++;                               // buf1: data (or offsets for var-len)
        J b2o=0,b2l=0;                                                          // buf2: extra (utf8 data / child)
        if(flds[c].dict_id<0&&(flds[c].arrow_type==ARROW_UTF8||                 // Utf8 (not dict): 3 bufs
           flds[c].arrow_type==ARROW_LARGEUTF8)){                               // validity + offsets + data
            b2o=b1o;b2l=b1l;                                                    // shift: offsets→b2, read data→b1
            b1o=BUF_O(bd,bi);b1l=BUF_L(bd,bi);bi++;}                            // buf2=data bytes
        if(flds[c].arrow_type==ARROW_LIST){                                     // List: offsets=b1, child=b2+b3
            b2o=b1o;b2l=b1l;                                                    // offsets in b2o (was b1)
            ni++;                                                               // skip child FieldNode
            bi++;                                                               // skip child validity
            b1o=BUF_O(bd,bi);b1l=BUF_L(bd,bi);bi++;}                            // child data buf
        out_cols[c]=read_column(body,b0o,b0l,b1o,b1l,b2o,b2l,                   // convert bufs → K vector
            ncnt,nr,&flds[c]);                                                  // pass null count + field info
        if(!out_cols[c]){for(I k=0;k<c;k++)r0(out_cols[k]);R -1;}}              // fail: free the partial cols
    R nr;}                                                                      // return batch row count

// ═══════════════════════════════════════════════════════════════════════════
// open_arrow — mmap file, parse footer+schema. Shared by read/stream.
// ═══════════════════════════════════════════════════════════════════════════
typedef struct{                                                                 // ArrowFile: parsed IPC file state
    const G*map;J fsz;                                                          // mmap base pointer + file size
    const G*fb;I fr;                                                            // footer flatbuf + root table offset
    ArrowField*flds;I nc;                                                       // schema field descriptors + count
    I nbat;const G*blk_dat;                                                     // batch count + Block struct data
    I bad_dict;                                                                 // 1 = compressed dict batch seen
}ArrowFile;                                                                     // end ArrowFile

// open_arrow.  Mmap IPC file, validate magic, parse footer+schema.            //
Z I open_arrow(const char*fn,ArrowFile*af){                                     // returns 0 ok, <0 error code
    af->bad_dict=0;                                                             // no compressed dicts seen yet
    I fd=open(fn,O_RDONLY);if(fd<0)R -1;                                        // open file readonly
    struct stat st;fstat(fd,&st);af->fsz=st.st_size;                            // get file size
    if(af->fsz<22){close(fd);R -2;}                                             // too small for valid Arrow IPC
    I mf=MAP_PRIVATE;                                                           // private mapping (COW)
#ifdef MAP_POPULATE                                                             // Linux: prefault pages
    mf|=MAP_POPULATE;                                                           // populate page tables eagerly
#endif                                                                          // end MAP_POPULATE guard
    af->map=(const G*)mmap(0,af->fsz,PROT_READ,mf,fd,0);close(fd);              // mmap entire file, close fd
    if(af->map==MAP_FAILED)R -3;                                                // mmap failed
    madvise((V*)af->map,af->fsz,MADV_SEQUENTIAL);                               // hint: sequential read pattern
#ifdef MADV_HUGEPAGE                                                            // Linux: use transparent huge pages
    madvise((V*)af->map,af->fsz,MADV_HUGEPAGE);                                 // reduce TLB misses for large files
#endif                                                                          // end MADV_HUGEPAGE guard
    if(memcmp(af->map,ARROW_MAGIC,ARROW_MAGIC_LEN)||                            // validate "ARROW1" at file start
       memcmp(af->map+af->fsz-ARROW_MAGIC_LEN,ARROW_MAGIC,ARROW_MAGIC_LEN)){    // and "ARROW1" at file end
        munmap((V*)af->map,af->fsz);R -4;}                                      // bad magic → cleanup + error
    I ftsz=*(const I*)(af->map+af->fsz-4-ARROW_MAGIC_LEN);                      // footer size (LE i32 before magic)
    if(ftsz<4||(J)ftsz>af->fsz-18){                                             // footer must fit between the
        munmap((V*)af->map,af->fsz);R -7;}                                      // magics (8+…+4+6) — else OOB
    af->fb=af->map+af->fsz-4-ARROW_MAGIC_LEN-ftsz;                              // footer flatbuf starts here
    af->fr=fb_deref(af->fb,0);                                                  // deref Footer root table
    I so=fb_field(af->fb,af->fr,FB_FOOTER_SCHEMA);                              // get Schema field offset
    if(!so){munmap((V*)af->map,af->fsz);R -5;}                                  // no schema → invalid file
    I st2=fb_deref(af->fb,so);                                                  // deref Schema table
    I fv=fb_vecoff(af->fb,st2,FB_SCHEMA_FIELDS);                                // get fields vector offset
    af->nc=fb_veclen(af->fb,fv);                                                // column count = len(fields)
    if(af->nc<=0){munmap((V*)af->map,af->fsz);R -6;}                            // no columns → invalid schema
    af->flds=(ArrowField*)malloc(af->nc*sizeof(ArrowField));                    // alloc field descriptors
    for(I i=0;i<af->nc;i++){I ft=fb_vecelt(af->fb,fv,i);                        // iterate schema fields
        af->flds[i].name=fb_string(af->fb,ft,FB_FIELD_NAME);                    // read column name string
        af->flds[i].name_len=fb_stringlen(af->fb,ft,FB_FIELD_NAME);             // read column name length
        I tid=fb_union_type(af->fb,ft,FB_FIELD_TYPE_TYPE);                      // Arrow type enum from union
        I ttbl=fb_union_val(af->fb,ft,FB_FIELD_TYPE_TYPE);                      // type detail table offset
        map_arrow_type(af->fb,tid,ttbl,&af->flds[i]);                           // fill l_type + elem_size
// Parse child type for List columns                                           //
        if(tid==ARROW_LIST){                                                    // List columns have child schema
            I cv=fb_vecoff(af->fb,ft,FB_FIELD_CHILDREN);                        // children vector offset
            if(fb_veclen(af->fb,cv)>0){                                         // has at least one child field
                I cf=fb_vecelt(af->fb,cv,0);                                    // first (only) child field
                I ctid=fb_union_type(af->fb,cf,FB_FIELD_TYPE_TYPE);             // child Arrow type enum
                I cttbl=fb_union_val(af->fb,cf,FB_FIELD_TYPE_TYPE);             // child type detail table
                ArrowField tmp;map_arrow_type(af->fb,ctid,cttbl,&tmp);          // map child → L type
                af->flds[i].child_type=tmp.l_type;                              // store child L type
                af->flds[i].child_elem_size=tmp.elem_size;}}                    // store child byte width
        I doff=fb_field(af->fb,ft,FB_FIELD_DICT);                               // check for dictionary encoding
        if(doff){I dt=fb_deref(af->fb,doff);                                    // has dict: deref DictionaryEncoding
            af->flds[i].dict_id=(I)fb_fieldi64(af->fb,dt,FB_DICT_ID);           // read dictionary id
            af->flds[i].l_type=KS;                                              // dict-encoded reads back as KS
            if(tid!=ARROW_UTF8&&tid!=ARROW_LARGEUTF8)                           // dict VALUES must be strings —
                af->flds[i].unsup=1;                                            // int/float dicts have no L form
            I ito=fb_field(af->fb,dt,FB_DICT_INDEXTYPE);                        // declared index type (Int)
            if(ito){I it=fb_deref(af->fb,ito);                                  // present: check the bit width
                I ibw=fb_fieldi32(af->fb,it,FB_INT_BITWIDTH);                   // absent bitWidth ⇒ default 32
                if(ibw&&ibw!=32)af->flds[i].unsup=1;}                           // only int32 indices supported
        }else af->flds[i].dict_id=-1;                                           // no dict encoding on field
        if(!af->flds[i].l_type&&tid!=ARROW_LIST)af->flds[i].unsup=1;            // unmapped type (decimal, …)
        if(tid==ARROW_LIST&&af->flds[i].child_elem_size<=0)                     // List of unmapped/var child
            af->flds[i].unsup=1;                                                // has no L form either
        if(tid==ARROW_DATE&&(af->flds[i].type_detail!=0||                       // Date64[ms] (explicit unit or
            !fb_field(af->fb,ttbl,FB_DATE_UNIT)))af->flds[i].unsup=1;           // absent = default ms): 8-byte
        if(tid==ARROW_TIME){I tbw=fb_fieldi32(af->fb,ttbl,1);                   // Time bitWidth (field 1);
            if(tbw&&tbw!=32)af->flds[i].unsup=1;}}                              // Time64[us|ns] unsupported
    I bvec=fb_vecoff(af->fb,af->fr,FB_FOOTER_BATCHES);                          // get recordBatches vector offset
    af->nbat=fb_veclen(af->fb,bvec);                                            // number of RecordBatch blocks
    af->blk_dat=fb_vecdat(af->fb,bvec);                                         // raw Block structs (24 bytes each)
    for(I b=0;b<af->nbat;b++){const G*bk=af->blk_dat+b*ARROW_BLOCK_SIZE;        // validate every Block against
        J bo=*(const J*)bk;I ml=*(const I*)(bk+8);J bl=*(const J*)(bk+16);      // the file size BEFORE any
        if(bo<8||ml<0||bl<0||bo+ml>af->fsz||bo+ml+bl>af->fsz){                  // message deref — a corrupt
            free(af->flds);munmap((V*)af->map,af->fsz);R -8;}}                  // footer must not walk OOB
    R 0;}                                                                       // success

// msg_at.  Resolve the IPC message at file offset boff → meta flatbuf ptr     //
// + body offset.  Handles both encapsulations: v5 ([-1][size][meta]) and      //
// v4 ([size][meta]); the body follows the metadata padded to 8 bytes.         //
Z const G*msg_at(const G*map,J boff,J*body_off){                                //
    const G*msg=map+boff;I cont=*(const I*)msg;                                 // first i32: -1 marker or size
    if(cont==-1){I msz=*(const I*)(msg+4);                                      // v5: size follows the marker
        *body_off=boff+8+((msz+7)&~7);R msg+8;}                                 // meta at +8, body after pad
    *body_off=boff+4+((cont+7)&~7);R msg+4;}                                    // v4: cont IS the meta size
// batch_rows.  Row count of the RecordBatch message at meta.                  //
Z J batch_rows(const G*meta){I mr=fb_deref(meta,0);                             // deref Message root table
    I rb=fb_union_val(meta,mr,FB_MSG_HEADER_TYPE);                              // resolve RecordBatch union
    R fb_fieldi64(meta,rb,FB_RB_LENGTH);}                                       // length field = row count
// batch_compressed.  1 if the RecordBatch declares an LZ4/ZSTD body — the    //
// buffers would be frame-compressed, so a raw read would return garbage.     //
Z I batch_compressed(const G*meta){I mr=fb_deref(meta,0);                       // deref Message root table
    I rb=fb_union_val(meta,mr,FB_MSG_HEADER_TYPE);                              // resolve RecordBatch union
    R fb_field(meta,rb,FB_RB_COMPRESSION)!=0;}                                  // compression field present?

// load_dictionaries.  Parse DictionaryBatch messages, intern strings.         //
Z V load_dictionaries(ArrowFile*af){                                            // reads footer dict blocks
    I dv=fb_vecoff(af->fb,af->fr,FB_FOOTER_DICTS);                              // get dictionaries vector offset
    I nd=fb_veclen(af->fb,dv);                                                  // number of dict batches
    if(nd<=0)R;                                                                 // no dictionaries → nothing to do
    const G*dd=fb_vecdat(af->fb,dv);                                            // raw Block structs for dicts
    for(I d=0;d<nd;d++){                                                        // iterate dictionary blocks
        const G*blk=dd+d*ARROW_BLOCK_SIZE;                                      // Block struct (offset,metalen,bodylen)
        J bo=*(const J*)blk;I ml=*(const I*)(blk+8);J bl=*(const J*)(blk+16);   // validate the Block bounds —
        if(bo<8||ml<0||bl<0||bo+ml>af->fsz||bo+ml+bl>af->fsz)continue;          // corrupt entries are skipped
        J body_off;                                                             // body region file offset
        const G*meta=msg_at(af->map,*(const J*)blk,&body_off);                  // resolve message encapsulation
        I mr=fb_deref(meta,0);                                                  // deref Message root table
// Message → header (DictionaryBatch)                                          //
        I db=fb_union_val(meta,mr,FB_MSG_HEADER_TYPE);                          // resolve DictionaryBatch from union
        if(!db)continue;                                                        // not a DictionaryBatch → skip
// DictionaryBatch: field 0=id(i64), field 1=data(RecordBatch)                 //
        J dict_id=fb_fieldi64(meta,db,0);                                       // dictionary id (matches schema)
        I data_off=fb_field(meta,db,1);                                         // data field → embedded RecordBatch
        if(!data_off)continue;                                                  // no data → skip
        ArrowField*ff=0;                                                        // owning field: gives the VALUE
        for(I c=0;c<af->nc;c++)if(af->flds[c].dict_id==(I)dict_id&&             // type (utf8 vs large-utf8) —
            !af->flds[c].unsup){ff=&af->flds[c];break;}                         // unsupported owners are skipped
        if(!ff||ff->dict_syms)continue;                                         // no owner / already loaded
        I rbt=fb_deref(meta,data_off);                                          // deref RecordBatch table
        if(fb_field(meta,rbt,FB_RB_COMPRESSION)){af->bad_dict=1;continue;}      // LZ4/ZSTD dict body: flag it
                                                                                // (callers raise a clean error)
        J dnrows=fb_fieldi64(meta,rbt,FB_RB_LENGTH);                            // number of dictionary entries
        I buvo=fb_vecoff(meta,rbt,FB_RB_BUFFERS);                               // buffers vector offset
        const G*bd=fb_vecdat(meta,buvo);                                        // raw Buffer structs
// Dictionary is a single Utf8 column: validity + offsets + data               //
// buf[0]=validity, buf[1]=offsets, buf[2]=data                                //
        J b1o=BUF_O(bd,1);                                                      // offsets buffer body offset
        J b2o=BUF_O(bd,2);                                                      // string data buffer body offset
        const G*body=af->map+body_off;                                          // body region base pointer
        const I*o32=(const I*)(body+b1o);                                       // Utf8: int32 string offsets
        const J*o64=(const J*)(body+b1o);                                       // LargeUtf8: int64 offsets
        I lg=ff->arrow_type==ARROW_LARGEUTF8;                                   // offset width of the values
        const G*sdata=body+b2o;                                                 // raw string data bytes
// Intern all dictionary values                                                //
        char**syms=(char**)malloc(dnrows*sizeof(char*));                        // alloc symbol pointer array
        for(J i=0;i<dnrows;i++){                                                // iterate dictionary entries
            J s=lg?o64[i]:(J)o32[i];                                            // s=byte start (width-correct)
            I l=(I)((lg?o64[i+1]:(J)o32[i+1])-s);                               // l=byte length
            syms[i]=sn((char*)(sdata+s),l);}                                    // sn: intern N-byte string as symbol
// Assign to matching fields                                                   //
        for(I c=0;c<af->nc;c++){                                                // scan schema fields
            if(af->flds[c].dict_id==(I)dict_id&&!af->flds[c].dict_syms){        // first load per field only
                af->flds[c].dict_syms=syms;                                     // attach symbol lookup table
                af->flds[c].dict_count=(I)dnrows;                               // store dictionary size
                af->flds[c].elem_size=4;}}}}                                    // indices are int32

// close_arrow.  Free ArrowFile resources and unmap.                           //
Z V close_arrow(ArrowFile*af){                                                  // cleanup after read/stream
    for(I c=0;c<af->nc;c++){char**sy=af->flds[c].dict_syms;                     // iterate schema fields
        if(!sy)continue;I dup=0;                                                // fields sharing one dict id
        for(I k=0;k<c;k++)if(af->flds[k].dict_syms==sy){dup=1;break;}           // share ONE syms array —
        if(!dup)free(sy);}                                                      // free it exactly once
    free(af->flds);munmap((V*)af->map,af->fsz);}                                // free fields + unmap file

// get_batch_meta.  Resolve batch[idx] Block → metadata + body ptrs.           //
Z V get_batch_meta(ArrowFile*af,I idx,                                          // idx=batch index in footer
    const G**meta,const G**body){                                               // out: flatbuf meta + body region
    const G*blk=af->blk_dat+idx*ARROW_BLOCK_SIZE;                               // Block struct for batch idx
    J bdy;*meta=msg_at(af->map,*(const J*)blk,&bdy);                            // Block.offset → message ptrs
    *body=af->map+bdy;}                                                         // body starts after padded meta

// ═══════════════════════════════════════════════════════════════════════════
// Parallel multi-batch read infrastructure.  Each worker owns a disjoint      //
// range of batches [start, end) and writes into the shared pre-allocated      //
// columns `cd` at each batch's fixed row offset `offsets[b]`.  Reads hit      //
// mmap'd pages; writes never overlap (batches partition the row space);       //
// every allocation (parse_batch's ktn) goes through the thread-safe heap.     //
// ═══════════════════════════════════════════════════════════════════════════
// ncores.  Probed online core count, clamped to [1, cap].                     //
Z I ncores(I cap){I mx=(I)sysconf(_SC_NPROCESSORS_ONLN);                        //
    if(mx<1)mx=1;R mx>cap?cap:mx;}                                              //
// run_workers.  Run fn over nw contexts of stride csz; inline when nw==1.     //
Z V run_workers(V*(*fn)(V*),V*cx,size_t csz,I nw){                              // cx = first of nw contexts
    if(nw==1){fn(cx);R;}                                                        // single worker: no threads
    pthread_t*t=(pthread_t*)malloc((size_t)nw*sizeof(pthread_t));               //
    for(I w=0;w<nw;w++)pthread_create(&t[w],NULL,fn,(G*)cx+(size_t)w*csz);      // spawn workers
    for(I w=0;w<nw;w++)pthread_join(t[w],NULL);free(t);}                        // join all

typedef struct{                                                                 // per-worker context
    ArrowFile*af;                                                               // shared file (read-only)
    I nc;                                                                       // column count
    K cd;                                                                       // shared pre-allocated column list
    J*offsets;                                                                  // per-batch starting row offsets
    I start,end;                                                                // batch range [start, end)
    I ok;                                                                       // 1 on success, 0 on parse error
}arrow_read_ctx;                                                                //

// arrow_read_worker.  Parse batches [start, end) and copy into shared cd.     //
Z V*arrow_read_worker(V*p){                                                     //
    arrow_read_ctx*c=(arrow_read_ctx*)p;                                        //
    K*batch_cols=(K*)malloc((size_t)c->nc*sizeof(K));                           // per-worker temp column buffer
    if(!batch_cols){c->ok=0;R NULL;}                                            // OOM
    for(I b=c->start;b<c->end;b++){                                             // iterate this worker's batches
        const G*meta,*body;J nr;                                                //
        get_batch_meta(c->af,b,&meta,&body);                                    // resolve batch pointers
        if(parse_batch(meta,body,c->nc,c->af->flds,batch_cols,&nr)<0){          // parse all columns
            c->ok=0;free(batch_cols);R NULL;                                    //
        }                                                                       //
        J off=c->offsets[b];                                                    // this batch's starting row
        for(I col=0;col<c->nc;col++){                                           // copy batch columns into shared cd
            I es=c->af->flds[col].elem_size;                                    // L element size
            if(col_is_sym(&c->af->flds[col]))                                   // symbol (utf8/dict): interned
                memcpy(kS(kK(c->cd)[col])+off,kS(batch_cols[col]),              // pointers are plain values —
                    (size_t)nr*sizeof(S));                                      // copy them, not index bytes
            else if(es>0)                                                       // numeric: memcpy raw bytes
                memcpy(kG(kK(c->cd)[col])+(off*es),kG(batch_cols[col]),         //
                    (size_t)nr*es);                                             //
            r0(batch_cols[col]);}}                                              // free worker's batch column
    free(batch_cols);R NULL;}                                                   //

// ═══════════════════════════════════════════════════════════════════════════
// arrow_read — all batches → single L table
// ═══════════════════════════════════════════════════════════════════════════
// arrow_read.  Read Arrow IPC file → L table. Entry point for 2:.             //
K arrow_read(K path){                                                           // path: symbol `:path/to/file.arrow`
    P(!path||kt(path)!=-KS,krr((char*)"arrow_read: expected symbol"))           // validate: must be symbol atom
    const char*fn=(const char*)ls(path);if(*fn==':')fn++;                       // strip leading colon if present
    ArrowFile af;I rc=open_arrow(fn,&af);                                       // mmap + parse footer/schema
    P(rc<0,krr((char*)"arrow_read: open failed"))                               // propagate open error
    load_dictionaries(&af);                                                     // parse DictionaryBatch messages
    I nc=af.nc;                                                                 // column count from schema
    for(I c=0;c<nc;c++)                                                         // gate BEFORE any allocation:
        if(af.flds[c].unsup||                                                   // unmapped type (decimal, …)
           (af.flds[c].arrow_type==ARROW_LIST&&af.nbat>1)){                     // List has no multi-batch merge
            close_arrow(&af);                                                   //
            R krr((char*)"arrow_read: unsupported column type");}               // clean error, no partial work
    for(I b=0;b<af.nbat&&!af.bad_dict;b++){const G*meta,*body;                  // gate compressed bodies: the
        get_batch_meta(&af,b,&meta,&body);                                      // raw buffers would decode as
        af.bad_dict|=batch_compressed(meta);}                                   // garbage, so refuse up front
    if(af.bad_dict){close_arrow(&af);                                           // (record OR dictionary batch)
        R krr((char*)"arrow_read: compressed IPC body unsupported");}           //
    K cn=ktn(KS,nc);                                                            // column-name symbol vector
    for(I c=0;c<nc;c++)                                                         // intern column names
        kS(cn)[c]=sn((char*)af.flds[c].name,af.flds[c].name_len);               // name bytes → interned symbol
                                                                                // ── Single batch fast path ──                                          //
    if(af.nbat==1){                                                             // common case: one RecordBatch
        const G*meta,*body;get_batch_meta(&af,0,&meta,&body);                   // resolve batch 0 pointers
        J nr;                                                                   // batch row count
        K*cols=(K*)malloc(nc*sizeof(K));                                        // temp array for parsed columns
        if(parse_batch(meta,body,nc,af.flds,cols,&nr)<0){                       // parse all columns from batch
            free(cols);r0(cn);close_arrow(&af);                                 // parse failed → cleanup
            R krr((char*)"arrow_read: batch parse failed");}                    // (parse_batch freed its cols)
        K cd=ktn(0,nc);                                                         // column data list (only NOW —
        for(I c=0;c<nc;c++)kK(cd)[c]=cols[c];                                   // never r0'd half-filled)
        free(cols);close_arrow(&af);R xT(xD(cn,cd));}                           // build dict → table, return
                                                                                // ── Multi-batch: count total rows, pre-alloc, fill in parallel ──      //
    J total=0;                                                                  // accumulate total rows
    J*offsets=(J*)malloc((size_t)(af.nbat+1)*sizeof(J));                        // per-batch starting row offsets
    for(I b=0;b<af.nbat;b++){const G*meta,*body;                                // scan all batches for row counts
        get_batch_meta(&af,b,&meta,&body);                                      // resolve batch b pointers
        offsets[b]=total;total+=batch_rows(meta);}                              // starting row += batch rows
    offsets[af.nbat]=total;                                                     // sentinel = total rows
    SymCache*uc=sym_cache_attach(af.flds,nc,total);                             // one utf8 cache, all batches
    K cd=ktn(0,nc);                                                             // pre-alloc column data list
    for(I c=0;c<nc;c++)                                                         // pre-alloc each column vector
        kK(cd)[c]=ktn(col_is_sym(&af.flds[c])?KS:af.flds[c].l_type,(I)total);   // symbol → KS, else mapped type
    // ── Parallel batch parse via pthread workers ──                          //
// Each worker owns a disjoint range [start,end) of batches.  It parses        //
// each batch into a private batch_cols buffer, then copies the rows           //
// into the shared pre-allocated columns at the batch's fixed offset           //
// (computed above).  Writes never overlap — each batch owns its own           //
// [offsets[b], offsets[b+1]) row range.  Reads are from mmap'd pages.         //
    I nw=af.nbat,mx=ncores(af.nbat);if(nw>mx)nw=mx;                             // one worker/batch, core-bounded
    arrow_read_ctx*ctxs=                                                        // per-worker batch-range contexts
        (arrow_read_ctx*)calloc((size_t)nw,sizeof(arrow_read_ctx));             //
    for(I w=0;w<nw;w++){                                                        // partition batches across workers
        ctxs[w].af=&af;ctxs[w].nc=nc;ctxs[w].cd=cd;ctxs[w].offsets=offsets;     // shared read-only inputs
        ctxs[w].start=(I)((J)w*af.nbat/nw);                                     // even batch partition
        ctxs[w].end=(I)((J)(w+1)*af.nbat/nw);ctxs[w].ok=1;}                     // assume success
    run_workers(arrow_read_worker,ctxs,sizeof(arrow_read_ctx),nw);              // parse + copy concurrently
    I all_ok=1;for(I w=0;w<nw;w++)if(!ctxs[w].ok){all_ok=0;break;}              // check errors
    free(ctxs);free(offsets);sym_cache_free(uc);                                //
    if(!all_ok){r0(cn);r0(cd);close_arrow(&af);                                 // parse fail → r0(cd) alone
        R krr((char*)"arrow_read: batch parse failed");}                        // frees children (no dbl-free)
    close_arrow(&af);R xT(xD(cn,cd));}                                          // build table, cleanup, return

// ═══════════════════════════════════════════════════════════════════════════
// arrow_stream — batch by batch → native splayed column files on disk
// Peak DRAM = 1 batch.  Emits the byte layout L's own `set` writes, so
// get / \l load the result like any native splay.
//   fixed width   [4B 0][i16 type][2B 0][i64 count][16B 0] + raw payload
//                 — the 32B vector header laid down verbatim, so L can
//                 mmap the file and use it as a vector in place
//   symbols / .d  [0xFF 0x01][i16 KS][i32 count] + NUL-terminated strings
//                 — interned pointers have no stable disk form, so the
//                 symbols serialize as their bytes
// Both layouts keep the count in a fixed header slot, so each batch just
// appends raw bytes and the count is PATCHED once at the end — no file
// rewrite, no buffering beyond the current batch.
// ═══════════════════════════════════════════════════════════════════════════
#define SPLAY_HDR 32                                                            // fixed-width vector header bytes
// splay_hdr.  Start a column/.d file: header with count 0 (patched later).     //
Z I splay_hdr(I fd,I t,I sym){                                                  // t=L type tag, sym=0xFF01 form?
    G h[SPLAY_HDR];memset(h,0,SPLAY_HDR);                                       // zeroed header scratch
    if(sym){h[0]=0xFF;h[1]=0x01;*(H*)(h+2)=(H)t;                                // symbol form: magic + i16 type
        R write(fd,h,8)==8?0:-1;}                                               // i32 count at +4 left 0 for now
    *(H*)(h+4)=(H)t;                                                            // mmap form: i16 type at +4
    R write(fd,h,SPLAY_HDR)==SPLAY_HDR?0:-1;}                                   // i64 count at +8 left 0 for now
// splay_patch.  Patch the final element count into its fixed header slot.      //
Z V splay_patch(I fd,I sym,J n){                                                // n=total rows streamed to fd
    if(sym){I n32=(I)n;(void)!pwrite(fd,&n32,4,4);}                             // 0xFF01 form: i32 count at +4
    else (void)!pwrite(fd,&n,8,8);}                                             // mmap form: i64 count at +8
// splay_syms.  Append nr interned symbols as NUL-terminated strings.           //
Z I splay_syms(I fd,S*s,J nr){                                                  // one batch buffer → one write
    J sz=0;for(J i=0;i<nr;i++)sz+=((I*)s[i])[-2]+1;                             // Σ len+NUL (O(1) intern len)
    G*buf=(G*)malloc(sz);P(!buf,-1)J p=0;                                       // batch-sized scratch buffer
    for(J i=0;i<nr;i++){I l=((I*)s[i])[-2]+1;                                   // string bytes incl the NUL
        memcpy(buf+p,s[i],l);p+=l;}                                             // interned str is NUL-terminated
    I ok=write(fd,buf,sz)==sz;free(buf);R ok?0:-1;}                             // single append per batch

// arrow_stream.  Stream Arrow IPC → native splayed table on disk.              //
K arrow_stream(K x){                                                            // x: (`src;`dst) symbol pair
    const char*src,*dst;                                                        // source Arrow file, dest directory
    if(x&&kt(x)==KS&&kn(x)==2){                                                 // input is symbol vector
        src=(const char*)kS(x)[0];dst=(const char*)kS(x)[1];                    // extract src + dst paths
    }else if(x&&!kt(x)&&kn(x)==2){                                              // input is generic list of 2
        K ksrc=kK(x)[0],kdst=kK(x)[1];                                          // extract K atoms
        if(kt(ksrc)!=-KS||kt(kdst)!=-KS)                                        // both must be symbol atoms
            R krr((char*)"arrow_stream: symbols expected");                     // type error
        src=(const char*)ls(ksrc);dst=(const char*)ls(kdst);                    // extract C strings
    }else R krr((char*)"arrow_stream: (src;dst) expected");                     // wrong shape
    if(*src==':')src++;if(*dst==':')dst++;                                      // strip leading colons
    ArrowFile af;I rc=open_arrow(src,&af);                                      // mmap + parse source file
    P(rc<0,krr((char*)"arrow_stream: open failed"))                             // propagate open error
    load_dictionaries(&af);                                                     // dict cols stream as real symbols
    I nc=af.nc;                                                                 // column count from schema
                                                                                // ── Validate: every column must have a native splay form ──
    for(I c=0;c<nc;c++)                                                         // reject BEFORE touching the disk
        if(af.flds[c].unsup||(!col_is_sym(&af.flds[c])&&                        // unmapped type, or no symbol /
           (af.flds[c].l_type<=0||af.flds[c].elem_size<=0))){                   // fixed-width form (e.g. List)
            close_arrow(&af);                                                   // unmap source + free schema
            R krr((char*)"arrow_stream: unsupported column type");}             // fail whole stream up front
    for(I b=0;b<af.nbat&&!af.bad_dict;b++){const G*meta,*body;                  // gate compressed bodies: raw
        get_batch_meta(&af,b,&meta,&body);                                      // buffer reads would splay
        af.bad_dict|=batch_compressed(meta);}                                   // garbage to disk
    if(af.bad_dict){close_arrow(&af);                                           // (record OR dictionary batch)
        R krr((char*)"arrow_stream: compressed IPC body unsupported");}         //
    mkdir(dst,0755);                                                            // create splay dir (ok if exists)
                                                                                // ── .d manifest: column names in the 0xFF01 symbol-vector form ──
    {char dp[512];snprintf(dp,512,"%s/.d",dst);                                 // .d file path
     I fd=open(dp,O_WRONLY|O_CREAT|O_TRUNC,0644);                               // create/truncate .d file
     if(fd<0){close_arrow(&af);                                                 // cannot create the manifest →
         R krr((char*)"arrow_stream: .d create failed");}                       // fail before any column file
     splay_hdr(fd,KS,1);                                                        // 0xFF01 KS header, count 0
     for(I c=0;c<nc;c++){                                                       // append each column name
         (void)!write(fd,af.flds[c].name,af.flds[c].name_len);                  // name bytes (flatbuf, no NUL)
         (void)!write(fd,"",1);}                                                // explicit NUL terminator
     splay_patch(fd,1,nc);close(fd);}                                           // patch count = column count
                                                                                // ── Column files: header now, count patched after the last batch ──
    I*cfds=(I*)malloc(nc*sizeof(I));                                            // file descriptors per column
    for(I c=0;c<nc;c++){                                                        // open one file per column
        char cp[512];                                                           // column file path buffer
        snprintf(cp,512,"%s/%.*s",dst,af.flds[c].name_len,af.flds[c].name);     // dst/colname
        cfds[c]=open(cp,O_WRONLY|O_CREAT|O_TRUNC,0644);                         // create/truncate column file
        if(cfds[c]>=0)splay_hdr(cfds[c],                                        // stamp the on-disk header:
            col_is_sym(&af.flds[c])?KS:af.flds[c].l_type,                       // KS → 0xFF01, else mmap form,
            col_is_sym(&af.flds[c]));}                                          // count 0 until the end
    J trows=0;for(I b=0;b<af.nbat;b++){const G*meta,*body;                      // pre-sum footer row counts so
        get_batch_meta(&af,b,&meta,&body);trows+=batch_rows(meta);}             // the utf8 dedup cache is sized
                                                                                // once and shared across batches
    SymCache*uc=sym_cache_attach(af.flds,nc,trows);                             // one utf8 cache, all batches
    J total=0;                                                                  // rows streamed so far
    for(I b=0;b<af.nbat;b++){                                                   // iterate Arrow RecordBatches
        const G*meta,*body;get_batch_meta(&af,b,&meta,&body);                   // resolve batch b ptrs
        K*cols=(K*)malloc(nc*sizeof(K));J nr;                                   // temp col array + row count
        if(parse_batch(meta,body,nc,af.flds,cols,&nr)<0){                       // parse batch → K columns
            free(cols);break;}                                                  // parse fail → stop streaming
        for(I c=0;c<nc;c++){                                                    // append each column chunk
            if(col_is_sym(&af.flds[c]))                                         // symbol column: strings, not
                splay_syms(cfds[c],kS(cols[c]),nr);                             // pointers, land on disk
            else (void)!write(cfds[c],kG(cols[c]),                              // fixed width: raw payload —
                (size_t)nr*af.flds[c].elem_size);                               // exactly what set stores
            r0(cols[c]);}                                                       // free batch column K object
        free(cols);total+=nr;}                                                  // free temp, advance counter
    for(I c=0;c<nc;c++){splay_patch(cfds[c],                                    // final count patch: the one
        col_is_sym(&af.flds[c]),total);close(cfds[c]);}                         // header write after all appends
    free(cfds);sym_cache_free(uc);close_arrow(&af);                             // free fds + cache, unmap source
    R kj(total);}                                                               // return total rows written

// ═══════════════════════════════════════════════════════════════════════════
// arrow_write — L table → Arrow IPC file
// ═══════════════════════════════════════════════════════════════════════════

// ── Null bitmap builder: scan for sentinels, produce bitmap ──             //
// build_bitmap.  Scan L K column for null sentinels → Arrow bitmap.           //
Z G*build_bitmap(K col,J*out_null_cnt){                                         // returns malloc'd bitmap
    J n=kn(col);I t=kt(col);J nc=0;                                             // n=rows, t=type, nc=null count
    J bm_bytes=(n+7)/8;                                                         // bitmap bytes (1 bit per row)
    G*bm=(G*)calloc(bm_bytes,1);                                                // all bits 0 = all null initially
#ifdef __AVX512F__                                                              // AVX-512 fast bitmap build
    if(t==KI||t==KD||t==KT){I*p=kI(col);                                        // int32: compare 16 at a time
        __m512i vnull=_mm512_set1_epi32(NI);J i;                                // broadcast NI sentinel
        for(i=0;i+16<=n;i+=16){                                                 // 16 ints per 512-bit iteration
            __mmask16 k=~_mm512_cmpeq_epi32_mask(                               // invert: k=1 where NOT null
                _mm512_loadu_si512(p+i),vnull);                                 // compare 16 ints vs NI
            *(unsigned short*)(bm+(i>>3))=k;                                    // store 16 validity bits
            nc+=16-_mm_popcnt_u32(k);}                                          // nulls = 16 - popcount(valid)
        for(;i<n;i++)if(p[i]!=NI)bm[i>>3]|=(1<<(i&7));else nc++;}               // scalar tail
    else if(t==KJ||t==KP||t==KN){J*p=kJ(col);                                   // int64: compare 8 at a time
        __m512i vnull=_mm512_set1_epi64(NJ);J i;                                // broadcast NJ sentinel
        for(i=0;i+8<=n;i+=8){                                                   // 8 longs per 512-bit iteration
            __mmask8 k=~_mm512_cmpeq_epi64_mask(                                // invert: k=1 where NOT null
                _mm512_loadu_si512(p+i),vnull);                                 // compare 8 longs vs NJ
            bm[i>>3]=k;nc+=8-_mm_popcnt_u32(k);}                                // store 8 bits, count nulls
        for(;i<n;i++)if(p[i]!=NJ)bm[i>>3]|=(1<<(i&7));else nc++;}               // scalar tail
#else                                                                           // scalar fallback
    if(t==KI||t==KD||t==KT){I*p=kI(col);                                        // int32: scalar NI check
        for(J i=0;i<n;i++)if(p[i]!=NI)bm[i>>3]|=(1<<(i&7));else nc++;}          // set bit if valid, count nulls
    else if(t==KJ||t==KP||t==KN){J*p=kJ(col);                                   // int64: scalar NJ check
        for(J i=0;i<n;i++)if(p[i]!=NJ)bm[i>>3]|=(1<<(i&7));else nc++;}          // set bit if valid, count nulls
#endif                                                                          // end AVX-512/scalar guard
    else if(t==KF||t==KZ){F*p=kF(col);                                          // float64: NaN = null
        for(J i=0;i<n;i++)if(p[i]==p[i])bm[i>>3]|=(1<<(i&7));else nc++;}        // NaN != NaN → null detected
    else if(t==KE){E*p=(E*)kG(col);                                             // float32: NaN = null (0Ne)
        for(J i=0;i<n;i++)if(p[i]==p[i])bm[i>>3]|=(1<<(i&7));else nc++;}        // NaN != NaN → null detected
    else if(t==KH){H*p=(H*)kG(col);                                             // short: 0Nh sentinel = null
        for(J i=0;i<n;i++)if(p[i]!=NH)bm[i>>3]|=(1<<(i&7));else nc++;}          // set bit if valid, count nulls
    else{memset(bm,0xFF,bm_bytes);nc=0;}                                        // KG,KB,KS: no null concept
    *out_null_cnt=nc;R bm;}                                                     // return bitmap + null count

// ═══════════════════════════════════════════════════════════════════════════
// Parallel per-column body prep.  Columns are independent: phase A scans
// each column (null bitmap + sizes, one intern-header pass for KS with the
// lengths cached), a serial layout step assigns disjoint body regions,
// then phase B fills those regions and each worker pwrites its finished
// column span while others still convert — the kernel copy overlaps the
// conversion and reads cache-hot pages.  Workers only read K data and
// write their own body range + ColBuf slot — no host calls.
// ═══════════════════════════════════════════════════════════════════════════
typedef struct{G*bm;J bm_len,dat_len,nc2,str_bytes;                             // bitmap + sizes + null count
    J off_bm,off_off,off_dat;I et;I*lens;}ColBuf;                               // body offsets, type, KS lengths
typedef struct{K col_list;ColBuf*cbs;G*body;                                    // shared inputs (read-only K)
    J nrows,fbase;I fd,nc,nw,w,phase;}aw_ctx;                                   // rows, file base+fd, stride
// aw_col_size.  Phase A: bitmap + byte sizes for one column.                   //
Z V aw_col_size(K col,ColBuf*cb,J nrows){                                       //
    cb->et=kt(col);cb->lens=0;cb->str_bytes=0;                                  // record type, clear KS state
    cb->bm=build_bitmap(col,&cb->nc2);                                          // build null bitmap, count nulls
    J bp=ARROW_PAD((nrows+7)/8);                                                // padded bitmap size (64B align)
    if(!cb->nc2){free(cb->bm);cb->bm=0;bp=0;}                                   // no nulls: discard bitmap
    cb->bm_len=bp;                                                              // store padded bitmap length
    if(cb->et==KS){cb->lens=(I*)malloc((size_t)(nrows?nrows:1)*sizeof(I));      // strings: cache the lengths so
        J d=0;for(J i=0;i<nrows;i++){I l=((I*)kS(col)[i])[-2];                  // the intern headers are walked
            cb->lens[i]=l;d+=l;}                                                // ONCE (phase B reuses lens)
        cb->str_bytes=d;                                                        // total string payload bytes
        cb->dat_len=ARROW_PAD((nrows+1)*4)+ARROW_PAD(d);}                       // offsets array + string bytes
    else if(cb->et==KB)cb->dat_len=ARROW_PAD((nrows+7)/8);                      // bools bitpack to 1 bit/value
    else cb->dat_len=ARROW_PAD((J)nrows*nt(cb->et));}                           // numeric: nrows * elem_size
// aw_col_fill.  Phase B: fill this column's body region (+ zero the pads),    //
// then append the finished span to the output file at its fixed offset.       //
Z V aw_col_fill(K col,ColBuf*cb,G*body,J nrows,I fd,J fbase){                   //
    if(cb->bm){J bb=(nrows+7)/8;memcpy(body+cb->off_bm,cb->bm,bb);              // copy bitmap into body
        memset(body+cb->off_bm+bb,0,cb->bm_len-bb);                             // zero pad to 64B boundary
        free(cb->bm);cb->bm=0;}                                                 // bitmap no longer needed
    G*dst=body+cb->off_dat;                                                     // this column's data region
    if(cb->et==KS){I*offs=(I*)(body+cb->off_off);J dp=0;                        // string offsets + running pos
        for(J i=0;i<nrows;i++){offs[i]=(I)dp;I l=cb->lens[i];                   // offset[i], cached length
            memcpy(dst+dp,kS(col)[i],l);dp+=l;}                                 // copy string bytes to body
        offs[nrows]=(I)dp;                                                      // final offset = total bytes
        memset(body+cb->off_off+(nrows+1)*4,0,                                  // zero offsets pad gap
            ARROW_PAD((nrows+1)*4)-(nrows+1)*4);                                //
        memset(dst+dp,0,ARROW_PAD(dp)-dp);                                      // zero string pad gap
        free(cb->lens);cb->lens=0;}                                             // done with cached lengths
    else if(cb->et==KB){memset(dst,0,cb->dat_len);                              // bools: zero, then OR bits in
        for(J i=0;i<nrows;i++)if(kG(col)[i])dst[i>>3]|=(1<<(i&7));}             // pack each bool into a bit
    else{J dsz=(J)nrows*nt(cb->et);                                             // fixed-width raw byte size
        if(cb->et==KD){I*d=(I*)dst;const I*s=kI(col);                           // Date: epoch shift L→Arrow
            for(J i=0;i<nrows;i++)d[i]=s[i]+ARROW_L_DATE_OFFSET;}               // L(2000) → Arrow(1970)
        else if(cb->et==KZ){J*d=(J*)dst;const F*s=kF(col);                      // datetime → timestamp ns
            for(J i=0;i<nrows;i++)                                              // convert each datetime
                d[i]=(J)((s[i]+ARROW_L_DATE_OFFSET)*86400.0*1e9);}              // days → ns since 1970
        else if(cb->et==KP){J*d=(J*)dst;const J*s=kJ(col);                      // timestamp: 2000-ns → 1970-ns
            for(J i=0;i<nrows;i++)                                              // null-preserving epoch shift
                d[i]=s[i]==NJ?NJ:s[i]+ARROW_L_TS_OFFSET_NS;}                    // (KN falls through: no epoch)
        else memcpy(dst,kG(col),dsz);                                           // other types: raw byte copy
        memset(dst+dsz,0,cb->dat_len-dsz);}                                     // zero pad to 64B boundary
    if(fd>=0)(void)!pwrite(fd,body+cb->off_bm,                                  // emit the whole column span
        (size_t)(cb->bm_len+cb->dat_len),fbase+cb->off_bm);}                    // (bitmap+data are contiguous)
// aw_worker.  Run the current phase over this worker's strided columns.       //
Z V*aw_worker(V*p){aw_ctx*c=(aw_ctx*)p;                                         //
    for(I i=c->w;i<c->nc;i+=c->nw){K col=kK(c->col_list)[i];                    // columns w, w+nw, w+2nw, …
        if(c->phase)aw_col_fill(col,c->cbs+i,c->body,c->nrows,                  // phase B: fill body region,
            c->fd,c->fbase);                                                    // then pwrite it to the file
        else aw_col_size(col,c->cbs+i,c->nrows);}                               // phase A: bitmap + sizes
    R NULL;}                                                                    //
// aw_fanout.  Run one phase across nw workers (inline when nw==1).            //
Z V aw_fanout(K col_list,ColBuf*cbs,G*body,J nrows,I nc,I nw,I phase,           //
    I fd,J fbase){                                                              // fd<0 = no emit (phase A)
    aw_ctx*cx=(aw_ctx*)calloc((size_t)nw,sizeof(aw_ctx));                       // per-worker contexts
    for(I w=0;w<nw;w++){cx[w].col_list=col_list;cx[w].cbs=cbs;                  // shared read-only inputs
        cx[w].body=body;cx[w].nrows=nrows;cx[w].nc=nc;                          //
        cx[w].fd=fd;cx[w].fbase=fbase;                                          // output file + body base
        cx[w].nw=nw;cx[w].w=w;cx[w].phase=phase;}                               // stride + worker id + phase
    run_workers(aw_worker,cx,sizeof(aw_ctx),nw);free(cx);}                      // run the phase concurrently

// arrow_write.  Write L table → Arrow IPC file. Entry point for 2:.           //
K arrow_write(K x){                                                             // x: (table;`path) pair
    P(!x||kt(x)||kn(x)!=2,krr((char*)"arrow_write: (table;path) expected"))     // must be 2-element generic list
    K tbl=kK(x)[0],kpath=kK(x)[1];                                              // extract table + path
    P(kt(tbl)!=XT||(kt(kpath)!=-KS&&kt(kpath)!=KS),                             // table + symbol required
      krr((char*)"arrow_write: table + symbol expected"))                       // type error
    const char*fn=kt(kpath)==-KS?(const char*)ls(kpath)                         // extract path: atom or vec[0]
        :(const char*)kS(kpath)[0];                                             // symbol vector → first element
    if(*fn==':')fn++;                                                           // strip leading colon
    K dict=kK(tbl)[0];                                                          // XT wraps dict via ->k
    K col_names=kK(dict)[0];                                                    // KS column names
    K col_list=kK(dict)[1];                                                     // type-0 list of columns
    I nc=kn(col_names);                                                         // column count
    for(I c=0;c<nc;c++){I t=kt(kK(col_list)[c]);                                // gate BEFORE any allocation:
        if(!(t==KB||t==KG||t==KH||t==KI||t==KJ||t==KE||t==KF||t==KS||           // only columns with an Arrow
             t==KD||t==KT||t==KP||t==KN||t==KZ))                                // form (no char/nested lists)
            R krr((char*)"arrow_write: unsupported column type");}              // clean error, file untouched
    J nrows=nc>0?kn(kK(col_list)[0]):0;                                         // row count from first column
                                                                                // ── Phase A (parallel): per-column null bitmap + byte sizes ──         //
    J body_sz=0;                                                                // running total body bytes
    ColBuf*cbs=(ColBuf*)calloc(nc,sizeof(ColBuf));                              // alloc column buffer descriptors
    I mx=ncores(8),nw=nc<mx?nc:mx;if(nw<1)nw=1;                                 // one worker/column, capped at 8
    aw_fanout(col_list,cbs,0,nrows,nc,nw,0,-1,0);                               // scan columns concurrently
    for(I c=0;c<nc;c++)body_sz+=cbs[c].bm_len+cbs[c].dat_len;                   // accumulate body size
                                                                                // ── Layout (serial): assign disjoint body regions + Buffer pairs ──    //
    J*buf_offs=(J*)malloc(nc*3*2*sizeof(J));I bi=0;J pos=0;                     // Buffer (offset,length) pairs
    for(I c=0;c<nc;c++){                                                        // walk columns in schema order
        buf_offs[bi++]=pos;buf_offs[bi++]=cbs[c].bm?((nrows+7)/8):0;            // record bitmap buf (off,len)
        cbs[c].off_bm=pos;pos+=cbs[c].bm_len;                                   // bitmap region (0 if no nulls)
        if(cbs[c].et==KS){                                                      // string: offsets + data bufs
            buf_offs[bi++]=pos;buf_offs[bi++]=(nrows+1)*4;                      // record offsets buf (off,len)
            cbs[c].off_off=pos;pos+=ARROW_PAD((nrows+1)*4);                     // offsets region, 64B padded
            buf_offs[bi++]=pos;buf_offs[bi++]=cbs[c].str_bytes;                 // record data buf (off,len)
            cbs[c].off_dat=pos;pos+=ARROW_PAD(cbs[c].str_bytes);                // string region, 64B padded
        }else if(cbs[c].et==KB){                                                // bools: bitpacked data buf
            buf_offs[bi++]=pos;buf_offs[bi++]=(nrows+7)/8;                      // record data buf (off,len)
            cbs[c].off_dat=pos;pos+=cbs[c].dat_len;                             // bitpacked region
        }else{buf_offs[bi++]=pos;buf_offs[bi++]=(J)nrows*nt(cbs[c].et);         // numeric data buf (off,len)
            cbs[c].off_dat=pos;pos+=cbs[c].dat_len;}}                           // raw payload region
    J*null_counts=(J*)malloc(nc*sizeof(J));                                     // null counts for RecordBatch
    for(I c=0;c<nc;c++)null_counts[c]=cbs[c].nc2;                               // (known after phase A; cbs
                                                                                //  stays live for phase B)
    // ═════════════════════════════════════════════════════════════════════  //
// Build Arrow IPC file using flatcc-generated Flatbuffer builders             //
    // ═════════════════════════════════════════════════════════════════════  //
    I outfd=open(fn,O_WRONLY|O_CREAT|O_TRUNC,0644);                             // create output Arrow file
    if(outfd<0){for(I c=0;c<nc;c++){free(cbs[c].bm);free(cbs[c].lens);}         // file create failed → cleanup
        free(cbs);free(buf_offs);free(null_counts);                             // phase-A buffers + descriptors
        R krr((char*)"arrow_write: create failed");}                            // return error
    flatcc_builder_t B;flatcc_builder_init(&B);                                 // init flatcc builder
                                                                                // ── Flatbuf emit helpers (undef'd at end of function) ──               //
    #define AFB(N) org_apache_arrow_flatbuf_##N                                 // generated-namespace prefix
                                                                                // FB_T0/1/2: one Field type union member with 0/1/2 scalar attrs        //
    #define FB_T0(T) AFB(Field_type_##T##_start)(&B);                        \
        AFB(Field_type_##T##_end)(&B);break;                                    // 0-attr: start/end only
    #define FB_T1(T,F1,V1) AFB(Field_type_##T##_start)(&B);                  \
        AFB(T##_##F1##_add)(&B,V1);AFB(Field_type_##T##_end)(&B);break;         // 1 scalar attr then end
    #define FB_T2(T,F1,V1,F2,V2) AFB(Field_type_##T##_start)(&B);            \
        AFB(T##_##F1##_add)(&B,V1);AFB(T##_##F2##_add)(&B,V2);               \
        AFB(Field_type_##T##_end)(&B);break;                                    // 2 scalar attrs then end
                                                                                // ── Helper: add schema fields to current builder context ──            //
    #define ADD_SCHEMA_FIELDS(B) {                                           \
        AFB(Schema_endianness_add)(&B,0);                                    \
        AFB(Schema_fields_start)(&B);                                        \
        for(I c2=0;c2<nc;c2++){K co=kK(col_list)[c2];                        \
            AFB(Schema_fields_push_start)(&B);                               \
            AFB(Field_name_create_str)(&B,(const char*)kS(col_names)[c2]);   \
            AFB(Field_nullable_add)(&B,1);                                   \
            SW(kt(co)){                                                      \
            case KB:FB_T0(Bool)                                              \
            case KG:FB_T2(Int,bitWidth,8,is_signed,0)                        \
            case KH:FB_T2(Int,bitWidth,16,is_signed,1)                       \
            case KI:FB_T2(Int,bitWidth,32,is_signed,1)                       \
            case KJ:FB_T2(Int,bitWidth,64,is_signed,1)                       \
            case KE:FB_T1(FloatingPoint,precision,1)                         \
            case KF:FB_T1(FloatingPoint,precision,2)                         \
            case KS:FB_T0(Utf8)                                              \
            case KD:FB_T1(Date,unit,0)                                       \
            case KT:FB_T2(Time,unit,1,bitWidth,32)                           \
            case KZ:case KP:FB_T1(Timestamp,unit,3)                          \
            case KN:FB_T1(Duration,unit,3)                                   \
            default:FB_T2(Int,bitWidth,32,is_signed,1)}                      \
            AFB(Schema_fields_push_end)(&B);}                                \
        AFB(Schema_fields_end)(&B);}                                            // end ADD_SCHEMA_FIELDS macro
                                                                                // ── Build all 3 flatbuf messages, then single write ──                    //
// Schema Message                                                              //
    flatcc_builder_reset(&B);                                                   // reuse builder for Schema msg
    AFB(Message_start_as_root)(&B);                                             // begin Message flatbuf
    AFB(Message_version_add)(&B,AFB(MetadataVersion_V5));                       // V5 = current Arrow IPC
    AFB(Message_bodyLength_add)(&B,0);                                          // schema has no body
    AFB(Message_header_Schema_start)(&B);                                       // begin Schema union value
    ADD_SCHEMA_FIELDS(B)                                                        // emit all column field types
    AFB(Message_header_Schema_end)(&B);                                         // finalize Schema
    AFB(Message_end_as_root)(&B);                                               // finalize Schema Message
    size_t ssz;V*sm=flatcc_builder_finalize_aligned_buffer(&B,&ssz);            // extract schema flatbuf bytes
    I spad=((I)ssz+7)&~7;                                                       // pad to 8-byte boundary
// RecordBatch Message                                                         //
    flatcc_builder_reset(&B);                                                   // reuse builder for RecordBatch
    AFB(Message_start_as_root)(&B);                                             // begin Message flatbuf
    AFB(Message_version_add)(&B,AFB(MetadataVersion_V5));                       // V5 = current Arrow IPC
    AFB(Message_bodyLength_add)(&B,body_sz);                                    // body length = all column data
    AFB(Message_header_RecordBatch_start)(&B);                                  // begin RecordBatch union value
    AFB(RecordBatch_length_add)(&B,nrows);                                      // set row count
    AFB(RecordBatch_nodes_start)(&B);                                           // begin FieldNode vector
    for(I c=0;c<nc;c++)                                                         // one FieldNode per column
        AFB(RecordBatch_nodes_push_create)(&B,nrows,null_counts[c]);            // (length, null_count)
    AFB(RecordBatch_nodes_end)(&B);                                             // finalize FieldNode vector
    AFB(RecordBatch_buffers_start)(&B);                                         // begin Buffer vector
    for(I c=0;c<bi;c+=2)                                                        // emit (offset,length) pairs
        AFB(RecordBatch_buffers_push_create)(&B,buf_offs[c],buf_offs[c+1]);     // Buffer(offset, length)
    AFB(RecordBatch_buffers_end)(&B);                                           // finalize Buffer vector
    AFB(Message_header_RecordBatch_end)(&B);                                    // finalize RecordBatch
    AFB(Message_end_as_root)(&B);                                               // finalize RecordBatch Message
    size_t rsz;V*rm=flatcc_builder_finalize_aligned_buffer(&B,&rsz);            // extract record batch flatbuf
    I rpad=((I)rsz+7)&~7;                                                       // pad to 8-byte boundary
    J batch_off=8+8+spad;                                                       // batch file offset: magic+schema msg
// Footer                                                                      //
    flatcc_builder_reset(&B);                                                   // reuse builder for Footer
    AFB(Footer_start_as_root)(&B);                                              // begin Footer flatbuf
    AFB(Footer_version_add)(&B,AFB(MetadataVersion_V5));                        // V5 = current Arrow IPC
    AFB(Footer_schema_start)(&B);                                               // embed Schema in Footer
    ADD_SCHEMA_FIELDS(B)                                                        // same fields as Schema Message
    AFB(Footer_schema_end)(&B);                                                 // finalize Footer Schema
    AFB(Footer_recordBatches_start)(&B);                                        // begin Block vector
    AFB(Footer_recordBatches_push_create)(&B,batch_off,(I)(rpad+8),body_sz);    // Block(offset,metaLen,bodyLen)
    AFB(Footer_recordBatches_end)(&B);                                          // finalize Block vector
    AFB(Footer_end_as_root)(&B);                                                // finalize Footer
    size_t fsz2;V*fm=flatcc_builder_finalize_aligned_buffer(&B,&fsz2);          // extract footer flatbuf bytes
                                                                                // ── Emit: header pwrite, then phase B — each worker converts its       //
                                                                                // columns into the body buffer and pwrites the finished span at        //
                                                                                // its fixed file offset (kernel copy overlaps conversion; pages       //
                                                                                // flush lazily via the page cache) — then the footer pwrite.          //
    I fs=(I)fsz2;I cont=-1;                                                     // footer size int32 + marker
    J hsz=8+8+(J)spad+8+(J)rpad;                                                // bytes before the body
    G*hdr=(G*)malloc(hsz);G*p=hdr;                                              // header assembly scratch
    memcpy(p,"ARROW1\0\0",8);p+=8;                                              // magic + 2 pad bytes
    memcpy(p,&cont,4);memcpy(p+4,&spad,4);p+=8;                                 // schema continuation + size
    memcpy(p,sm,ssz);memset(p+ssz,0,spad-(J)ssz);p+=spad;                       // schema flatbuf, zero pad
    memcpy(p,&cont,4);memcpy(p+4,&rpad,4);p+=8;                                 // batch continuation + size
    memcpy(p,rm,rsz);memset(p+rsz,0,rpad-(J)rsz);                               // batch flatbuf, zero pad
    (void)!pwrite(outfd,hdr,(size_t)hsz,0);free(hdr);                           // header at file offset 0
    G*body=0;                                                                   // 64-byte aligned body buffer
    if(posix_memalign((V**)&body,64,body_sz?body_sz:64)){                       // alloc failed → clean error
        flatcc_builder_aligned_free(sm);flatcc_builder_aligned_free(rm);        // (not a crash later in the
        flatcc_builder_aligned_free(fm);flatcc_builder_clear(&B);               //  workers): free every
        for(I c=0;c<nc;c++){free(cbs[c].bm);free(cbs[c].lens);}                 //  phase-A buffer + builder
        free(cbs);free(null_counts);free(buf_offs);close(outfd);                //  output, close the file,
        R krr((char*)"arrow_write: out of memory");}                            //  and raise
    aw_fanout(col_list,cbs,body,nrows,nc,nw,1,outfd,hsz);                       // phase B: fill + pwrite spans
    free(body);free(cbs);                                                       // body emitted by the workers
    J ftl=(J)fsz2+10;G*ft=(G*)malloc(ftl);                                      // footer assembly scratch
    memcpy(ft,fm,fsz2);memcpy(ft+fsz2,&fs,4);                                   // footer flatbuf + size int32
    memcpy(ft+fsz2+4,"ARROW1",6);                                               // trailing magic
    (void)!pwrite(outfd,ft,(size_t)ftl,hsz+body_sz);free(ft);                   // footer after the body
    flatcc_builder_aligned_free(sm);                                            // free schema flatbuf
    flatcc_builder_aligned_free(rm);                                            // free record batch flatbuf
    flatcc_builder_aligned_free(fm);                                            // free footer flatbuf
    flatcc_builder_clear(&B);                                                   // destroy flatcc builder
    free(null_counts);free(buf_offs);close(outfd);                              // free arrays, close output fd
    #undef ADD_SCHEMA_FIELDS                                                    // emit helpers no longer needed
    #undef FB_T0                                                                //
    #undef FB_T1                                                                //
    #undef FB_T2                                                                //
    #undef AFB                                                                  //
    R r1(kpath);}                                                               // return path (incref)

