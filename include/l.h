/*
 * l.h — the slice of the L C ABI that arrow_interop uses.
 *
 * Vendored so this adapter builds offline.  The functions declared here are
 * resolved from the host `l` process when the shared library is loaded via
 * `2:` — nothing is linked at build time.  This is NOT the full embedding
 * API: only the types, accessors, and calls that arrow_io.c references.
 */
#ifndef L_H
#define L_H

#ifdef __cplusplus
extern "C" {
#endif

#if defined(__GNUC__) || defined(__clang__)
#  define L_API __attribute__((visibility("default")))                          // export under -fvisibility=hidden
#else
#  define L_API
#endif

/* ── K — a TAGGED 64-bit value, NOT a pointer ───────────────────────────────
 *   vtag(x) != 0  → an ATOM; the value is inline (or in an 8B cell for the
 *                   wide types KJ/KF/KN/KZ).
 *   vtag(x) == 0  → a HEAP object (vector/list/dict/table).  va(x) is byte 0
 *                   of the payload; a header at negative offsets holds the
 *                   subtype vt(x) and the element count vn(x). */
typedef unsigned long K;
typedef char *S; typedef unsigned char G; typedef short H;                      // S=string G=byte H=i16
typedef int I; typedef long long J; typedef float E; typedef double F;          // I=i32 J=i64 E=f32 F=f64

#define K_TAGSH 58                                                              // tag occupies the top 6 bits
#define K_PMASK (~0UL >> 6)                                                     // low 58 bits = value / pointer
#define vtag(x) ((H)((K)(x) >> K_TAGSH))                                        // type tag (0 = heap object)
#define va(x)   ((G*)((K)(x) & K_PMASK))                                        // payload / inline-value base
#define vt(x)   (*(H*)(va(x) - 28))                                             // heap subtype (KI/KS/XT/…)
#define vn(x)   (*(J*)(va(x) - 24))                                             // heap element count

/* Type tags (the subset this adapter reads or writes). */
#define KB 1                                                                    // boolean
#define KG 4                                                                    // byte
#define KH 5                                                                    // short  (i16)
#define KI 6                                                                    // int    (i32) — L's default int
#define KJ 7                                                                    // long   (i64)
#define KE 8                                                                    // real   (f32)
#define KF 9                                                                    // float  (f64)
#define KC 10                                                                   // char
#define KS 11                                                                   // symbol (interned char*)
#define KP 12                                                                   // timestamp (ns)
#define KD 14                                                                   // date (days)
#define KZ 15                                                                   // datetime (f64)
#define KN 16                                                                   // timespan (i64)
#define KT 19                                                                   // time (ms)
#define XT 98                                                                   // table (flip of a column dict)

static inline S ls(K x){ return (S)va(x); }                                     // symbol atom → interned char*

/* Vector payload — typed base pointers (length is vn(x)). */
#define vG(x) va(x)                                                             // byte / char array
#define vI(x) ((I*)va(x))                                                       // int array
#define vJ(x) ((J*)va(x))                                                       // long array
#define vF(x) ((F*)va(x))                                                       // float array
#define vS(x) ((S*)va(x))                                                       // symbol array (char* each)
#define vK(x) ((K*)va(x))                                                       // mixed list / dict payload

/* Legacy-compat accessors.  kt() returns the legacy SIGNED type (atom -T,
 * vector +T) so `kt(x) == -KS` / `== KS` checks read naturally; kn() is the
 * element count (atom = 1).  kG/kI/… alias the v* payload pointers.          */
#define kt(x) (vtag(x) ? -(H)vtag(x) : vt(x))                                   // legacy-signed type
#define kn(x) (vtag(x) ? (J)1 : vn(x))                                          // element count (atom = 1)
#define kG(x) vG(x)
#define kI(x) vI(x)
#define kJ(x) vJ(x)
#define kF(x) vF(x)
#define kS(x) vS(x)
#define kK(x) vK(x)
#define NJ ((J)0x8000000000000000LL)                                           // long / ns-temporal null sentinel

/* ── Host calls used by this adapter (resolved at 2: load time) ────────────*/
L_API K kj(J);                                                                  // long atom
L_API S sn(S, I);                                                               // intern n bytes -> symbol
L_API S ss(S);                                                                  // intern a NUL-terminated symbol
L_API J nt(unsigned);                                                           // storage byte-width by type tag
L_API K ktn(I type, I n);                                                       // typed vector of n elts
L_API K xD(K keys, K vals);                                                     // dict
L_API K xT(K dict);                                                             // table from a column dict
L_API K r1(K);                                                                  // retain (refcount++)
L_API void r0(K);                                                               // release (refcount--/free)
L_API K krr(S msg);                                                             // raise an error from a string

#ifdef __cplusplus
}
#endif

#endif /* L_H */
