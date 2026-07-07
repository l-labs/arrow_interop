// arrow_flatbuf.h — minimal hand-rolled Flatbuffer READER for Arrow IPC.      //
// Covers exactly what Arrow metadata needs: tables, vtables, vectors,         //
// strings, unions. Arrow Flatbuffer schemas: apache/arrow format/ dir.        //
// Include AFTER all third-party headers: the terse macro legend below         //
// (R/Z/SW/...) is textual and must not see foreign code.                      //
#ifndef ARROW_FLATBUF_H                                                         //
#define ARROW_FLATBUF_H                                                         //
#include "l.h"                                                                  // G/H/I/J/S type aliases
                                                                                //
// ── Terse macro legend (house vocabulary, used here and in arrow_io.c) ──── //
// Z  = static                 (file-local function / object)                  //
// V  = void                                                                   //
// R  = return                                                                 //
// SW = switch, CS(n,x) = case n: x; break;                                    //
// P(x,y) = guard: if (x) return y   U(x) = null-guard: if (!x) return 0       //
#define Z static                                                                //
typedef void V;                                                                 //
#define R return                                                                //
#define SW switch                                                               //
#define CS(n,x) case n:x;break;                                                 //
#define P(x,y) {if(x)R(y);}                                                     //
#define U(x) P(!(x),0)                                                          //
                                                                                //
// ── Flatbuffer primitives ────────────────────────────────────────────────  //
// A flatbuffer stores RELATIVE int32 offsets. A table begins with a signed    //
// offset back to its vtable; the vtable lists per-field object offsets so     //
// absent optional fields cost nothing on the wire.                            //
                                                                                //
// fb_ri8/ri16/ri32/ri64.  Read little-endian scalar at byte offset o.         //
Z inline I fb_ri8 (const G*b,I o){R*(const signed char*)(b+o);}                 //
Z inline H fb_ri16(const G*b,I o){R*(const H*)(b+o);}                           //
Z inline I fb_ri32(const G*b,I o){R*(const I*)(b+o);}                           //
Z inline J fb_ri64(const G*b,I o){R*(const J*)(b+o);}                           //
                                                                                //
// fb_deref.  Follow a relative offset: int32 at o → absolute position.        //
Z inline I fb_deref(const G*b,I o){R o+fb_ri32(b,o);}                           //
                                                                                //
// ── Table access ─────────────────────────────────────────────────────────  //
// vtable layout: [0:i16]=vtable size, [2:i16]=object size,                    //
// [4+2*idx:i16]=field offset within the object (0 = field absent).            //
                                                                                //
// fb_vtable.  Absolute vtable position for the table at tbl.                  //
Z inline I fb_vtable(const G*b,I tbl){R tbl-fb_ri32(b,tbl);}                    // vtable = tbl - soffset
                                                                                //
// fb_field.  Absolute position of field idx in table tbl (0 if absent).       //
Z inline I fb_field(const G*b,I tbl,I idx){                                     //
    I vt=fb_vtable(b,tbl);                                                      // vtable position
    I slot=4+2*idx;                                                             // field slot within vtable
    U(slot<fb_ri16(b,vt))                                                       // beyond vtable = absent
    I off=fb_ri16(b,vt+slot);                                                   // relative offset in object
    R off?tbl+off:0;}                                                           // 0 = field absent
                                                                                //
// fb_fieldi8/i16/i32/i64.  Read scalar field idx (0 when absent).             //
#define FB_FIELD_RD(NM,T,RD) Z inline T NM(const G*b,I tbl,I idx){            \
    I o=fb_field(b,tbl,idx);R o?RD(b,o):0;}                                     // one reader per scalar width
FB_FIELD_RD(fb_fieldi8 ,I,fb_ri8 )                                              //
FB_FIELD_RD(fb_fieldi16,I,fb_ri16)                                              //
FB_FIELD_RD(fb_fieldi32,I,fb_ri32)                                              //
FB_FIELD_RD(fb_fieldi64,J,fb_ri64)                                              //
                                                                                //
// ── Vector access ────────────────────────────────────────────────────────  //
// A vector field holds an offset → [len:i32][elements...].                    //
                                                                                //
// fb_vecoff.  Absolute position of vector field idx (0 if absent).            //
Z inline I fb_vecoff(const G*b,I tbl,I idx){                                    //
    I o=fb_field(b,tbl,idx);R o?fb_deref(b,o):0;}                               // follow indirection
// fb_veclen.  Element count of the vector at voff.                            //
Z inline I fb_veclen(const G*b,I voff){R voff?fb_ri32(b,voff):0;}               // length prefix
// fb_vecdat.  Pointer to first element (raw structs / scalars).               //
Z inline const G*fb_vecdat(const G*b,I voff){R voff?b+voff+4:0;}                // data follows the length
// fb_vecelt.  Absolute position of element i in a vector of table offsets.    //
Z inline I fb_vecelt(const G*b,I voff,I i){                                     //
    R voff?fb_deref(b,voff+4+4*i):0;}                                           // deref offset-table entry
                                                                                //
// ── String access ────────────────────────────────────────────────────────  //
// A string is a byte vector: [len:i32][chars...][NUL].                        //
                                                                                //
// fb_string.  Pointer to the chars of string field idx (0 if absent).         //
Z inline const char*fb_string(const G*b,I tbl,I idx){                           //
    I o=fb_field(b,tbl,idx);                                                    //
    U(o)                                                                        //
    R(const char*)(b+fb_deref(b,o)+4);}                                         // skip the length prefix
// fb_stringlen.  Byte length of string field idx (0 if absent).               //
Z inline I fb_stringlen(const G*b,I tbl,I idx){                                 //
    I o=fb_field(b,tbl,idx);                                                    //
    U(o)                                                                        //
    R fb_ri32(b,fb_deref(b,o));}                                                // the length prefix itself
                                                                                //
// ── Union access ─────────────────────────────────────────────────────────  //
// A union occupies TWO vtable slots: type byte at idx, value table at idx+1.  //
                                                                                //
Z inline I fb_union_type(const G*b,I tbl,I idx){                                //
    R fb_fieldi8(b,tbl,idx);}                                                   // discriminator byte
Z inline I fb_union_val(const G*b,I tbl,I idx){                                 //
    I o=fb_field(b,tbl,idx+1);                                                  // value is the NEXT field
    R o?fb_deref(b,o):0;}                                                       // follow offset to table
                                                                                //
// ── Arrow schema field indices ───────────────────────────────────────────  //
// Flatbuffer vtable slot numbers from the official Arrow .fbs files (fbs/).   //
                                                                                //
// Footer: 0=version(i16) 1=schema(table) 2=dictionaries(vec) 3=batches(vec)   //
#define FB_FOOTER_SCHEMA     1                                                  //
#define FB_FOOTER_DICTS      2                                                  //
#define FB_FOOTER_BATCHES    3                                                  //
                                                                                //
// Schema: 0=endianness(i16) 1=fields(vec of Field)                            //
#define FB_SCHEMA_FIELDS     1                                                  //
                                                                                //
// Field: 0=name(str) 1=nullable(bool) 2=type_type(union disc) 3=type(union)   //
//        4=dictionary(DictionaryEncoding) 5=children(vec of Field)            //
#define FB_FIELD_NAME        0                                                  //
#define FB_FIELD_TYPE_TYPE   2                                                  //
#define FB_FIELD_DICT        4                                                  //
#define FB_FIELD_CHILDREN    5                                                  //
                                                                                //
// Int: 0=bitWidth(i32) 1=is_signed(bool)                                      //
#define FB_INT_BITWIDTH      0                                                  //
                                                                                //
// FloatingPoint: 0=precision(i16: 0=half 1=single 2=double)                   //
#define FB_FLOAT_PRECISION   0                                                  //
                                                                                //
// Date: 0=unit(i16: 0=day 1=ms)                                               //
#define FB_DATE_UNIT         0                                                  //
                                                                                //
// Time: 0=unit(i16) 1=bitWidth(i32)                                           //
#define FB_TIME_UNIT         0                                                  //
                                                                                //
// Timestamp: 0=unit(i16) 1=timezone(str)                                      //
#define FB_TS_UNIT           0                                                  //
                                                                                //
// Duration: 0=unit(i16)                                                       //
#define FB_DUR_UNIT          0                                                  //
                                                                                //
// DictionaryEncoding: 0=id(i64) 1=indexType(Int) 2=isOrdered(bool)            //
#define FB_DICT_ID           0                                                  //
#define FB_DICT_INDEXTYPE    1                                                  // absent ⇒ default int32 indices
                                                                                //
// Message: 0=version(i16) 1=header_type(ubyte) 2=header(union) 3=bodyLen(i64) //
#define FB_MSG_HEADER_TYPE   1                                                  //
                                                                                //
// RecordBatch: 0=length(i64) 1=nodes(vec FieldNode) 2=buffers(vec Buffer)     //
//              3=compression(BodyCompression; present ⇒ LZ4/ZSTD body)        //
#define FB_RB_LENGTH         0                                                  //
#define FB_RB_NODES          1                                                  //
#define FB_RB_BUFFERS        2                                                  //
#define FB_RB_COMPRESSION    3                                                  //
                                                                                //
// Footer Block — an inline STRUCT, not a table: offset(i64),                  //
// metaDataLength(i32, padded to 8), bodyLength(i64) = 24 bytes each.          //
#define ARROW_BLOCK_SIZE 24                                                     //
                                                                                //
// FieldNode inline struct: length(i64), null_count(i64) = 16 bytes.           //
#define ARROW_FIELDNODE_SIZE 16                                                 //
                                                                                //
// Buffer inline struct: offset(i64), length(i64) = 16 bytes.                  //
#define ARROW_BUFDESC_SIZE   16                                                 //
                                                                                //
#endif                                                                          // ARROW_FLATBUF_H                                                     //
