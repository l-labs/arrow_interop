// arrow_types.h — Arrow ↔ L type mapping and IPC constants.                   //
// Apache Arrow Columnar Format: https://arrow.apache.org/docs/format/         //
#ifndef ARROW_TYPES_H                                                           //
#define ARROW_TYPES_H                                                           //

// ── Arrow IPC magic and alignment ──────────────────────────────────────── //
#define ARROW_MAGIC     "ARROW1"                                                //
#define ARROW_MAGIC_LEN 6                                                       //
#define ARROW_ALIGN     64                                                      // buffer alignment (AVX-512)
#define ARROW_PAD(n)    (((n)+ARROW_ALIGN-1)&~(ARROW_ALIGN-1))                  // round up to 64

// ── Arrow Flatbuffer type enum (from Schema.fbs) ───────────────────────── //
// These match the Type union discriminator in the Arrow Flatbuffer schema.    //
enum ArrowTypeId {                                                              //
    ARROW_NONE=0,                                                               //
    ARROW_NULL=1,       ARROW_INT=2,        ARROW_FLOAT=3,                      //
    ARROW_BINARY=4,     ARROW_UTF8=5,       ARROW_BOOL=6,                       //
    ARROW_DECIMAL=7,    ARROW_DATE=8,       ARROW_TIME=9,                       //
    ARROW_TIMESTAMP=10, ARROW_INTERVAL=11,  ARROW_LIST=12,                      //
    ARROW_STRUCT=13,    ARROW_UNION=14,     ARROW_FIXEDBINARY=15,               //
    ARROW_FIXEDLIST=16, ARROW_MAP=17,       ARROW_DURATION=18,                  //
    ARROW_LARGEBINARY=19, ARROW_LARGEUTF8=20,                                   //
    ARROW_LARGELIST=21, ARROW_RUNENDENCODED=22,                                 //
    ARROW_BINARYVIEW=23, ARROW_UTF8VIEW=24,                                     //
    ARROW_LISTVIEW=25, ARROW_LARGELISTVIEW=26                                   //
};                                                                              //

// ── Arrow Int bit widths ───────────────────────────────────────────────── //
// The Int type in Flatbuffers has bitWidth and is_signed fields.              //
// We map: 8→KG, 16→KH, 32→KI, 64→KJ (signed only for now).                    //

// ── Arrow Float precision ──────────────────────────────────────────────── //
enum ArrowFloatPrec { ARROW_HALF=0, ARROW_SINGLE=1, ARROW_DOUBLE=2 };           //

// ── Arrow Time unit (also used by Timestamp / Duration) ───────────────── //
enum ArrowTimeUnit { ARROW_SEC=0, ARROW_MS=1, ARROW_US=2, ARROW_NS=3 };         //

// ── Epoch offsets: Arrow (1970) vs L (2000) ─────────────────────────── //
#define ARROW_L_DATE_OFFSET  10957                                              // days between 1970-01-01 and 2000-01-01
#define ARROW_L_TS_OFFSET_NS 946684800000000000LL                               // nanoseconds between epochs

#endif                                                                          // ARROW_TYPES_H                                                       //
