/ test_leaks.q — RSS-bounded leak loop over read + write + stream + ERROR
/ paths (error-path leaks are the classic FFI bug).  Symbols come from a
/ FIXED pool so the intern table stabilizes after warm-up.
/ Run from the repo root: l tests/test_leaks.q      (L_STRESS=1 -> 2000 iters)

p:"build/arrow_io"
ar:hsym[`$p] 2: (`arrow_read; 1)
aw:hsym[`$p] 2: (`arrow_write; 1)
as:hsym[`$p] 2: (`arrow_stream; 1)

pass:0; fail:0
T:{[nm;ok] $[ok;pass+:1;fail+:1]; $[ok;::;show "  FAIL ",nm]}
rss:{"J"$first system "ps -o rss= -p ",string .z.i}   / resident KB

/ mid-size table: syms (fixed pool) + nulls + temporal + floats
n:50000
ix:til n
t:([]s:(`$"lk",/:string til 64)[ix mod 64];
    i:@[`int$ix mod 1000;where 0=ix mod 11;:;0N];
    f:@[1.5*ix;where 0=ix mod 7;:;0n];
    p:2020.01.01D00:00:00.000000000+1000000000j*ix)
F:hsym `$"/tmp/arrow_leak.arrow"
SD:hsym `$"/tmp/arrow_leak_splay"

/ error-path fixtures: bad magic (open fail) + not-a-file (missing path)
hsym[`$"/tmp/arrow_leak_junk.arrow"] 0: enlist "this is not an arrow file"
BAD:hsym `$"/tmp/arrow_leak_junk.arrow"
MISS:hsym `$"/tmp/arrow_leak_missing.arrow"
BF:hsym `$"/tmp/arrow_adv/foot_huge.arrow"           / footer-bounds error
                                                     / path (if adversarial
                                                     / suite ran first)
one:{[x]
    aw (t;F);                                        / write
    r:ar F;                                          / read back
    as (F;SD);                                       / stream to splay
    e1:@[ar;BAD;{`err}];                             / error: bad magic
    e2:@[ar;MISS;{`err}];                            / error: missing file
    e3:@[ar;BF;{`err}];                              / error: bad footer len
    e4:@[aw;(([]c:"abc");F);{`err}];                 / error: bad write table
    count r}

it:$[""~getenv `L_STRESS;600;2000]
warm:50
j:0; while[j<warm; one[]; j+:1]                      / warm-up: caches, pool
r0:rss[]
j:0; while[j<it; one[]; j+:1]
r1:rss[]
grow:r1-r0
show "RSS after warmup: ",string[r0]," KB; after ",string[it],
    " iters: ",string[r1]," KB; growth: ",string[grow]," KB"

/ 32 MB bound: read+write+stream of a 50k-row table x hundreds of iters
/ must not accumulate — growth means a per-iteration leak.
T["rss growth < 32MB";grow<32768]
T["loop result sane";50000=one[]]
T["splay still loads";50000=count get SD]

show ""
show "=== Results: ",string[pass]," passed, ",string[fail]," failed ==="
\\
