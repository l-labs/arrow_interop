/ test_edges.q — hostile edges: sliced batches, float specials, sentinel
/ collisions, temporal extremes, unicode, degenerate shapes, unsupported
/ Arrow forms (clean errors).  Fixtures: uv run --with pyarrow tests/gen_edges.py
/ Run from the repo root: l tests/test_edges.q

p:"build/arrow_io"
ar:hsym[`$p] 2: (`arrow_read; 1)
aw:hsym[`$p] 2: (`arrow_write; 1)

pass:0; fail:0
T:{[nm;ok] $[ok;pass+:1;fail+:1]; $[ok;::;show "  FAIL ",nm]}
E:{[f;x] @[f;x;{`err}]}                              / trap to `err
D:"/tmp/arrow_edges/"
L:{hsym `$D,x}

/ ── sliced record batches (non-zero source offsets, rebased by the writer) ──
show "-- sliced batches --"
mk:{[o;n] i:o+til n;                                 / mirror of gen_edges base
    s:@[(`$"v",/:string i mod 10);where 0=i mod 7;:;`];
    f:@[1.5*i;where 0=i mod 5;:;0n];
    ([]i:`int$i;s;f)}
T["sliced (13,50)";(ar L "sliced.arrow")~mk[13;50]]
T["sliced odd offset (9,31)";(ar L "sliced_odd.arrow")~mk[9;31]]

/ ── float specials ──
show "-- float specials --"
sp:ar L "specials.arrow"; spf:sp`f; spe:sp`e
T["f64 +-inf";(spf[0]=0w)&spf[1]=-0w]
T["f64 -0.0 sign";-0w=1%spf 2]
T["f64 nan+null->0n";(null spf 3)&null spf 5]
T["f32 +-inf";(spe[0]=0we)&spe[1]=-0we]
T["f32 nan+null->0Ne";(null spe 3)&null spe 5]
T["f32 value";spe[4]=1.5e]

/ ── sentinel collision: valid Arrow values equal to L null sentinels ──
/ DOCUMENTED caveat: such values arrive as L nulls; MIN+1 survives.
show "-- sentinel collisions --"
sn:ar L "sentinel.arrow"; snj:sn`j; sni:sn`i; snh:sn`h
T["i64 MIN -> 0Nj";null snj 0]
T["i64 MIN+1 kept";snj[1]=-9223372036854775807j]
T["i32 MIN -> 0N";null sni 0]
T["i32 MIN+1 kept";sni[1]=-2147483647i]
T["i16 MIN -> 0Nh";null snh 0]
T["i16 MIN+1 kept";snh[1]=-32767h]

/ ── temporal extremes ──
show "-- temporal extremes --"
tx:ar L "temporal_x.arrow"; txp:tx`p; txd:tx`d
T["ts 1800 (ns)";txp[0]=1800.01.01D00:00:00.000000000]
T["ts 2200 (ns)";txp[1]=2200.01.01D00:00:00.000000000]
T["ts null";null txp 2]
T["ts us unit";(tx`u)~txp]
T["date 1900/2100";(txd[0]=1900.01.01)&txd[1]=2100.01.01]
T["date null";null txd 2]

/ ── unicode + boundary-length strings ──
show "-- unicode --"
un:(ar L "unicode.arrow")`s
T["unicode syms";un~(`;`$"héllo";`$"日本語";`$"🚀🔥";`$255#"a";`$"tab\there")]
F:hsym `$D,"unicode_out.arrow"
aw (([]s:un);F)
T["unicode write rt";un~(ar F)`s]

/ ── degenerate shapes ──
show "-- degenerate shapes --"
T["zero-col clean err";`err~E[ar;L "zerocol.arrow"]]
wt:ar L "wide10k.arrow"
T["10k cols read";10000=count cols wt]
T["10k cols values";(wt[`c0]~0 1 2i)&wt[`c9999]~9999 10000 10001i]
WF:hsym `$D,"wide_out.arrow"
aw (wt;WF)
T["10k cols write rt";wt~ar WF]

/ ── LargeUtf8 (64-bit offsets) ──
show "-- large utf8 --"
T["large_utf8";((ar L "largeutf8.arrow")`s)~(`aa;`;`$"β";`)]

/ ── unsupported Arrow forms: clean q errors, never a crash ──
show "-- unsupported forms --"
T["dict int8 idx err";`err~E[ar;L "dict8.arrow"]]
T["dict int values err";`err~E[ar;L "dicti.arrow"]]
T["date64 err";`err~E[ar;L "date64.arrow"]]
T["time64 err";`err~E[ar;L "time64.arrow"]]
T["decimal err";`err~E[ar;L "decimal.arrow"]]

/ ── time32[s] scales to L milliseconds ──
T["time32 s->ms";((ar L "time_s.arrow")`t)~00:00:00.000 01:01:01.000 0Nt]

show ""
show "=== Results: ",string[pass]," passed, ",string[fail]," failed ==="
\\
