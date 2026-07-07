/ test_stream.q — stream + multi-batch depth: dict/null/temporal columns
/ through arrow_stream splays, 2-batch boundary edge, and the multi-batch
/ dictionary regression (decoded symbols must copy at pointer width).
/ Fixtures: uv run --with pyarrow tests/gen_edges.py
/ Run from the repo root: l tests/test_stream.q

p:"build/arrow_io"
ar:hsym[`$p] 2: (`arrow_read; 1)
aw:hsym[`$p] 2: (`arrow_write; 1)
as:hsym[`$p] 2: (`arrow_stream; 1)

pass:0; fail:0
T:{[nm;ok] $[ok;pass+:1;fail+:1]; $[ok;::;show "  FAIL ",nm]}
D:"/tmp/arrow_edges/"

/ expected stream_mb table — mirrors the formulas in gen_edges.py
n:10000
ix:til n
d:(`$"d",/:string til 5)[ix mod 5]
s:@[(`$"s",/:string til 100)[ix mod 100];where 0=ix mod 7;:;`]
iv:@[`int$ix mod 1000;where 0=ix mod 11;:;0N]
pp:@[2020.01.01D00:00:00.000000000+1000000000j*ix;where 0=ix mod 13;:;"p"$0N]
e:`real$ix mod 97
h:@[`short$ix mod 100;where 0=ix mod 17;:;0Nh]
want:([]d;s;i:iv;p:pp;e;h)

/ ── multi-batch parallel READ of the mixed table ──
show "-- multi-batch read (6 batches, mixed types) --"
mb:ar hsym `$D,"stream_mb.arrow"
T["mb read matches formula";mb~want]
T["mb dict col type";11=type mb`d]
T["mb temporal type";12=type mb`p]

/ ── stream -> splay -> get: must load and value-match ──
show "-- stream to splay --"
sd:hsym `$D,"stream_splay"
nr:as (hsym `$D,"stream_mb.arrow";sd)
T["stream row count";nr=10000]
g:get sd
T["splay loads+matches";g~want]                       / the OLD suite only
T["splay dict col intact";g[`d]~want`d]               / counted rows here
T["splay null temporal";g[`p]~want`p]
T["splay short nulls";g[`h]~want`h]

/ ── 2-batch tiny stream: batch boundary at row 1 ──
show "-- tiny 2-batch stream --"
td:hsym `$D,"tiny_splay"
nt:as (hsym `$D,"stream_tiny2.arrow";td)
T["tiny stream count";nt=2]
T["tiny splay";(get td)~([]j:7 8j;s:`one`two)]

/ ── multi-batch dictionary regression (10 batches, 8-byte symbol copies) ──
show "-- multi-batch dict regression --"
md:ar hsym `$D,"multibatch_dict.arrow"
mexp:([]d:(`k0`k1`k2)[(til 1000) mod 3];v:`long$til 1000)
T["10-batch dict col";md~mexp]
sd2:hsym `$D,"mbdict_splay"
T["10-batch dict stream";1000=as (hsym `$D,"multibatch_dict.arrow";sd2)]
T["10-batch dict splay";(get sd2)~mexp]

/ ── streamed splay written back out through arrow_write round-trips ──
F:hsym `$D,"stream_back.arrow"
aw (g;F)
T["splay -> arrow rt";want~ar F]

show ""
show "=== Results: ",string[pass]," passed, ",string[fail]," failed ==="
\\
