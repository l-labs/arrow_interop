/ test_arrow.q — Arrow IPC round-trip test suite
/ Run from the repo root (after cmake --build build): l tests/test_arrow.q
/ Interop tests 11-12 need fixtures: uv run --with pyarrow tests/make_fixtures.py

p:"build/arrow_io"
ar:hsym[`$p] 2: (`arrow_read; 1)
aw:hsym[`$p] 2: (`arrow_write; 1)
as:hsym[`$p] 2: (`arrow_stream; 1)

pass:0; fail:0
T:{[nm;ok] $[ok;pass+:1;fail+:1]; show $[ok;"  PASS ";"  FAIL "],nm}

show "=== Arrow Round-Trip Test Suite ==="
show ""

/ ── 1. Basic types ──
show "-- Basic types --"
t:([]i:1 2 3 4 5i;j:10 20 30 40 50;f:1.5 2.5 3.5 4.5 5.5;s:`a`b`c`d`e;b:10110b)
aw (t;`:/tmp/t_basic.arrow); t2:ar `:/tmp/t_basic.arrow
T["int+long+float+sym+bool";t~t2]

/ ── 2. Short + Byte + Real ──
show "-- Short/Byte/Real --"
t:([]h:1 2 3 4 5h;g:0x0102030405;e:1.5 2.5 3.5 4.5 5.5e)
aw (t;`:/tmp/t_hge.arrow); t2:ar `:/tmp/t_hge.arrow
T["short+byte+real";t~t2]

/ ── 3. Date ──
show "-- Dates --"
d:.z.d - 0 1 2 3 4
t:([]d;v:1 2 3 4 5i)
aw (t;`:/tmp/t_date.arrow); t2:ar `:/tmp/t_date.arrow
T["date round-trip";t~t2]

/ ── 4. Nulls (int) ──
show "-- Nulls --"
x:1 2 3 4 5i; x[1]:0N; x[3]:0N
t:([]x;v:1 2 3 4 5i)
aw (t;`:/tmp/t_nulli.arrow); t2:ar `:/tmp/t_nulli.arrow
T["int nulls";t~t2]

/ ── 5. Nulls (float) ──
y:1 2 3 4 5.0; y[1]:0n; y[3]:0n
t:([]y;v:1 2 3 4 5i)
aw (t;`:/tmp/t_nullf.arrow); t2:ar `:/tmp/t_nullf.arrow
T["float nulls";t~t2]

/ ── 6. Empty table ──
show "-- Edge cases --"
te:([]x:`int$();y:`float$())
aw (te;`:/tmp/t_empty.arrow); t2:ar `:/tmp/t_empty.arrow
T["empty table";te~t2]

/ ── 7. Single row ──
ts:([]x:enlist 42i;y:enlist 3.14)
aw (ts;`:/tmp/t_single.arrow); t2:ar `:/tmp/t_single.arrow
T["single row";ts~t2]

/ ── 8. Wide table (20 columns) ──
tw:([]c0:5?100i;c1:5?100i;c2:5?100i;c3:5?100i;c4:5?100i;c5:5?100i;c6:5?100i;c7:5?100i;c8:5?100i;c9:5?100i;c10:5?100i;c11:5?100i;c12:5?100i;c13:5?100i;c14:5?100i;c15:5?100i;c16:5?100i;c17:5?100i;c18:5?100i;c19:5?100i)
aw (tw;`:/tmp/t_wide.arrow); t2:ar `:/tmp/t_wide.arrow
T["wide (20 cols)";tw~t2]

/ ── 9. Symbols (interning) ──
show "-- Symbols --"
t:([]sym:1000?`AAPL`GOOG`MSFT`AMZN`META`TSLA`NVDA`NFLX;v:1000?100.0)
aw (t;`:/tmp/t_sym.arrow); t2:ar `:/tmp/t_sym.arrow
T["1K symbols";t~t2]

/ ── 10. Large table (1M) ──
show "-- Large --"
t:([]sym:1000000?`AAPL`GOOG`MSFT;p:1000000?100.0;v:1000000?1000i)
(-43)!`wr1m; aw (t;`:/tmp/t_1m.arrow); (-44)!`wr1m
(-43)!`rd1m; t2:ar `:/tmp/t_1m.arrow; (-44)!`rd1m
T["1M round-trip";t~t2]

/ ── 11. Read pyarrow-written file ──
show "-- Interop --"
t3:@[ar;`:/tmp/py_written.arrow;{`err}]
T["read pyarrow file";98=type t3]

/ ── 12. Read dictionary-encoded ──
t4:@[ar;`:/tmp/small_dict.arrow;{`err}]
T["dict-encoded";98=type t4]

/ ── 13. Streaming ──
show "-- Streaming --"
n:as (`:/tmp/t_1m.arrow;`:/tmp/stream_out)
T["stream 1M to disk";n=1000000]
/ the splay must load via get and value-match a direct read of the source
T["stream splay read-back";(ar `:/tmp/t_1m.arrow)~get `:/tmp/stream_out]

/ ── 14. All-null column ──
show "-- All null --"
x:5#0N; t:([]x;v:1 2 3 4 5i)
aw (t;`:/tmp/t_allnull.arrow); t2:ar `:/tmp/t_allnull.arrow
T["all-null int column";t~t2]

/ ── 15. Timestamp (KP) + Timespan (KN) — Arrow Timestamp[ns]/Duration[ns] ──
show "-- Temporal (KP/KN) --"
ts:2000.01.01D00:00:00.0+1000000000j*til 5; ts[2]:"p"$0N         / KP w/ null
tn:"n"$86400000000000j*1 2 3 4 5; tn[1]:"n"$0N                   / KN w/ null
t:([]ts;tn;v:1 2 3 4 5i)
aw (t;`:/tmp/t_tsn.arrow); t2:ar `:/tmp/t_tsn.arrow
T["timestamp+timespan round-trip";t~t2]
T["timestamp stays KP (12)";12=type t2`ts]
T["timespan stays KN (16)";16=type t2`tn]

show ""
show "=== Results: ",string[pass]," passed, ",string[fail]," failed ==="
show (-45)!0
\\
