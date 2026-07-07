/ test_matrix.q — L side of the randomized round-trip matrix + L-origin trips.
/ Run AFTER `uv run --with pyarrow tests/gen_matrix.py gen`, from repo root:
/   l tests/test_matrix.q
/ then `uv run --with pyarrow tests/gen_matrix.py check` verifies the outputs.

p:"build/arrow_io"
ar:hsym[`$p] 2: (`arrow_read; 1)
aw:hsym[`$p] 2: (`arrow_write; 1)

pass:0; fail:0
T:{[nm;ok] $[ok;pass+:1;fail+:1]; $[ok;::;show "  FAIL ",nm]}

\l /tmp/arrow_matrix/manifest.q

/ ── Part 1: pyarrow-written cases: read each, write back for the oracle ──
show "-- matrix: pyarrow -> L -> pyarrow (oracle checks the _out files) --"
rt:{[c] f:hsym `$"/tmp/arrow_matrix/",c,".arrow";
    o:hsym `$"/tmp/arrow_matrix/",c,"_out.arrow";
    t:ar f; aw (t;o); `ok}
i:0
while[i<count CASES; c:CASES[i];
    r:@[rt;c;{`err}]; T["matrix ",c;`ok~r]; i+:1]

/ ── Part 2: L-origin round trips (L is its own oracle: t ~ read write t) ──
show "-- matrix: L -> file -> L (~ match) --"
n:10007                                              / prime: odd batch tails
F:hsym `$"/tmp/arrow_matrix/l_origin.arrow"
RT:{[t] aw (t;F); t~ar F}

/ every writable type, no nulls
b:0=(til n) mod 3
g:`byte$(til n) mod 256
h:`short$(til n) mod 30000
iv:`int$(til n)*7
j:123456789j*til n
e:`real$(til n) mod 977
f:1.5*til n
s:(`$"s",/:string til 50)[(til n) mod 50]
d:2020.01.01+(til n) mod 5000
tm:`time$(til n) mod 86400000
ts:2020.01.01D00:00:00.000000000+1000000000j*til n
nn:`timespan$123456789j*til n
t1:([]b;g;h;i:iv;j;e;f;s;d;t:tm;p:ts;n:nn)
T["L-origin all types";RT t1]

/ nulls in every nullable column (short/real included — null-width classes)
hn:@[h;where 0=(til n) mod 7;:;0Nh]
inn:@[iv;where 0=(til n) mod 11;:;0N]
jn:@[j;where 0=(til n) mod 13;:;0Nj]
en:@[e;where 0=(til n) mod 5;:;0Ne]
fn:@[f;where 0=(til n) mod 3;:;0n]
dn:@[d;where 0=(til n) mod 17;:;0Nd]
tn:@[tm;where 0=(til n) mod 19;:;0Nt]
pn:@[ts;where 0=(til n) mod 23;:;"p"$0N]
nnn:@[nn;where 0=(til n) mod 29;:;"n"$0N]
t2:([]h:hn;i:inn;j:jn;e:en;f:fn;d:dn;t:tn;p:pn;n:nnn)
T["L-origin nulls (all widths)";RT t2]

/ all-null columns of each width
m:17
t3:([]h:m#0Nh;i:m#0N;j:m#0Nj;e:m#0Ne;f:m#0n;p:m#"p"$0N;n:m#"n"$0N;d:m#0Nd;t:m#0Nt)
T["L-origin all-null";RT t3]

/ one row / zero rows
T["L-origin 1 row";RT ([]i:enlist 0N;s:enlist `x;f:enlist -0.0)]
T["L-origin 0 rows";RT ([]i:`int$();s:`symbol$();f:`float$();p:`timestamp$())]

/ empty-string symbols mixed with real ones
T["L-origin empty syms";RT ([]s:`a``b``;v:1 2 3 4 5i)]

/ single distinct symbol (dedup cache single-entry)
T["L-origin 1-sym";RT ([]s:n#`only;v:til n)]

/ high-cardinality symbols (dedup cache growth path)
T["L-origin hi-card syms";RT ([]s:`$"u",/:string til n;v:til n)]

/ infinities survive; NaN becomes null (they are one bit pattern class)
tf:([]f:0w -0w 1.5 0n)
aw (tf;F); rf:(ar F)`f
T["L-origin inf/nan";rf~0w -0w 1.5 0n]
tw:([]f:enlist -0.0); aw (tw;F)
T["L-origin -0.0 sign";-0w=1%first (ar F)`f]

/ int sentinel collisions: null cells write as Arrow nulls and read back as
/ nulls; near-sentinel values (MIN+1) survive untouched
tc:([]i:(0N;-2147483647i;0i);j:(0Nj;-9223372036854775807j;0j);
    h:(0Nh;-32767h;0h))
aw (tc;F); rc:ar F
T["L-origin sentinel=null";tc~rc]
T["L-origin sentinel doc";(null (rc`i) 0)&null (rc`j) 0]

/ KZ datetime writes, reads back as timestamp (documented one-way trip)
tz:([]z:2020.01.01T12:00:00.000 2021.06.15T06:30:00.000)
aw (tz;F); rz:(ar F)`z
T["L-origin KZ->KP";(12=type rz)&rz~2020.01.01D12:00:00.000000000 2021.06.15D06:30:00.000000000]

/ unsupported L column types are a clean error, not a bad file
T["L-origin char col err";`err~@[aw;(([]c:"abc";v:1 2 3i);F);{`err}]]
T["L-origin nested col err";`err~@[aw;(([]l:(1 2;3 4;5 6);v:1 2 3i);F);{`err}]]

show ""
show "=== Results: ",string[pass]," passed, ",string[fail]," failed ==="
\\
