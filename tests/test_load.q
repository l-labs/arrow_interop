/ test_load.q — basic load + write/read round-trip smoke test
/ Run from the repo root (after cmake --build build): l tests/test_load.q
p:"build/arrow_io"
show "loading: ",p
ar:hsym[`$p] 2: (`arrow_read; 1)
aw:hsym[`$p] 2: (`arrow_write; 1)
show "loaded, type: ",string type ar
t:([]i:1 2 3i;f:1.5 2.5 3.5;s:`a`b`c)
aw (t;`:/tmp/t_load.arrow)
t2:ar `:/tmp/t_load.arrow
show t2
show $[t~t2;"PASS round-trip";"FAIL round-trip"]
\\
