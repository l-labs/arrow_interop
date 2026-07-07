# arrow_interop

Apache Arrow IPC reader/writer/streamer for L, as a shared library loaded via
`2:`. Self-contained and offline: no libarrow, no network fetch — the flatcc
runtime (Apache-2.0) and the `l.h` API header are vendored in-repo.

## Quickstart

```sh
cmake -B build && cmake --build build     # → build/arrow_io.so
```

```q
.arrow.read:  `:build/arrow_io 2: (`arrow_read; 1)
.arrow.write: `:build/arrow_io 2: (`arrow_write; 1)
.arrow.stream:`:build/arrow_io 2: (`arrow_stream; 1)

.arrow.write[([]sym:`AAPL`GOOG;price:150.5 175.3); `:/tmp/out.arrow]
t:.arrow.read `:/tmp/out.arrow                    / .arrow / .feather v2 file
n:.arrow.stream[`:/tmp/huge.arrow; `:/tmp/db/t]   / splay to disk, 1 batch DRAM
```

To build against an L source tree instead of the vendored header:
`cmake -B build -DL_ROOT=/path/to/l`.

## Type mapping

| L type | Arrow type | Read | Write | Notes |
|--------|-----------|------|-------|-------|
| KB bool | Bool | y | y | bitpacked on the wire, byte-per-value in L |
| KG byte | UInt8 | y | y | memcpy |
| KH short / KI int / KJ long | Int16/32/64 | y | y | memcpy; nulls ↔ validity bitmap |
| KE real / KF float | Float32/64 | y | y | NaN ↔ null |
| KS symbol | Utf8 | y | y | interned on read (dedup cache) |
| KD date | Date32[day] | y | y | epoch shift 2000 ↔ 1970 (10957 days) |
| KT time | Time32[ms] | y | y | |
| KP timestamp | Timestamp[ns] | y | y | epoch shift, null-preserving |
| KN timespan | Duration[ns] | y | y | no epoch shift |
| KZ datetime | Timestamp[ns] | — | y | write-only; reads back as KP |
| — | Dictionary<Int32,Utf8> | y | — | index → interned symbol |
| — | List<T> | y | — | generic list of typed vectors |

Nulls: L sentinels (`0Ni`, `0Nj`, `0n`) ↔ Arrow validity bitmaps, expanded with
AVX-512 masked loads where available. Multi-batch files are read in parallel
(one worker per batch, core-bounded, disjoint row ranges); `arrow_write`
converts and emits columns in parallel into one RecordBatch. Measured on a
10M-row × 4-column table (240MB IPC, 8-core arm64): read 23ms, multi-batch
read 21ms, write 129ms; 10M high-cardinality Utf8 rows (1M distinct) read in
160ms — a dedup cache interns each distinct string once.

## Tests

```sh
L_BIN=/path/to/l sh tests/run_all.sh           # everything, ~30s (needs uv)
```

Suites (each also runs standalone; fixtures via `uv run --with pyarrow
tests/gen_edges.py` / `gen_matrix.py gen`):

* `test_arrow.q` — the original 18 round-trip assertions.
* `test_matrix.q` + `gen_matrix.py` — seeded randomized matrix: types ×
  lengths {0,1,7,4096,1M} × null densities {0,.05,1} × batch counts
  {1,2,10,100} × dict cardinalities; pyarrow writes → L reads → L writes →
  pyarrow verifies (pyarrow is the oracle), plus L-origin `~` round trips.
  `L_STRESS=1` triples the case count.
* `test_edges.q` + `gen_edges.py` — sliced batches (non-zero offsets), ±inf /
  -0.0 / NaN, sentinel collisions, pre-1900/post-2100 temporals, unicode +
  255-byte symbols, 0-column and 10 000-column tables, LargeUtf8, and clean
  errors for unsupported forms (decimal, date64, time64, non-int32 dict
  indices, int-valued dicts).
* `test_stream.q` — multi-batch stream → splay `get` compare with dict +
  null + temporal + f32/i16 columns; 2-batch boundary edge; 10-batch
  dictionary regression.
* `test_adversarial.py` — corrupted files (systematic truncations, magic
  flips, footer-length corruption, empty/zero files, LZ4/ZSTD-compressed
  bodies) each in a fresh subprocess: any signal exit is a failure; every
  case must produce a clean q error, never garbage data.
* `test_leaks.q` — RSS-bounded loop over read + write + stream + error paths.

**Sentinel-collision caveat**: L nulls are in-band sentinels. A *valid*
Arrow value equal to a sentinel — int16 `-32768`, int32 `-2147483648`,
int64 `-9223372036854775808`, or any float/double NaN — reads as an L null
and re-writes as an Arrow null. Utf8 nulls read as the empty symbol, bool
nulls as `0b`, and uint8 nulls as `0x00` (L bytes have no null form); these
also do not round-trip. Everything else does.
