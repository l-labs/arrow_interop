# gen_edges.py — hostile-edge and stream-depth fixtures for the deep suite.
# Run: uv run --with pyarrow tests/gen_edges.py
# Consumed by tests/test_edges.q and tests/test_stream.q.
import os
import pyarrow as pa
import pyarrow.ipc as ipc

DIR = "/tmp/arrow_edges"
os.makedirs(DIR, exist_ok=True)


def w(name, table, batches=None):
    path = os.path.join(DIR, name)
    with ipc.new_file(path, table.schema) as wr:
        if batches is None:
            wr.write_table(table)
        else:
            for b in batches:
                wr.write_table(b)
    print("wrote", path)


# ── sliced record batches: non-zero offsets in every buffer kind ──────────
# pyarrow rebases sliced buffers on IPC write; the reader must agree exactly.
N = 100
base = pa.table({
    "i": pa.array(range(N), type=pa.int32()),
    "s": pa.array([None if i % 7 == 0 else "v%d" % (i % 10) for i in range(N)]),
    "f": pa.array([None if i % 5 == 0 else i * 1.5 for i in range(N)]),
})
sl = base.slice(13, 50)
w("sliced.arrow", sl)
odd = base.slice(9, 31)                       # offset not a multiple of 8:
w("sliced_odd.arrow", odd)                    # validity-bitmap shift stress

# ── float specials: +-inf, -0.0, NaN, null are all distinct on the wire ───
w("specials.arrow", pa.table({
    "f": pa.array([float("inf"), float("-inf"), -0.0, float("nan"), 1.5, None],
                  type=pa.float64()),
    "e": pa.array([float("inf"), float("-inf"), -0.0, float("nan"), 1.5, None],
                  type=pa.float32()),
}))

# ── sentinel collision: VALID values equal to L's null sentinels ──────────
w("sentinel.arrow", pa.table({
    "j": pa.array([-(1 << 63), -(1 << 63) + 1, 0], type=pa.int64()),
    "i": pa.array([-(1 << 31), -(1 << 31) + 1, 0], type=pa.int32()),
    "h": pa.array([-(1 << 15), -(1 << 15) + 1, 0], type=pa.int16()),
}))

# ── temporal extremes: pre-2000 and far-future, several units ─────────────
w("temporal_x.arrow", pa.table({
    "p": pa.array([-5364662400_000_000_000, 7258118400_000_000_000, None],
                  type=pa.timestamp("ns")),          # 1800-01-01 / 2200-01-01
    "u": pa.array([-5364662400_000_000, 7258118400_000_000, None],
                  type=pa.timestamp("us")),
    "d": pa.array([-25567, 47482, None], type=pa.date32()),  # 1900 / 2100
}))

# ── unicode + boundary-length strings ──────────────────────────────────────
w("unicode.arrow", pa.table({
    "s": pa.array(["", "héllo", "日本語",
                   "\U0001f680\U0001f525", "a" * 255, "tab\there"]),
}))

# ── zero-column table: no L form, must be a clean error ───────────────────
w("zerocol.arrow", pa.table({}))

# ── 10_000-column table ────────────────────────────────────────────────────
w("wide10k.arrow", pa.table(
    {("c%d" % i): pa.array([i, i + 1, i + 2], type=pa.int32())
     for i in range(10_000)}))

# ── LargeUtf8: 64-bit offsets (plain and dictionary-encoded) ───────────────
w("largeutf8.arrow", pa.table({
    "s": pa.array(["aa", None, "β", ""], type=pa.large_utf8()),
}))

# ── unsupported forms: each must be a clean error, never a crash ───────────
w("dict8.arrow", pa.table({"d": pa.DictionaryArray.from_arrays(
    pa.array([0, 1, 0], type=pa.int8()), pa.array(["x", "y"]))}))
w("dicti.arrow", pa.table(
    {"d": pa.array([10, 20, 10], type=pa.int64()).dictionary_encode()}))
w("date64.arrow", pa.table({"d": pa.array([0, 86400000], type=pa.date64())}))
w("time64.arrow", pa.table({"t": pa.array([0, 1], type=pa.time64("us"))}))
w("decimal.arrow", pa.table(
    {"x": pa.array([1, 2], type=pa.decimal128(10, 2))}))

# ── time32 in seconds: must scale to L milliseconds ────────────────────────
w("time_s.arrow", pa.table(
    {"t": pa.array([0, 3661, None], type=pa.time32("s"))}))

# ── stream-depth fixtures ──────────────────────────────────────────────────
# 6-batch stream mixing dict + utf8 + nulls + temporal + f32 + i16: formulas
# are mirrored in tests/test_stream.q, which rebuilds the expected table.
NS = 10_000
dcty = pa.array(["d%d" % (i % 5) for i in range(NS)]).dictionary_encode()
st = pa.table({
    "d": dcty,
    "s": pa.array([None if i % 7 == 0 else "s%d" % (i % 100)
                   for i in range(NS)]),
    "i": pa.array([None if i % 11 == 0 else i % 1000 for i in range(NS)],
                  type=pa.int32()),
    "p": pa.array([None if i % 13 == 0 else 1577836800_000_000_000 + i * 10**9
                   for i in range(NS)], type=pa.timestamp("ns")),
    "e": pa.array([float(i % 97) for i in range(NS)], type=pa.float32()),
    "h": pa.array([None if i % 17 == 0 else i % 100 for i in range(NS)],
                  type=pa.int16()),
})
step = (NS + 5) // 6
w("stream_mb.arrow", st, batches=[st.slice(o, step) for o in range(0, NS, step)])

# 2-batch tiny stream: 1 row per batch (batch-boundary edge)
tiny = pa.table({"j": pa.array([7, 8], type=pa.int64()),
                 "s": pa.array(["one", "two"])})
w("stream_tiny2.arrow", tiny, batches=[tiny.slice(0, 1), tiny.slice(1, 1)])

# multi-batch DICTIONARY column (10 batches): regression for the dict-copy
# width bug — every batch's decoded symbols must land 8 bytes wide.
NB = 10
mb = pa.table({
    "d": pa.array(["k%d" % (i % 3) for i in range(NB * 100)]).dictionary_encode(),
    "v": pa.array(range(NB * 100), type=pa.int64()),
})
w("multibatch_dict.arrow", mb,
  batches=[mb.slice(o, 100) for o in range(0, NB * 100, 100)])

print("edges fixtures complete")
