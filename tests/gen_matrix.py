# gen_matrix.py — seeded randomized round-trip matrix; pyarrow is the oracle.
#
#   uv run --with pyarrow tests/gen_matrix.py gen     # write case files
#   uv run --with pyarrow tests/gen_matrix.py check   # verify L-written output
#
# Flow per case (driven by tests/test_matrix.q between gen and check):
#   pyarrow writes case_NNN.arrow  ->  L reads it  ->  L writes case_NNN_out.arrow
#   ->  check regenerates the case from its seed, applies the documented L
#   mapping (nulls <-> sentinels, dict -> utf8, temporal -> ns, utf8 null -> "")
#   and asserts the L output equals the mapped original.
#
# Cases sample: type x length {0,1,7,4096,1_000_000} x null density {0,.05,1}
# x batch count {1,2,10,100} x dict cardinality {1,100,100_000}.  Fixed seeds:
# a failure names its case and the case regenerates exactly.  L_STRESS=1
# raises the case count 40 -> 120.
import os, sys, random
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.ipc as ipc

DIR = sys.argv[2] if len(sys.argv) > 2 else "/tmp/arrow_matrix"
BASE_SEED = 20260706
N_CASES = 120 if os.environ.get("L_STRESS") else 40

TYPES = ["bool", "u8", "i16", "i32", "i64", "f32", "f64", "utf8", "dict",
         "date32", "ts_s", "ts_ms", "ts_us", "ts_ns", "dur_ns", "time_ms"]
LENGTHS = [0, 1, 7, 4096, 1_000_000]
LEN_W = [2, 3, 5, 8, 1]                      # keep 1M-row cases rare (runtime)
NULLD = [0.0, 0.05, 1.0]
BATCHES = [1, 2, 10, 100]
CARDS = [1, 100, 100_000]

I16_MIN, I32_MIN, I64_MIN = -(1 << 15), -(1 << 31), -(1 << 63)


def pool(rng, card):
    """Distinct utf8 pool of size card (occasionally contains "")."""
    p = ["s%d_%x" % (i, (i * 2654435761) & 0xFFFF) for i in range(card)]
    if card > 2 and rng.random() < 0.3:
        p[1] = ""                            # empty string is a legal value
    return p


def gen_column(rng, typ, n, nd, card):
    """One column as a pyarrow array.  Sentinel-colliding values are NOT
    generated here (they are pinned in tests/gen_edges.py); u8 never takes
    nulls (L bytes have no null form)."""
    if typ == "u8":
        nd = 0.0
    nulls = [rng.random() < nd for _ in range(n)]
    r = rng
    if typ == "bool":
        v, t = [r.random() < 0.5 for _ in range(n)], pa.bool_()
    elif typ == "u8":
        v, t = [r.randrange(256) for _ in range(n)], pa.uint8()
    elif typ == "i16":
        v, t = [r.randint(-32000, 32000) for _ in range(n)], pa.int16()
    elif typ == "i32":
        v, t = [r.randint(-(2**31) + 2, 2**31 - 1) for _ in range(n)], pa.int32()
    elif typ == "i64":
        v, t = [r.randint(-(2**62), 2**62) for _ in range(n)], pa.int64()
    elif typ == "f32":
        v, t = [r.choice([-1.5, 0.0, 2.25, 1024.5, -3e8, 7.0]) * r.randrange(1, 9)
                for _ in range(n)], pa.float32()
    elif typ == "f64":
        v, t = [r.uniform(-1e12, 1e12) for _ in range(n)], pa.float64()
    elif typ in ("utf8", "dict"):
        p = pool(r, card)
        v, t = [p[r.randrange(card)] for _ in range(n)], pa.utf8()
    elif typ == "date32":
        v, t = [r.randint(-10000, 20000) for _ in range(n)], pa.date32()
    elif typ == "ts_s":
        v, t = [r.randint(-4_000_000_000, 4_000_000_000) for _ in range(n)], \
               pa.timestamp("s")
    elif typ == "ts_ms":
        v, t = [r.randint(-4 * 10**12, 4 * 10**12) for _ in range(n)], \
               pa.timestamp("ms")
    elif typ == "ts_us":
        v, t = [r.randint(-4 * 10**15, 4 * 10**15) for _ in range(n)], \
               pa.timestamp("us")
    elif typ == "ts_ns":
        v, t = [r.randint(-4 * 10**18, 4 * 10**18) for _ in range(n)], \
               pa.timestamp("ns")
    elif typ == "dur_ns":
        v, t = [r.randint(-4 * 10**18, 4 * 10**18) for _ in range(n)], \
               pa.duration("ns")
    elif typ == "time_ms":
        v, t = [r.randrange(86_400_000) for _ in range(n)], pa.time32("ms")
    else:
        raise ValueError(typ)
    arr = pa.array([None if nl else x for x, nl in zip(v, nulls)], type=t)
    return arr.dictionary_encode() if typ == "dict" else arr


def gen_case(i):
    """Deterministic table + batch count for case i."""
    rng = random.Random(BASE_SEED + i)
    n = rng.choices(LENGTHS, weights=LEN_W)[0]
    ncols = rng.randint(1, 4)
    nb = max(1, min(rng.choice(BATCHES), max(n, 1)))
    cols, names = [], []
    for c in range(ncols):
        typ = rng.choice(TYPES)
        nd = rng.choice(NULLD)
        card = min(rng.choice(CARDS), max(n, 1))
        cols.append(gen_column(rng, typ, n, nd, card))
        names.append("c%d_%s" % (c, typ))
    return pa.table(dict(zip(names, cols))), nb


def null_out_sentinels(c, mn, t):
    """Values equal to the L null sentinel become null after a round trip
    (the documented sentinel-collision caveat)."""
    hit = pc.fill_null(pc.equal(c, mn), False)
    return pc.if_else(hit, pa.scalar(None, type=t), c)


def expect_column(c):
    """Map one original column to its expected post-round-trip form."""
    c = c.combine_chunks()
    t = c.type
    if pa.types.is_dictionary(t):
        c, t = c.cast(pa.utf8()), pa.utf8()          # dict decodes to symbols
    if pa.types.is_string(t) or pa.types.is_large_string(t):
        return pc.fill_null(c, "").cast(pa.utf8())   # utf8 null -> empty sym
    if pa.types.is_boolean(t):
        return pc.fill_null(c, False)                # bool null -> 0b
    if pa.types.is_int16(t):
        return null_out_sentinels(c, I16_MIN, t)
    if pa.types.is_int32(t):
        return null_out_sentinels(c, I32_MIN, t)
    if pa.types.is_int64(t):
        return null_out_sentinels(c, I64_MIN, t)
    if pa.types.is_floating(t):
        nan = pc.fill_null(pc.is_nan(c), False)      # NaN and null are one
        return pc.if_else(nan, pa.scalar(None, type=t), c)
    if pa.types.is_timestamp(t):
        return c.cast(pa.timestamp("ns"))            # L timestamps are ns
    if pa.types.is_time32(t) and t.unit == "s":
        return c.cast(pa.time32("ms"))               # L times are ms
    return c                                         # date32/u8/dur/time_ms


def do_gen():
    os.makedirs(DIR, exist_ok=True)
    man = []
    for i in range(N_CASES):
        t, nb = gen_case(i)
        name = "case_%03d" % i
        with ipc.new_file(os.path.join(DIR, name + ".arrow"), t.schema) as w:
            if t.num_rows == 0 or nb == 1:
                w.write_table(t)
            else:
                step = (t.num_rows + nb - 1) // nb
                for o in range(0, t.num_rows, step):
                    w.write_table(t.slice(o, step))
        man.append(name)
    with open(os.path.join(DIR, "manifest.q"), "w") as f:
        f.write("CASES:(" + ";".join('"%s"' % m for m in man) + ");\n")
    print("wrote %d matrix cases -> %s" % (N_CASES, DIR))


def do_check():
    npass = nfail = 0
    for i in range(N_CASES):
        name = "case_%03d" % i
        orig, _ = gen_case(i)
        try:
            got = ipc.open_file(os.path.join(DIR, name + "_out.arrow")) \
                     .read_all().combine_chunks()
            assert got.column_names == orig.column_names, "column names"
            for cn in orig.column_names:
                e = expect_column(orig.column(cn))
                g = got.column(cn).combine_chunks()
                if isinstance(g, pa.ChunkedArray):
                    g = g.chunk(0) if g.num_chunks else pa.array([], e.type)
                assert g.type == e.type, "%s: type %s != %s" % (cn, g.type, e.type)
                assert g.equals(e), "%s: values differ" % cn
            npass += 1
        except Exception as ex:
            nfail += 1
            print("  FAIL %s: %s" % (name, ex))
    print("=== matrix oracle: %d passed, %d failed ===" % (npass, nfail))
    sys.exit(1 if nfail else 0)


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "gen"
    do_gen() if mode == "gen" else do_check()
