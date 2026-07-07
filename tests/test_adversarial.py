# test_adversarial.py — corrupted-file harness.  Every case runs the L binary
# in its OWN subprocess so a SIGSEGV is a detected FAILURE, not a wedged run.
# A case passes iff the process exits 0 AND prints either a clean q error
# ("ERR: ...") or, where valid data is expected, "OK ...".
#
# Run from the repo root:
#   L_BIN=/path/to/l uv run --with pyarrow tests/test_adversarial.py
import os, subprocess, sys, tempfile
import pyarrow as pa
import pyarrow.ipc as ipc

L_BIN = os.environ.get("L_BIN")
if not L_BIN:
    sys.exit("set L_BIN to the l binary path")
DIR = "/tmp/arrow_adv"
os.makedirs(DIR, exist_ok=True)

# ── base file: multi-batch, dict + utf8 + numeric, so every message kind
# (schema / dictionary batch / record batches / footer) is in play ─────────
base = os.path.join(DIR, "base.arrow")
t = pa.table({
    "a": pa.array(range(2000), type=pa.int64()),
    "s": pa.array(["s%d" % (i % 50) for i in range(2000)]),
    "d": pa.array(["d%d" % (i % 5) for i in range(2000)]).dictionary_encode(),
})
with ipc.new_file(base, t.schema) as w:
    for o in range(0, 2000, 700):
        w.write_table(t.slice(o, 700))
BASE = open(base, "rb").read()
FSZ = len(BASE)

PROBE = """ar:hsym[`$"build/arrow_io"] 2: (`arrow_read; 1)
as:hsym[`$"build/arrow_io"] 2: (`arrow_stream; 1)
r:@[%(fn)s;%(arg)s;{"ERR: ",x}]
show $[10=type r;r;"OK rows=",string count r]
\\\\
"""


def run_case(name, path, expect, fn="ar", arg=None):
    """expect: 'err' (clean error required) or 'ok' (data required)."""
    if arg is None:
        arg = 'hsym `$"%s"' % path
    qf = os.path.join(DIR, "probe.q")
    with open(qf, "w") as f:
        f.write(PROBE % {"fn": fn, "arg": arg})
    try:
        r = subprocess.run([L_BIN, qf], stdin=subprocess.DEVNULL,
                           capture_output=True, text=True, timeout=60)
    except subprocess.TimeoutExpired:
        return name, False, "TIMEOUT (wedged)"
    out = r.stdout + r.stderr
    if r.returncode != 0:
        return name, False, "CRASH rc=%d" % r.returncode
    if expect == "err" and "ERR: " not in out:
        return name, False, "no clean error (garbage accepted?): %r" % out[:80]
    if expect == "ok" and "OK rows=" not in out:
        return name, False, "expected data, got: %r" % out[:80]
    return name, True, ""


def variant(name, data):
    path = os.path.join(DIR, name + ".arrow")
    open(path, "wb").write(data)
    return path


cases = []

# control: untouched base file must read fine
cases.append(("control_valid", variant("control", BASE), "ok"))

# ── systematic truncations: header / schema / dict / batches / footer ─────
fracs = [0.001, 0.01, 0.03, 0.08, 0.15, 0.25, 0.40, 0.55, 0.70, 0.85, 0.95,
         0.99]
cuts = sorted({0, 1, 7, 8, FSZ - 1, FSZ - 7, FSZ - 10, FSZ - 17} |
              {int(FSZ * f) for f in fracs})
for c in cuts:
    cases.append(("trunc_%d" % c, variant("trunc_%d" % c, BASE[:c]), "err"))

# ── magic corruption ───────────────────────────────────────────────────────
for nm, off in [("magic_head", 0), ("magic_head3", 3), ("magic_tail", FSZ - 1),
                ("magic_tail6", FSZ - 6)]:
    d = bytearray(BASE); d[off] ^= 0xFF
    cases.append((nm, variant(nm, bytes(d)), "err"))

# ── footer-length corruption ───────────────────────────────────────────────
for nm, val in [("foot_zero", 0), ("foot_neg", -1), ("foot_huge", 0x7FFFFFF0),
                ("foot_fsz", FSZ), ("foot_fsz17", FSZ - 17)]:
    d = bytearray(BASE)
    d[FSZ - 10:FSZ - 6] = (val & 0xFFFFFFFF).to_bytes(4, "little")
    cases.append((nm, variant(nm, bytes(d)), "err"))

# ── degenerate files ───────────────────────────────────────────────────────
cases.append(("empty_file", variant("empty", b""), "err"))
cases.append(("magic_only", variant("magiconly", b"ARROW1"), "err"))
cases.append(("all_zero", variant("zeros", b"\0" * 4096), "err"))
cases.append(("double_magic", variant("dblmagic",
              b"ARROW1\0\0" + b"\0" * 64 + b"ARROW1"), "err"))

# ── compressed IPC bodies: must be a clean error, NEVER garbage data ──────
for codec in ("zstd", "lz4"):
    path = os.path.join(DIR, "comp_%s.arrow" % codec)
    with ipc.new_file(path, t.schema,
                      options=ipc.IpcWriteOptions(compression=codec)) as w:
        w.write_table(t)
    cases.append(("compressed_" + codec, path, "err"))

# ── arrow_stream on hostile inputs (subprocess: crash = fail) ─────────────
sdst = os.path.join(DIR, "adv_splay")
for nm, src in [("stream_trunc", os.path.join(DIR, "trunc_%d.arrow" % cuts[8])),
                ("stream_badfoot", os.path.join(DIR, "foot_huge.arrow")),
                ("stream_zstd", os.path.join(DIR, "comp_zstd.arrow"))]:
    cases.append((nm, src, "err", "as",
                  '(hsym `$"%s";hsym `$"%s")' % (src, sdst)))
cases.append(("stream_valid", base, "ok", "as",
              '(hsym `$"%s";hsym `$"%s")' % (base, sdst)))

npass = nfail = 0
for case in cases:
    nm, path, expect = case[0], case[1], case[2]
    fn = case[3] if len(case) > 3 else "ar"
    arg = case[4] if len(case) > 4 else None
    name, ok, why = run_case(nm, path, expect, fn, arg)
    if ok:
        npass += 1
    else:
        nfail += 1
        print("  FAIL %s: %s" % (name, why))
print("=== adversarial: %d passed, %d failed ===" % (npass, nfail))
sys.exit(1 if nfail else 0)
