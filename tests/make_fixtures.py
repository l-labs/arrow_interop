# make_fixtures.py — generate pyarrow-written Arrow IPC fixtures for tests 11-12.
# Run: uv run --with pyarrow tests/make_fixtures.py
import pyarrow as pa
import pyarrow.ipc as ipc


def write(path, table):
    with ipc.new_file(path, table.schema) as w:
        w.write_table(table)
    print("wrote", path)


# plain table: int64, float64, utf8
write("/tmp/py_written.arrow", pa.table({
    "a": pa.array([1, 2, 3, None, 5], type=pa.int64()),
    "b": pa.array([1.5, 2.5, None, 4.5, 5.5], type=pa.float64()),
    "s": pa.array(["x", "y", "z", "x", "y"], type=pa.utf8()),
}))

# dictionary-encoded string column
write("/tmp/small_dict.arrow", pa.table({
    "sym": pa.array(["AA", "BB", "AA", "CC", "BB"]).dictionary_encode(),
    "v": pa.array([1, 2, 3, 4, 5], type=pa.int32()),
}))
