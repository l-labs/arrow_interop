# arrow_interop — Apache Arrow IPC reader / writer / streamer for L.
#
# Builds arrow_io.so, a shared library loaded into the `l` binary via `2:`.
# Exports: arrow_read, arrow_write, arrow_stream.
#
# Fully self-contained / offline:
#   * include/l.h            — vendored L C API header (the ABI contract)
#   * flatcc/                — vendored flatcc runtime headers (Apache-2.0)
#   * flatcc/src/*.c         — vendored flatcc runtime sources (v0.6.1)
#   * generated/             — flatcc-generated Arrow Flatbuffer code
#
# Build:
#   cmake -B build && cmake --build build
#   # → build/arrow_io.so
#
# To build against an L source tree instead of the vendored header:
#   cmake -B build -DL_ROOT=/path/to/l   (expects ${L_ROOT}/include/l.h)
cmake_minimum_required(VERSION 3.25...4.0)
project(arrow_io LANGUAGES C)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

# ── L header: vendored copy by default, -DL_ROOT overrides ────────────────
if(NOT DEFINED L_ROOT)
    set(L_ROOT "${CMAKE_CURRENT_SOURCE_DIR}")
endif()
if(NOT EXISTS "${L_ROOT}/include/l.h")
    message(FATAL_ERROR
        "arrow_interop: ${L_ROOT}/include/l.h not found. "
        "Pass -DL_ROOT=/path/to/l or restore the vendored include/l.h.")
endif()

# ── flatcc runtime: compiled from the vendored copy (no fetch, no syslib) ──
set(FLATCC_RT_SRC
    flatcc/src/builder.c
    flatcc/src/emitter.c
    flatcc/src/refmap.c)

# ── Target ──────────────────────────────────────────────────────────────────
add_library(arrow_io SHARED
    arrow_io.c
    ${FLATCC_RT_SRC})

target_include_directories(arrow_io PRIVATE
    "${L_ROOT}/include"
    "${CMAKE_CURRENT_SOURCE_DIR}")

target_compile_options(arrow_io PRIVATE
    -O3 -fPIC
    $<$<NOT:$<PLATFORM_ID:Windows>>:-march=native>)

target_link_libraries(arrow_io PRIVATE m)

set_target_properties(arrow_io PROPERTIES
    OUTPUT_NAME "arrow_io"
    SUFFIX ".so"
    PREFIX ""
    LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}"
    C_STANDARD 99
    C_EXTENSIONS ON)

if(APPLE)
    # L symbols (ktn, sn, krr, …) resolve from the host process at load time.
    target_link_options(arrow_io PRIVATE -undefined dynamic_lookup)
endif()

message(STATUS "arrow_interop: arrow_io.so → ${CMAKE_BINARY_DIR}/")
