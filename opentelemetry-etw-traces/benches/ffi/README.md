# C++ / Rust ETW FFI benchmark

An opt-in Windows x64 experiment comparing a native Rust span lifecycle with a
C++ application calling a Rust DLL through a C ABI. The SDK and ETW exporter
remain in Rust; no C++ OpenTelemetry SDK is involved.

This standalone Cargo workspace is not part of the repository's normal build.
It is a benchmark prototype, **not a supported production FFI API**.

## Paths compared

| Variant | Timed per-span work |
| --- | --- |
| `native` | Rust creates a Server span, attaches its context, sets `Status::Ok`, ends it, detaches, and drops it. |
| `wrapper` | A Rust loop calls the same three C ABI wrapper functions used by C++, including conversions, ownership, checks, and cleanup. |
| `cpp` | A C++ loop calls the Rust DLL three times per span: begin/attach, set status, end/release. |

All variants use the **same DLL**, always-on sampling, empty resource, span name,
and zero or three attributes (string, integer, boolean). Native Rust enters the
DLL once per timed batch of 10,000 spans, not once per span. Inputs and provider
setup are outside timing; per-span ownership/copying and cleanup are included.
There is no synthetic application work.

Native Rust borrows static input strings. The FFI wrapper copies foreign strings
before returning and boxes an active-span handle. Attributes are passed together
to `begin`, not through separate calls. The wrapper control distinguishes those
costs from C++ caller/DLL-call overhead.

The ABI uses opaque handles and C-layout values (`ffi.h`, `src/lib.rs`). Rust
must free its own allocations. End consumes and clears the span handle. Active
spans must end on their creating thread; providers must outlive their spans.
Null/UTF-8/status/thread checks do not make arbitrary, dangling, or concurrently
misused foreign pointers safe. Fallible instrumentation entrypoints catch Rust
unwinding, not aborts or out-of-memory termination.

## Build and run

Prerequisites: Windows x64, Rust with this repository's pinned toolchain and
Clippy/rustfmt, Git, MSVC with the Windows SDK, and CMake supporting the installed
Visual Studio version. Start in an x64 Visual Studio developer PowerShell with
`cargo`, `cmake`, and `git` on `PATH`.

```powershell
cd .\opentelemetry-etw-traces\benches\ffi
.\build.ps1
```

The build checks Rust formatting/Clippy, builds the release DLL, and builds the
C++ driver and ETL validator with C++17, `/O2`, and `/MD`. CMake downloads
nlohmann/json 3.11.3 with a pinned archive SHA-256; it is only a benchmark
dependency. Cargo dependencies are locked in this directory's `Cargo.lock`.

Then, from an **elevated** PowerShell in this directory:

```powershell
# Contract checks, one memory process/case, one latency process/case:
.\run-all.ps1 -Smoke

# Full balanced comparison and separate memory run; generates build\summary.json:
.\run-all.ps1
```

The controller starts uniquely named file-mode ETW sessions, enables only the
child PID, waits for enablement, and pins the child to affinity mask 1. Capture uses
64 KiB buffers, minimum 2048 / maximum 4096 buffers, the performance-counter clock,
and Verbose/all keywords. It stops its sessions in `finally`, checks loss counters,
and decodes events after timing. It does not install a monitoring agent.

Full latency measurement uses six fresh processes per variant/attribute case,
all six variant-order permutations, and seven batches/process. Each batch has
1,000 untimed warm-ups followed by 10,000 timed complete spans. Reported values
are pooled medians of batch averages, not per-call percentiles.

Memory measurements use three processes/case, with one cold and 1,000 steady
operations on a fresh worker. A process-local native heap probe includes the
Rust DLL's imports as well as the EXE/CRT; it is **not installed in latency mode**.
Both CRT and Rust-DLL allocator self-tests must pass. Reported bytes are requested
heap bytes, not RSS, allocator overhead, all DLL/kernel allocations, or stack use.

Run outputs, raw ETLs, manifests, environment inventories collected before/after
each mode, and loss counters are under `results`. Allow disk space for several
million captured events. Generated files are ignored by Git. Source and binary
hashes are checked against the build manifests; rebuild after changes.

Contract checks cover input ownership after the caller overwrites buffers, active
context cleanup, invalid arguments, repeated end, and wrong-thread rejection.
The decoder validates every matching span's schema, name, kind, status, IDs,
attributes, and absence of an unexpected parent. Live IDs/timestamps differ:
this is semantic parity, not a byte-for-byte comparison.

## Preliminary standalone measurements

The source header in `src/lib.rs` contains the results. They were measured on
2026-11-03 **before repository packaging**, against unchanged exporter revision
`2a34861319802fc33865f1825385f698a50f3f17`, OTel API/SDK 0.33.1,
tracelogging_dynamic 1.2.4, rustc 1.99.0, and MSVC 19.51.36260. Do not relabel them
as measurements of a later checkout; rerun the controller for that checkout.

Per-run inventories recorded AMD EPYC 7763, Microsoft Virtual Machine,
8 guest-visible cores / 16 logical processors, 63.95 GiB RAM, and Windows 11
Enterprise x64 10.0.26300. The CPU model's "64-Core" name is not the VM allocation.
The workload and capture settings were as described above.

| Attributes | Native Rust | Rust via wrapper | C++ via DLL | C++ vs native |
| --- | --- | --- | --- | --- |
| 0 | 2.932 us | 3.093 us | 3.067 us | +4.6% |
| 3 | 3.200 us | 3.621 us | 3.641 us | +13.8% |

Steady heap allocations were 6/7 for native Rust and 8/13 for either wrapper
caller (0/3 attributes), plus occasional reallocations. All retained 512 requested
bytes per worker, freed on thread exit. All 2,790,036 latency/memory events decoded
with zero event or buffer loss.

The C++ versus Rust-wrapper pooled difference was -0.84% / +0.56%; paired process
differences varied in both directions. The evidence points to wrapper ownership,
conversion, and bookkeeping costs rather than a large additional DLL-call cost.
It does not establish statistical equivalence or a universal FFI overhead bound.
Results are machine-dependent and not a controlled comparison with Linux
user-events, the C++ exporter, or async/many-thread application workloads.
