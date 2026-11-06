# Experimental C/C++ Span API and ETW benchmark

An opt-in Windows x64 experiment, **not a supported production FFI API**.
This standalone Cargo workspace is outside the repository's normal build.
The architecture remains **C/C++ -> C ABI Span API -> Rust OpenTelemetry SDK
-> ETW exporter**. There is no C++ SDK, direct-export shortcut, or completed-span
export API.

## Interfaces and ownership

`ffi.h` is usable from C and C++. `EtwText` contains UTF-8 bytes and their length,
not a NUL-terminated string; null is valid only for zero length. Attribute kinds
are 1 (string), 2 (int64), and 3 (boolean, represented by 0 or 1).

| API | Ownership and lifecycle |
| --- | --- |
| `etw_span_begin` / `etw_span_end` | Existing API: begin copies inputs and allocates an active handle; end ends/exports, restores context, frees in Rust, and nulls the slot. |
| `etw_span_handle_create` | Allocates inactive, reusable opaque storage once, on its owner thread. |
| `etw_span_begin_reuse` | Initializes inactive storage, copying all strings. Caller buffers may be overwritten or freed after return. |
| `etw_span_begin_borrowed` | Initializes inactive storage without copying string bytes. Keep them alive and unchanged until synchronous end returns. |
| `etw_span_begin_static` | Initializes inactive storage with explicitly borrowed process-lifetime strings; see the stronger contract below. |
| `etw_span_status` | Works on either kind of active handle: 0 = Unset, 1 = Ok, 2 = Error. |
| `etw_span_end_reuse` | Ends through the SDK and ETW processor, restores the previous context, and resets to inactive without freeing storage. |
| `etw_span_handle_destroy` | Destroys inactive reusable storage in Rust and nulls the slot. It never implicitly ends an active span. |

Each concurrently active span needs **its own handle**. All handle operations,
including inactive reuse and destruction, belong to the thread that created the
handle. End nested spans in strict **LIFO order across both APIs**; do not
independently detach/replace the attached SDK context while they are active.
Keep the provider alive until all active spans and calls using it have finished
on every thread. This provider-lifetime obligation is not runtime-enforced.
Inactive handles retain no provider and may outlive it or use a different one.
Before thread exit, end all its spans and destroy its reusable handles.

Results are `ETW_OK` (0), `ETW_BAD_ARGUMENT` (1), `ETW_WRONG_THREAD` (2),
`ETW_PANIC` (3), `ETW_INVALID_STATE` (4), and `ETW_OUT_OF_ORDER` (5).
Begin(active), end/status(inactive), destroy(active), and mixing the consuming
and reusable lifecycle APIs are rejected without changing the handle.
Out-of-order end and sequential wrong-thread calls likewise preserve state.
Failed begin leaves reusable storage inactive; existing begin clears its output.
Create/begin output slots must not already own another live handle.

Fallible lifecycle entrypoints catch Rust unwinding, not aborts or OOM.
Even if export panics, end consumes/resets the span and restores context;
the export may be incomplete. Success means SDK end completed, not that ETW
delivery was guaranteed. Null/UTF-8/lifecycle checks cannot make arbitrary,
dangling, misaligned, or concurrently accessed foreign pointers safe. C/C++
must never `free` or `delete` Rust handles.

### Borrowing local strings until synchronous end returns

Use `etw_span_begin_borrowed` with ordinary local `std::string` or `char[]`
inputs. **All span-name, attribute-key, and string-value bytes must remain alive
and unchanged from begin through the return of `etw_span_end_reuse`.** Do not
modify, move, destroy, or reallocate their storage while the span is active.
String literals and local strings can be mixed under this same contract.

End synchronously exports and resets the handle. After it returns `ETW_OK`, or
`ETW_PANIC` following export cleanup, the bytes may be changed or freed immediately;
the inactive handle can still be reused. A rejected end, such as wrong-thread or
out-of-order end, leaves the span active and **does not release the borrow**.
A failed begin retains no new input bytes; if the handle was already active,
the original span's input lifetime obligation remains.

Only string bytes are borrowed. The attribute descriptor array need only live
during begin, and numeric values are copied. The FFI validates UTF-8 without
copying its contents, then moves its attribute vector directly into the SDK.

This relies on the fixed provider created by `etw_provider_create`: the existing
Rust SDK 0.33.1 uses `AlwaysOn`, and the existing contrib ETW `Processor` exports
synchronously without retaining span data. The SDK removes the span's data on
end; the processor serializes it into the ETW event buffer and drops it before
returning. The wrapper resets the active span and restores context before end
returns. No changes to the SDK, ETW processor, or exporter APIs are required.

Internally the SDK's string types require `&'static str`, so the FFI erases the
shorter foreign lifetime in `Strings::read`. Its safety depends on that fixed,
non-retaining pipeline and cleanup on both success and unwinding. This is not a
contract for arbitrary SDK samplers/processors that cache or queue spans. Changes
to the provider or pinned SDK need to recheck the no-retention invariant.

### Explicit static-string contract

`etw_span_begin_static` retains its original, stronger contract. **Every span-name, attribute-key,
and string-value byte must remain immutable and valid until process exit**,
including after end, handle destruction, and provider shutdown. Keep containing
modules loaded for that entire lifetime. An ordinary temporary `std::string`,
mutable buffer, or literal in an unloadable DLL does not satisfy this contract.
There is no mixed static/dynamic mode: use a copying begin if any string cannot
meet the promise.

The **attribute descriptor array needs to live only during begin** and may be
overwritten/released immediately afterward. Integer/boolean values are copied.
The opt-in conversions in `Strings::read` document both borrowing contracts.
UTF-8 validation uses `str::from_utf8`, without allocating; it cannot establish
lifetime. Neither copying API silently borrows strings.

### C: reusable storage with temporary inputs

This example aborts on instrumentation errors for brevity; applications can
instead handle the result codes. The provider's 0/3 argument selects only the
benchmark's native workload, not a restriction on ABI begin's attribute count.

```c
#include "ffi.h"
#include <stdlib.h>
#include <string.h>

static void check(uint32_t code) { if (code != ETW_OK) abort(); }

void example(void)
{
    EtwProvider *provider = NULL;
    EtwSpan *span = NULL;
    check(etw_provider_create(0, &provider));
    check(etw_span_handle_create(&span)); /* once, outside the loop */
    for (unsigned i = 0; i < 100; ++i) {
        char name[] = "temporary operation";
        EtwText text = {name, sizeof(name) - 1};
        check(etw_span_begin_reuse(provider, text, NULL, 0, span));
        memset(name, 'X', sizeof(name) - 1); /* safe: Rust owns a copy */
        uint32_t status = etw_span_status(span, 1);
        uint32_t end = etw_span_end_reuse(span); /* always end after begin */
        check(end);
        check(status);
    }
    check(etw_span_handle_destroy(&span));
    check(etw_provider_destroy(&provider));
}
```

For the original API, omit handle creation/destruction and call
`etw_span_begin(..., &span)` / `etw_span_end(&span)` each cycle. Its dynamic-input
ownership guarantee is unchanged.

### C++: reusable storage borrowing local std::string inputs

The provider must remain alive for this function. This example aborts on errors
for brevity. String construction is application work; passing `data()`/`size()`
and calling the borrowed FFI do not copy the string contents.

```cpp
#include "ffi.h"
#include <cstdlib>
#include <string>

static EtwText text(const std::string &value) { return {value.data(), value.size()}; }
static void check(uint32_t code) { if (code != ETW_OK) std::abort(); }

void example(EtwProvider *provider)
{
    EtwSpan *span = nullptr;
    check(etw_span_handle_create(&span));
    for (unsigned i = 0; i < 100; ++i) {
        std::string name = "operation-" + std::to_string(i);
        std::string key = "request.path";
        std::string value = "/items/" + std::to_string(i);
        {
            const EtwAttribute attributes[] = {
                {text(key), 1, text(value), 0},
            };
            check(etw_span_begin_borrowed(provider, text(name), attributes, 1, span));
        } // Descriptors may disappear; name/key/value must remain unchanged.
        const auto status = etw_span_status(span, 1);
        const auto end = etw_span_end_reuse(span); // synchronous export + cleanup
        check(end);
        check(status);
        name.clear();  // Safe now: no input string references remain in Rust.
        key.clear();
        value.clear();
    } // Local strings may be destroyed before the reusable handle.
    check(etw_span_handle_destroy(&span));
}
```

### C++: reusable storage with immutable process-lifetime strings

Here all strings are literals in the executable. If moved to a DLL/plugin,
that module must remain loaded until process exit. The local descriptor array
is deliberately not static.

```cpp
#include "ffi.h"
#include <cstdlib>

template <size_t N>
constexpr EtwText text(const char (&value)[N]) { return {value, N - 1}; }
static void check(uint32_t code) { if (code != ETW_OK) std::abort(); }

void example(EtwProvider *provider) // caller keeps provider alive
{
    EtwSpan *span = nullptr;
    check(etw_span_handle_create(&span));
    for (unsigned i = 0; i < 100; ++i) {
        {
            const EtwAttribute attributes[] = {
                {text("string_key"), 1, text("benchmark-value"), 0},
                {text("integer_key"), 2, text(""), 42},
                {text("bool_key"), 3, text(""), 1},
            };
            check(etw_span_begin_static(provider, text("BenchmarkOperation"),
                                        attributes, 3, span));
        } // descriptors may disappear; their referenced bytes must not
        const auto status = etw_span_status(span, 1);
        const auto end = etw_span_end_reuse(span);
        check(end);
        check(status);
    }
    check(etw_span_handle_destroy(&span));
}
```

## What still allocates or copies

Reusable storage removes the per-span wrapper `Box` allocation; there is still
one allocation when creating each handle. Copying begins still allocate/copy
the nonempty span name, every key, and every string value. The borrowed and static
begins reference those bytes instead. Neither retains the descriptor array.

All paths still run the native Rust SDK. With attributes, the FFI builds **one
`Vec<KeyValue>` and moves its buffer directly into `SpanBuilder.attributes`**.
SDK 0.33.1 takes that buffer into its span data; there is no second FFI attribute
vector or deep clone. This is SDK-owned attribute storage, not an additional
conversion buffer. Zero attributes need no vector allocation. SDK span/context,
thread-local initialization, and exporter serialization allocations/copies remain.
The complete tracing pipeline is **not allocation-free or zero-copy**.

This checkout also ports the production optimizations applicable to its existing
nested ETW layout from upstream [PR #875](https://github.com/open-telemetry/opentelemetry-rust-contrib/pull/875)
(inspected head `c2c370df62d9a875769e030048600e753b28cd5b`):
stack-backed RFC3339 timestamps and hexadecimal IDs, borrowed error descriptions,
and one attribute classification pass shared by Parts B/C. Field order,
first-duplicate handling, fractional seconds, extended years, and leap seconds
are preserved. Flat Geneva mapping and the PR's separate stage-benchmark APIs
are **not** included. These optimizations also reduce the native exporter
baseline; compare all FFI variants against the native result from this checkout.

The target for reusable/static is to remove the copying wrapper's incremental
**2 allocations/span with zero attributes, 6 with these three attributes**,
relative to native SDK tracing. Reusable/copying removes just the handle
allocation. The elevated measurements below meet these targets for this
workload; they are not allocation guarantees for other workloads or SDK versions.
Do not substitute historical benchmark counts or timings for a run of this checkout.

## Benchmark paths

| Variant | Caller and timed complete-span work |
| --- | --- |
| `native` | Rust SDK begin/attach, status, end/detach/drop. |
| `wrapper` | Rust calls the existing copying/allocating C ABI wrapper. |
| `cpp` | C++ calls that same existing wrapper in the Rust DLL. |
| `rust-reuse` | Rust calls reusable begin with copying inputs, status, reusable end. |
| `rust-static` | Rust calls reusable static begin, status, reusable end. |
| `cpp-reuse` | C++ calls the same reusable copying API. |
| `cpp-static` | C++ calls the same reusable static API. |

Every case runs with zero and three attributes (string, integer, boolean), the
same DLL, always-on sampler, empty resource, name, Server kind, and Ok status.
The Rust loops enter the DLL once per batch; C++ makes three DLL calls/span.
No synthetic application work is added. Reusable handles are created on the
measuring thread before warm-up/timing and destroyed after measurement.
They are reused across batches, not recreated inside `etw_rust_batch_reuse`.

The seven timed variants and recorded results below predate the borrowed-until-end
entrypoint. The C/C++ contract checks now also cover temporary strings with
`etw_span_begin_borrowed`; no new latency/allocation measurements are claimed for it.

Handle creation/destruction have separate `handle_create_ns`/`handle_destroy_ns`
latency observations (one each per process, including timer noise; non-reusable
cases measure the no-op control). The memory probe separately reports
`handle_create`/`handle_destroy` allocation samples, `idle_with_handle_bytes`,
and retained bytes after destroying the handle. Steady-state allocation samples
exclude setup/destruction, although live/peak bytes include the resident handle.

Full latency runs use fourteen fresh processes/case: seven cyclic variant orders
and their reversals, balancing every position twice. Each process has seven
batches of 1,000 untimed warm-ups and 10,000 timed complete spans. Reported
values are pooled medians of batch averages, **not per-call percentiles**.
Smoke uses one process/case and is not a statistically balanced performance study.
Full memory runs use three processes/case, with one cold and 1,000 steady spans
on a fresh worker thread. Both CRT and Rust-DLL allocator self-tests must pass.

The native heap probe includes Rust DLL and EXE/CRT imports; it is absent in
latency mode. Bytes mean requested heap bytes, not RSS, allocator overhead,
all DLL/kernel allocations, or total stack use. Reports compare incremental
allocations to the contemporaneous native baseline, not to old results.

## Build and checks

Prerequisites: Windows x64, repository-pinned Rust with Clippy/rustfmt, Git,
MSVC with the Windows SDK, and CMake/CTest supporting your Visual Studio version.
In an x64 Visual Studio developer PowerShell with those tools on `PATH`, from
the repository root:

```powershell
Set-Location .\opentelemetry-etw-traces\benches\ffi
.\build.ps1
```

The script checks formatting, all-target Clippy, Rust SDK contract tests, the
release DLL, and C/C++ builds with `/O2`, `/MD`, `/W4 /WX`. CTest runs C and C++
ABI contracts **without requiring an ETW session or elevation**. Standalone:

```powershell
cargo fmt --manifest-path Cargo.toml -- --check
cargo clippy --locked --all-targets --manifest-path Cargo.toml -- -Dwarnings
cargo test --locked --manifest-path Cargo.toml
ctest --test-dir build -C Release --output-on-failure
```

For the optimized exporter itself, from the repository root:

```powershell
cargo fmt -p opentelemetry-etw-traces -- --check
cargo clippy -p opentelemetry-etw-traces --all-targets --all-features -- -Dwarnings
cargo test -p opentelemetry-etw-traces --all-features
# Elevated PowerShell: SDK -> exporter -> live ETW payload regression tests
cargo test -p opentelemetry-etw-traces --all-features -- --ignored --test-threads=1
```

The tests cover handle reuse, invalid lifecycles, dynamic buffers overwritten
and freed before end, static byte borrowing with temporary descriptors, borrowed
temporary strings released after end (including export panic), nested
parent/context restoration, provider changes while inactive, wrong-thread
rejection, export-panic cleanup, and equivalent SDK-exported content for the
four Rust variants. Borrowing tests compare original input addresses with the
addresses observed during synchronous export. Their observer retains only owned
snapshots, never the borrowed `SpanData`. No test intentionally supplies dangling input.
C/C++ contracts exercise the real DLL; ETL validation below checks exported
content for **all seven variants**, including both C++ reusable paths.

CMake downloads nlohmann/json 3.11.3 with a pinned archive SHA-256, only for the
benchmark. Cargo dependencies are locked in this directory's `Cargo.lock`.

### ETW measurements

From an **elevated** PowerShell in the benchmark directory, after building:

```powershell
.\run-all.ps1 -Smoke  # contract capture + 14 memory/14 latency cases; summary-smoke.json
.\run-all.ps1         # balanced latency + full memory comparison; summary.json
```

The controller starts uniquely named file-mode ETW sessions, enables only the
child PID, waits for enablement, and pins it to affinity mask 1. Capture uses
64 KiB buffers, minimum 2048 / maximum 4096 buffers, performance-counter clock,
and Verbose/all keywords. It stops only its sessions in `finally`, checks loss
counters, and decodes every matching event after timing. The decoder validates
schema, name, kind, status, IDs, attributes, and absence of an unexpected parent.
IDs/timestamps naturally differ: equivalence is semantic, not byte-for-byte.

Raw ETLs, loss counters, source/binary hashes, and environment inventories live
under ignored `results`; summaries live under ignored `build`. Rebuild after
source changes because run scripts reject stale manifests. Allow several GB
for a full fourteen-pass capture. These results are not a comparison to a C++
exporter, Linux user-events, or asynchronous/many-thread application workloads.

### Recorded validation before the borrowed-until-end addition

The following Windows results were recorded before `etw_span_begin_borrowed` was
added. New contract checks require a fresh build/run; the current C++ capture
expects 18 contract events across the zero/three-attribute cases (previously 14).

On the original Windows checkout, Rust 1.99.0 formatting/Clippy, nine Rust ABI contracts,
the optimized exporter's 24 unit tests and two doc tests, the release DLL,
MSVC 19.51.36260 C/C++ builds, and both CTest contracts passed. Both live ETW
exporter integration tests also passed, including SDK-driven checks of timestamps,
IDs, duplicates, borrowed/owned Unicode error descriptions, and reused exporter
buffers. PowerShell syntax, balanced ordering, and smoke/full report generation
were also checked.

The initial non-elevated attempt failed with `logman` access denied. A separate
Windows UAC-elevated PowerShell successfully ran both smoke and full comparisons
on 2026-11-05. The full run validated **15,134,084 latency/memory events** plus
14 C++ contract events, with zero reported event/buffer loss. The preceding smoke
run validated another 1,092,028 latency/memory events and 14 contract events.
These measure the **new ABI and ported nested exporter optimizations**, not the
branch's historical results.

Steady requested-heap allocation counts per span (Rust and C++ caller results
agree):

| Attributes | Native SDK + optimized ETW | Existing copying wrapper | Reusable/copying | Reusable/static |
| --- | --- | --- | --- | --- |
| 0 | 2 | 4 | 3 | 2 |
| 3 | 3 | 9 | 8 | 3 |

Reusable/static therefore adds **zero allocations above the native baseline**
for both workloads; it removes the wrapper's incremental 2/6 allocations.
Remaining requested bytes/span are 384/552, matching native. Each reusable
handle's separate setup allocates one 56-byte block. The worker retains 512
requested bytes after handle destruction, all freed at thread exit; while
inactive with a handle, it retains 568 bytes. EventBuilder buffer growth can
still reallocate (none occurred in the full run's steady samples), and the full
pipeline still copies bytes into ETW payloads.

Full-run latency, microseconds per complete span, pooled median of 98 batch
averages per case (14 processes x 7 batches):

| Variant | 0 attributes | 3 attributes |
| --- | --- | --- |
| `native` | 2.725 | 2.895 |
| `wrapper` | 2.853 | 3.318 |
| `cpp` | 2.888 | 3.355 |
| `rust-reuse` | 2.827 | 3.273 |
| `rust-static` | 2.789 | 3.072 |
| `cpp-reuse` | 2.864 | 3.320 |
| `cpp-static` | 2.751 | 3.108 |

Separate handle setup/destruction costs, microseconds, medians of one cold
observation per process; these include timer/cold-code noise and are **not**
included in the per-span times above:

| Variant | Create, 0 / 3 attributes | Destroy, 0 / 3 attributes |
| --- | --- | --- |
| `rust-reuse` | 6.50 / 8.25 | 0.30 / 0.30 |
| `rust-static` | 6.75 / 7.50 | 0.30 / 0.30 |
| `cpp-reuse` | 7.00 / 7.00 | 0.30 / 0.30 |
| `cpp-static` | 7.35 / 7.60 | 0.30 / 0.40 |

The C++ static path was 0.96% / 7.35% above native latency (0/3 attributes),
despite matching allocation counts. Checks, UTF-8 scanning, conversions and
ABI calls still cost time. These are machine-specific observations, not an
equivalence proof or latency guarantee. In particular, the Rust copying-wrapper
3-attribute case had one process median of 6.719 us; other process medians ranged
from 3.245 us upward. The raw distributions and paired differences are retained,
not silently discarded.

Provenance: dirty checkout based on `8cc75072`, including this ABI work and the
PR #875 nested-path port. OpenTelemetry API/SDK 0.33.1, Chrono 0.4.45 and
TraceLogging Dynamic 1.2.4; Rust 1.99.0, MSVC 19.51.36260, release `/O2 /MD`,
default exporter features (no `serde_json`). Per-run inventories recorded a
Microsoft VM, AMD EPYC 7763, 8 guest-visible cores / 16 logical processors,
63.95 GiB RAM, Windows 11 Enterprise 10.0.26300. The CPU model's "64-Core" name
is not the guest allocation. Capture and affinity settings are described above.

`build\summary.json` contains the full report and references
`results\20261105-132155-958-latency` and `results\20261105-133224-895-memory`,
including environment inventories and source/binary hashes. The earlier
`build\summary-smoke.json` is only a one-process/case smoke report. Result files
remain local and ignored; the tables here persist the measured summary.

For the local tools installed/discovered during that validation, these exact
commands reproduce the build; use an elevated shell for the final two commands:

```powershell
$env:CARGO_HOME = 'C:\Users\labhas\work\.tools\cargo'
$env:RUSTUP_HOME = 'C:\Users\labhas\work\.tools\rustup'
$env:PATH = "$env:CARGO_HOME\bin;C:\Program Files\Microsoft Visual Studio\18\Enterprise\Common7\IDE\CommonExtensions\Microsoft\CMake\CMake\bin;C:\Program Files (x86)\Microsoft Visual Studio\Installer;$env:PATH"
Set-Location 'C:\Users\labhas\work\opentelemetry-rust-contrib\opentelemetry-etw-traces\benches\ffi'
.\build.ps1
.\run-all.ps1 -Smoke
.\run-all.ps1
```

Alternatively, after building, request UAC elevation for just the controller
(its progress and results are written to `build\controller.log` and `results`):

```powershell
$controller = Join-Path (Get-Location) 'run-all.ps1'
Start-Process pwsh -Verb RunAs -WindowStyle Hidden -Wait `
    -ArgumentList @('-NoProfile', '-File', "`"$controller`"", '-Smoke')
```
