#pragma once
#include <stddef.h>
#include <stdint.h>

/* EXPERIMENTAL benchmark ABI, not a supported production API.
 * C/C++ -> C ABI -> Rust OpenTelemetry SDK -> ETW exporter.
 * All opaque storage is allocated/freed by Rust; never free/delete it in C/C++.
 * Pointers must be valid/aligned, slots writable, and handle access serialized.
 * Checks do not make dangling pointers or concurrent misuse safe.
 */
typedef struct EtwProvider EtwProvider;
typedef struct EtwSpan EtwSpan;
/* UTF-8 byte count (no terminator required); NULL is allowed only at length 0. */
typedef struct EtwText { const char *data; size_t length; } EtwText;
typedef struct EtwAttribute
{
  EtwText key;
  uint32_t kind; /* 1 = string, 2 = int64, 3 = bool (integer_value must be 0 or 1). */
  EtwText string_value;
  int64_t integer_value;
} EtwAttribute;
enum EtwResult
{
  ETW_OK = 0, ETW_BAD_ARGUMENT = 1, ETW_WRONG_THREAD = 2, ETW_PANIC = 3,
  ETW_INVALID_STATE = 4, ETW_OUT_OF_ORDER = 5
};

#ifdef __cplusplus
extern "C"
{
#endif
/* attribute_count is the benchmark native-input count (0 or 3).
 * Create output slots must not already own live handles.
 * Providers must outlive ALL active spans and calls using them, on all threads.
 * This is a caller obligation, not a runtime reference-count check.
 */
__declspec(dllimport) uint32_t etw_provider_create(size_t, EtwProvider **);
__declspec(dllimport) uint32_t etw_provider_destroy(EtwProvider **);
__declspec(dllimport) uint32_t etw_provider_enabled(const EtwProvider *);
/* Writes 36 bytes, without a terminator. */
__declspec(dllimport) void etw_provider_guid(char *);

/* Existing copying API: output is a new active handle, attached to this thread.
 * All input strings and descriptors may be overwritten/freed after begin returns.
 * End consumes/frees the handle in Rust and clears the caller's slot.
 */
__declspec(dllimport) uint32_t etw_span_begin(
    const EtwProvider *, EtwText, const EtwAttribute *, size_t, EtwSpan **);
/* Both kinds of active handle: status 0 = Unset, 1 = Ok, 2 = Error. */
__declspec(dllimport) uint32_t etw_span_status(EtwSpan *, uint32_t);
__declspec(dllimport) uint32_t etw_span_end(EtwSpan **);

/* Reusable API: create once, begin/end repeatedly, destroy once while inactive.
 * Each concurrently active span needs its OWN handle; begin(active), end(inactive),
 * status(inactive), destroy(active), and mixing consuming/reusable APIs return
 * ETW_INVALID_STATE without changing the handle.
 * All operations, including inactive use and destruction, must run on the creating
 * thread. Sequential wrong-thread calls return ETW_WRONG_THREAD.
 *
 * Begin attaches the SDK context. End synchronously ends/exports through the SDK,
 * restores the previous context, and resets the handle WITHOUT freeing storage.
 * End in strict LIFO order across BOTH APIs (otherwise ETW_OUT_OF_ORDER); do not
 * independently detach/replace the attached context while a span is active.
 * Inactive handles do not retain a provider and may outlive it or use another one.
 * End all spans and destroy their handles before the owner thread exits.
 *
 * C/C++ sequence (check every result; full examples in README.md):
 *   EtwSpan *span = NULL;
 *   EtwText name = {"operation", 9};
 *   etw_span_handle_create(&span);
 *   etw_span_begin_reuse(provider, name, NULL, 0, span);
 *   etw_span_status(span, 1);
 *   etw_span_end_reuse(span);  // repeat begin/end, using the same storage
 *   etw_span_handle_destroy(&span);
 * Use begin_borrowed or begin_static to avoid string copies, with the lifetimes below.
 */
__declspec(dllimport) uint32_t etw_span_handle_create(EtwSpan **);
__declspec(dllimport) uint32_t etw_span_begin_reuse(
    const EtwProvider *, EtwText, const EtwAttribute *, size_t, EtwSpan *);
/* BORROW UNTIL SYNCHRONOUS END RETURNS:
 * ALL span-name, key and string-value bytes must remain readable and immutable
 * from begin until etw_span_end_reuse returns ETW_OK or ETW_PANIC after resetting
 * the span. Local std::string/char[] inputs are allowed: keep their owners alive
 * and do not modify, move, or reallocate their storage while the span is active.
 * The descriptor array is needed only during begin. A failed begin retains no
 * NEW input bytes. Rejected end calls (e.g. wrong thread/order) leave the span
 * active, so its bytes must still remain valid. Check the end result before reuse.
 *
 * This contract relies on the fixed synchronous, non-retaining ETW pipeline
 * created by etw_provider_create. No borrowed string survives completed end;
 * strings may then be changed/freed while the inactive handle remains reusable.
 * UTF-8 checks do not verify lifetime. No string contents are copied by the FFI.
 */
__declspec(dllimport) uint32_t etw_span_begin_borrowed(
    const EtwProvider *, EtwText, const EtwAttribute *, size_t, EtwSpan *);
/* EXPLICIT STATIC-LIFETIME OPT-IN:
 * ALL span-name, key and string-value bytes must remain immutable and valid until
 * PROCESS EXIT, even after end/destroy/provider shutdown. Keep their containing
 * modules loaded for that entire lifetime. A pointer/UTF-8 check cannot prove this.
 * Rust borrows these bytes as &'static str without allocating/copying them.
 * The attribute DESCRIPTOR ARRAY need only be valid during begin; its storage is
 * NOT borrowed. The ordinary begin APIs NEVER infer this promise from pointers.
 */
__declspec(dllimport) uint32_t etw_span_begin_static(
    const EtwProvider *, EtwText, const EtwAttribute *, size_t, EtwSpan *);
__declspec(dllimport) uint32_t etw_span_end_reuse(EtwSpan *);
__declspec(dllimport) uint32_t etw_span_handle_destroy(EtwSpan **);
/* Fallible lifecycle calls contain Rust unwinding (not abort/OOM). End has already
 * consumed/reset the span on ETW_PANIC; context is restored but export may be
 * incomplete. Other rejected lifecycle calls preserve state. ETW_OK means SDK end
 * completed, not guaranteed ETW delivery. No completed-span export API exists.
 */

/* Benchmark controls; handle setup/destruction is outside reusable batch timing.
 * Keep this DLL loaded until process exit when using its static-input batch.
 */
__declspec(dllimport) uint32_t etw_rust_batch(const EtwProvider *, uint32_t, size_t);
__declspec(dllimport) uint32_t etw_rust_batch_reuse(
    const EtwProvider *, EtwSpan *, uint32_t, size_t);
__declspec(dllimport) void etw_prepare_thread();
__declspec(dllimport) uint32_t etw_current_span_valid();
__declspec(dllimport) uint64_t etw_empty_call(uint64_t);
__declspec(dllimport) void etw_allocator_self_test();
#ifdef __cplusplus
}
#endif
