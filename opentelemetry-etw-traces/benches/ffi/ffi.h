#pragma once
#include <cstddef>
#include <cstdint>

struct EtwProvider;
struct EtwSpan;
struct EtwText { const char *data; size_t length; };
struct EtwAttribute
{
  EtwText key;
  uint32_t kind;
  EtwText string_value;
  int64_t integer_value;
};
extern "C"
{
__declspec(dllimport) uint32_t etw_provider_create(size_t, EtwProvider **);
__declspec(dllimport) uint32_t etw_provider_destroy(EtwProvider **);
__declspec(dllimport) uint32_t etw_provider_enabled(const EtwProvider *);
__declspec(dllimport) void etw_provider_guid(char *);
__declspec(dllimport) uint32_t etw_span_begin(
    const EtwProvider *, EtwText, const EtwAttribute *, size_t, EtwSpan **);
__declspec(dllimport) uint32_t etw_span_status(EtwSpan *, uint32_t);
__declspec(dllimport) uint32_t etw_span_end(EtwSpan **);
__declspec(dllimport) uint32_t etw_rust_batch(const EtwProvider *, uint32_t, size_t);
__declspec(dllimport) void etw_prepare_thread();
__declspec(dllimport) uint32_t etw_current_span_valid();
__declspec(dllimport) uint64_t etw_empty_call(uint64_t);
__declspec(dllimport) void etw_allocator_self_test();
}
