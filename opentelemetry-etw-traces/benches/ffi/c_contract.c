#include "ffi.h"
#include <string.h>

#define CHECK(call) do { if ((call) != ETW_OK) return __LINE__; } while (0)
#define TEXT(literal) {literal, sizeof(literal) - 1}

int main(void)
{
  EtwProvider *provider = NULL;
  EtwSpan *span = NULL;
  const EtwText literal_name = TEXT("BenchmarkOperation");
  CHECK(etw_provider_create(0, &provider));
  CHECK(etw_span_handle_create(&span));
  for (unsigned i = 0; i < 2; ++i)
  {
    char name[] = "BenchmarkOperation";
    EtwText dynamic_name = {name, sizeof(name) - 1};
    CHECK(etw_span_begin_reuse(provider, dynamic_name, NULL, 0, span));
    memset(name, 'X', sizeof(name) - 1);
    CHECK(etw_span_status(span, 1));
    CHECK(etw_span_end_reuse(span));
    if (etw_current_span_valid() || etw_span_end_reuse(span) != ETW_INVALID_STATE)
      return __LINE__;
    CHECK(etw_span_begin_borrowed(provider, literal_name, NULL, 0, span));
    CHECK(etw_span_status(span, 1));
    CHECK(etw_span_end_reuse(span));
    {
      char borrowed_name[] = "BenchmarkOperation";
      EtwText borrowed_text = {borrowed_name, sizeof(borrowed_name) - 1};
      CHECK(etw_span_begin_borrowed(provider, borrowed_text, NULL, 0, span));
      CHECK(etw_span_status(span, 1));
      CHECK(etw_span_end_reuse(span));
      memset(borrowed_name, 'X', sizeof(borrowed_name) - 1);
    }
  }
  CHECK(etw_span_handle_destroy(&span));
  if (span != NULL) return __LINE__;
  CHECK(etw_provider_destroy(&provider));
  return 0;
}
