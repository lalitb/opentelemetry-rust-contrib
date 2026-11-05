#include "ffi.h"
#include "heap_probe.h"
#include <array>
#include <fstream>
#include <iostream>
#include <thread>

constexpr const char *revision = ETW_BENCH_REVISION;
constexpr size_t iterations = 10000, warmup = 1000, batches = 7;
static_assert(sizeof(EtwText) == 16 && sizeof(EtwAttribute) == 48, "Unexpected x64 ABI");

template <size_t N> constexpr EtwText Text(const char (&value)[N]) { return {value, N - 1}; }
void Check(uint32_t status)
{
  if (status != 0) throw std::runtime_error("FFI status " + std::to_string(status));
}
void Require(bool condition, const char *message)
{
  if (!condition) throw std::runtime_error(message);
}
struct Workload
{
  EtwProvider *provider{};
  EtwSpan *handle{};
  std::array<EtwAttribute, 3> attributes{{
      {Text("string_key"), 1, Text("benchmark-value"), 0},
      {Text("integer_key"), 2, Text(""), 42},
      {Text("bool_key"), 3, Text(""), 1}}};
  size_t count;
  unsigned variant;
  Workload(size_t n, unsigned v) : count(n), variant(v) { Check(etw_provider_create(n, &provider)); }
  ~Workload() { if (provider) etw_provider_destroy(&provider); }
  bool Reusable() const { return variant >= 3; }
  void CreateHandle() { if (Reusable()) Check(etw_span_handle_create(&handle)); }
  void DestroyHandle() { if (handle) Check(etw_span_handle_destroy(&handle)); }
  __declspec(noinline) void CppEmit()
  {
    if (Reusable())
    {
      const auto begin = variant == 6 ? etw_span_begin_static : etw_span_begin_reuse;
      Check(begin(provider, Text("BenchmarkOperation"), attributes.data(), count, handle));
      const auto status = etw_span_status(handle, 1);
      const auto end = etw_span_end_reuse(handle);
      Check(status);
      Check(end);
      return;
    }
    EtwSpan *span{};
    Check(etw_span_begin(provider, Text("BenchmarkOperation"), attributes.data(), count, &span));
    const auto status = etw_span_status(span, 1);
    const auto end = etw_span_end(&span);
    Check(status);
    Check(end);
  }
  void Run(size_t n)
  {
    if (variant < 2) Check(etw_rust_batch(provider, variant, n));
    else if (variant == 3 || variant == 4)
      Check(etw_rust_batch_reuse(provider, handle, variant == 4, n));
    else for (size_t i = 0; i < n; ++i) CppEmit();
  }
  void Close() { Check(etw_provider_destroy(&provider)); }
};

int64_t Tick() { LARGE_INTEGER value{}; QueryPerformanceCounter(&value); return value.QuadPart; }
double Frequency() { LARGE_INTEGER value{}; QueryPerformanceFrequency(&value); return double(value.QuadPart); }

void ContractChecks(Workload &workload)
{
  Require(etw_current_span_valid() == 0, "Unexpected parent");
  EtwSpan *span{};
  const unsigned char invalid[] = {0xff};
  Require(etw_span_begin(workload.provider, {reinterpret_cast<const char *>(invalid), 1},
                         nullptr, 0, &span) == 1 && !span,
          "Invalid UTF-8 accepted");
  Require(etw_span_begin(nullptr, Text("BenchmarkOperation"), nullptr, 0, &span) == 1 && !span,
          "Null provider accepted");
  char name[] = "BenchmarkOperation";
  char key[] = "string_key";
  char value[] = "benchmark-value";
  auto attrs = workload.attributes;
  attrs[0].key = Text(key);
  attrs[0].string_value = Text(value);
  Check(etw_span_begin(workload.provider, Text(name), attrs.data(), workload.count, &span));
  Require(etw_current_span_valid() == 1, "Span not attached");
  memset(name, 'X', sizeof(name) - 1);
  memset(key, 'X', sizeof(key) - 1);
  memset(value, 'X', sizeof(value) - 1);
  Check(etw_span_status(span, 1));
  Check(etw_span_end(&span));
  Require(!span && !etw_current_span_valid(), "End did not restore context or consume handle");
  Require(etw_span_end(&span) == 1, "Second end accepted");

  Check(etw_span_begin(workload.provider, Text("BenchmarkOperation"),
                       workload.attributes.data(), workload.count, &span));
  uint32_t wrong_status{}, wrong_end{};
  std::thread wrong_thread([&] {
    auto alias = span;
    wrong_status = etw_span_status(alias, 1);
    wrong_end = etw_span_end(&alias);
    if (alias != span) wrong_end = 99;
  });
  wrong_thread.join();
  Require(wrong_status == 2 && wrong_end == 2, "Wrong-thread calls were not rejected");
  Require(etw_span_status(span, 99) == 1, "Invalid status accepted");
  Check(etw_span_status(span, 1));
  Check(etw_span_end(&span));
  workload.CppEmit();
  Require(!etw_current_span_valid(), "Context leaked");

  for (const auto begin : {etw_span_begin_reuse, etw_span_begin_static})
  {
    EtwSpan *reusable{};
    Check(etw_span_handle_create(&reusable));
    auto *address = reusable;
    Require(etw_span_end_reuse(reusable) == ETW_INVALID_STATE, "Inactive end accepted");
    Require(etw_span_status(reusable, 1) == ETW_INVALID_STATE, "Inactive status accepted");
    for (unsigned cycle = 0; cycle < 2; ++cycle)
    {
      char dynamic_name[] = "BenchmarkOperation";
      char dynamic_key[] = "string_key";
      char dynamic_value[] = "benchmark-value";
      auto descriptors = workload.attributes;
      const bool copying = begin == etw_span_begin_reuse;
      if (copying)
      {
        descriptors[0].key = Text(dynamic_key);
        descriptors[0].string_value = Text(dynamic_value);
      }
      Check(begin(workload.provider, copying ? Text(dynamic_name) : Text("BenchmarkOperation"),
                  descriptors.data(), workload.count, reusable));
      // Only dynamic bytes are overwritten; static input bytes always remain literals.
      memset(dynamic_name, 'X', sizeof(dynamic_name) - 1);
      memset(dynamic_key, 'X', sizeof(dynamic_key) - 1);
      memset(dynamic_value, 'X', sizeof(dynamic_value) - 1);
      descriptors = {};
      Require(begin(workload.provider, Text("BenchmarkOperation"), nullptr, 0, reusable) ==
                  ETW_INVALID_STATE, "Active begin accepted");
      Require(etw_span_handle_destroy(&reusable) == ETW_INVALID_STATE && reusable == address,
              "Active destroy consumed handle");
      Require(etw_span_end(&reusable) == ETW_INVALID_STATE, "Consuming end accepted reuse handle");
      std::thread wrong([&] {
        auto *alias = reusable;
        wrong_status = begin(workload.provider, Text("BenchmarkOperation"), nullptr, 0, alias);
        wrong_end = etw_span_end_reuse(alias);
        if (etw_span_handle_destroy(&alias) != ETW_WRONG_THREAD || alias != reusable)
          wrong_end = 99;
      });
      wrong.join();
      Require(wrong_status == ETW_WRONG_THREAD && wrong_end == ETW_WRONG_THREAD,
              "Reusable wrong-thread calls accepted");
      Check(etw_span_status(reusable, 1));
      Check(etw_span_end_reuse(reusable));
      Require(reusable == address && !etw_current_span_valid(), "Reusable end did not reset");
      Require(etw_span_end_reuse(reusable) == ETW_INVALID_STATE, "Repeated end accepted");
    }
    Check(etw_span_handle_destroy(&reusable));
    Require(!reusable, "Destroy did not clear slot");
  }
}

nlohmann::json Memory(Workload &workload)
{
  heap_probe::Install();
  auto result = nlohmann::json{{"crt_self_test", heap_probe::SelfTest()}};
  heap_probe::thread_id.store(GetCurrentThreadId());
  heap_probe::Begin();
  etw_allocator_self_test();
  const auto self_test = heap_probe::End();
  heap_probe::thread_id.store(0);
  Require(self_test.allocations == 3 && self_test.requested == 419 &&
      self_test.reallocations == 1 && self_test.reallocated == 401 &&
      self_test.peak_live == 563 && !heap_probe::live, "Rust DLL allocator coverage failed");
  result["rust_dll_self_test"] = heap_probe::Json(self_test);
  workload.CreateHandle();
  workload.Run(1);
  workload.DestroyHandle();
  heap_probe::Sample cold{}, setup{}, teardown{};
  std::array<heap_probe::Sample, 1000> samples{};
  size_t retained{}, blocks{}, with_handle{};
  std::exception_ptr error;
  std::thread worker([&] {
    try
    {
      etw_prepare_thread();
      heap_probe::thread_id.store(GetCurrentThreadId());
      heap_probe::Begin();
      workload.CreateHandle();
      setup = heap_probe::End();
      auto measure = [&] {
        heap_probe::Begin();
        workload.Run(1);
        const auto sample = heap_probe::End();
        Require(sample.writes == 1, "Expected one Rust DLL ETW write");
        return sample;
      };
      cold = measure();
      for (auto &sample : samples) sample = measure();
      Require(etw_current_span_valid() == 0, "Worker context leaked");
      with_handle = heap_probe::live;
      heap_probe::Begin();
      workload.DestroyHandle();
      teardown = heap_probe::End();
      retained = heap_probe::live;
      blocks = heap_probe::live_blocks;
    }
    catch (...)
    {
      heap_probe::recording = false;
      error = std::current_exception();
      if (workload.handle) etw_span_handle_destroy(&workload.handle);
    }
  });
  worker.join();
  result["after_thread_exit_bytes"] = heap_probe::live;
  result["after_thread_exit_blocks"] = heap_probe::live_blocks;
  heap_probe::Restore();
  if (error) std::rethrow_exception(error);
  Require(!heap_probe::errors && !heap_probe::failures && !heap_probe::live &&
          !heap_probe::live_blocks, "Allocation tracking/teardown failed");
  result["cold"] = heap_probe::Json(cold);
  result["handle_create"] = heap_probe::Json(setup);
  result["handle_destroy"] = heap_probe::Json(teardown);
  result["idle_with_handle_bytes"] = with_handle;
  result["samples"] = nlohmann::json::array();
  for (const auto &sample : samples) result["samples"].push_back(heap_probe::Json(sample));
  result["idle_retained_bytes"] = retained;
  result["idle_retained_blocks"] = blocks;
  result["hooks"] = heap_probe::inventory;
  return result;
}

int main(int argc, char **argv)
{
  try
  {
    if (argc == 2 && std::string(argv[1]) == "contracts")
    {
      for (const size_t count : {size_t{0}, size_t{3}})
      {
        Workload workload(count, 2);
        ContractChecks(workload);
        workload.Close();
      }
      std::cout << "C++ ABI contracts passed (ETW capture not required)\n";
      return 0;
    }
    if (argc != 6) throw std::runtime_error("mode variant attributes ready gate | contracts");
    const std::string mode = argv[1], variant = argv[2];
    const std::array<std::string, 7> variants{
        "native", "wrapper", "cpp", "rust-reuse", "rust-static", "cpp-reuse", "cpp-static"};
    const auto found = std::find(variants.begin(), variants.end(), variant);
    Require(found != variants.end(), "Unknown variant");
    const auto kind = static_cast<unsigned>(found - variants.begin());
    Require(mode == "latency" || mode == "memory" || mode == "check", "Unknown mode");
    const size_t count = std::stoul(argv[3]);
    Require(count == 0 || count == 3, "Invalid attribute count");
    Workload workload(count, kind);
    char guid[36]{};
    etw_provider_guid(guid);
    const size_t expected = mode == "latency" ? batches * (iterations + warmup)
                           : mode == "memory" ? 1002 : 7;
    nlohmann::json output{
        {"pid", GetCurrentProcessId()}, {"mode", mode}, {"variant", variant},
        {"attribute_count", count}, {"revision", revision},
        {"provider_guid", "{" + std::string(guid, sizeof(guid)) + "}"},
        {"expected_events", expected}, {"expected_spans", expected},
        {"compiler", _MSC_FULL_VER}, {"ffi_calls_per_span", kind == 0 ? 0 : 3}};
    {
      std::ofstream ready(std::string(argv[4]) + ".tmp");
      ready << output.dump(2);
      Require(bool(ready), "Cannot publish readiness");
    }
    Require(!!MoveFileExA((std::string(argv[4]) + ".tmp").c_str(), argv[4],
                         MOVEFILE_REPLACE_EXISTING), "Cannot publish readiness");
    const auto deadline = GetTickCount64() + 120000;
    while (GetFileAttributesA(argv[5]) == INVALID_FILE_ATTRIBUTES)
    {
      Require(GetTickCount64() < deadline, "Capture setup timed out");
      Sleep(50);
    }
    while (!etw_provider_enabled(workload.provider))
    {
      Require(GetTickCount64() < deadline, "Provider is disabled");
      Sleep(10);
    }
    etw_prepare_thread();
    if (mode == "check")
    {
      ContractChecks(workload);
      output["contracts_passed"] = true;
    }
    else if (mode == "latency")
    {
      const auto frequency = Frequency();
      const auto setup_start = Tick();
      workload.CreateHandle();
      output["handle_create_ns"] = double(Tick() - setup_start) * 1e9 / frequency;
      std::array<double, batches> samples{};
      for (auto &sample : samples)
      {
        workload.Run(warmup);
        const auto start = Tick();
        workload.Run(iterations);
        sample = double(Tick() - start) * 1e9 / frequency / iterations;
        Require(etw_current_span_valid() == 0, "Batch context leaked");
      }
      output["batch_ns"] = samples;
      const auto destroy_start = Tick();
      workload.DestroyHandle();
      output["handle_destroy_ns"] = double(Tick() - destroy_start) * 1e9 / frequency;
      constexpr size_t empty_iterations = 100000;
      uint64_t value = 0;
      const auto start = Tick();
      for (size_t i = 0; i < empty_iterations; ++i)
      {
        value = etw_empty_call(value);
        value = etw_empty_call(value);
        value = etw_empty_call(value);
      }
      output["empty_exported_call_ns"] = double(Tick() - start) * 1e9 / frequency /
                                        (3 * empty_iterations);
      Require(value == 3 * empty_iterations, "Empty-call control failed");
    }
    else output["probe"] = Memory(workload);
    Require(etw_provider_enabled(workload.provider) != 0, "Provider disabled during run");
    DWORD_PTR mask{}, system_mask{};
    Require(!!GetProcessAffinityMask(GetCurrentProcess(), &mask, &system_mask), "Affinity query failed");
    output["affinity_mask"] = mask;
    workload.Close();
    std::cout << output.dump(2) << '\n';
    return 0;
  }
  catch (const std::exception &error)
  {
    std::cerr << error.what() << '\n';
    return 1;
  }
}
