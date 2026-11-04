#pragma once
#include <windows.h>
#include <evntprov.h>
#include <intrin.h>
#include <nlohmann/json.hpp>
#include <array>
#include <atomic>
#include <cstring>
#include <stdexcept>
#include <vector>

namespace heap_probe
{
struct Block { HANDLE heap{}; void *pointer{}; size_t bytes{}; };
struct Sample
{
  size_t allocations{}, requested{}, reallocations{}, reallocated{};
  size_t live_before{}, live_after{}, peak_live{};
  size_t max_observed_stack{};
  std::array<size_t, 3> write_depth{};
  size_t writes{};
};
inline std::array<Block, 2048> blocks{};
inline std::atomic<DWORD> thread_id{0};
inline thread_local unsigned depth = 0;
inline bool recording = false;
inline size_t live = 0, live_blocks = 0, failures = 0, errors = 0;
inline Sample sample;
inline uintptr_t stack_origin = 0;
inline decltype(&HeapAlloc) real_alloc = nullptr;
inline decltype(&HeapReAlloc) real_realloc = nullptr;
inline decltype(&HeapFree) real_free = nullptr;
inline decltype(&EventWriteTransfer) real_transfer = nullptr;
struct Patch { ULONG_PTR *slot; ULONG_PTR original; };
inline std::vector<Patch> patches;
inline nlohmann::json inventory = nlohmann::json::array();

inline Block *Find(HANDLE heap, void *pointer)
{
  for (auto &block : blocks)
    if (block.pointer == pointer && block.heap == heap) return &block;
  return nullptr;
}
inline void Add(HANDLE heap, void *pointer, size_t size)
{
  for (auto &block : blocks)
    if (!block.pointer)
    {
      block = {heap, pointer, size};
      live += size;
      ++live_blocks;
      if (live > sample.peak_live) sample.peak_live = live;
      return;
    }
  ++errors;
}
inline void Remove(Block &block)
{
  live -= block.bytes;
  --live_blocks;
  block = {};
}
inline bool Relevant()
{
  return GetCurrentThreadId() == thread_id.load(std::memory_order_relaxed) && depth == 0;
}
inline void ObserveStack(void *return_address)
{
  const auto address = reinterpret_cast<uintptr_t>(return_address);
  if (recording && stack_origin > address)
  {
    const auto distance = stack_origin - address;
    if (distance > sample.max_observed_stack) sample.max_observed_stack = distance;
  }
}
inline LPVOID WINAPI Allocate(HANDLE heap, DWORD flags, SIZE_T size)
{
  if (!Relevant()) return real_alloc(heap, flags, size);
  ObserveStack(_AddressOfReturnAddress());
  ++depth;
  auto result = real_alloc(heap, flags, size);
  if (recording)
  {
    ++sample.allocations;
    sample.requested += size;
    if (result) Add(heap, result, size); else ++failures;
  }
  --depth;
  return result;
}
inline LPVOID WINAPI Reallocate(HANDLE heap, DWORD flags, LPVOID old, SIZE_T size)
{
  if (!Relevant()) return real_realloc(heap, flags, old, size);
  ObserveStack(_AddressOfReturnAddress());
  ++depth;
  auto *tracked = Find(heap, old);
  auto result = real_realloc(heap, flags, old, size);
  if (recording)
  {
    ++sample.reallocations;
    sample.reallocated += size;
    if (!result) ++failures;
  }
  if (result)
  {
    const bool retain = tracked != nullptr || recording;
    if (tracked) Remove(*tracked);
    if (retain) Add(heap, result, size);
  }
  --depth;
  return result;
}
inline BOOL WINAPI Free(HANDLE heap, DWORD flags, LPVOID pointer)
{
  if (!Relevant()) return real_free(heap, flags, pointer);
  ObserveStack(_AddressOfReturnAddress());
  ++depth;
  auto result = real_free(heap, flags, pointer);
  if (result)
    if (auto *block = Find(heap, pointer)) Remove(*block);
  --depth;
  return result;
}
inline ULONG WINAPI Transfer(REGHANDLE handle, PCEVENT_DESCRIPTOR descriptor,
                            LPCGUID activity, LPCGUID related, ULONG count,
                            PEVENT_DATA_DESCRIPTOR data)
{
  if (Relevant() && recording)
  {
    auto address = _AddressOfReturnAddress();
    ObserveStack(address);
    ++sample.writes;
    if (descriptor->Opcode < sample.write_depth.size() &&
        stack_origin > reinterpret_cast<uintptr_t>(address))
      sample.write_depth[descriptor->Opcode] =
          stack_origin - reinterpret_cast<uintptr_t>(address);
  }
  return real_transfer(handle, descriptor, activity, related, count, data);
}
template <class T, class U>
inline void Bind(ULONG_PTR *slot, T replacement, U &original,
                 const char *module, const char *name)
{
  auto address = reinterpret_cast<U>(*slot);
  if (original && original != address)
    throw std::runtime_error("Heap entry points differ; incomplete coverage");
  original = address;
  DWORD protection{}, ignored{};
  if (!VirtualProtect(slot, sizeof(*slot), PAGE_READWRITE, &protection))
    throw std::runtime_error("IAT protection failed");
  patches.push_back({slot, *slot});
  *slot = reinterpret_cast<ULONG_PTR>(replacement);
  if (!VirtualProtect(slot, sizeof(*slot), protection, &ignored))
    throw std::runtime_error("IAT protection restoration failed");
  inventory.push_back({{"module", module}, {"function", name}});
}
inline void Install()
{
  constexpr std::array<const char *, 5> modules{
      nullptr, "ucrtbase.dll", "vcruntime140.dll", "msvcp140.dll", "etw_ffi_bench.dll"};
  for (const char *module : modules)
  {
    auto base = reinterpret_cast<BYTE *>(GetModuleHandleA(module));
    if (!base) continue;
    auto dos = reinterpret_cast<IMAGE_DOS_HEADER *>(base);
    auto nt = reinterpret_cast<IMAGE_NT_HEADERS *>(base + dos->e_lfanew);
    auto directory = nt->OptionalHeader.DataDirectory[IMAGE_DIRECTORY_ENTRY_IMPORT];
    if (!directory.VirtualAddress) continue;
    auto imports = reinterpret_cast<IMAGE_IMPORT_DESCRIPTOR *>(base + directory.VirtualAddress);
    for (; imports->Name; ++imports)
    {
      if (!imports->OriginalFirstThunk) continue;
      auto names = reinterpret_cast<IMAGE_THUNK_DATA *>(base + imports->OriginalFirstThunk);
      auto slots = reinterpret_cast<IMAGE_THUNK_DATA *>(base + imports->FirstThunk);
      for (; names->u1.AddressOfData; ++names, ++slots)
      {
        if (IMAGE_SNAP_BY_ORDINAL(names->u1.Ordinal)) continue;
        auto entry = reinterpret_cast<IMAGE_IMPORT_BY_NAME *>(base + names->u1.AddressOfData);
        const char *name = reinterpret_cast<const char *>(entry->Name);
        const char *label = module ? module : "benchmark.exe";
        if (strcmp(name, "HeapAlloc") == 0)
          Bind(&slots->u1.Function, Allocate, real_alloc, label, name);
        else if (strcmp(name, "HeapReAlloc") == 0)
          Bind(&slots->u1.Function, Reallocate, real_realloc, label, name);
        else if (strcmp(name, "HeapFree") == 0)
          Bind(&slots->u1.Function, Free, real_free, label, name);
        else if (strcmp(name, "EventWriteTransfer") == 0)
          Bind(&slots->u1.Function, Transfer, real_transfer, label, name);
      }
    }
  }
  if (!real_alloc || !real_realloc || !real_free || !real_transfer)
    throw std::runtime_error("Missing native heap coverage");
}
inline void Restore()
{
  thread_id.store(0);
  for (auto patch : patches)
  {
    DWORD old{}, ignored{};
    if (!VirtualProtect(patch.slot, sizeof(*patch.slot), PAGE_READWRITE, &old))
      throw std::runtime_error("IAT restoration failed");
    *patch.slot = patch.original;
    if (!VirtualProtect(patch.slot, sizeof(*patch.slot), old, &ignored))
      throw std::runtime_error("IAT page restoration failed");
  }
  patches.clear();
}
inline void Begin()
{
  sample = {};
  sample.live_before = sample.peak_live = live;
  recording = true;
}
inline Sample End()
{
  recording = false;
  sample.live_after = live;
  return sample;
}
inline nlohmann::json Json(const Sample &value)
{
  return {{"allocations", value.allocations}, {"requested_bytes", value.requested},
          {"reallocations", value.reallocations}, {"reallocated_bytes", value.reallocated},
          {"live_before", value.live_before}, {"live_after", value.live_after},
          {"peak_live_bytes", value.peak_live},
          {"peak_above_start_bytes", value.peak_live - value.live_before},
          {"max_observed_stack_bytes", value.max_observed_stack},
          {"span_write_stack_bytes", value.write_depth[0]},
          {"start_write_stack_bytes", value.write_depth[1]},
          {"stop_write_stack_bytes", value.write_depth[2]}, {"writes", value.writes}};
}
inline nlohmann::json SelfTest()
{
  auto crt = GetModuleHandleA("ucrtbase.dll");
  auto alloc = reinterpret_cast<void *(__cdecl *)(size_t)>(GetProcAddress(crt, "malloc"));
  auto resize = reinterpret_cast<void *(__cdecl *)(void *, size_t)>(GetProcAddress(crt, "realloc"));
  auto release = reinterpret_cast<void (__cdecl *)(void *)>(GetProcAddress(crt, "free"));
  if (!alloc || !resize || !release) throw std::runtime_error("Missing CRT self-test APIs");
  thread_id.store(GetCurrentThreadId());
  Begin();
  void *a = alloc(73);
  void *b = alloc(257);
  b = resize(b, 401);
  void *c = Allocate(GetProcessHeap(), 0, 89);
  release(a);
  release(b);
  Free(GetProcessHeap(), 0, c);
  const auto result = End();
  thread_id.store(0);
  if (result.allocations != 3 || result.requested != 419 ||
      result.reallocations != 1 || result.reallocated != 401 ||
      result.peak_live != 563 || live || live_blocks || errors || failures)
    throw std::runtime_error("Native allocation/free/peak self-test failed");
  return Json(result);
}
}
