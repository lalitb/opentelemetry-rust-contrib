#include <windows.h>
#include <evntcons.h>
#include <evntrace.h>
#include <tdh.h>
#include <objbase.h>
#include <nlohmann/json.hpp>
#include <algorithm>
#include <cstddef>
#include <cstring>
#include <iostream>
#include <set>
#include <stdexcept>
#include <string>
#include <unordered_set>
#include <vector>

struct SessionProperties
{
  EVENT_TRACE_PROPERTIES properties{};
  wchar_t names[2048]{};
  SessionProperties()
  {
    properties.Wnode.BufferSize = sizeof(*this);
    properties.LoggerNameOffset = offsetof(SessionProperties, names);
    properties.LogFileNameOffset = offsetof(SessionProperties, names) + 1024 * sizeof(wchar_t);
  }
};

std::string Narrow(const wchar_t *text)
{
  if (!text || !*text) return {};
  const int count = WideCharToMultiByte(CP_UTF8, 0, text, -1, nullptr, 0, nullptr, nullptr);
  if (!count) throw std::runtime_error("UTF-8 conversion failed");
  std::string result(static_cast<size_t>(count), '\0');
  if (!WideCharToMultiByte(CP_UTF8, 0, text, -1, result.data(), count, nullptr, nullptr))
    throw std::runtime_error("UTF-8 conversion failed");
  result.pop_back();
  return result;
}

int Control(int argc, wchar_t **argv)
{
  const bool stop = argc == 3 && std::wstring(argv[1]) == L"stop";
  if (!stop && (argc != 5 || std::wstring(argv[1]) != L"enable"))
    throw std::runtime_error("stop SESSION | enable SESSION PID GUID");
  SessionProperties session;
  auto status = ControlTraceW(0, argv[2], &session.properties,
                             stop ? EVENT_TRACE_CONTROL_STOP : EVENT_TRACE_CONTROL_QUERY);
  if (status != ERROR_SUCCESS)
    throw std::runtime_error("ControlTrace failed: " + std::to_string(status));
  if (stop)
  {
    std::cout << nlohmann::json{
        {"events_lost", session.properties.EventsLost},
        {"log_buffers_lost", session.properties.LogBuffersLost},
        {"realtime_buffers_lost", session.properties.RealTimeBuffersLost}}.dump(2) << '\n';
    return 0;
  }
  ULONG pid = std::stoul(argv[3]);
  EVENT_FILTER_DESCRIPTOR filter{};
  filter.Ptr = reinterpret_cast<ULONGLONG>(&pid);
  filter.Size = sizeof(pid);
  filter.Type = EVENT_FILTER_TYPE_PID;
  ENABLE_TRACE_PARAMETERS parameters{};
  parameters.Version = ENABLE_TRACE_PARAMETERS_VERSION_2;
  parameters.EnableFilterDesc = &filter;
  parameters.FilterDescCount = 1;
  GUID provider{};
  if (FAILED(CLSIDFromString(argv[4], &provider)))
    throw std::runtime_error("Invalid provider GUID");
  status = EnableTraceEx2(session.properties.Wnode.HistoricalContext, &provider,
                         EVENT_CONTROL_CODE_ENABLE_PROVIDER, 5, ~0ULL, 0, 0, &parameters);
  if (status != ERROR_SUCCESS)
    throw std::runtime_error("EnableTraceEx2 failed: " + std::to_string(status));
  return 0;
}

ULONG target_pid = 0;
int expected_attributes = -1;
std::string callback_error;

GUID expected_provider{};
size_t count = 0, bytes_total = 0;
std::unordered_set<std::string> span_ids;
nlohmann::json samples = nlohmann::json::array();

std::vector<BYTE> Nested(EVENT_RECORD *event, std::initializer_list<const wchar_t *> path)
{
  std::vector<PROPERTY_DATA_DESCRIPTOR> descriptors;
  for (auto *name : path)
    descriptors.push_back({reinterpret_cast<ULONGLONG>(name), ULONG_MAX, 0});
  ULONG size{};
  auto status = TdhGetPropertySize(event, 0, nullptr, static_cast<ULONG>(descriptors.size()),
                                   descriptors.data(), &size);
  if (status != ERROR_SUCCESS)
    throw std::runtime_error("Nested property lookup: " + Narrow(*(path.end() - 1)) +
                             " status=" + std::to_string(status));
  std::vector<BYTE> value(size);
  if (TdhGetProperty(event, 0, nullptr, static_cast<ULONG>(descriptors.size()),
                     descriptors.data(), size, value.data()) != ERROR_SUCCESS)
    throw std::runtime_error("Nested property read failed");
  return value;
}

std::string Text(EVENT_RECORD *event, std::initializer_list<const wchar_t *> path)
{
  const auto data = Nested(event, path);
  uint16_t length{};
  if (data.size() < sizeof(length)) throw std::runtime_error("Expected counted ANSI string");
  memcpy(&length, data.data(), sizeof(length));
  if (data.size() != sizeof(length) + length)
    throw std::runtime_error("Incorrect counted string length");
  return std::string(data.begin() + sizeof(length), data.end());
}

template <typename T>
T Number(EVENT_RECORD *event, std::initializer_list<const wchar_t *> path)
{
  const auto data = Nested(event, path);
  if (data.size() != sizeof(T)) throw std::runtime_error("Unexpected scalar size");
  T value{};
  memcpy(&value, data.data(), sizeof(T));
  return value;
}

bool HexId(const std::string &text, size_t length)
{
  return text.size() == length && text.find_first_not_of('0') != std::string::npos &&
      std::all_of(text.begin(), text.end(), [](char c) {
        return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
      });
}

void WINAPI RustEvent(EVENT_RECORD *event)
{
  if (event->EventHeader.ProcessId != target_pid ||
      !IsEqualGUID(event->EventHeader.ProviderId, expected_provider)) return;
  try
  {
    if (event->EventHeader.EventDescriptor.Opcode != 0)
      throw std::runtime_error("Unexpected activity event");
    const auto name = Text(event, {L"PartB", L"name"});
    const auto trace = Text(event, {L"PartA", L"ext_dt", L"traceId"});
    const auto span = Text(event, {L"PartA", L"ext_dt", L"spanId"});
    const auto start = Text(event, {L"PartB", L"startTime"});
    const auto end = Text(event, {L"PartA", L"time"});
    if (Number<uint16_t>(event, {L"__csver__"}) != 1024 ||
        Text(event, {L"PartB", L"_typeName"}) != "Span" ||
        name != "BenchmarkOperation" || Number<uint8_t>(event, {L"PartB", L"kind"}) != 1 ||
        Number<uint8_t>(event, {L"PartB", L"success"}) != 1 ||
        !HexId(trace, 32) || !HexId(span, 16) || !span_ids.insert(span).second ||
        start.size() < 20 || end.size() < 20 || start.back() != 'Z' || end.back() != 'Z')
      throw std::runtime_error("Span payload mismatch");
    if (expected_attributes == 3 &&
        (Text(event, {L"PartC", L"string_key"}) != "benchmark-value" ||
         Number<int64_t>(event, {L"PartC", L"integer_key"}) != 42 ||
         Number<int32_t>(event, {L"PartC", L"bool_key"}) != 1))
      throw std::runtime_error("Attribute mismatch");

    ULONG size{};
    if (TdhGetEventInformation(event, 0, nullptr, nullptr, &size) != ERROR_INSUFFICIENT_BUFFER)
      throw std::runtime_error("Cannot size schema");
    std::vector<BYTE> storage(size);
    auto *info = reinterpret_cast<TRACE_EVENT_INFO *>(storage.data());
    if (TdhGetEventInformation(event, 0, nullptr, info, &size) != ERROR_SUCCESS)
      throw std::runtime_error("Cannot read schema");
    for (ULONG i = 0; i < info->PropertyCount; ++i)
      if (Narrow(reinterpret_cast<const wchar_t *>(
              storage.data() + info->EventPropertyInfoArray[i].NameOffset)) == "parentId")
        throw std::runtime_error("Unexpected parent: active context leaked between spans");
    std::set<std::string> fields;
    for (ULONG i = 0; i < info->TopLevelPropertyCount; ++i)
      fields.insert(Narrow(reinterpret_cast<const wchar_t *>(
          storage.data() + info->EventPropertyInfoArray[i].NameOffset)));
    std::set<std::string> expected{"__csver__", "PartA", "PartB"};
    if (expected_attributes == 3) expected.insert("PartC");
    if (fields != expected || !info->EventNameOffset ||
        Narrow(reinterpret_cast<const wchar_t *>(storage.data() + info->EventNameOffset)) != "Span")
      throw std::runtime_error("Unexpected schema");
    if (samples.size() < 2)
      samples.push_back({{"name", name}, {"trace_id", trace}, {"span_id", span},
          {"start_time", start}, {"end_time", end}, {"fields", fields},
          {"payload_bytes", event->UserDataLength}});
    ++count;
    bytes_total += event->UserDataLength;
  }
  catch (const std::exception &error) { callback_error = error.what(); }
}

int wmain(int argc, wchar_t **argv)
{
  try
  {
    if (argc >= 2 && std::wstring(argv[1]) != L"inspect")
      return Control(argc, argv);
    if (argc != 6) throw std::runtime_error("inspect ETL PID attributes GUID");
    target_pid = std::stoul(argv[3]);
    expected_attributes = std::stoi(argv[4]);
    if (expected_attributes != 0 && expected_attributes != 3)
      throw std::runtime_error("Expected zero or three attributes");
    if (FAILED(CLSIDFromString(argv[5], &expected_provider)))
      throw std::runtime_error("Invalid provider GUID");
    EVENT_TRACE_LOGFILEW log{};
    log.LogFileName = argv[2];
    log.ProcessTraceMode = PROCESS_TRACE_MODE_EVENT_RECORD;
    log.EventRecordCallback = RustEvent;
    auto handle = OpenTraceW(&log);
    if (handle == INVALID_PROCESSTRACE_HANDLE) throw std::runtime_error("OpenTrace failed");
    const auto status = ProcessTrace(&handle, 1, nullptr, nullptr);
    CloseTrace(handle);
    if (status != ERROR_SUCCESS || !callback_error.empty() || !count)
      throw std::runtime_error("Capture validation failed: " + callback_error);
    std::cout << nlohmann::json{{"validated_events", count}, {"payload_bytes", bytes_total},
        {"unique_span_ids", span_ids.size()}, {"samples", samples}}.dump(2) << '\n';
    return 0;
  }
  catch (const std::exception &error)
  {
    std::cerr << error.what() << '\n';
    return 1;
  }
}
