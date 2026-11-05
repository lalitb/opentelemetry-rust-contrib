cmake_minimum_required(VERSION 3.24)
project(etw_ffi_benchmark LANGUAGES C CXX)

if(NOT WIN32 OR NOT MSVC OR NOT CMAKE_SIZEOF_VOID_P EQUAL 8)
  message(FATAL_ERROR "This benchmark requires Windows x64 and MSVC.")
endif()

include(FetchContent)
FetchContent_Declare(nlohmann_json
  URL https://github.com/nlohmann/json/releases/download/v3.11.3/json.tar.xz
  URL_HASH SHA256=d6c65aca6b1ed68e7a182f4757257b107ae403032760ed6ef121c9d55e81757d
  DOWNLOAD_EXTRACT_TIMESTAMP TRUE)
FetchContent_MakeAvailable(nlohmann_json)

find_package(Git REQUIRED)
execute_process(COMMAND "${GIT_EXECUTABLE}" rev-parse HEAD
  WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
  OUTPUT_VARIABLE revision OUTPUT_STRIP_TRAILING_WHITESPACE COMMAND_ERROR_IS_FATAL ANY)
execute_process(COMMAND "${GIT_EXECUTABLE}" status --porcelain
  WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
  OUTPUT_VARIABLE dirty COMMAND_ERROR_IS_FATAL ANY)
if(dirty)
  string(APPEND revision "+working-tree")
endif()
file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/revision.txt" "${revision}\n")

add_library(etw_ffi_bench SHARED IMPORTED)
set_target_properties(etw_ffi_bench PROPERTIES
  IMPORTED_LOCATION "${CMAKE_CURRENT_SOURCE_DIR}/target/release/etw_ffi_bench.dll"
  IMPORTED_IMPLIB "${CMAKE_CURRENT_SOURCE_DIR}/target/release/etw_ffi_bench.dll.lib")

add_executable(driver driver.cc)
target_link_libraries(driver PRIVATE etw_ffi_bench)
target_compile_definitions(driver PRIVATE ETW_BENCH_REVISION="${revision}")
add_custom_command(TARGET driver POST_BUILD
  COMMAND "${CMAKE_COMMAND}" -E copy_if_different
    "$<TARGET_FILE:etw_ffi_bench>" "$<TARGET_FILE_DIR:driver>")

add_executable(capture capture.cc)
target_link_libraries(capture PRIVATE tdh)
add_executable(c_contract c_contract.c)
target_compile_features(c_contract PRIVATE c_std_11)
target_compile_options(c_contract PRIVATE /O2 /W4 /WX)
target_link_libraries(c_contract PRIVATE etw_ffi_bench)
set_target_properties(c_contract PROPERTIES
  MSVC_RUNTIME_LIBRARY MultiThreadedDLL
  RUNTIME_OUTPUT_DIRECTORY "$<1:${CMAKE_CURRENT_BINARY_DIR}/bin>")
foreach(target driver capture)
  target_compile_features(${target} PRIVATE cxx_std_17)
  target_compile_definitions(${target} PRIVATE NOMINMAX WIN32_LEAN_AND_MEAN)
  target_compile_options(${target} PRIVATE /O2 /W4 /WX /external:anglebrackets /external:W0)
  target_link_libraries(${target} PRIVATE nlohmann_json::nlohmann_json advapi32 ole32)
  set_target_properties(${target} PROPERTIES
    MSVC_RUNTIME_LIBRARY MultiThreadedDLL
    RUNTIME_OUTPUT_DIRECTORY "$<1:${CMAKE_CURRENT_BINARY_DIR}/bin>")
endforeach()

enable_testing()
add_test(NAME c_contract COMMAND c_contract)
add_test(NAME cpp_contract COMMAND driver contracts)
