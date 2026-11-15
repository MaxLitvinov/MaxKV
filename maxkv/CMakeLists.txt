cmake_minimum_required(VERSION 3.16)
project(maxkv VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()
find_package(Threads REQUIRED)

add_library(maxkv_core
  src/store.cpp src/resp.cpp src/wal.cpp src/engine.cpp src/server.cpp)
target_include_directories(maxkv_core PUBLIC include)
target_compile_options(maxkv_core PRIVATE -Wall -Wextra -Wpedantic)
target_link_libraries(maxkv_core PUBLIC Threads::Threads)

add_executable(maxkv src/main.cpp)
target_link_libraries(maxkv PRIVATE maxkv_core)

add_executable(maxkv_tests tests/test_main.cpp)
target_link_libraries(maxkv_tests PRIVATE maxkv_core)

add_executable(maxkv_bench bench/bench.cpp)
target_link_libraries(maxkv_bench PRIVATE Threads::Threads)

enable_testing()
add_test(NAME maxkv_tests COMMAND maxkv_tests)
