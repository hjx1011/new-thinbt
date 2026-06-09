cmake_minimum_required(VERSION 3.15)
project(thinBT VERSION 1.0.0 LANGUAGES CXX C)

# Release 优化 — -O3 + 无断言开销，性能差异可达 10 倍以上
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG" CACHE STRING "")
set(CMAKE_C_FLAGS_RELEASE "-O3 -DNDEBUG" CACHE STRING "")

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_C_STANDARD 99)

find_package(OpenSSL REQUIRED)

# Third-party header-only libraries
add_subdirectory(third_party)

# Common utilities
add_library(thinbt_common STATIC
    src/common/hash.cpp
    src/common/file_util.cpp
    src/common/net_util.cpp
)
target_include_directories(thinbt_common PUBLIC
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_SOURCE_DIR}/third_party/asio/asio/include
)
target_link_libraries(thinbt_common PUBLIC
    OpenSSL::SSL OpenSSL::Crypto
    yyjson
)

# CDC library (depends on common)
add_library(thinbt_cdc STATIC
    src/cdc/fastcdc.cpp
)
target_link_libraries(thinbt_cdc PUBLIC thinbt_common)

# Seed library (depends on common)
add_library(thinbt_seed STATIC
    src/seed/seed_reader.cpp
    src/seed/seed_writer.cpp
)
target_link_libraries(thinbt_seed PUBLIC thinbt_common)

# Daemon library (depends on cdc + seed)
add_library(thinbt_daemon STATIC
    src/daemon/chunk_assembler.cpp
    src/daemon/io_worker.cpp
    src/daemon/segment_io.cpp
    src/daemon/scheduler.cpp
    src/daemon/protocol.cpp
    src/daemon/peer_session.cpp
    src/daemon/peer_manager.cpp
    src/daemon/task_manager.cpp
    src/daemon/ipc_server.cpp
    src/daemon/tracker_client.cpp
    src/daemon/tracker_server.cpp
    src/daemon/tracker_acceptor.cpp
)
target_link_libraries(thinbt_daemon PUBLIC thinbt_cdc thinbt_seed moodycamel)

# Daemon executable
add_executable(thinbtd src/daemon/main.cpp)
target_link_libraries(thinbtd thinbt_daemon)

# CLI executable
add_executable(tbt src/cli/tbt.cpp src/cli/cli_commands.cpp)
target_link_libraries(tbt thinbt_common thinbt_seed thinbt_cdc)

# Tests
enable_testing()
function(add_thinbt_test name)
    add_executable(${name} tests/${name}.cpp)
    target_link_libraries(${name} thinbt_daemon)
    add_test(NAME ${name} COMMAND ${name})
endfunction()

add_thinbt_test(test_seed)
target_link_libraries(test_seed thinbt_seed)

add_thinbt_test(test_fastcdc)
target_link_libraries(test_fastcdc thinbt_cdc)

add_thinbt_test(test_chunk_assembler)

add_thinbt_test(test_segment_io)

add_thinbt_test(test_loopback)
