#!/bin/sh
set -e
export PATH=/usr/local/bin:$PATH

CXX=g++
CXXFLAGS="-std=c++17 -O3 -DNDEBUG -pthread"
INC="-Isrc -Ithird_party/asio/asio/include -Ithird_party/yyjson -Ithird_party/moodycamel -I/usr/local/include"
LDFLAGS="-L/usr/local/lib -lssl -lcrypto -lpthread"

cd "$(dirname "$0")"
rm -rf build_remote
mkdir -p build_remote

# yyjson (C)
$CXX $CXXFLAGS $INC -c third_party/yyjson/yyjson.c -o build_remote/yyjson.o

# common
for f in src/common/hash.cpp src/common/file_util.cpp src/common/net_util.cpp; do
    $CXX $CXXFLAGS $INC -c $f -o build_remote/$(basename $f .cpp).o
done

# cdc
$CXX $CXXFLAGS $INC -c src/cdc/fastcdc.cpp -o build_remote/fastcdc.o

# seed
for f in src/seed/seed_reader.cpp src/seed/seed_writer.cpp; do
    $CXX $CXXFLAGS $INC -c $f -o build_remote/$(basename $f .cpp).o
done

# daemon
for f in src/daemon/chunk_assembler.cpp src/daemon/io_worker.cpp src/daemon/verify_worker.cpp src/daemon/segment_io.cpp src/daemon/scheduler.cpp src/daemon/protocol.cpp src/daemon/peer_session.cpp src/daemon/peer_manager.cpp src/daemon/task_manager.cpp src/daemon/ipc_server.cpp src/daemon/tracker_client.cpp src/daemon/tracker_server.cpp src/daemon/tracker_acceptor.cpp src/daemon/sendfile_pool.cpp src/daemon/file_read_pool.cpp; do
    $CXX $CXXFLAGS $INC -c $f -o build_remote/$(basename $f .cpp).o
done

# main
$CXX $CXXFLAGS $INC -c src/daemon/main.cpp -o build_remote/main.o

# link thinbtd
$CXX $CXXFLAGS build_remote/*.o -o build_remote/thinbtd $LDFLAGS

# compile + link tbt
for f in src/cli/tbt.cpp src/cli/cli_commands.cpp; do
    $CXX $CXXFLAGS $INC -c $f -o build_remote/$(basename $f .cpp).o
done
CLI_OBJS="build_remote/yyjson.o build_remote/hash.o build_remote/file_util.o build_remote/net_util.o build_remote/fastcdc.o build_remote/seed_reader.o build_remote/seed_writer.o build_remote/tbt.o build_remote/cli_commands.o"
$CXX $CXXFLAGS $CLI_OBJS -o build_remote/tbt $LDFLAGS

echo "=== BUILD OK ==="
ls -lh build_remote/thinbtd build_remote/tbt
