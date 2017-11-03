cmake_minimum_required(VERSION 2.8)
project(rocksdb_manifest_edit)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_BUILD_TYPE RELEASE)
find_package(Threads REQUIRED)

add_definitions(-DOS_LINUX)

add_definitions(-DROCKSDB_PLATFORM_POSIX -DROCKSDB_LIB_IO_POSIX)

include_directories(src)

include_directories(rocksdb)
include_directories(rocksdb/include)
include_directories(rocksdb/third-party/gtest-1.7.0/fused-src)

include_directories(terark-zip-rocksdb/src)

include_directories(terichdb/src)
include_directories(terichdb/terark-base/src)

add_subdirectory(rocksdb)

set(SOURCE_FILES
        src/manifest_process.cpp
        src/main.cpp)

add_executable(rocksdb_manifest_edit ${SOURCE_FILES})

TARGET_LINK_LIBRARIES(rocksdb_manifest_edit rocksdb)
