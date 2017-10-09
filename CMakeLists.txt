cmake_minimum_required(VERSION 3.8)
project(rocksdb_manifest_dump)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_BUILD_TYPE RELEASE)
find_package(Threads REQUIRED)

#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -W -Wextra -Wall")
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wsign-compare -Wshadow -Wno-unused-parameter -Wno-unused-variable -Woverloaded-virtual -Wnon-virtual-dtor -Wno-missing-field-initializers")
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
#if(NOT CMAKE_BUILD_TYPE STREQUAL "Debug")
#    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-omit-frame-pointer")
#    include(CheckCXXCompilerFlag)
#    CHECK_CXX_COMPILER_FLAG("-momit-leaf-frame-pointer" HAVE_OMIT_LEAF_FRAME_POINTER)
#    if(HAVE_OMIT_LEAF_FRAME_POINTER)
#        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -momit-leaf-frame-pointer")
#    endif()
#endif()
#
#set(OPTIMIZE_DEBUG_DEFAULT 0)        # Debug build is unoptimized by default use -DOPTDBG=1 to optimize
#
#if(DEFINED OPTDBG)
#    set(OPTIMIZE_DEBUG ${OPTDBG})
#else()
#    set(OPTIMIZE_DEBUG ${OPTIMIZE_DEBUG_DEFAULT})
#endif()
#
#if(CMAKE_COMPILER_IS_GNUCXX)
#    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-builtin-memcmp")
#endif()
#
#option(ROCKSDB_LITE "Build RocksDBLite version" OFF)
#if(ROCKSDB_LITE)
#    add_definitions(-DROCKSDB_LITE)
#    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-exceptions")
#endif()

add_definitions(-DOS_LINUX)

add_definitions(-DROCKSDB_PLATFORM_POSIX -DROCKSDB_LIB_IO_POSIX)

#option(WITH_FALLOCATE "build with fallocate" ON)

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

add_executable(rocksdb_manifest_dump ${SOURCE_FILES})

TARGET_LINK_LIBRARIES(rocksdb_manifest_dump rocksdb)