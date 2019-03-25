cmake_minimum_required(VERSION 3.9)
project(softdb VERSION 1.0.0 LANGUAGES C CXX)

# This project can use C11, but will gracefully decay down to C89.
set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED OFF)
set(CMAKE_C_EXTENSIONS OFF)

# C11 apparently faster than C17.
# This project requires C++11.
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(SOFTDB_BUILD_TESTS "Build SOFTDB's unit tests" ON)
option(SOFTDB_BUILD_BENCHMARKS "Build SOFTDB's benchmarks" ON)
option(SOFTDB_INSTALL "Install SOFTDB's header and library" ON)

include(TestBigEndian)
test_big_endian(SOFTDB_IS_BIG_ENDIAN)

include(CheckIncludeFile)
check_include_file("unistd.h" HAVE_UNISTD_H)

include(CheckLibraryExists)
check_library_exists(crc32c crc32c_value "" HAVE_CRC32C)
check_library_exists(tcmalloc malloc "" HAVE_TCMALLOC)

include(CheckSymbolExists)
check_symbol_exists(fdatasync "unistd.h" HAVE_FDATASYNC)


include(CheckCXXSourceCompiles)

# Test whether -Wthread-safety is available. See
# https://clang.llvm.org/docs/ThreadSafetyAnalysis.html
# -Werror is necessary because unknown attributes only generate warnings.
set(OLD_CMAKE_REQUIRED_FLAGS ${CMAKE_REQUIRED_FLAGS})
list(APPEND CMAKE_REQUIRED_FLAGS -Werror -Wthread-safety)
check_cxx_source_compiles("
struct __attribute__((lockable)) Lock {
  void Acquire() __attribute__((exclusive_lock_function()));
  void Release() __attribute__((unlock_function()));
};
struct ThreadSafeType {
  Lock lock_;
  int data_ __attribute__((guarded_by(lock_)));
};
int main() { return 0; }
"  HAVE_CLANG_THREAD_SAFETY)
set(CMAKE_REQUIRED_FLAGS ${OLD_CMAKE_REQUIRED_FLAGS})

# Test whether C++17 __has_include is available.
check_cxx_source_compiles("
#if defined(__has_include) &&  __has_include(<string>)
#include <string>
#endif
int main() { std::string str; return 0; }
" HAVE_CXX17_HAS_INCLUDE)

set(SOFTDB_PUBLIC_INCLUDE_DIR "include/softdb")
set(SOFTDB_PORT_CONFIG_DIR "include/port")

configure_file(
        "${PROJECT_SOURCE_DIR}/port/port_config.h.in"
        "${PROJECT_BINARY_DIR}/${SOFTDB_PORT_CONFIG_DIR}/port_config.h"
)


include_directories(
        "${PROJECT_BINARY_DIR}/include"
        "${PROJECT_SOURCE_DIR}"
)

if(BUILD_SHARED_LIBS)
    # Only export SOFTDB_EXPORT symbols from the shared library.
    add_compile_options(-fvisibility=hidden)
endif(BUILD_SHARED_LIBS)

add_executable(softdb
        include/softdb/export.h
        include/softdb/slice.h
        util/status.cpp
        include/softdb/status.h
        include/softdb/db.h
        util/options.cpp
        include/softdb/options.h
        util/random.h
        port/port_config.h
        port/port_stdcxx.h
        port/thread_annotations.h
        port/atomic_pointer.h
        util/arena.h
        util/arena.cpp
        port/port.h
        db/skiplist.h
        db/memtable.cpp
        db/memtable.h
        table/dbformat.cpp
        table/dbformat.h
        util/comparator.cpp
        include/softdb/comparator.h
        util/no_destructor.h
        util/coding.cpp
        util/coding.h
        db/db_impl.cpp
        db/db_impl.h
        db/log_format.h
        util/crc32c.cpp
        util/crc32c.h
        util/env.cpp
        include/softdb/env.h
        db/log_writer.cpp
        db/log_writer.h
        db/log_reader.cpp
        db/log_reader.h
        db/write_batch.cpp
        include/softdb/write_batch.h
        db/write_batch_internal.h
        util/mutexlock.h
        db/filename.cpp
        db/filename.h
        util/logging.cpp
        util/logging.h
        db/version_set.cpp
        db/version_set.h
        #test/softdbtest.cpp
        #test/softdbtest.h
        util/posix_logger.h
        util/env_posix_test_helper.h
        util/env_posix.cpp
        table/iterator.cpp
        include/softdb/iterator.h
        db/nvm_skiplist.h
        db/nvm_memtable.cpp
        db/nvm_memtable.h
        db/nvm_interval.h db/nvm_ISL.h db/nvm_index.h util/hashutil.h util/bitsutil.h util/hashtable.h util/basetable.h db/snapshot.h db/db_iter.cpp db/db_iter.h table/iterator_wrapper.h table/merger.cpp table/merger.h db/db_bench.cpp util/histogram.cpp util/histogram.h util/testutil.cpp util/testutil.h)



# Needed by port_stdcxx.h
find_package(Threads REQUIRED)
target_link_libraries(softdb Threads::Threads)