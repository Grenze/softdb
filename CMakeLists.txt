cmake_minimum_required(VERSION 3.9)
project(softdb VERSION 0.1 LANGUAGES C CXX)

# This project can use C11, but will gracefully decay down to C89.
set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED OFF)
set(CMAKE_C_EXTENSIONS OFF)

#add_definitions(-DNDEBUG)

# C11 apparently faster than C17.
# This project requires C++11.
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(SOFTDB_BUILD_TESTS "Build SOFTDB's unit tests" OFF)
option(SOFTDB_BUILD_BENCHMARKS "Build SOFTDB's benchmarks" OFF)
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

add_library(softdb "")
target_sources(softdb
        PRIVATE
        "${PROJECT_BINARY_DIR}/${SOFTDB_PORT_CONFIG_DIR}/port_config.h"
        "${PROJECT_SOURCE_DIR}/db/db_impl.cpp"
        "${PROJECT_SOURCE_DIR}/db/db_impl.h"
        "${PROJECT_SOURCE_DIR}/db/db_iter.cpp"
        "${PROJECT_SOURCE_DIR}/db/db_iter.h"
        "${PROJECT_SOURCE_DIR}/db/dbformat.cpp"
        "${PROJECT_SOURCE_DIR}/db/dbformat.h"
        "${PROJECT_SOURCE_DIR}/db/filename.cpp"
        "${PROJECT_SOURCE_DIR}/db/filename.h"
        "${PROJECT_SOURCE_DIR}/db/log_format.h"
        "${PROJECT_SOURCE_DIR}/db/log_reader.cpp"
        "${PROJECT_SOURCE_DIR}/db/log_reader.h"
        "${PROJECT_SOURCE_DIR}/db/log_writer.cpp"
        "${PROJECT_SOURCE_DIR}/db/log_writer.h"
        "${PROJECT_SOURCE_DIR}/db/memtable.cpp"
        "${PROJECT_SOURCE_DIR}/db/memtable.h"
        "${PROJECT_SOURCE_DIR}/db/nvm_index.h"
        "${PROJECT_SOURCE_DIR}/db/nvm_memtable.cpp"
        "${PROJECT_SOURCE_DIR}/db/nvm_memtable.h"
        "${PROJECT_SOURCE_DIR}/db/nvm_skiplist.h"
        "${PROJECT_SOURCE_DIR}/db/nvm_array.h"
        "${PROJECT_SOURCE_DIR}/db/skiplist.h"
        "${PROJECT_SOURCE_DIR}/db/snapshot.h"
        "${PROJECT_SOURCE_DIR}/db/version_set.cpp"
        "${PROJECT_SOURCE_DIR}/db/version_set.h"
        "${PROJECT_SOURCE_DIR}/db/write_batch_internal.h"
        "${PROJECT_SOURCE_DIR}/db/write_batch.cpp"
        "${PROJECT_SOURCE_DIR}/port/atomic_pointer.h"
        "${PROJECT_SOURCE_DIR}/port/port_stdcxx.h"
        "${PROJECT_SOURCE_DIR}/port/port.h"
        "${PROJECT_SOURCE_DIR}/port/thread_annotations.h"
        "${PROJECT_SOURCE_DIR}/table/iterator_wrapper.h"
        "${PROJECT_SOURCE_DIR}/table/iterator.cpp"
        "${PROJECT_SOURCE_DIR}/table/merger.cpp"
        "${PROJECT_SOURCE_DIR}/table/merger.h"
        "${PROJECT_SOURCE_DIR}/util/arena.cpp"
        "${PROJECT_SOURCE_DIR}/util/arena.h"
        "${PROJECT_SOURCE_DIR}/util/basetable.h"
        "${PROJECT_SOURCE_DIR}/util/bitsutil.h"
        "${PROJECT_SOURCE_DIR}/util/cuckoofilter.h"
        "${PROJECT_SOURCE_DIR}/util/coding.cpp"
        "${PROJECT_SOURCE_DIR}/util/coding.h"
        "${PROJECT_SOURCE_DIR}/util/comparator.cpp"
        "${PROJECT_SOURCE_DIR}/util/crc32c.cpp"
        "${PROJECT_SOURCE_DIR}/util/crc32c.h"
        "${PROJECT_SOURCE_DIR}/util/env.cpp"
        "${PROJECT_SOURCE_DIR}/util/hashtable.h"
        "${PROJECT_SOURCE_DIR}/util/hashutil.h"
        "${PROJECT_SOURCE_DIR}/util/histogram.h"
        "${PROJECT_SOURCE_DIR}/util/histogram.cpp"
        "${PROJECT_SOURCE_DIR}/util/logging.cpp"
        "${PROJECT_SOURCE_DIR}/util/logging.h"
        "${PROJECT_SOURCE_DIR}/util/mutexlock.h"
        "${PROJECT_SOURCE_DIR}/util/no_destructor.h"
        "${PROJECT_SOURCE_DIR}/util/options.cpp"
        "${PROJECT_SOURCE_DIR}/util/random.h"
        "${PROJECT_SOURCE_DIR}/util/singletable.h"
        "${PROJECT_SOURCE_DIR}/util/status.cpp"
        "${PROJECT_SOURCE_DIR}/util/testutil.cpp"
        "${PROJECT_SOURCE_DIR}/util/testutil.h"

        # Only CMake 3.3+ supports PUBLIC sources in targets exported by "install".
        $<$<VERSION_GREATER:CMAKE_VERSION,3.2>:PUBLIC>
        "${SOFTDB_PUBLIC_INCLUDE_DIR}/comparator.h"
        "${SOFTDB_PUBLIC_INCLUDE_DIR}/db.h"
        "${SOFTDB_PUBLIC_INCLUDE_DIR}/env.h"
        "${SOFTDB_PUBLIC_INCLUDE_DIR}/export.h"
        "${SOFTDB_PUBLIC_INCLUDE_DIR}/iterator.h"
        "${SOFTDB_PUBLIC_INCLUDE_DIR}/options.h"
        "${SOFTDB_PUBLIC_INCLUDE_DIR}/slice.h"
        "${SOFTDB_PUBLIC_INCLUDE_DIR}/status.h"
        "${SOFTDB_PUBLIC_INCLUDE_DIR}/write_batch.h"
        )

# POSIX code is specified separately so we can leave it out in the future.
target_sources(softdb
        PRIVATE
        "${PROJECT_SOURCE_DIR}/util/env_posix.cpp"
        "${PROJECT_SOURCE_DIR}/util/posix_logger.h"
        )


target_include_directories(softdb
        PUBLIC
        $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
        )
target_compile_definitions(softdb
        PRIVATE
        # Used by include/export.h when building shared libraries.
        SOFTDB_COMPILE_LIBRARY
        # Used by port/port.h.
        SOFTDB_PLATFORM_POSIX=1
        )
if (NOT HAVE_CXX17_HAS_INCLUDE)
    target_compile_definitions(softdb
            PRIVATE
            SOFTDB_HAS_PORT_CONFIG_H=1
            )
endif(NOT HAVE_CXX17_HAS_INCLUDE)

if(BUILD_SHARED_LIBS)
    target_compile_definitions(softdb
            PUBLIC
            # Used by include/export.h.
            SOFTDB_SHARED_LIBRARY
            )
endif(BUILD_SHARED_LIBS)

if(HAVE_CLANG_THREAD_SAFETY)
    target_compile_options(softdb
            PUBLIC
            -Werror -Wthread-safety)
endif(HAVE_CLANG_THREAD_SAFETY)

if(HAVE_CRC32C)
    target_link_libraries(softdb crc32c)
endif(HAVE_CRC32C)
if(HAVE_SNAPPY)
    target_link_libraries(softdb snappy)
endif(HAVE_SNAPPY)
if(HAVE_TCMALLOC)
    target_link_libraries(softdb tcmalloc)
endif(HAVE_TCMALLOC)

# Needed by port_stdcxx.h
find_package(Threads REQUIRED)
target_link_libraries(softdb Threads::Threads)

add_executable(softdbtest
        "${PROJECT_SOURCE_DIR}/test/softdbtest.cpp"
        )
target_link_libraries(softdbtest softdb)

add_executable(db_bench
        "${PROJECT_SOURCE_DIR}/db/db_bench.cpp"
        )
target_link_libraries(db_bench softdb)






if(SOFTDB_BUILD_TESTS)
    enable_testing()

    function(softdb_test test_file)
        get_filename_component(test_target_name "${test_file}" NAME_WE)

        add_executable("${test_target_name}" "")
        target_sources("${test_target_name}"
                PRIVATE
                "${PROJECT_BINARY_DIR}/${SOFTDB_PORT_CONFIG_DIR}/port_config.h"
                "${PROJECT_SOURCE_DIR}/util/testharness.cpp"
                "${PROJECT_SOURCE_DIR}/util/testharness.h"
                "${PROJECT_SOURCE_DIR}/util/testutil.cpp"
                "${PROJECT_SOURCE_DIR}/util/testutil.h"

                "${test_file}"
                )
        target_link_libraries("${test_target_name}" softdb)
        target_compile_definitions("${test_target_name}"
                PRIVATE
                SOFTDB_PLATFORM_POSIX=1
                )
        if (NOT HAVE_CXX17_HAS_INCLUDE)
            target_compile_definitions("${test_target_name}"
                    PRIVATE
                    SOFTDB_HAS_PORT_CONFIG_H=1
                    )
        endif(NOT HAVE_CXX17_HAS_INCLUDE)

        add_test(NAME "${test_target_name}" COMMAND "${test_target_name}")
    endfunction(softdb_test)

    softdb_test("${PROJECT_SOURCE_DIR}/util/env_test.cpp")
    softdb_test("${PROJECT_SOURCE_DIR}/util/status_test.cpp")
    softdb_test("${PROJECT_SOURCE_DIR}/util/no_destructor_test.cpp")

    if(NOT BUILD_SHARED_LIBS)
        softdb_test("${PROJECT_SOURCE_DIR}/db/autocompact_test.cpp")
        softdb_test("${PROJECT_SOURCE_DIR}/db/corruption_test.cpp")
        softdb_test("${PROJECT_SOURCE_DIR}/db/db_test.cpp")
        softdb_test("${PROJECT_SOURCE_DIR}/db/dbformat_test.cpp")
        softdb_test("${PROJECT_SOURCE_DIR}/db/filename_test.cpp")
        softdb_test("${PROJECT_SOURCE_DIR}/db/log_test.cpp")
        softdb_test("${PROJECT_SOURCE_DIR}/db/recovery_test.cpp")
        softdb_test("${PROJECT_SOURCE_DIR}/db/skiplist_test.cpp")
        softdb_test("${PROJECT_SOURCE_DIR}/db/version_set_test.cpp")
        softdb_test("${PROJECT_SOURCE_DIR}/db/write_batch_test.cpp")


        softdb_test("${PROJECT_SOURCE_DIR}/util/arena_test.cpp")
        softdb_test("${PROJECT_SOURCE_DIR}/util/bloom_test.cpp")
        softdb_test("${PROJECT_SOURCE_DIR}/util/cache_test.cpp")
        softdb_test("${PROJECT_SOURCE_DIR}/util/coding_test.cpp")
        softdb_test("${PROJECT_SOURCE_DIR}/util/crc32c_test.cpp")
        softdb_test("${PROJECT_SOURCE_DIR}/util/hash_test.cpp")
        softdb_test("${PROJECT_SOURCE_DIR}/util/logging_test.cpp")

        # TODO(costan): This test also uses
        #               "${PROJECT_SOURCE_DIR}/util/env_posix_test_helper.h"
        softdb_test("${PROJECT_SOURCE_DIR}/util/env_posix_test.cpp")
    endif(NOT BUILD_SHARED_LIBS)
endif(SOFTDB_BUILD_TESTS)

if(SOFTDB_BUILD_BENCHMARKS)
    function(softdb_benchmark bench_file)
        get_filename_component(bench_target_name "${bench_file}" NAME_WE)

        add_executable("${bench_target_name}" "")
        target_sources("${bench_target_name}"
                PRIVATE
                "${PROJECT_BINARY_DIR}/${SOFTDB_PORT_CONFIG_DIR}/port_config.h"
                "${PROJECT_SOURCE_DIR}/util/histogram.cpp"
                "${PROJECT_SOURCE_DIR}/util/histogram.h"
                "${PROJECT_SOURCE_DIR}/util/testharness.cpp"
                "${PROJECT_SOURCE_DIR}/util/testharness.h"
                "${PROJECT_SOURCE_DIR}/util/testutil.cpp"
                "${PROJECT_SOURCE_DIR}/util/testutil.h"

                "${bench_file}"
                )
        target_link_libraries("${bench_target_name}" softdb)
        target_compile_definitions("${bench_target_name}"
                PRIVATE
                SOFTDB_PLATFORM_POSIX=1
                )
        if (NOT HAVE_CXX17_HAS_INCLUDE)
            target_compile_definitions("${bench_target_name}"
                    PRIVATE
                    SOFTDB_HAS_PORT_CONFIG_H=1
                    )
        endif(NOT HAVE_CXX17_HAS_INCLUDE)
    endfunction(softdb_benchmark)

    if(NOT BUILD_SHARED_LIBS)
        softdb_benchmark("${PROJECT_SOURCE_DIR}/db/db_bench.cpp")
    endif(NOT BUILD_SHARED_LIBS)

    check_library_exists(sqlite3 sqlite3_open "" HAVE_SQLITE3)
    if(HAVE_SQLITE3)
        softdb_benchmark("${PROJECT_SOURCE_DIR}/doc/bench/db_bench_sqlite3.cpp")
        target_link_libraries(db_bench_sqlite3 sqlite3)
    endif(HAVE_SQLITE3)

    # check_library_exists is insufficient here because the library names have
    # different manglings when compiled with clang or gcc, at least when installed
    # with Homebrew on Mac.
    set(OLD_CMAKE_REQURED_LIBRARIES ${CMAKE_REQUIRED_LIBRARIES})
    list(APPEND CMAKE_REQUIRED_LIBRARIES kyotocabinet)
    check_cxx_source_compiles("
#include <kcpolydb.h>

int main() {
  kyotocabinet::TreeDB* db = new kyotocabinet::TreeDB();
  delete db;
  return 0;
}
  "  HAVE_KYOTOCABINET)
    set(CMAKE_REQUIRED_LIBRARIES ${OLD_CMAKE_REQURED_LIBRARIES})
    if(HAVE_KYOTOCABINET)
        softdb_benchmark("${PROJECT_SOURCE_DIR}/doc/bench/db_bench_tree_db.cpp")
        target_link_libraries(db_bench_tree_db kyotocabinet)
    endif(HAVE_KYOTOCABINET)
endif(SOFTDB_BUILD_BENCHMARKS)

if(SOFTDB_INSTALL)
    include(GNUInstallDirs)
    install(TARGETS softdb
            EXPORT softdbTargets
            RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
            LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
            ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
            )
    install(
            FILES
            "${PROJECT_SOURCE_DIR}/${SOFTDB_PUBLIC_INCLUDE_DIR}/comparator.h"
            "${PROJECT_SOURCE_DIR}/${SOFTDB_PUBLIC_INCLUDE_DIR}/db.h"
            "${PROJECT_SOURCE_DIR}/${SOFTDB_PUBLIC_INCLUDE_DIR}/env.h"
            "${PROJECT_SOURCE_DIR}/${SOFTDB_PUBLIC_INCLUDE_DIR}/export.h"
            "${PROJECT_SOURCE_DIR}/${SOFTDB_PUBLIC_INCLUDE_DIR}/iterator.h"
            "${PROJECT_SOURCE_DIR}/${SOFTDB_PUBLIC_INCLUDE_DIR}/options.h"
            "${PROJECT_SOURCE_DIR}/${SOFTDB_PUBLIC_INCLUDE_DIR}/slice.h"
            "${PROJECT_SOURCE_DIR}/${SOFTDB_PUBLIC_INCLUDE_DIR}/status.h"
            "${PROJECT_SOURCE_DIR}/${SOFTDB_PUBLIC_INCLUDE_DIR}/write_batch.h"
            DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/softdb
    )

    include(CMakePackageConfigHelpers)
    write_basic_package_version_file(
            "${PROJECT_BINARY_DIR}/softdbConfigVersion.cmake"
            COMPATIBILITY SameMajorVersion
    )
    install(
            EXPORT softdbTargets
            NAMESPACE softdb::
            DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/softdb"
    )
    install(
            FILES
            "${PROJECT_SOURCE_DIR}/cmake/softdbConfig.cmake"
            "${PROJECT_BINARY_DIR}/softdbConfigVersion.cmake"
            DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/softdb"
    )
endif(SOFTDB_INSTALL)

