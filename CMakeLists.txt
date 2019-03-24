cmake_minimum_required(VERSION 3.9)
project(softdb)

# This project can use C11, but will gracefully decay down to C89.
set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED OFF)
set(CMAKE_C_EXTENSIONS OFF)

#C11 apparently faster than C17.
# This project requires C++11.
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_executable(softdb
        export.h
        slice.h
        status.cpp
        status.h
        db.h
        options.cpp
        options.h
        random.h
        port_config.h
        port_stdcxx.h
        thread_annotations.h
        atomic_pointer.h
        arena.h
        arena.cpp
        port.h
        skiplist.h
        memtable.cpp
        memtable.h
        dbformat.cpp
        dbformat.h
        comparator.cpp
        comparator.h
        no_destructor.h
        coding.cpp
        coding.h
        db_impl.cpp
        db_impl.h
        log_format.h
        crc32c.cpp
        crc32c.h
        env.cpp
        env.h
        log_writer.cpp
        log_writer.h
        log_reader.cpp
        log_reader.h
        write_batch.cpp
        write_batch.h
        write_batch_internal.h
        mutexlock.h
        filename.cpp
        filename.h
        logging.cpp
        logging.h
        version_set.cpp
        version_set.h
        test/softdbtest.cpp
        test/softdbtest.h
        posix_logger.h
        env_posix_test_helper.h
        env_posix.cpp
        iterator.cpp
        iterator.h
        nvm_skiplist.h
        nvm_memtable.cpp
        nvm_memtable.h
        nvm_interval.h nvm_ISL.h nvm_index.h hashutil.h bitsutil.h hashtable.h basetable.h snapshot.h db_iter.cpp db_iter.h iterator_wrapper.h merger.cpp merger.h db_bench.cpp histogram.cpp histogram.h testutil.cpp testutil.h)



# Needed by port_stdcxx.h
find_package(Threads REQUIRED)
target_link_libraries(softdb Threads::Threads)