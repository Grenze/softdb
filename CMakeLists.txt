cmake_minimum_required(VERSION 3.13)
project(softdb)

set(CMAKE_CXX_STANDARD 17)



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
        nvm_builder.cpp
        nvm_builder.h
        )




# Needed by port_stdcxx.h
find_package(Threads REQUIRED)
target_link_libraries(softdb Threads::Threads)