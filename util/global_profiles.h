//
// Created by lingo on 20-4-14.
//

//#define write_amp
#define split_up

#ifdef split_up

#ifndef SOFTDB_GLOBAL_PROFILES_H
#define SOFTDB_GLOBAL_PROFILES_H

#include <iostream>
#include <atomic>
#include <chrono>

namespace profiles {

extern std::atomic<uint64_t> DBImpl_Get;

extern std::atomic<uint64_t> mutex_wait;
extern std::atomic<uint64_t> mems_Get;
extern std::atomic<uint64_t> Version_Get;

extern std::atomic<uint64_t> read_lock_wait;
extern std::atomic<uint64_t> index_stab;
extern std::atomic<uint64_t> interval_Get;
extern std::atomic<uint64_t> compact;

inline static void Clear() {
    DBImpl_Get = 0;
    mutex_wait = 0;
    mems_Get = 0;
    Version_Get = 0;
    read_lock_wait = 0;
    index_stab = 0;
    interval_Get = 0;
    compact = 0;
}

inline static void Message(std::ostream& os) {
    os << "DBImpl_Get: \t" << DBImpl_Get << "\n";
    os << "mutex_wait: \t" << mutex_wait << "\n";
    os << "mems_Get: \t" << mems_Get << "\n";
    os << "Version_Get: \t" << Version_Get << "\n";
    os << "read_lock_wait: \t" << read_lock_wait << "\n";
    os << "index_stab: \t" << index_stab << "\n";
    os << "interval_Get: \t" << interval_Get << "\n";
    os << "compact: \t" << compact << "\n";
}

inline static uint64_t NowNanos() {
    return static_cast<uint64_t>(::std::chrono::duration_cast<::std::chrono::nanoseconds>(
            ::std::chrono::steady_clock::now().time_since_epoch())
            .count());
}

};


#endif //SOFTDB_GLOBAL_PROFILES_H

#endif