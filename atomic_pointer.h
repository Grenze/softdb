//
// Created by lingo on 19-1-15.
//

#ifndef SOFTDB_ATOMIC_POINTER_H
#define SOFTDB_ATOMIC_POINTER_H


// port_config.h availability is automatically detected via __has_include
// in newer compilers. If SOFTDB_HAS_PORT_CONFIG_H is defined, it overrides the
// configuration detection.
#if defined(SOFTDB_HAS_PORT_CONFIG_H)

#if SOFTDB_HAS_PORT_CONFIG_H
#include "port_config.h"
#endif  // SOFTDB_HAS_PORT_CONFIG_H

#elif defined(__has_include)

#if __has_include("port_config.h")
#include "port_config.h"
#endif  // __has_include("port_config.h")

#endif  // defined(SOFTDB_HAS_PORT_CONFIG_H)

#if HAVE_CRC32C
#include <crc32c/crc32c.h>
#endif  // HAVE_CRC32C
#if HAVE_SNAPPY
#include <snappy.h>
#endif  // HAVE_SNAPPY

#include <stddef.h>
#include <stdint.h>
#include <cassert>
#include <condition_variable>  // NOLINT
#include <mutex>               // NOLINT
#include <string>
#include "atomic_pointer.h"
#include "thread_annotations.h"

namespace softdb {
    namespace port {

        //static const bool kLittleEndian = !SOFTDB_IS_BIG_ENDIAN;

        class CondVar;

// Thinly wraps std::mutex.
        class LOCKABLE Mutex {
                public:
                Mutex() = default;
                ~Mutex() = default;

                Mutex(const Mutex&) = delete;
                Mutex& operator=(const Mutex&) = delete;

                void Lock() EXCLUSIVE_LOCK_FUNCTION() { mu_.lock(); }
                void Unlock() UNLOCK_FUNCTION() { mu_.unlock(); }
                void AssertHeld() ASSERT_EXCLUSIVE_LOCK() { }

                private:
                friend class CondVar;
                std::mutex mu_;
        };

// Thinly wraps std::condition_variable.
        class CondVar {
        public:
            explicit CondVar(Mutex* mu) : mu_(mu) { assert(mu != nullptr); }
            ~CondVar() = default;

            CondVar(const CondVar&) = delete;
            CondVar& operator=(const CondVar&) = delete;

            void Wait() {
                std::unique_lock<std::mutex> lock(mu_->mu_, std::adopt_lock);
                cv_.wait(lock);
                lock.release();
            }
            void Signal() { cv_.notify_one(); }
            void SignalAll() { cv_.notify_all(); }
        private:
            std::condition_variable cv_;
            Mutex* const mu_;
        };

        inline bool Snappy_Compress(const char* input, size_t length,
                                    ::std::string* output) {
#if HAVE_SNAPPY
            output->resize(snappy::MaxCompressedLength(length));
  size_t outlen;
  snappy::RawCompress(input, length, &(*output)[0], &outlen);
  output->resize(outlen);
  return true;
#endif  // HAVE_SNAPPY

            return false;
        }

        inline bool Snappy_GetUncompressedLength(const char* input, size_t length,
                                                 size_t* result) {
#if HAVE_SNAPPY
            return snappy::GetUncompressedLength(input, length, result);
#else
            return false;
#endif  // HAVE_SNAPPY
        }

        inline bool Snappy_Uncompress(const char* input, size_t length, char* output) {
#if HAVE_SNAPPY
            return snappy::RawUncompress(input, length, output);
#else
            return false;
#endif  // HAVE_SNAPPY
        }

        inline bool GetHeapProfile(void (*func)(void*, const char*, int), void* arg) {
            return false;
        }

        inline uint32_t AcceleratedCRC32C(uint32_t crc, const char* buf, size_t size) {
#if HAVE_CRC32C
            return ::crc32c::Extend(crc, reinterpret_cast<const uint8_t*>(buf), size);
#else
            return 0;
#endif  // HAVE_CRC32C
        }

    }  // namespace port
}  // namespace softdb

#endif //SOFTDB_ATOMIC_POINTER_H
