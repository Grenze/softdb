//
// Created by lingo on 19-1-15.
//

#ifndef SOFTDB_ATOMIC_POINTER_H
#define SOFTDB_ATOMIC_POINTER_H


#include <stdint.h>

#include <atomic>

#ifdef OS_WIN
#include <windows.h>
#endif

#if defined(_M_X64) || defined(__x86_64__)
#define ARCH_CPU_X86_FAMILY 1
#elif defined(_M_IX86) || defined(__i386__) || defined(__i386)
#define ARCH_CPU_X86_FAMILY 1
#elif defined(__ARMEL__)
#define ARCH_CPU_ARM_FAMILY 1
#elif defined(__aarch64__)
#define ARCH_CPU_ARM64_FAMILY 1
#elif defined(__ppc__) || defined(__powerpc__) || defined(__powerpc64__)
#define ARCH_CPU_PPC_FAMILY 1
#elif defined(__mips__)
#define ARCH_CPU_MIPS_FAMILY 1
#endif

namespace softdb {
    namespace port {

// Define MemoryBarrier() if available
// Windows on x86
#if defined(OS_WIN) && defined(COMPILER_MSVC) && defined(ARCH_CPU_X86_FAMILY)
        // windows.h already provides a MemoryBarrier(void) macro
// http://msdn.microsoft.com/en-us/library/ms684208(v=vs.85).aspx
#define softdb_HAVE_MEMORY_BARRIER

// Mac OS
#elif defined(__APPLE__)
        inline void MemoryBarrier() {
  std::atomic_thread_fence(std::memory_order_seq_cst);
}
#define softdb_HAVE_MEMORY_BARRIER

// Gcc on x86
#elif defined(ARCH_CPU_X86_FAMILY) && defined(__GNUC__)
        inline void MemoryBarrier() {
            // See http://gcc.gnu.org/ml/gcc/2003-04/msg01180.html for a discussion on
            // this idiom. Also see http://en.wikipedia.org/wiki/Memory_ordering.
            __asm__ __volatile__("" : : : "memory");
        }
#define softdb_HAVE_MEMORY_BARRIER

// Sun Studio
#elif defined(ARCH_CPU_X86_FAMILY) && defined(__SUNPRO_CC)
        inline void MemoryBarrier() {
  // See http://gcc.gnu.org/ml/gcc/2003-04/msg01180.html for a discussion on
  // this idiom. Also see http://en.wikipedia.org/wiki/Memory_ordering.
  asm volatile("" : : : "memory");
}
#define softdb_HAVE_MEMORY_BARRIER

// ARM Linux
#elif defined(ARCH_CPU_ARM_FAMILY) && defined(__linux__)
typedef void (*LinuxKernelMemoryBarrierFunc)(void);
// The Linux ARM kernel provides a highly optimized device-specific memory
// barrier function at a fixed memory address that is mapped in every
// user-level process.
//
// This beats using CPU-specific instructions which are, on single-core
// devices, un-necessary and very costly (e.g. ARMv7-A "dmb" takes more
// than 180ns on a Cortex-A8 like the one on a Nexus One). Benchmarking
// shows that the extra function call cost is completely negligible on
// multi-core devices.
//
inline void MemoryBarrier() {
  (*(LinuxKernelMemoryBarrierFunc)0xffff0fa0)();
}
#define softdb_HAVE_MEMORY_BARRIER

// ARM64
#elif defined(ARCH_CPU_ARM64_FAMILY)
inline void MemoryBarrier() {
  asm volatile("dmb sy" : : : "memory");
}
#define softdb_HAVE_MEMORY_BARRIER

// PPC
#elif defined(ARCH_CPU_PPC_FAMILY) && defined(__GNUC__)
inline void MemoryBarrier() {
  // TODO for some powerpc expert: is there a cheaper suitable variant?
  // Perhaps by having separate barriers for acquire and release ops.
  asm volatile("sync" : : : "memory");
}
#define softdb_HAVE_MEMORY_BARRIER

// MIPS
#elif defined(ARCH_CPU_MIPS_FAMILY) && defined(__GNUC__)
inline void MemoryBarrier() {
  __asm__ __volatile__("sync" : : : "memory");
}
#define softdb_HAVE_MEMORY_BARRIER

#endif

// AtomicPointer built using platform-specific MemoryBarrier().
#if defined(softdb_HAVE_MEMORY_BARRIER)
        class AtomicPointer {
        private:
            void* rep_;
        public:
            AtomicPointer() { }
            explicit AtomicPointer(void* p) : rep_(p) {}
            inline void* NoBarrier_Load() const { return rep_; }
            inline void NoBarrier_Store(void* v) { rep_ = v; }
            inline void* Acquire_Load() const {
                void* result = rep_;
                MemoryBarrier();
                return result;
            }
            inline void Release_Store(void* v) {
                MemoryBarrier();
                rep_ = v;
            }
        };

// AtomicPointer based on C++11 <atomic>.
#else
        class AtomicPointer {
 private:
  std::atomic<void*> rep_;
 public:
  AtomicPointer() { }
  explicit AtomicPointer(void* v) : rep_(v) { }
  inline void* Acquire_Load() const {
    return rep_.load(std::memory_order_acquire);
  }
  inline void Release_Store(void* v) {
    rep_.store(v, std::memory_order_release);
  }
  inline void* NoBarrier_Load() const {
    return rep_.load(std::memory_order_relaxed);
  }
  inline void NoBarrier_Store(void* v) {
    rep_.store(v, std::memory_order_relaxed);
  }
};

#endif

#undef softdb_HAVE_MEMORY_BARRIER
#undef ARCH_CPU_X86_FAMILY
#undef ARCH_CPU_ARM_FAMILY
#undef ARCH_CPU_ARM64_FAMILY
#undef ARCH_CPU_PPC_FAMILY

    }  // namespace port
}  // namespace softdb


#endif //SOFTDB_ATOMIC_POINTER_H