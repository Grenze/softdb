//
// Created by lingo on 19-1-16.
//

#ifndef SOFTDB_MUTEXLOCK_H
#define SOFTDB_MUTEXLOCK_H


#include "port/port.h"
#include "port/thread_annotations.h"

namespace softdb {

// Helper class that locks a mutex on construction and unlocks the mutex when
// the destructor of the MutexLock object is invoked.
//
// Typical usage:
//
//   void MyClass::MyMethod() {
//     MutexLock l(&mu_);       // mu_ is an instance variable
//     ... some complex code, possibly with multiple return paths ...
//   }

    class SCOPED_LOCKABLE MutexLock {
            public:
            explicit MutexLock(port::Mutex *mu) EXCLUSIVE_LOCK_FUNCTION(mu)
            : mu_(mu)  {
                this->mu_->Lock();
            }
            ~MutexLock() UNLOCK_FUNCTION() { this->mu_->Unlock(); }

            MutexLock(const MutexLock&) = delete;
            MutexLock& operator=(const MutexLock&) = delete;

            private:
            port::Mutex *const mu_;
    };

}  // namespace softdb


#endif //SOFTDB_MUTEXLOCK_H
