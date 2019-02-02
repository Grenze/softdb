//
// Created by lingo on 19-1-15.
//

#ifndef SOFTDB_STATUS_H
#define SOFTDB_STATUS_H

#include <algorithm>
#include <string>
#include "slice.h"
#include "export.h"

namespace softdb {

    class SOFTDB_EXPORT Status {
            public:
            // Create a success status.
            Status() noexcept : state_(nullptr) { }
            ~Status() { delete[] state_; }

            Status(const Status& rhs);
            Status& operator=(const Status& rhs);

            Status(Status&& rhs) noexcept : state_(rhs.state_) { rhs.state_ = nullptr; }
            Status& operator=(Status&& rhs) noexcept;

            // Return a success status.
            static Status OK() { return Status(); }

            // Return error status of an appropriate type.
            static Status NotFound(const Slice& msg, const Slice& msg2 = Slice()) {
                return Status(kNotFound, msg, msg2);
            }
            static Status Corruption(const Slice& msg, const Slice& msg2 = Slice()) {
                return Status(kCorruption, msg, msg2);
            }
            static Status NotSupported(const Slice& msg, const Slice& msg2 = Slice()) {
                return Status(kNotSupported, msg, msg2);
            }
            static Status InvalidArgument(const Slice& msg, const Slice& msg2 = Slice()) {
                return Status(kInvalidArgument, msg, msg2);
            }
            static Status IOError(const Slice& msg, const Slice& msg2 = Slice()) {
                return Status(kIOError, msg, msg2);
            }

            // Returns true iff the status indicates success.
            bool ok() const { return (state_ == nullptr); }

            // Returns true iff the status indicates a NotFound error.
            bool IsNotFound() const { return code() == kNotFound; }

            // Returns true iff the status indicates a Corruption error.
            bool IsCorruption() const { return code() == kCorruption; }

            // Returns true iff the status indicates an IOError.
            bool IsIOError() const { return code() == kIOError; }

            // Returns true iff the status indicates a NotSupportedError.
            bool IsNotSupportedError() const { return code() == kNotSupported; }

            // Returns true iff the status indicates an InvalidArgument.
            bool IsInvalidArgument() const { return code() == kInvalidArgument; }

            // Return a string representation of this status suitable for printing.
            // Returns the string "OK" for success.
            std::string ToString() const;

            private:
            // OK status has a null state_.  Otherwise, state_ is a new[] array
            // of the following form:
            //    state_[0..3] == length of message
            //    state_[4]    == code
            //    state_[5..]  == message
            const char* state_;

            enum Code {
                kOk = 0,
                kNotFound = 1,
                kCorruption = 2,
                kNotSupported = 3,
                kInvalidArgument = 4,
                kIOError = 5
            };

            Code code() const {
                return (state_ == nullptr) ? kOk : static_cast<Code>(state_[4]);
            }

            Status(Code code, const Slice& msg, const Slice& msg2);
            static const char* CopyState(const char* s);
    };

    inline Status::Status(const Status& rhs) {
        state_ = (rhs.state_ == nullptr) ? nullptr : CopyState(rhs.state_);
    }
    inline Status& Status::operator=(const Status& rhs) {
        // The following condition catches both aliasing (when this == &rhs),
        // and the common case where both rhs and *this are ok.
        if (state_ != rhs.state_) {
            delete[] state_;
            state_ = (rhs.state_ == nullptr) ? nullptr : CopyState(rhs.state_);
        }
        return *this;
    }
    inline Status& Status::operator=(Status&& rhs) noexcept {
    std::swap(state_, rhs.state_);
    return *this;
}

}  // namespace softdb


#endif //SOFTDB_STATUS_H
