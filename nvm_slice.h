//
// Created by lingo on 19-2-16.
//

#ifndef SOFTDB_NVM_SLICE_H
#define SOFTDB_NVM_SLICE_H

#include <assert.h>
#include <stddef.h>
#include <string.h>
#include <string>
#include "export.h"

namespace softdb {

class InternalKeyComparator;
// put NvmSlice under version_set.h
class NvmSlice {
public:
    // Create an empty slice.
    NvmSlice(const InternalKeyComparator& comparator) : data_(""), size_(0), comparator_(comparator) { }

    // Create a slice that refers to d[0,n-1].
    NvmSlice(const InternalKeyComparator& comparator, const char* d, size_t n) : data_(d), size_(n), comparator_(comparator) { }

    // Create a slice that refers to the contents of "s"
    NvmSlice(const InternalKeyComparator& comparator, const std::string& s) : data_(s.data()), size_(s.size()), comparator_(comparator) { }

    // Create a slice that refers to s[0,strlen(s)-1]
    NvmSlice(const InternalKeyComparator& comparator, const char* s) : data_(s), size_(strlen(s)), comparator_(comparator)  { }

    // Intentionally copyable.
    NvmSlice(const NvmSlice&) = default;
    NvmSlice& operator=(const NvmSlice&) = default;

    // Return a pointer to the beginning of the referenced data
    const char* data() const { return data_; }

    // Return the length (in bytes) of the referenced data
    size_t size() const { return size_; }

    // Return true iff the length of the referenced data is zero
    bool empty() const { return size_ == 0; }

    // Return the ith byte in the referenced data.
    // REQUIRES: n < size()
    char operator[](size_t n) const {
        assert(n < size());
        return data_[n];
    }

    // Change this slice to refer to an empty array
    void clear() { data_ = ""; size_ = 0; }

    // Drop the first "n" bytes from this slice.
    void remove_prefix(size_t n) {
        assert(n <= size());
        data_ += n;
        size_ -= n;
    }

    // Return a string that contains the copy of the referenced data.
    std::string ToString() const { return std::string(data_, size_); }

    // Three-way comparison.  Returns value:
    //   <  0 iff "*this" <  "b",
    //   == 0 iff "*this" == "b",
    //   >  0 iff "*this" >  "b"
    int compare(const NvmSlice& b) const;

    // Return true iff "x" is a prefix of "*this"
    bool starts_with(const NvmSlice& x) const {
        return ((size_ >= x.size_) &&
                (memcmp(data_, x.data_, x.size_) == 0));
    }

    struct KeyComparator {
        const InternalKeyComparator comparator;
        // initialize the InternalKeyComparator
        explicit KeyComparator(const InternalKeyComparator& c) : comparator(c) { }
        // to use InternalKeyComparator, we need to extract internal key from entry.
        int operator()(const char*a, const char* b) const;
    };

    KeyComparator comparator_;

private:
    const char* data_;
    size_t size_;
};

// put things blow out of here, to where user comparator lies(versions_),
// there it will be easy to implement.
inline std::ostream& operator<<(std::ostream& os, const NvmSlice& x) {
    os << x.ToString();
    return os;
}

inline bool operator==(const NvmSlice& x, const NvmSlice& y) {
    return ((x.size() == y.size()) &&
            (memcmp(x.data(), y.data(), x.size()) == 0));
}

inline bool operator!=(const NvmSlice& x, const NvmSlice& y) {
    return !(x == y);
}

inline bool operator<(const NvmSlice& x, const NvmSlice& y) {
    return x.compare(y) < 0;
}

inline bool operator<=(const NvmSlice& x, const NvmSlice& y) {
    return x.compare(y) <= 0;
}

inline bool operator>(const NvmSlice& x, const NvmSlice& y) {
    return !(x <= y);
}

inline bool operator>=(const NvmSlice& x, const NvmSlice& y) {
    return !(x < y);
}



inline int NvmSlice::compare(const NvmSlice& b) const {
    const size_t min_len = (size_ < b.size_) ? size_ : b.size_;
    int r = memcmp(data_, b.data_, min_len);
    if (r == 0) {
        if (size_ < b.size_) r = -1;
        else if (size_ > b.size_) r = +1;
    }
    return r;
}



}


#endif //SOFTDB_NVM_SLICE_H
