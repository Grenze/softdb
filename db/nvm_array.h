//
// Created by lingo on 19-12-12.
//

#ifndef SOFTDB_NVM_ARRAY_H
#define SOFTDB_NVM_ARRAY_H

#include <assert.h>
#include "softdb/iterator.h"

namespace softdb {

template<typename Key, class Comparator>
class NvmArray {
private:
    struct Node;

public:

    // Create a new NvmArray object that will use "cmp" for comparing keys.
    // cap+2(head_ and tail_)
    explicit NvmArray(Comparator cmp, int cap);

    ~NvmArray();

    // Returns true iff an entry that compares equal to key is in the array.
    bool Contains(const Key& key) const;

    // Returns inserted keys count.
    inline int GetCount() const { return num_; }

    // Iteration over the contents of a nvm array
    class Iterator {
    public:
        // Initialize an iterator over array.
        // The returned iterator is not valid.
        explicit Iterator(const NvmArray* array);

        // Returns true iff the iterator is positioned at a valid node.
        bool Valid() const;

        // Returns the key at current node.
        // REQUIRES: Valid()
        const Key& key() const;

        // Advances to the next position.
        // REQUIRES: Valid()
        void Next();

        // Advances to the previous position.
        // REQUIRES: Valid()
        void Prev();

        // Advances to the first entry with a key >= target
        void Seek(const Key& target);

        // Position at the first entry in array.
        // Final state of iterator is Valid() iff array is not empty.
        void SeekToFirst();

        // Position at the last entry in array.
        // Final state of iterator is Valid() iff array is not empty.
        void SeekToLast();

        // Supported by cuckoo hash. range[1, num_]
        void Jump(const uint32_t& pos);

        // Supported by cuckoo hash. range[1, num_]
        void WaveSearch(const Key& target);

        // Set node's key's status to obsolete, delete the key while GC
        void Abandon();

        // Get node's key's status while GC
        bool KeyIsObsolete() const;

    private:
        const NvmArray* array_;
        Node* node_;
        // Intentionally copyable
    };

    class Worker {
    public:
        explicit Worker(NvmArray* array)
                        : array_(array),
                          node_(array_->head_ + 1) { }
        bool Insert(const Key& key);
    private:
        NvmArray* array_;
        Node* node_;
    };

private:

    // Immutable after construction
    Comparator const compare_;

    int num_; // number of keys

    const int capacity;

    Node* const nodes_;

    Node* const head_;

    Node* tail_;

    bool Equal(const Key& a, const Key& b) const { return (compare_(a, b) == 0); }

    int KeyNodeComp(const Key& key, Node* n) const;

    // Return the earliest node that comes at or after key
    // Return nullptr if there is no such node.
    // anchor = nullptr indicates from first element of array.
    Node* FindGreaterOrEqual(const Key& key, Node* anchor = nullptr) const;

    // No copying allowed
    NvmArray(const NvmArray&);
    void operator=(const NvmArray);


};

template<typename Key, class Comparator>
struct NvmArray<Key, Comparator>::Node {
    explicit Node() : obsolete(false) { }
    Key key;
    bool obsolete;
};

template<typename Key, class Comparator>
inline NvmArray<Key, Comparator>::Iterator::Iterator(const NvmArray* array) {
    array_ = array;
    node_ = array_->head_;
}

template<typename Key, class Comparator>
inline bool NvmArray<Key, Comparator>::Iterator::Valid() const {
    return (node_ != array_->head_ && node_ != array_->tail_);
}

template<typename Key, class Comparator>
inline const Key& NvmArray<Key, Comparator>::Iterator::key() const {
    assert(Valid());
    return node_->key;
}

template<typename Key, class Comparator>
inline void NvmArray<Key, Comparator>::Iterator::Next() {
    assert(Valid());
    node_++;
}

template<typename Key, class Comparator>
inline void NvmArray<Key, Comparator>::Iterator::Prev() {
    assert(Valid());
    node_--;
}

template<typename Key, class Comparator>
inline void NvmArray<Key, Comparator>::Iterator::Seek(const Key &target) {
    // node_ can be (head_, tail_], if tail_, node_ is invalid
    node_ = array_->FindGreaterOrEqual(target);
}

template<typename Key, class Comparator>
inline void NvmArray<Key, Comparator>::Iterator::SeekToFirst() {
    node_ = array_->head_ + 1;
}

template<typename Key, class Comparator>
inline void NvmArray<Key, Comparator>::Iterator::SeekToLast() {
    node_ = array_->tail_ - 1;
}

// Jump according to hash result, but still no idea about user key correction
template<typename Key, class Comparator>
inline void NvmArray<Key, Comparator>::Iterator::Jump(const uint32_t &pos) {
    assert(pos > 0 && pos <= array_->num_);
    node_ = array_->head_ + pos;
}

// REQUIRES: Iterator::Jump() has been called and user key is correct.
template<typename Key, class Comparator>
inline void NvmArray<Key, Comparator>::Iterator::WaveSearch(const Key &target) {
    node_ = array_->FindGreaterOrEqual(target, node_);
}

template<typename Key, class Comparator>
inline void NvmArray<Key, Comparator>::Iterator::Abandon() {
    assert(Valid());
    node_->obsolete = true;
}

template<typename Key, class Comparator>
inline bool NvmArray<Key, Comparator>::Iterator::KeyIsObsolete() const {
    assert(Valid());
    return node_->obsolete;
}

// Return false iff full.
template<typename Key, class Comparator>
bool NvmArray<Key, Comparator>::Worker::Insert(const Key &key) {
    assert(node_ == array_->tail_);
    array_->tail_++;
    array_->num_++;
    node_->key = key;
    node_++;
    return array_->num_ != array_->capacity;
}

// > 0 : key is after node
template<typename Key, class Comparator>
inline int NvmArray<Key, Comparator>::KeyNodeComp(const Key& key, Node* n) const {
    return compare_(key, n->key);
}

template<typename Key, class Comparator>
typename NvmArray<Key, Comparator>::Node* NvmArray<Key, Comparator>::FindGreaterOrEqual(
                            const Key &key, Node *anchor) const {
    //assert(anchor == nullptr || (anchor - head_ > 0 && tail_ - anchor > 0));
    uint32_t left = (anchor == nullptr) ? 1 : anchor - head_;
    uint32_t right = num_;
    uint32_t medium = 0;
    while (left <= right) {
        medium = (left + right) / 2;
        int split = KeyNodeComp(key, nodes_ + medium);
        if (split > 0) {
            left = medium + 1;
        } else if(split < 0) {
            right = medium - 1;
        } else {
            return nodes_ + medium;
        }
    }
    return nodes_ + left;
}

template<typename Key, class Comparator>
NvmArray<Key, Comparator>::NvmArray(Comparator cmp, int cap)
        : compare_(cmp),
          num_(0),
          capacity(cap),
          nodes_(new Node[cap + 2]),
          head_(&nodes_[0]),
          tail_(&nodes_[1]) {

          }

template<typename Key, class Comparator>
NvmArray<Key, Comparator>::~NvmArray() {
    delete[] nodes_;
}

template<typename Key, class Comparator>
bool NvmArray<Key,Comparator>::Contains(const Key &key) const {
    Node* x = FindGreaterOrEqual(key);
    if (x != tail_ && Equal(key, x->key)) {
        return true;
    } else {
        return false;
    }
}

}


#endif //SOFTDB_NVM_ARRAY_H
