//
// Created by lingo on 19-2-12.
//

#ifndef SOFTDB_NVM_SKIPLIST_H
#define SOFTDB_NVM_SKIPLIST_H

#include <assert.h>
#include "iterator.h"
#include "random.h"

namespace softdb {


template<typename Key, class Comparator>
class NvmSkipList {
private:
    struct Node;

public:
    // Create a new NvmSkipList object that will use "cmp" for comparing keys,
    // its Nodes are arranged in form of array which length is num+2(head and tail).
    explicit NvmSkipList(Comparator cmp, int num);

    // Returns true iff an entry that compares equal to key is in the list.
    bool Contains(const Key& key) const;

    // Iteration over the contents of a nvm skip list
    class Iterator {
        // Initialize an iterator over the specified list.
        // The returned iterator is no valid.
        explicit Iterator(const NvmSkipList* list);

        // Returns true iff the iterator is positioned at a valid node.
        bool Valid() const;

        // Returns the key at the current position.
        // REQUIRES: Valid()
        const Key& key() const;

        // Advances to the next position.
        // REQUIRES: Valid()
        void Next();

        // Advances to the previous position.
        // REQUIRES: Valid()
        void Prev();

        // Advance to the first entry with a key >= target
        void Seek(const Key& target);

        // Position at the first entry in list.
        // Final state of iterator is Valid() iff list is not empty.
        void SeekToFirst();

        // Position at the last entry in list.
        // Final state of iterator is Valid() iff list is not empty.
        void SeekToLast();

        // imm_'s iter_ and nvm_imm_'s iter move parallel.
        void Insert(const Key& key, Node** prev);

        // Finish Insert (link prev.next[level] to tail_).
        void Finish(Node** prev);

    private:

        const NvmSkipList* list_;
        Node* node_;
        // Intentionally copyable
    };

private:
    enum { kMaxHeight = 12 };

    // Immutable after construction
    Comparator const compare_;

    Node* const nodes_;

    int const num_;

    Node* const head_; // (offset:0)

    Node* const tail_; // (offset:num_+1)

    int max_height; // Height of the entire list

    inline int GetMaxHeight() const {
        return max_height;
    }

    inline void SetMaxHeight(int h) {
        max_height = h;
    }

    Random rnd_;

    // Node* NewNode(const Key& key, int height);

    int RandomHeight();
    bool Equal(const Key& a, const Key& b) const { return (compare_(a, b) == 0); }

    // Return true iff key is greater than data stored in "n"
    bool KeyIsAfterNode(const Key& key, Node* n) const;

    // Return the earliest node that comes at or after key.
    // Return nullptr if there is no such node.
    //
    // if prev is non-null, fills prev[level] with pointer to previous
    // node at "level" for every level in [0..max_height -1].
    Node* FindGreaterOrEqual(const Key& key, Node** prev) const;

    // Return the latest node with a key < key.
    // Return head_ if there is no such node.
    Node* FindLessThan(const Key& key) const;

    // Return the last node in the list.
    // Return head_ if list is empty.
    Node* FindLast() const;

    // Return the node at offset n.
    Node* Locate(int n) const;

    // No copying allowed
    NvmSkipList(const NvmSkipList&);
    void operator=(const NvmSkipList);

};

// Implementation details follow
template<typename Key, class Comparator>
struct NvmSkipList<Key,Comparator>::Node {
    explicit Node() { };

    Key key;

    Node* Next(int n) {
        assert(n >= 0);
        return next_[n];
    }

    void SetNext(int n, Node* x) {
        next_[n] = x;
    }

    void SetHeight(int height) {
        next_ = new Node*[height];
    }

private:
    Node** next_;
};

template<typename Key, class Comparator>
inline NvmSkipList<Key,Comparator>::Iterator::Iterator(const NvmSkipList* list) {
    list_ = list;
    node_ = nullptr;
}

template<typename Key, class Comparator>
inline bool NvmSkipList<Key,Comparator>::Iterator::Valid() const {
    return (node_ != nullptr || node_ == list_->head_ || node_ == list_->tail_);
}

template<typename Key, class Comparator>
inline const Key& NvmSkipList<Key,Comparator>::Iterator::key() const {
    assert(Valid());
    return node_->key;
}

template<typename Key, class Comparator>
inline void NvmSkipList<Key,Comparator>::Iterator::Next() {
    assert(Valid());
    node_++;
}

template<typename Key, class Comparator>
inline void NvmSkipList<Key,Comparator>::Iterator::Prev() {
    assert(Valid());
    node_--;
}

// If cuckoo hash may spend less step to seek target,
// then use cuckoo hash to assist.
template<typename Key, class Comparator>
inline void NvmSkipList<Key,Comparator>::Iterator::Seek(const Key& target) {
    node_ = list_->FindGreaterOrEqual(target, nullptr);
}

template<typename Key, class Comparator>
inline void NvmSkipList<Key,Comparator>::Iterator::SeekToFirst() {
    node_ = list_->head_;
    node_++;
}

template<typename Key, class Comparator>
inline void NvmSkipList<Key,Comparator>::Iterator::SeekToLast() {
    node_ = list_->tail_;
    node_--;
}

// REQUIRES: Before first call, Node** prev should have been
// initiated to Node*[KMaxHeight] filled with head_,
// and SeekToFirst() has been called.
// When node_ == nodes_[num_], call Finish externally.
template<typename Key, class Comparator>
void NvmSkipList<Key,Comparator>::Iterator::Insert(const Key& key, Node** prev) {
    assert(Valid());
    node_->key = key;
    int height = list_->RandomHeight();
    node_->SetHeight(height);
    if (height > list_->GetMaxHeight()) {
        list_->SetMaxHeight(height);
    }
    for (int i = 0; i < height; i++) {
        prev[i]->SetNext(i, node_);
        prev[i] = node_;
    }
}

template<typename Key, class Comparator>
void NvmSkipList<Key,Comparator>::Iterator::Finish(Node** prev) {
    assert(node_ == list_->tail_);
    for (int i = 0; i < list_->kMaxHeight; i++) {
        prev[i]->SetNext(i, list_->tail_);
    }
}

template<typename Key, class Comparator>
int NvmSkipList<Key,Comparator>::RandomHeight() {
    // Increase height with probability 1 in kBranching
    static const unsigned int kBranching = 4;
    int height = 1;
    while (height < kMaxHeight && ((rnd_.Next() % kBranching) == 0)) {
        height++;
    }
    assert(height > 0);
    assert(height <= kMaxHeight);
    return height;
}

template<typename Key, class Comparator>
bool NvmSkipList<Key,Comparator>::KeyIsAfterNode(const Key& key, Node* n) const {
    // null n is considered infinite
    return (n != nullptr) && (compare_(n->key, key) < 0);
}

template<typename Key, class Comparator>
NvmSkipList<Key,Comparator>::NvmSkipList(Comparator cmp, int num)
            : compare_(cmp),
              num_(num),
              nodes_(new Node[num_+2]),
              head_(nodes_[0]),
              tail_(nodes_[num_+1]),
              max_height(1),
              rnd_(0xdeadbeef) {
    head_->SetHeight(kMaxHeight);
    for (int i = 0; i< kMaxHeight; i++) {
        head_->SetNext(i, tail_);
    }
}






}   // namespace softdb

#endif //SOFTDB_NVM_SKIPLIST_H
