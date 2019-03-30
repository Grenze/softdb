//
// Created by lingo on 19-2-26.
//


#include "merger.h"

#include "softdb/comparator.h"
#include "softdb/iterator.h"
#include "iterator_wrapper.h"

namespace softdb {

namespace {
class MergingIterator : public Iterator {
public:
    MergingIterator(const Comparator* comparator, Iterator** children, int n)
            : comparator_(comparator),
              children_(new IteratorWrapper[n]),
              n_(n),
              current_(nullptr),
              direction_(kForward) {
        for (int i = 0; i < n; i++) {
            children_[i].Set(children[i]);
        }
    }

    virtual ~MergingIterator() {
        delete[] children_;
    }

    virtual bool Valid() const {
        return (current_ != nullptr);
    }

    virtual void SeekToFirst() {
        for (int i = 0; i < n_; i++) {
            children_[i].SeekToFirst();
        }
        FindSmallest();
        direction_ = kForward;
    }

    virtual void SeekToLast() {
        for (int i = 0; i < n_; i++) {
            children_[i].SeekToLast();
        }
        FindLargest();
        direction_ = kReverse;
    }

    virtual void Seek(const Slice& target) {
        for (int i = 0; i < n_; i++) {
            children_[i].Seek(target);
        }
        FindSmallest();
        direction_ = kForward;
    }

    virtual void Next() {
        assert(Valid());

        // Ensure that all children are positioned after key().
        // If we are moving in the forward direction, it is already
        // true for all of the non-current_ children since current_ is
        // the smallest child and key() == current_->key().  Otherwise,
        // we explicitly position the non-current_ children.
        if (direction_ != kForward) {
            for (int i = 0; i < n_; i++) {
                IteratorWrapper* child = &children_[i];
                if (child != current_) {
                    child->Seek(key());
                    if (child->Valid() &&
                        comparator_->Compare(key(), child->key()) == 0) {
                        child->Next();
                    }
                }
            }
            direction_ = kForward;
        }

        current_->Next();
        FindSmallest();
    }

    virtual void Prev() {
        assert(Valid());

        // Ensure that all children are positioned before key().
        // If we are moving in the reverse direction, it is already
        // true for all of the non-current_ children since current_ is
        // the largest child and key() == current_->key().  Otherwise,
        // we explicitly position the non-current_ children.
        if (direction_ != kReverse) {
            for (int i = 0; i < n_; i++) {
                IteratorWrapper* child = &children_[i];
                if (child != current_) {
                    child->Seek(key());
                    if (child->Valid()) {
                        // Child is at first entry >= key().  Step back one to be < key()
                        child->Prev();
                    } else {
                        // Child has no entries >= key().  Position at last entry.
                        child->SeekToLast();
                    }
                }
            }
            direction_ = kReverse;
        }

        current_->Prev();
        FindLargest();
    }

    virtual Slice key() const {
        assert(Valid());
        return current_->key();
    }

    virtual Slice value() const {
        assert(Valid());
        return current_->value();
    }

    virtual Status status() const {
        Status status;
        for (int i = 0; i < n_; i++) {
            status = children_[i].status();
            if (!status.ok()) {
                break;
            }
        }
        return status;
    }

    virtual Slice Raw() const {
        assert(Valid());
        return current_->Raw();
    }

    virtual Slice RawKey() const {
        assert(Valid());
        return current_->RawKey();
    }

private:
    void FindSmallest();
    void FindLargest();

    // We might want to use a heap in case there are lots of children.
    // For now we use a simple array since we expect a very small number
    // of children in softdb.
    // For instance, in level-3 there is a file overlaps more than 100 files in level-4,
    // Will it be a disaster to finish the compact? Solution: See ShouldStopBefore.
    const Comparator* comparator_;
    IteratorWrapper* children_;
    int n_;
    IteratorWrapper* current_;

    // Which direction is the iterator moving?
    enum Direction {
        kForward,
        kReverse
    };
    Direction direction_;
};

void MergingIterator::FindSmallest() {
    IteratorWrapper* smallest = nullptr;
    for (int i = 0; i < n_; i++) {
        IteratorWrapper* child = &children_[i];
        if (child->Valid()) {
            if (smallest == nullptr) {
                smallest = child;
            } else if (comparator_->Compare(child->key(), smallest->key()) < 0) {
                smallest = child;
            }
        }
    }
    current_ = smallest;
}

void MergingIterator::FindLargest() {
    IteratorWrapper* largest = nullptr;
    for (int i = n_-1; i >= 0; i--) {
        IteratorWrapper* child = &children_[i];
        if (child->Valid()) {
            if (largest == nullptr) {
                largest = child;
            } else if (comparator_->Compare(child->key(), largest->key()) > 0) {
                largest = child;
            }
        }
    }
    current_ = largest;
}
}  // namespace

Iterator* NewMergingIterator(const Comparator* cmp, Iterator** list, int n) {
    assert(n >= 0);
    if (n == 0) {
        return NewEmptyIterator();
    } else if (n == 1) {
        return list[0];
    } else {
        return new MergingIterator(cmp, list, n);
    }
}

}  // namespace softdb