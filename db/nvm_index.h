//
// Created by lingo on 19-2-17.
//

#ifndef SOFTDB_NVM_INDEX_H
#define SOFTDB_NVM_INDEX_H

#include "dbformat.h"
#include "util/random.h"
#include "nvm_memtable.h"
#include "table/merger.h"
#include <vector>

namespace softdb {

// Decode raw key from index
static void Decode(const char* ptr, std::ostream& os) {
    Slice internal_key = GetLengthPrefixedSlice(ptr);
    const size_t n = internal_key.size();
    assert(n >= 8);
    uint64_t seq = DecodeFixed64(internal_key.data() + n - 8) >> 8;
    os << Slice(internal_key.data(), n - 8).ToString() << "[" << seq << "]";
}


// TODO: Need rwlock urcu seqlock etc. good enough to run it under single writer/multiple readers scenario.
template<typename Key, class Comparator>
class IntervalSkipList {
public:

    class Interval;

private:

    class IntervalSLNode;

    class IntervalListElt;

    class IntervalList;

    pthread_rwlock_t rwlock;

public:

    inline void ReadLock() {
        pthread_rwlock_rdlock(&rwlock);
    }

    inline void ReadUnlock() {
        pthread_rwlock_unlock(&rwlock);
    }

    inline void WriteLock() {
        pthread_rwlock_wrlock(&rwlock);
    }

    inline void WriteUnlock() {
        pthread_rwlock_unlock(&rwlock);
    }

private:

    enum { MAX_FORWARD = 32 };     // Maximum number of forward pointers

    int maxLevel;

    Random random;

    IntervalSLNode* const head_;

    const Comparator comparator_;

    uint64_t timestamp_; // mark every interval with an timestamp, start from 1.

    uint64_t iCount_;  // interval count, automatically changed inside insert(Interval) and delete(Interval) only.

    typedef IntervalListElt* ILE_handle;

    int randomLevel();  // choose a new node level at random

    // Three-way comparison.  Returns value:
    //   <  0 iff "a" <  "b",
    //   == 0 iff "a" == "b",
    //   >  0 iff "a" >  "b".
    inline int KeyCompare(const Key &a, const Key &b, const bool ukey = false) const {
        return comparator_(a, b, ukey);
    }


    bool contains(const Interval* I, const Key& K) const;

    bool contains_interval(const Interval* I, const Key& i, const Key& s) const;

    // Search for search key, and return a pointer to the
    // IntervalSLNode x found, as well as setting the update vector
    // showing pointers into x.
    IntervalSLNode* search(const Key& searchKey,
                           IntervalSLNode** update) const;

    // insert a new single Key
    // into list, returning a pointer to its location.
    IntervalSLNode* insert(const Key& searchKey);



    // adjust markers after insertion of x with update vector "update"
    void adjustMarkersOnInsert(IntervalSLNode* x,
                               IntervalSLNode** update);

    // Remove markers for interval m from the edges and nodes on the
    // level i path from l to r.
    void removeMarkFromLevel(const Interval* m, int i,
                             IntervalSLNode* l,
                             IntervalSLNode* r);

    // place markers for Interval I.  I must have been inserted in the list.
    // left is the left endpoint of I and right is the right endpoint if I.
    // *** needs to be fixed:
    void placeMarkers(IntervalSLNode* left,
                      IntervalSLNode* right,
                      const Interval* I);

    // remove markers for Interval I starting at left, the left endpoint
    // of I, and and stopping at the right endpoint of I.
    Interval *removeMarkers(IntervalSLNode* left,
                            const Interval* I);

    void deleteMarkers(IntervalSLNode* left,
                       const Interval* I);

    // adjust markers to prepare for deletion of x, which has update vector
    // "update"
    void adjustMarkersOnDelete(IntervalSLNode* x,
                               IntervalSLNode** update);

    // remove node x, which has updated vector update.
    void remove(IntervalSLNode* x,
                IntervalSLNode** update);

    inline static bool timeCmp(Interval* l, Interval* r) {
        return l->stamp_ > r->stamp_;
    }



    template<class InputIterator>
    IntervalSkipList(Comparator cmp, InputIterator b, InputIterator e)
                        : maxLevel(0),
                          random(0xdeadbeef),
                          head_(new IntervalSLNode(MAX_FORWARD)),
                          comparator_(cmp),
                          timestamp_(1),
                          iCount_(0) {
        for (int i = 0; i < MAX_FORWARD; i++) {
            head_->forward[i] = nullptr;
        }
    }

    template <class InputIterator>
    int insert(InputIterator b, InputIterator e) {
        int i = 0;
        for(; b!= e; ++b){
            insert(*b);
            ++i;
        }
        return i;
    }


    bool is_contained(const Key &searchKey) const {
        IntervalSLNode *x = head_;
        for (int i = maxLevel;
             i >= 0 && (x->isHeader() || KeyCompare(x->key, searchKey) != 0); i--) {
            while (x->forward[i] != 0 && KeyCompare(searchKey, x->forward[i]->key) >= 0) {
                x = x->forward[i];
            }
            // Pick up markers on edge as you drop down a level, unless you are at
            // the searchKey node already, in which case you pick up the
            // eqMarkers just prior to exiting loop.
            if (!x->isHeader() && KeyCompare(x->key, searchKey) != 0) {
                return true;
            } else if (!x->isHeader()) { // we're at searchKey
                return true;
            }
        }
        return false;
    }


    // return node containing Key if found, otherwise nullptr
    IntervalSLNode* search(const Key& searchKey) const;



    // FindSmallerOrEqual
    // To support point query

    // It is assumed that when a marker is placed on an edge,
    // it will be placed in the eqMarkers sets of a node on either
    // end of the edge if the interval for the marker covers the node.
    // Similarly, when a marker is removed from an edge markers will
    // be removed from eqMarkers sets on nodes adjacent to the edge.
    template<class OutputIterator>
    OutputIterator
    find_intervals(const Key &searchKey, OutputIterator out, int& overlaps) const {
        overlaps = 0;
        IntervalSLNode *x = head_;
        for (int i = maxLevel;
             i >= 0 && (x->isHeader() || KeyCompare(x->key, searchKey) != 0); i--) {
            while (x->forward[i] != 0 && KeyCompare(searchKey, x->forward[i]->key) >= 0) {
                x = x->forward[i];
            }
            // Pick up markers on edge as you drop down a level, unless you are at
            // the searchKey node already, in which case you pick up the
            // eqMarkers just prior to exiting loop.
            if (!x->isHeader() && KeyCompare(x->key, searchKey) != 0) {
                out = x->markers[i]->copy(out);
                overlaps += x->markers[i]->count;
            } else if (!x->isHeader()) { // we're at searchKey
                out = x->eqMarkers->copy(out);
                overlaps += x->eqMarkers->count;
            }
        }
        // Do not miss any intervals that has the same user key as searchKey
        if (x->forward[0] != 0 && KeyCompare(x->forward[0]->key, searchKey, true) == 0) {
            out = x->forward[0]->startMarker->copy(out);
        }
        return out;
    }

    // FindSmallerOrEqual
    // To support compaction

    template<class OutputIterator>
    OutputIterator
    find_intervals(const Key &searchKey, OutputIterator out) const {
        IntervalSLNode *x = head_;
        for (int i = maxLevel;
             i >= 0 && (x->isHeader() || KeyCompare(x->key, searchKey) != 0); i--) {
            while (x->forward[i] != 0 && KeyCompare(searchKey, x->forward[i]->key) >= 0) {
                x = x->forward[i];
            }
            // Pick up markers on edge as you drop down a level, unless you are at
            // the searchKey node already, in which case you pick up the
            // eqMarkers just prior to exiting loop.
            if (!x->isHeader() && KeyCompare(x->key, searchKey) != 0) {
                out = x->markers[i]->copy(out);
            } else if (!x->isHeader()) { // we're at searchKey
                out = x->eqMarkers->copy(out);
            }
        }
        return out;
    }

    // Every time we insert a new interval, check its end points if overlaps too much intervals.

    int stab_intervals(const Key& searchKey) const {
        IntervalSLNode *x = head_;
        int ret = 0;
        for (int i = maxLevel;
             i >= 0 && (x->isHeader() || KeyCompare(x->key, searchKey) != 0); i--) {
            while (x->forward[i] != 0 && KeyCompare(searchKey, x->forward[i]->key) >= 0) {
                x = x->forward[i];
            }
            // Pick up markers on edge as you drop down a level, unless you are at
            // the searchKey node already, in which case you pick up the
            // eqMarkers just prior to exiting loop.
            if (!x->isHeader() && KeyCompare(x->key, searchKey) != 0) {
                ret += x->markers[i]->count;
            } else if (!x->isHeader()) { // we're at searchKey
                ret += x->eqMarkers->count;
            }
        }
        return ret;
    }

    // FindGreaterOrEqual
    // To support scan query

    // REQUIRES: node's internal key not deleted.
    template<class OutputIterator>
    OutputIterator
    find_intervals(const Key &searchKey, OutputIterator out,
                   Key& left, Key& right, int& overlaps) const {
        overlaps = 0;
        IntervalSLNode *x = head_;
        IntervalSLNode *before = head_;
        bool equal = false;
        int i = 0;
        for (i = maxLevel;
             i >= 0 && (x->isHeader() || KeyCompare(x->key, searchKey) != 0); i--) {
            while (x->forward[i] != 0 && KeyCompare(searchKey, x->forward[i]->key) >= 0) {
                // before x at level i
                before = x;
                x = x->forward[i];
            }
            // Pick up markers on edge as you drop down a level, unless you are at
            // the searchKey node already, in which case you pick up the
            // eqMarkers just prior to exiting loop.
            if (!x->isHeader() && KeyCompare(x->key, searchKey) != 0) {
                out = x->markers[i]->copy(out);
                overlaps += x->markers[i]->count;
            } else if (!x->isHeader()) { // we're at searchKey
                out = x->eqMarkers->copy(out);
                overlaps += x->eqMarkers->count;
                equal = true;
            }
        }

        if (!equal) {
            // no need to deal with situation where x is last node cause iterator invalid.
            before = x;
        } else {
            // before can be head_ where left is set to 0(nullptr)
            for (;i >= 0; i--) {
                while (before->forward[i] != x) {
                    before = before->forward[i];
                }
            }
            assert(before->forward[0] == x);
        }

        if (before != head_) {
            out = before->endMarker->copy(out);
        }
        // head_->key = 0
        left = before->key;

        assert(x != nullptr);

        // set right greater than first interval's left point.
        if (x == head_) {
            x = x->forward[0];
            out = x->startMarker->copy(out);
        }
        x = x->forward[0];

        // x drops in (head_ + 1, nullptr]
        assert(x != head_->forward[0]);

        // always fetch the closest interval starts after x and the closest interval ends before x.
        while (x != nullptr) {
            if (x->startMarker->count != 0) {
                // merge procedure might increase startMarker or endMarker temporarily.
                // As it finished, startMarker + endMarker = 1.
                out = x->startMarker->copy(out);
                break;
            }
            x = x->forward[0];
        }

        right = (x != nullptr) ? x->key : 0;

        return out;
    }

    // FindSmallerOrEqual
    // To support compaction

    // REQUIRES: node's internal key not deleted.
    template<class OutputIterator>
    OutputIterator
    find_intervals(const Key &searchKey, OutputIterator out,
                   Key& right, const uint64_t time_border, const Key& right_border) const {
        IntervalSLNode *x = head_;
        for (int i = maxLevel;
             i >= 0 && (x->isHeader() || KeyCompare(x->key, searchKey) != 0); i--) {
            while (x->forward[i] != 0 && KeyCompare(searchKey, x->forward[i]->key) >= 0) {
                x = x->forward[i];
            }
            // Pick up markers on edge as you drop down a level, unless you are at
            // the searchKey node already, in which case you pick up the
            // eqMarkers just prior to exiting loop.
            if (!x->isHeader() && KeyCompare(x->key, searchKey) != 0) {
                out = x->markers[i]->copy(out);
            } else if (!x->isHeader()) { // we're at searchKey
                out = x->eqMarkers->copy(out);
            }
        }
        // x is always on a node with a key.
        assert(x != head_ && x != nullptr);

        assert(KeyCompare(x->key, searchKey) == 0);

        IntervalSLNode* after = x->forward[0];

        // fetch first interval that starts at (x->key, right_border) under time_border.
        while (after->key != right_border) {
            if (after->startMarker->count != 0 &&
                after->startMarker->get_first()->getInterval()->stamp_ < time_border) {
                assert(after->startMarker->count == 1);
                assert(after->endMarker->count == 0);
                out = after->startMarker->copy(out);
                break;
            }
            after = after->forward[0];
        }

        right = (after->key != right_border) ? after->key : 0;

        return out;
    }

    // find the last node.

    IntervalSLNode* find_last() const {
        IntervalSLNode *x = head_;
        int level = maxLevel;
        while (true) {
            IntervalSLNode* next = x->forward[level];
            if (next == nullptr) {
                if (level == 0) {
                    return x;
                } else {
                    level--;
                }
            } else {
                x = next;
            }
        }
    }



    // No copying allowed
    IntervalSkipList(const IntervalSkipList&);
    void operator=(const IntervalSkipList);



public:

    // User may be interested about methods below.

    explicit IntervalSkipList(Comparator cmp);

    ~IntervalSkipList();

    inline uint64_t size() const { return iCount_; }   //number of intervals

    void clearIndex();

    // print every nodes' information
    void print(std::ostream& os) const;
    // print every node' key in order
    void printOrdered(std::ostream& os) const;

    inline uint64_t NextTimestamp() const { return timestamp_; }

    inline void IncTimestamp() { timestamp_++; }


    // Insert a new table generated by imm_ or generated by merged nvm_imm_.
    void insert(const Key& l, const Key& r, NvmMemTable* table, uint64_t timestamp);

    // insert an interval into list
    void insert(const Interval* I);

    // Return the tables contain this searchKey(user key). Called by point query.
    // And stab the intervals include searchKey(internal key).
    void search(const Key& searchKey, std::vector<Interval*>& intervals, int& overlaps);

    //Return the tables contain this searchKey(internal key). Called by DoCompactionWork.
    void search(const Key& searchKey, std::vector<Interval*>& intervals);

    // remove an interval from list
    bool remove(const Interval* I);

    // stab the intervals include searchKey(internal key)
    int stab(const Key& searchKey);

    class IteratorHelper {
    public:
        explicit IteratorHelper(IntervalSkipList* const list)
                                : list_(list) {

        }

        inline void ReadLock() {
            list_->ReadLock();
        }

        inline void ReadUnlock() {
            list_->ReadUnlock();
        }

        inline void WriteLock() {
            list_->WriteLock();
        }

        inline void WriteUnlock() {
            list_->WriteUnlock();
        }

        // caller's duty to use lock.
        // REQUIRES: target not nullptr.
        void Seek(const Key& target, std::vector<Interval*>& intervals,
                  Key& left, Key& right, int& overlaps) {
            assert(target != nullptr);
            if (list_->iCount_ == 0) {
                left = 0;
                right = 0;
                return;
            }
            list_->find_intervals(target, std::back_inserter(intervals), left, right, overlaps);
        }

        void SeekToFirst(std::vector<Interval*>& intervals, Key& left, Key& right) {
            // no data
            if (list_->iCount_ == 0) {
                left = 0;
                right = 0;
                return;
            } else {
                int para = 0;
                Seek(list_->head_->forward[0]->key, intervals, left, right, para);
            }
        }

        void SeekToLast(std::vector<Interval*>& intervals, Key& left, Key& right) {
            // no data
            if (list_->iCount_ == 0) {
                left = 0;
                right = 0;
                return;
            } else {
                IntervalSLNode* tmp = list_->find_last();
                int para = 0;
                Seek(tmp->key, intervals, left, right, para);
            }
        }

        // Used in compact iterator.
        inline void Seek(const Key& target, std::vector<Interval*>& intervals, Key& right, const Key& right_border, const uint64_t time_border) {
            list_->find_intervals(target, std::back_inserter(intervals), right, time_border, right_border);
        }

        void ShowIndex() const {
            //list_->print(std::cout);
            list_->printOrdered(std::cout);
        }

    private:
        IntervalSkipList* const list_;

    };

};

template<typename Key, class Comparator>
IntervalSkipList<Key, Comparator>::IntervalSkipList(Comparator cmp)
                                : maxLevel(0),
                                  random(0xdeadbeef),
                                  head_(new IntervalSLNode(MAX_FORWARD)),
                                  comparator_(cmp),
                                  timestamp_(1),
                                  iCount_(0) {
    pthread_rwlockattr_t attr;
    // write thread has priority over read thread.
    pthread_rwlockattr_setkind_np(&attr, PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
    pthread_rwlock_init(&rwlock, &attr);
    pthread_rwlockattr_destroy(&attr);
    for (int i = 0; i < MAX_FORWARD; i++) {
        head_->forward[i] = nullptr;
    }
}

template<typename Key, class Comparator>
IntervalSkipList<Key, Comparator>::~IntervalSkipList() {
    //std::vector<Interval*> intervals;
    WriteLock();
    IntervalSLNode* cursor = head_;
    while (cursor) {
        IntervalSLNode* next = cursor->forward[0];
        //if (cursor->startMarker->count != 0) {
        //    assert(cursor->startMarker->count == 1);
        //    intervals.push_back(const_cast<Interval*>(cursor->startMarker->get_first()->getInterval()));
        //}
        delete cursor;
        cursor = next;
    }
    //for (auto i : intervals) {
    //    i->Unref();
    //}
    WriteUnlock();
    pthread_rwlock_destroy(&rwlock);
}

template<typename Key, class Comparator>
void IntervalSkipList<Key, Comparator>::insert(const Key& l,
                                                 const Key& r,
                                                 NvmMemTable* table,
                                                 uint64_t timestamp) {

    uint64_t mark = (timestamp == 0) ? timestamp_++ : timestamp;
    // init refs_(1)
    Interval* I = new Interval(l, r, mark, table);

    //printOrdered(std::cout);

    insert(I);
/*
    if (timestamp != 0) {
        std::cout<<"Compact insert: ";
    } else {
        std::cout<<"Imm insert: ";
    }
    I->print(std::cout);
    std::cout<<std::endl;
*/
}

template<typename Key, class Comparator>
inline void IntervalSkipList<Key, Comparator>::search(const Key& searchKey,
                                               std::vector<Interval*>& intervals, int& overlaps) {
    find_intervals(searchKey, std::back_inserter(intervals), overlaps);
    std::sort(intervals.begin(), intervals.end(), timeCmp);
}

template<typename Key, class Comparator>
inline void IntervalSkipList<Key, Comparator>::search(const Key &searchKey,
                                               std::vector<Interval*> &intervals) {
    find_intervals(searchKey, std::back_inserter(intervals));
}

template<typename Key, class Comparator>
inline int IntervalSkipList<Key, Comparator>::stab(const Key &searchKey) {
    return stab_intervals(searchKey);
}

template<typename Key, class Comparator>
void IntervalSkipList<Key, Comparator>::clearIndex() {
    //std::vector<Interval*> intervals;
    WriteLock();
    IntervalSLNode* cursor = head_;
    while (cursor) {
        IntervalSLNode* next = cursor->forward[0];
        //if (cursor->startMarker->count != 0) {
        //    assert(cursor->startMarker->count == 1);
        //    intervals.push_back(const_cast<Interval*>(cursor->startMarker->get_first()->getInterval()));
        //}
        delete cursor;
        cursor = next;
    }
    //for (auto i : intervals) {
    //    i->Unref();
    //}
    for (int i = 0; i < MAX_FORWARD; i++) {
        head_->forward[i] = nullptr;
    }
    maxLevel = 0;
    timestamp_ = 1;
    iCount_ = 0;
    WriteUnlock();
}

// Not used
template<typename Key, class Comparator>
typename IntervalSkipList<Key, Comparator>::
IntervalSLNode* IntervalSkipList<Key, Comparator>::search(const Key& searchKey) const {
    IntervalSLNode* x = head_;
    for(int i = maxLevel; i >= 0; i--) {
        while (x->forward[i] != 0 && KeyCompare(x->forward[i]->key, searchKey) < 0) {
            x = x->forward[i];
        }
    }
    x = x->forward[0];
    if(x != nullptr && KeyCompare(x->key, searchKey) == 0)
        return x;
    else
        return nullptr;
}

template<typename Key, class Comparator>
void IntervalSkipList<Key, Comparator>::print(std::ostream& os) const {
    os << "\nAn Interval_skip_list"<< "("<<iCount_<<")"<<":  \n";
    // start from first node with key
    IntervalSLNode* n = head_->forward[0];

    while ( n != 0 ) {
        n->print(os);
        n = n->forward[0];
    }
}

template<typename Key, class Comparator>
void IntervalSkipList<Key, Comparator>::printOrdered(std::ostream& os) const {
    IntervalSLNode* n = head_->forward[0];
    os << "keys in list:  ";
    while ( n != 0 ) {
        Decode(n->key, os);
        os << " ";
        //os << n->key << " ";
        n = n->forward[0];
    }
    os << std::endl;
}

template<typename Key, class Comparator>
std::ostream& operator<<(std::ostream& os,
                         const IntervalSkipList<Key, Comparator>& isl) {
    isl.print(os);
    return os;
}

template<typename Key, class Comparator>
typename IntervalSkipList<Key, Comparator>::
IntervalSLNode* IntervalSkipList<Key, Comparator>::search(const Key& searchKey,
                                                            IntervalSLNode** update) const {
    IntervalSLNode* x = head_;
    // Find location of searchKey, building update vector indicating
    // pointers to change on insertion.
    for(int i = maxLevel; i >= 0; i--) {
        while (x->forward[i] != 0 && KeyCompare(x->forward[i]->key, searchKey) < 0) {
            x = x->forward[i];
        }
        update[i] = x;
    }
    x = x->forward[0];
    return x;
}

template<typename Key, class Comparator>
void IntervalSkipList<Key, Comparator>::insert(const Interval* I) {
    // insert end points of interval
    IntervalSLNode* left = insert(I->inf_);
    IntervalSLNode* right = insert(I->sup_);
    left->ownerCount++;
    left->startMarker->insert(I);
    right->ownerCount++;
    right->endMarker->insert(I);

    // place markers on interval
    placeMarkers(left, right, I);
    iCount_++;
}

template<typename Key, class Comparator>
typename IntervalSkipList<Key, Comparator>::
IntervalSLNode* IntervalSkipList<Key, Comparator>::insert(const Key& searchKey) {
    // array for maintaining update pointers
    IntervalSLNode* update[MAX_FORWARD];
    IntervalSLNode* x;
    int i;

    // Find location of searchKey, building update vector indicating
    // pointers to change on insertion.
    x = search(searchKey, update);
    if(x == 0 || KeyCompare(x->key, searchKey) != 0 ) {
        // put a new node in the list for this searchKey
        int newLevel = randomLevel();
        if (newLevel > maxLevel) {
            for(i = maxLevel + 1; i <= newLevel; i++){
                update[i] = head_;
            }
            maxLevel = newLevel;
        }
        x = new IntervalSLNode(searchKey, newLevel);

        // add x to the list
        for(i = 0; i <= newLevel; i++) {
            x->forward[i] = update[i]->forward[i];
            update[i]->forward[i] = x;
        }

        // adjust markers to maintain marker invariant
        adjustMarkersOnInsert(x, update);
    }
    // else, the searchKey is in the list already, and x points to it.
    return x;
}

    // Adjust markers on this IS-list to maintain marker invariant now that
    // node x has just been inserted, with update vector `update.'

template<typename Key, class Comparator>
void IntervalSkipList<Key, Comparator>::adjustMarkersOnInsert(IntervalSLNode* x,
                                                                IntervalSLNode** update) {
    // Phase 1:  place markers on edges leading out of x as needed.

    // Starting at bottom level, place markers on outgoing level i edge of x.
    // If a marker has to be promoted from level i to i+1 of higher, place it
    // in the promoted set at each step.

    IntervalList promoted;
    // list of intervals that identify markers being
    // promoted, initially empty.

    IntervalList newPromoted;
    // temporary set to hold newly promoted markers.

    IntervalList removePromoted;
    // holding place for elements to be removed  from promoted list.

    IntervalList tempMarkList;  // temporary mark list
    ILE_handle m;
    int i;

    for(i = 0; (i <= x->level() - 2) && x->forward[i+1] != 0; i++) {
        IntervalList* markList = update[i]->markers[i];
        for(m = markList->get_first(); m != nullptr ; m = m->get_next()) {
            if(contains_interval(m->getInterval(), x->key, x->forward[i+1]->key)) {
                // promote m

                // remove m from level i path from x->forward[i] to x->forward[i+1]
                removeMarkFromLevel(m->getInterval(),
                                    i,
                                    x->forward[i],
                                    x->forward[i+1]);
                // add m to newPromoted
                newPromoted.insert(m->getInterval());
            } else {
                // place m on the level i edge out of x
                x->markers[i]->insert(m->getInterval());
                // do *not* place m on x->forward[i]; it must already be there.
            }
        }

        for(m = promoted.get_first(); m != nullptr; m = m->get_next()) {
            if(!contains_interval(m->getInterval(), x->key, x->forward[i+1]->key)) {
                // Then m does not need to be promoted higher.
                // Place m on the level i edge out of x and remove m from promoted.
                x->markers[i]->insert(m->getInterval());
                // mark x->forward[i] if needed
                if(contains(m->getInterval(), x->forward[i]->key))
                    x->forward[i]->eqMarkers->insert(m->getInterval());
                removePromoted.insert(m->getInterval());
            } else {
                // continue to promote m
                // Remove m from the level i path from x->forward[i]
                // to x->forward[i+1].
                removeMarkFromLevel(m->getInterval(),
                                    i,
                                    x->forward[i],
                                    x->forward[i+1]);
            }
        }
        promoted.removeAll(&removePromoted);
        removePromoted.clear();
        promoted.copy(&newPromoted);
        newPromoted.clear();
    }
    // Combine the promoted set and updated[i]->markers[i]
    // and install them as the set of markers on the top edge out of x
    // that is non-null.

    x->markers[i]->copy(&promoted);
    x->markers[i]->copy(update[i]->markers[i]);
    for(m = promoted.get_first(); m != nullptr; m = m->get_next())
        if(contains(m->getInterval(), x->forward[i]->key))
            x->forward[i]->eqMarkers->insert(m->getInterval());

    // Phase 2:  place markers on edges leading into x as needed.

    // Markers on edges leading into x may need to be promoted as high as
    // the top edge coming into x, but never higher.

    promoted.clear();

    for (i = 0; (i <= x->level() - 2) && !update[i+1]->isHeader(); i++) {
        tempMarkList.copy(update[i]->markers[i]);
        for(m = tempMarkList.get_first();
            m != nullptr;
            m = m->get_next()){
            if(contains_interval(m->getInterval(), update[i+1]->key, x->key)) {
                // m needs to be promoted
                // add m to newPromoted
                newPromoted.insert(m->getInterval());

                // Remove m from the path of level i edges between updated[i+1]
                // and x (it will be on all those edges or else the invariant
                // would have previously been violated.
                removeMarkFromLevel(m->getInterval(), i, update[i+1] , x);
            }
        }
        tempMarkList.clear();  // reclaim storage

        for(m = promoted.get_first(); m != nullptr; m = m->get_next()) {
            if (!update[i]->isHeader() &&
                contains_interval(m->getInterval(), update[i]->key, x->key) &&
                !update[i+1]->isHeader() &&
                ! contains_interval(m->getInterval(), update[i+1]->key, x->key) ) {
                // Place m on the level i edge between update[i] and x, and
                // remove m from promoted.
                update[i]->markers[i]->insert(m->getInterval());
                // mark update[i] if needed
                if(contains(m->getInterval(), update[i]->key))
                    update[i]->eqMarkers->insert(m->getInterval());
                removePromoted.insert(m->getInterval());
            } else {
                // Strip m from the level i path from update[i+1] to x.
                removeMarkFromLevel(m->getInterval(), i, update[i+1], x);
            }

        }
        // remove non-promoted marks from promoted
        promoted.removeAll(&removePromoted);
        removePromoted.clear();  // reclaim storage

        // add newPromoted to promoted and make newPromoted empty
        promoted.copy(&newPromoted);
        newPromoted.clear();
    }

    /* Assertion:  i=x->level()-1 OR update[i+1] is the head_.

       If i=x->level()-1 then either x has only one level, or the top-level
       pointer into x must not be from the head_, since otherwise we would
       have stopped on the previous iteration.  If x has 1 level, then
       promoted is empty.  If x has 2 or more levels, and i!=x->level()-1,
       then the edge on the next level up (level i+1) is from the head_.  In
       any of these cases, all markers in the promoted set should be
       deposited on the current level i edge into x.  An edge out of the
       head_ should never be marked.  Note that in the case where x has only
       1 level, we try to copy the contents of the promoted set onto the
       marker set of the edge out of the head_ into x at level i=0, but of
       course, the promoted set will be empty in this case, so no markers
       will be placed on the edge.  */

    update[i]->markers[i]->copy(&promoted);
    for(m = promoted.get_first(); m != nullptr; m = m->get_next())
        if(contains(m->getInterval(), update[i]->key))
            update[i]->eqMarkers->insert(m->getInterval());

    // Place markers on x for all intervals the cross x.
    // (Since x is a new node, every marker comming into x must also leave x).
    for(i = 0; i < x->level(); i++)
        x->eqMarkers->copy(x->markers[i]);

    promoted.clear(); // reclaim storage

} // end adjustMarkersOnInsert

template<typename Key, class Comparator>
void IntervalSkipList<Key, Comparator>::removeMarkFromLevel(const Interval* m, int i,
                                                              IntervalSLNode* l,
                                                              IntervalSLNode* r) {
    IntervalSLNode *x;
    for(x = l; x != 0 && x != r; x = x->forward[i]) {
        x->markers[i]->remove(m);
        x->eqMarkers->remove(m);
    }
    if(x != 0) x->eqMarkers->remove(m);
}

template<typename Key, class Comparator>
void IntervalSkipList<Key, Comparator>::placeMarkers(IntervalSLNode* left,
                                                       IntervalSLNode* right,
                                                       const Interval* I)
{
    // Place markers for the interval I.  left is the left endpoint
    // of I and right is the right endpoint of I, so it isn't necessary
    // to search to find the endpoints.

    IntervalSLNode* x = left;
    if (contains(I, x->key)) x->eqMarkers->insert(I);
    int i = 0;  // start at level 0 and go up
    while (x->forward[i] != 0 && contains_interval(I, x->key, x->forward[i]->key)) {
        // find level to put mark on
        while (i != x->level()-1
              && x->forward[i+1] != 0
              && contains_interval(I, x->key, x->forward[i+1]->key))
            i++;
        // Mark current level i edge since it is the highest edge out of
        // x that contains I, except in the case where current level i edge
        // is null, in which case it should never be marked.
        if (x->forward[i] != 0) {
            x->markers[i]->insert(I);
            x = x->forward[i];
            // Add I to eqMarkers set on node unless currently at right endpoint
            // of I and I doesn't contain right endpoint.
            if (contains(I, x->key)) x->eqMarkers->insert(I);
        }
    }

    // mark non-ascending path
    while (KeyCompare(x->key, right->key) != 0) {
        // find level to put mark on
        while (i!=0 && (x->forward[i] == 0 ||
                       !contains_interval(I, x->key, x->forward[i]->key)))
            i--;
        // At this point, we can assert that i=0 or x->forward[i]!=0 and
        // I contains
        // (x->key,x->forward[i]->key).  In addition, x is between left and
        // right so i=0 implies I contains (x->key,x->forward[i]->key).
        // Hence, the interval must be marked.  Note that it is impossible
        // for us to be at the end of the list because x->key is not equal
        // to right->key.
        x->markers[i]->insert(I);
        x = x->forward[i];
        if (contains(I, x->key)) x->eqMarkers->insert(I);
    }
}  // end placeMarkers



// REQUIRES: Interval exists. If not, return true.
template<typename Key, class Comparator>
bool IntervalSkipList<Key, Comparator>::remove(const Interval* I) {
    // arrays for maintaining update pointers
    IntervalSLNode* update[MAX_FORWARD];

    IntervalSLNode* left = search(I->inf_, update);
    if(left == 0 || left->ownerCount <= 0) {
        return false;
    }
    assert(left->key == I->inf_);

    //Interval* ih = removeMarkers(left, I);
    deleteMarkers(left, I);

    left->startMarker->remove(I);
    left->ownerCount--;
    if(left->ownerCount == 0) remove(left, update);

    // Note:  we search for right after removing left since some
    // of left's forward pointers may point to right.  We don't
    // want any pointers of update vector pointing to a node that is gone.

    IntervalSLNode* right = search(I->sup_, update);
    if(right == 0 || right->ownerCount <= 0) {
        return false;
    }
    assert(right->key == I->sup_);
    right->endMarker->remove(I);
    right->ownerCount--;
    if(right->ownerCount == 0) remove(right, update);
    iCount_--;
    return true;
}

template<typename Key, class Comparator>
void IntervalSkipList<Key, Comparator>::deleteMarkers(IntervalSLNode* left,
                                                 const Interval* I) {
    // Remove markers for interval I, which has left as it's left
    // endpoint,  following a staircase pattern.

    // remove marks from ascending path
    IntervalSLNode* x = left;
    if (contains(I, x->key)) {
        x->eqMarkers->remove(I);
    }
    int i = 0;  // start at level 0 and go up
    while (x->forward[i] != 0 && contains_interval(I, x->key, x->forward[i]->key)) {
        // find level to take mark from
        while (i != x->level()-1
               && x->forward[i+1] != 0
               && contains_interval(I, x->key, x->forward[i+1]->key))
            i++;
        // Remove mark from current level i edge since it is the highest edge out
        // of x that contains I, except in the case where current level i edge
        // is null, in which case there are no markers on it.
        if (x->forward[i] != 0) {
            x->markers[i]->remove(I);
            x = x->forward[i];
            // remove I from eqMarkers set on node unless currently at right
            // endpoint of I and I doesn't contain right endpoint.
            if (contains(I, x->key)){
                x->eqMarkers->remove(I);
            }
        }
    }

    // remove marks from non-ascending path
    while (KeyCompare(x->key, I->sup_) != 0) {
        // find level to remove mark from
        while (i != 0 && (x->forward[i] == 0 ||
                          ! contains_interval(I, x->key, x->forward[i]->key)))
            i--;
        // At this point, we can assert that i=0 or x->forward[i]!=0 and
        // I contains
        // (x->key,x->forward[i]->key).  In addition, x is between left and
        // right so i=0 implies I contains (x->key,x->forward[i]->key).
        // Hence, the interval is marked and the mark must be removed.
        // Note that it is impossible for us to be at the end of the list
        // because x->key is not equal to right->key.
        x->markers[i]->remove(I);
        x = x->forward[i];
        if (contains(I, x->key)){
            x->eqMarkers->remove(I);
        }
    }
}

template<typename Key, class Comparator>
typename IntervalSkipList<Key, Comparator>::Interval*
IntervalSkipList<Key, Comparator>::removeMarkers(IntervalSLNode* left,
                                                 const Interval* I) {
    // Remove markers for interval I, which has left as it's left
    // endpoint,  following a staircase pattern.

    // Interval_handle res=0, tmp=0; // af: assignment not possible with std::list
    Interval* res = nullptr;
    Interval* tmp = nullptr;
    // remove marks from ascending path
    IntervalSLNode* x = left;
    if (contains(I, x->key)) {
        if(x->eqMarkers->remove(I, tmp)){
            res = tmp;
        }
    }
    int i = 0;  // start at level 0 and go up
    while (x->forward[i] != 0 && contains_interval(I, x->key, x->forward[i]->key)) {
        // find level to take mark from
        while (i != x->level()-1
              && x->forward[i+1] != 0
              && contains_interval(I, x->key, x->forward[i+1]->key))
            i++;
        // Remove mark from current level i edge since it is the highest edge out
        // of x that contains I, except in the case where current level i edge
        // is null, in which case there are no markers on it.
        if (x->forward[i] != 0) {
            if(x->markers[i]->remove(I, tmp)){
                res = tmp;
            }
            x = x->forward[i];
            // remove I from eqMarkers set on node unless currently at right
            // endpoint of I and I doesn't contain right endpoint.
            if (contains(I, x->key)){
                if(x->eqMarkers->remove(I, tmp)){
                    res = tmp;
                }
            }
        }
    }

    // remove marks from non-ascending path
    while (KeyCompare(x->key, I->sup_) != 0) {
        // find level to remove mark from
        while (i != 0 && (x->forward[i] == 0 ||
                         ! contains_interval(I, x->key, x->forward[i]->key)))
            i--;
        // At this point, we can assert that i=0 or x->forward[i]!=0 and
        // I contains
        // (x->key,x->forward[i]->key).  In addition, x is between left and
        // right so i=0 implies I contains (x->key,x->forward[i]->key).
        // Hence, the interval is marked and the mark must be removed.
        // Note that it is impossible for us to be at the end of the list
        // because x->key is not equal to right->key.
        if(x->markers[i]->remove(I, tmp)){
            res = tmp;
        }
        x = x->forward[i];
        if (contains(I, x->key)){
            if(x->eqMarkers->remove(I, tmp)){
                res = tmp;
            }
        }
    }
    return res;
}

template<typename Key, class Comparator>
void IntervalSkipList<Key, Comparator>::adjustMarkersOnDelete(IntervalSLNode* x,
                                                            IntervalSLNode** update) {
    // x is node being deleted.  It is still in the list.
    // update is the update vector for x.
    IntervalList demoted;
    IntervalList newDemoted;
    IntervalList tempRemoved;
    ILE_handle m;
    int i;
    IntervalSLNode *y;

    // Phase 1:  lower markers on edges to the left of x as needed.

    for(i = x->level()-1; i >= 0; i--){
        // find marks on edge into x at level i to be demoted
        for(m = update[i]->markers[i]->get_first(); m != nullptr;
            m = m->get_next()) {
            if(x->forward[i]==0 ||
               ! contains_interval(m->getInterval(), update[i]->key,
                                                     x->forward[i]->key)) {
                newDemoted.insert(m->getInterval());
            }
        }
        // Remove newly demoted marks from edge.
        update[i]->markers[i]->removeAll(&newDemoted);
        // NOTE:  update[i]->eqMarkers is left unchanged because any markers
        // there before demotion must be there afterwards.

        // Place previously demoted marks on this level as needed.
        for(m = demoted.get_first(); m != nullptr; m = m->get_next()){
            // Place mark on level i from update[i+1] to update[i], not including
            // update[i+1] itself, since it already has a mark if it needs one.
            for(y = update[i+1]; y != 0 && y != update[i]; y = y->forward[i]) {
                if (y != update[i+1] && contains(m->getInterval(), y->key))
                    y->eqMarkers->insert(m->getInterval());
                y->markers[i]->insert(m->getInterval());
            }
            if(y!=0 && y!=update[i+1] && contains(m->getInterval(), y->key))
                y->eqMarkers->insert(m->getInterval());

            // if this is the lowest level m needs to be placed on,
            // then place m on the level i edge out of update[i]
            // and remove m from the demoted set.
            if(x->forward[i]!=0 &&
               contains_interval(m->getInterval(), update[i]->key,
                                                   x->forward[i]->key))
            {
                update[i]->markers[i]->insert(m->getInterval());
                tempRemoved.insert(m->getInterval());
            }
        }
        demoted.removeAll(&tempRemoved);
        tempRemoved.clear();
        demoted.copy(&newDemoted);
        newDemoted.clear();
    }

    // Phase 2:  lower markers on edges to the right of D as needed

    demoted.clear();
    // newDemoted is already empty

    for(i = x->level()-1; i >= 0; i--){
        for(m = x->markers[i]->get_first(); m != nullptr ;m = m->get_next()){
            if(x->forward[i] != 0 &&
               (update[i]->isHeader() ||
                !contains_interval(m->getInterval(), update[i]->key,
                                                     x->forward[i]->key)))
            {
                newDemoted.insert(m->getInterval());
            }
        }

        for(m = demoted.get_first(); m != nullptr; m = m->get_next()){
            // Place mark on level i from x->forward[i] to x->forward[i+1].
            // Don't place a mark directly on x->forward[i+1] since it is already
            // marked.
            for(y = x->forward[i];y != x->forward[i+1];y = y->forward[i]){
                y->eqMarkers->insert(m->getInterval());
                y->markers[i]->insert(m->getInterval());
            }

            if(x->forward[i]!=0 && !update[i]->isHeader() &&
               contains_interval(m->getInterval(), update[i]->key,
                                                   x->forward[i]->key))
            {
                tempRemoved.insert(m->getInterval());
            }
        }
        demoted.removeAll(&tempRemoved);
        demoted.copy(&newDemoted);
        newDemoted.clear();
    }
}  // end adjustMarkersOnDelete


template<typename Key, class Comparator>
void IntervalSkipList<Key, Comparator>::remove(IntervalSLNode* x,
                                                 IntervalSLNode** update) {
    // Remove interval skip list node x.  The markers that the interval
    // x belongs to have already been removed.

    adjustMarkersOnDelete(x, update);

    // now splice out x.
    for(int i = 0; i <= x->level() - 1; i++)
        update[i]->forward[i] = x->forward[i];

    // and finally deallocate it
    delete x;
}

template<typename Key, class Comparator>
int IntervalSkipList<Key, Comparator>::randomLevel() {
    //Increase height with probability 1 in kBranching
    static const unsigned int kBranching = 4;
    int height = 1;
    while (height < MAX_FORWARD && ((random.Next() % kBranching) == 0)){
        height++;
    }
    return height;
}



// class IntervalSLNode
template<typename Key, class Comparator>
class IntervalSkipList<Key, Comparator>::IntervalSLNode {

private:
    friend class IntervalSkipList;
    const bool is_header;
    const Key key;
    const int topLevel;   // top level of forward pointers in this node
    // Levels are numbered 0..topLevel.
    IntervalSLNode** forward;   // array of forward pointers
    IntervalList** markers;    // array of interval markers, one for each pointer
    IntervalList* eqMarkers;  // See comment of find_intervals
    IntervalList* startMarker;  // Any intervals start at this node will put its mark on it
    IntervalList* endMarker; // Any intervals end at this node will put its mark on it
    int ownerCount; // number of interval end points with Key equal to key

public:
    explicit IntervalSLNode(const Key &searchKey, int levels);
    explicit IntervalSLNode(int levels);    // constructor for the head_

    ~IntervalSLNode();

    // number of levels of this node
    inline int level() const {
        return topLevel + 1;
    }

    inline bool isHeader() const {
        return is_header;
    }

    void print(std::ostream &os) const;

private:
    // No copying allowed
    IntervalSLNode(const IntervalSLNode&);
    void operator=(const IntervalSLNode);

};

template<typename Key, class Comparator>
IntervalSkipList<Key, Comparator>::
IntervalSLNode::IntervalSLNode(const Key &searchKey, int levels)
        : is_header(false), key(searchKey), topLevel(levels), ownerCount(0) {
    // levels is actually one less than the real number of levels
    forward = new IntervalSLNode*[levels+1];
    markers = new IntervalList*[levels+1];
    eqMarkers = new IntervalList();
    startMarker = new IntervalList();
    endMarker = new IntervalList();
    for (int i = 0; i <= levels; i++) {
        forward[i] = 0;
        // initialize an empty interval list
        markers[i] = new IntervalList();
    }
}

template<typename Key, class Comparator>
IntervalSkipList<Key, Comparator>::
IntervalSLNode::IntervalSLNode(int levels)
        : is_header(true), key(0), topLevel(levels), ownerCount(0) {
    forward = new IntervalSLNode*[levels+1];
    markers = new IntervalList*[levels+1];
    eqMarkers = new IntervalList();
    startMarker = new IntervalList();
    endMarker = new IntervalList();
    for (int i = 0; i <= levels; i++) {
        forward[i] = 0;
        // initialize an empty interval list
        markers[i] = new IntervalList();
    }
}

template<typename Key, class Comparator>
IntervalSkipList<Key, Comparator>::
IntervalSLNode::~IntervalSLNode() {
    for (int i = 0; i <= topLevel; i++) {
        delete markers[i];
    }
    delete[] forward;
    delete[] markers;
    delete eqMarkers;
    delete startMarker;
    delete endMarker;
}

template<typename Key, class Comparator>
void IntervalSkipList<Key, Comparator>::
IntervalSLNode::print(std::ostream &os) const {
    int i;
    os << "IntervalSLNode key:  ";
    if (is_header) {
        os << "HEADER";
    } else {
        // Raw data
        Decode(key, os);
        //os << key;
    }
    os << "\n";
    os << "number of levels: " << level() << std::endl;
    os << "owning intervals:  ";
    os << "ownerCount = " << ownerCount << std::endl;
    os <<  std::endl;
    os << "forward pointers:\n";
    for (i = 0; i <= topLevel; i++)
    {
        os << "forward[" << i << "] = ";
        if(forward[i] != nullptr) {
            Decode(forward[i]->key, os);
            //os << forward[i]->key;
        } else {
            os << "nullptr";
        }
        os << std::endl;
    }
    os << "markers:\n";
    for(i = 0; i <= topLevel; i++)
    {
        os << "markers[" << i << "] = ";
        if(markers[i] != nullptr) {
            markers[i]->print(os);
            os << " (count: "<<markers[i]->count<<")";
        } else {
            os << "nullptr";
        }
        os << "\n";
    }
    os << "EQ markers:  ";
    eqMarkers->print(os);
    os << " (count: "<<eqMarkers->count<<")";
    os << std::endl;
    os << "START markers:  ";
    startMarker->print(os);
    os << " (count: "<<startMarker->count<<")";
    os << std::endl;
    os << "END markers:  ";
    endMarker->print(os);
    os << " (count: "<<endMarker->count<<")";
    os << std::endl << std::endl;
}

// class Interval
template<typename Key, class Comparator>
class IntervalSkipList<Key, Comparator>::Interval {
private:
    friend class IntervalSkipList;
    const Key inf_;
    const Key sup_;
    const uint64_t stamp_;  // fresh intervals have greater timestamp
    NvmMemTable* const table_;
    std::atomic<int> refs_;

    Interval(const Interval&);
    Interval operator=(const Interval);

    ~Interval() = default;

    // Only index can generate Interval.
    explicit Interval(const Key& inf, const Key& sup,
                      uint64_t stamp, NvmMemTable* const table);

public:

    void print(std::ostream &os) const;

    // Below methods to be called outside index.
    inline const Key& inf() const { return inf_; }

    inline const Key& sup() const { return sup_; }

    inline const uint64_t stamp() const { return stamp_; }

    inline NvmMemTable* const get_table() const { return table_; }

    void Ref() { refs_++; }

    void Unref() {
        refs_--;
        assert(refs_ >= 0);
        if (refs_ == 0) {
            delete table_;
            delete this;
        }
    }

};

template<typename Key, class Comparator>
IntervalSkipList<Key, Comparator>::
Interval::Interval(const Key& inf,
                   const Key& sup,
                   const uint64_t stamp,
                   NvmMemTable* const table)
                  : inf_(inf), sup_(sup), stamp_(stamp), table_(table), refs_(1) {
    assert(table_ != nullptr);
}

template<typename Key, class Comparator>
void IntervalSkipList<Key, Comparator>::
Interval::print(std::ostream& os) const {
    //os << "[" << inf_ << ", " << sup_ << "]" << " {timestamp: " << stamp_ << "} ";
    os << "[" ;
    Decode(inf_, os);
    os << ", ";
    Decode(sup_, os);
    os << "]" << " {timestamp: "<< stamp_ <<"} ";
}

template<typename Key, class Comparator>
bool IntervalSkipList<Key, Comparator>::contains(const Interval* I,
                                                   const Key& K) const {
    return KeyCompare(I->inf_, K) <= 0 && KeyCompare(K, I->sup_) <= 0;
}

template<typename Key, class Comparator>
bool IntervalSkipList<Key, Comparator>::contains_interval(const Interval* I,
                                                            const Key& i,
                                                            const Key& s) const {
    return KeyCompare(I->inf_, i) <= 0 && KeyCompare(s, I->sup_) <= 0;
}


// class IntervalListElt
template<typename Key, class Comparator>
class IntervalSkipList<Key, Comparator>::IntervalListElt {
private:
    friend class IntervalList;
    typedef IntervalListElt* ILE_handle;
    const Interval* const I;
    ILE_handle next;

    // No copying allowed
    IntervalListElt(const IntervalListElt&);
    void operator=(const IntervalListElt);
public:
    explicit IntervalListElt(const Interval*I);
    ~IntervalListElt() { }

    inline void set_next(ILE_handle nextElt) { next = nextElt; }

    inline ILE_handle get_next() const { return next; }

    inline const Interval* getInterval() const { return I; }

    bool operator==(const IntervalListElt& e) const {
        return I == e.I && next == e.next;
    }

    void print(std::ostream& os) const { I->print(os); }

};


template<typename Key, class Comparator>
IntervalSkipList<Key, Comparator>::
        IntervalListElt::IntervalListElt(const Interval* const interval)
        : I(interval), next(nullptr) { }


// class IntervalList
template<typename Key, class Comparator>
class IntervalSkipList<Key, Comparator>::IntervalList {
private:
    typedef IntervalListElt* ILE_handle;
    ILE_handle first_;

    // No copying allowed
    IntervalList(const IntervalList&);
    void operator=(const IntervalList);
public:
    int count;

    explicit IntervalList();
    ~IntervalList();

    void insert(const Interval* I);

    bool remove(const Interval* I, Interval*& res);

    void remove(const Interval* I);

    void removeAll(IntervalList* l);

    // tips: Allocator may fit this better
    inline ILE_handle create_list_element(const Interval* const I) {
        count++;
        return new IntervalListElt(I);
    }

    inline void erase_list_element(ILE_handle I) {
        delete I;
        count--;
    }

    inline ILE_handle get_first() const { return first_; }

    void copy(IntervalList* from); // add contents of "from" to self

    template <class OutputIterator>
    OutputIterator
    copy(OutputIterator out) const {
        ILE_handle e = first_;
        while (e != nullptr) {
            out = const_cast<Interval*>(e->I);
            ++out;
            e = e->next;
        }
        return out;
    }

    bool contains(const Interval* I) const;

    void clear();  // remove elements of self to make self an empty list.

    void print(std::ostream& os) const;

};

template<typename Key, class Comparator>
IntervalSkipList<Key, Comparator>::IntervalList::IntervalList()
        : first_(nullptr), count(0) { }

template<typename Key, class Comparator>
IntervalSkipList<Key, Comparator>::IntervalList::~IntervalList() {
    clear();
}

template<typename Key, class Comparator>
inline void IntervalSkipList<Key, Comparator>::IntervalList::insert(const Interval* const I) {
    ILE_handle temp = create_list_element(I);
    temp->set_next(first_);
    first_ = temp;
}


template<typename Key, class Comparator>
bool IntervalSkipList<Key, Comparator>::IntervalList::remove(const Interval* I,
                                                               Interval*& res) {
    ILE_handle x, last;
    x = first_;
    last = nullptr;
    while (x != nullptr && x->getInterval() != I) {
        last = x;
         x = x->get_next();
    }
    // after the tail | at the head | in the between
    if(x == nullptr) {
        return false;
    } else if (last == nullptr) {
        first_ = x->get_next();
        res = const_cast<Interval*>(x->getInterval());
        erase_list_element(x);
    } else {
        last->set_next(x->get_next());
        res = const_cast<Interval*>(x->getInterval());
        erase_list_element(x);
    }
    return true;
}

template<typename Key, class Comparator>
void IntervalSkipList<Key, Comparator>::IntervalList::remove(const Interval* I) {
    ILE_handle x, last;
    x = first_;
    last = nullptr;
    while (x != nullptr && x->getInterval() != I) {
        last = x;
        x = x->get_next();
    }
    // after the tail | at the head | in the between
    if(x == nullptr) {
        return ;
    } else if (last == nullptr) {
        first_ = x->get_next();
        erase_list_element(x);
    } else {
        last->set_next(x->get_next());
        erase_list_element(x);
    }
}

template<typename Key, class Comparator>
void IntervalSkipList<Key, Comparator>::IntervalList::removeAll(IntervalList *l) {
    ILE_handle x;
    for (x = l->get_first(); x != nullptr; x = x->get_next()) {
        remove(x->getInterval());
    }
}


template<typename Key, class Comparator>
void IntervalSkipList<Key, Comparator>::IntervalList::copy(IntervalList* from) {
    ILE_handle e = from->get_first();
    while (e != nullptr) {
        insert(e->getInterval());
        e = e->get_next();
    }
}


template<typename Key, class Comparator>
bool IntervalSkipList<Key, Comparator>::IntervalList::contains(const Interval* I) const {
    ILE_handle x = first_;
    while (x != 0 && I != x->getInterval()) {
        x = x->get_next();
    }
    return x != nullptr;
}

template<typename Key, class Comparator>
void IntervalSkipList<Key, Comparator>::IntervalList::clear() {
    ILE_handle x = first_;
    ILE_handle y;
    while (x != nullptr) { // was 0
        y = x;
        x = x->get_next();
        erase_list_element(y);
    }
    first_ = nullptr;
}

template<typename Key, class Comparator>
void IntervalSkipList<Key, Comparator>::IntervalList::print(std::ostream &os) const {
    ILE_handle e = first_;
    while (e != nullptr) {
        e->print(os);
        e = e->get_next();
    }
}



}

#endif //SOFTDB_NVM_INDEX_H
