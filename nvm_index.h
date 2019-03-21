//
// Created by lingo on 19-2-17.
//

#ifndef SOFTDB_NVM_INDEX_H
#define SOFTDB_NVM_INDEX_H

#include "dbformat.h"
#include "random.h"
#include "nvm_memtable.h"
#include "merger.h"
#include <vector>

namespace softdb {


// TODO: Need rwlock urcu seqlock etc. good enough to run it under single writer/multiple readers scenario.
template<typename Value, class Comparator>
class IntervalSkipList {
public:

    class Interval;

private:

    class IntervalSLnode;

    class IntervalListElt;

    class IntervalList;

    // Maximum number of forward pointers
    enum { MAX_FORWARD = 32 };

    int maxLevel;

    Random random;

    IntervalSLnode* const head_;

    const Comparator comparator_;

    uint64_t timestamp_; // mark every interval with an unique timestamp, start from 1.

    uint64_t iCount_;   // interval count

    // Is there any need?
    //Interval container;

    typedef IntervalListElt* ILE_handle;

    int randomLevel();  // choose a new node level at random

    // Three-way comparison.  Returns value:
    //   <  0 iff "a" <  "b",
    //   == 0 iff "a" == "b",
    //   >  0 iff "a" >  "b".
    inline int ValueCompare(const Value &a, const Value &b, const bool ukey = false) const {
        return comparator_(a, b, ukey);
    }


    bool contains(const Interval* I, const Value& V) const;

    bool contains_interval(const Interval* I, const Value& i, const Value& s) const;

    // Search for search key, and return a pointer to the
    // intervalSLnode x found, as well as setting the update vector
    // showing pointers into x.
    IntervalSLnode* search(const Value& searchKey,
                           IntervalSLnode** update) const;

    // insert a new single value
    // into list, returning a pointer to its location.
    IntervalSLnode* insert(const Value& searchKey);



    // adjust markers after insertion of x with update vector "update"
    void adjustMarkersOnInsert(IntervalSLnode* x,
                               IntervalSLnode** update);

    // Remove markers for interval m from the edges and nodes on the
    // level i path from l to r.
    void removeMarkFromLevel(const Interval* m, int i,
                             IntervalSLnode* l,
                             IntervalSLnode* r);

    // place markers for Interval I.  I must have been inserted in the list.
    // left is the left endpoint of I and right is the right endpoint if I.
    // *** needs to be fixed:
    void placeMarkers(IntervalSLnode* left,
                      IntervalSLnode* right,
                      const Interval* I);

    // remove markers for Interval I starting at left, the left endpoint
    // of I, and and stopping at the right endpoint of I.
    Interval *removeMarkers(IntervalSLnode* left,
                            const Interval* I);

    // adjust markers to prepare for deletion of x, which has update vector
    // "update"
    void adjustMarkersOnDelete(IntervalSLnode* x,
                               IntervalSLnode** update);

    // remove node x, which has updated vector update.
    void remove(IntervalSLnode* x,
                IntervalSLnode** update);

    inline static bool timeCmp(Interval* l, Interval* r) {
        return l->stamp() > r->stamp();
    }



    template<class InputIterator>
    IntervalSkipList(Comparator cmp, InputIterator b, InputIterator e)
            : maxLevel(0),
              random(0xdeadbeef),
              timestamp_(1),
              iCount_(0),
              comparator_(cmp),
              head_(new IntervalSLnode(MAX_FORWARD)) {
        for (int i = 0; i < MAX_FORWARD; i++) {
            head_->forward[i] = nullptr;
        }
        iCount_ += insert(b, e);
    }

    template <class InputIterator>
    int insert(InputIterator b, InputIterator e)
    {
        int i = 0;
        for(; b!= e; ++b){
            insert(*b);
            ++i;
        }
        return i;
    }


    bool is_contained(const Value &searchKey) const {
        IntervalSLnode *x = head_;
        for (int i = maxLevel;
             i >= 0 && (x->isHeader() || ValueCompare(x->key, searchKey) != 0); i--) {
            while (x->forward[i] != 0 && ValueCompare(searchKey, x->forward[i]->key) >= 0) {
                x = x->forward[i];
            }
            // Pick up markers on edge as you drop down a level, unless you are at
            // the searchKey node already, in which case you pick up the
            // eqMarkers just prior to exiting loop.
            if (!x->isHeader() && ValueCompare(x->key, searchKey) != 0) {
                return true;
            } else if (!x->isHeader()) { // we're at searchKey
                return true;
            }
        }
        return false;
    }


    // return node containing Value if found, otherwise nullptr
    IntervalSLnode* search(const Value& searchKey) const;

    // insert an interval into list
    void insert(const Interval* I);

    // FindSmallerOrEqual
    // To support point query

    // It is assumed that when a marker is placed on an edge,
    // it will be placed in the eqMarkers sets of a node on either
    // end of the edge if the interval for the marker covers the node.
    // Similarly, when a marker is removed from an edge markers will
    // be removed from eqMarkers sets on nodes adjacent to the edge.
    template<class OutputIterator>
    OutputIterator
    find_intervals(const Value &searchKey, OutputIterator out) const {
        IntervalSLnode *x = head_;
        for (int i = maxLevel;
             i >= 0 && (x->isHeader() || ValueCompare(x->key, searchKey) != 0); i--) {
            while (x->forward[i] != 0 && ValueCompare(searchKey, x->forward[i]->key) >= 0) {
                x = x->forward[i];
            }
            // Pick up markers on edge as you drop down a level, unless you are at
            // the searchKey node already, in which case you pick up the
            // eqMarkers just prior to exiting loop.
            if (!x->isHeader() && ValueCompare(x->key, searchKey) != 0) {
                out = x->markers[i]->copy(out);
            } else if (!x->isHeader()) { // we're at searchKey
                out = x->eqMarkers->copy(out);
            }
        }
        // Do not miss any intervals that has the same user key as searchKey
        if (x->forward[0] != 0 && ValueCompare(x->forward[0]->key, searchKey, true) == 0) {
            out = x->forward[0]->startMarker->copy(out);
        }
        return out;
    }

    // FindSmallerOrEqual
    // To support scan query

    // REQUIRES: node's internal key not deleted.
    template<class OutputIterator>
    OutputIterator
    find_intervals(const Value &searchKey, OutputIterator out,
                   Value& left, Value& right) const {
        IntervalSLnode *x = head_;
        IntervalSLnode *before = head_;
        IntervalSLnode* watch = nullptr;
        bool equal = false;
        int i = 0;
        for (i = maxLevel;
             i >= 0 && (x->isHeader() || ValueCompare(x->key, searchKey) != 0); i--) {
            while (x->forward[i] != 0 && ValueCompare(searchKey, x->forward[i]->key) >= 0) {
                // before x at level i
                before = x;
                x = x->forward[i];
                //std::cout<<"move"<<std::endl;
                assert(x != head_);
                watch = x;
            }
            // Pick up markers on edge as you drop down a level, unless you are at
            // the searchKey node already, in which case you pick up the
            // eqMarkers just prior to exiting loop.
            if (!x->isHeader() && ValueCompare(x->key, searchKey) != 0) {
                out = x->markers[i]->copy(out);
            } else if (!x->isHeader()) { // we're at searchKey
                out = x->eqMarkers->copy(out);
                equal = true;
            }
        }
        //assert(x != head_);
        assert(watch != head_);

        // always fetch intervals belong to left and right
        if (x->forward[0] != 0) {
            out = x->forward[0]->startMarker->copy(out);
        }
        right = (x->forward[0] != 0) ? x->forward[0]->key : 0;

        if (equal) {
            // [before, searchKey(x), x->forward[0]](before can be head_ where left is set to 0)
            for (;i >= 0; i--) {
                while (before->forward[i] != x) {
                    before = before->forward[i];
                }
            }
            // now before x at level 0
            // whether before is head_ doesn't matter.
            out = before->endMarker->copy(out);
            left = before->key;
        } else {
            // [x, searchKey, x->forward[0]](x->forward[0] can be nullptr where right is set to 0)
            assert(x != nullptr);
            //assert(x != head_);
            left = x->key;
            out = x->endMarker->copy(out);
        }

        return out;
    }

    IntervalSLnode* find_last() const {
        IntervalSLnode *x = head_;
        int level = maxLevel;
        while (true) {
            IntervalSLnode* next = x->forward[level];
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


    bool remove(const Interval* I);  // remove an interval from list

    // No copying allowed
    IntervalSkipList(const IntervalSkipList&);
    void operator=(const IntervalSkipList);



public:

    // User may be interesting about methods below.

    explicit IntervalSkipList(Comparator cmp);

    ~IntervalSkipList();

    inline uint64_t size() const { return iCount_; }   //number of intervals

    void clearIndex();

    // print every nodes' information
    void print(std::ostream& os) const;
    // print every nodes' values in order
    void printOrdered(std::ostream& os) const;

    inline uint64_t NextTimestamp() const { return timestamp_; }


    // Insert a new table generated by imm_ or generated by merged nvm_imm_.
    void insert(const Value& l, const Value& r, NvmMemTable* table, uint64_t timestamp = 0);

    // Return the tables contain this searchKey.
    void search(const Value& searchKey, std::vector<Interval*>& intervals) const;

    // After merge old intervals to insert new ones, remove the old.
    void remove(const Value& l, const Value& r, uint64_t timestamp);


    class IteratorHelper {
    public:
        explicit IteratorHelper(const IntervalSkipList* list)
                                : list_(list) {

        }

        void Release() {
            // release the intervals in last search
            for (auto &interval : intervals) {
                interval->Unref();
            }
        }


        // Every Seek operation will fetch some intervals, protected by read lock,
        // we should reference these intervals, prevent them from interval delete operation,
        // the next time we execute Seek operation, we should first release these intervals.
        void Seek(const Value& target, std::vector<Iterator*>& iterators, Value& left, Value& right) {
            if (list_->head_->forward[0] == nullptr) {
                return;
            }
            // target < first node's key.
            // If skip this situation, left and right will be set to [0, firstKey],
            // and when we call next, we will skip the firstKey and traverse the first interval,
            // an other Seek() will never be triggered.
            if (list_->ValueCompare(target, list_->head_->forward[0]->key) < 0) {
                //std::cout<<"hello"<<std::endl;
                SeekToFirst(iterators, left, right);
            }

            // release the intervals in last search
            Release();
            intervals.clear();
            // read lock
            list_->find_intervals(target, std::back_inserter(intervals), left, right);
            for (auto &interval : intervals) {
                interval->Ref();
                //std::cout<<interval->stamp()<<" ";
                iterators.push_back(interval->get_table()->NewIterator());
            }
            //std::cout<<std::endl;
            // read unlock
        }

        void SeekToFirst(std::vector<Iterator*>& iterators, Value& left, Value& right) {
            // no data
            if (list_->head_->forward[0] == nullptr) {
                return;
            } else {
                Seek(list_->head_->forward[0]->key, iterators, left, right);
            }
        }

        void SeekToLast(std::vector<Iterator*>& iterators, Value& left, Value& right) {
            IntervalSLnode* tmp = list_->find_last();
            // no data
            if (tmp == list_->head_) {
                return;
            } else {
                Seek(tmp->key, iterators, left, right);
            }
        }

    private:
        const IntervalSkipList* list_;
        std::vector<Interval*> intervals;

    };





};

template<typename Value, class Comparator>
IntervalSkipList<Value, Comparator>::IntervalSkipList(Comparator cmp)
                                    : maxLevel(0),
                                      random(0xdeadbeef),
                                      timestamp_(1),
                                      iCount_(0),
                                      comparator_(cmp),
                                      head_(new IntervalSLnode(MAX_FORWARD)) {
    for (int i = 0; i < MAX_FORWARD; i++) {
        head_->forward[i] = nullptr;
    }
}

template<typename Value, class Comparator>
IntervalSkipList<Value, Comparator>::~IntervalSkipList() {
    IntervalSLnode* cursor = head_;
    while (cursor){
        IntervalSLnode* next = cursor->forward[0];
        delete cursor;
        cursor = next;
    }
}

// record the most levels found with it's count, and set a threshold.
template<typename Value, class Comparator>
void IntervalSkipList<Value, Comparator>::insert(const Value& l,
                                                 const Value& r,
                                                 NvmMemTable* table,
                                                 uint64_t timestamp) {
    uint64_t mark = (timestamp == 0) ? timestamp_++ : timestamp;
    Interval* I = new Interval(l, r, mark, table);
    // write lock
    insert(I);
    I->Ref();
    // write unlock
}

// record the most levels found with it's count, and set a threshold.
template<typename Value, class Comparator>
void IntervalSkipList<Value, Comparator>::search(const Value& searchKey,
                                                std::vector<Interval*>& intervals) const {
    // read lock
    find_intervals(searchKey, std::back_inserter(intervals));
    for (auto &interval : intervals) {
        interval->Ref();
    }
    // read unlock
    std::sort(intervals.begin(), intervals.end(), timeCmp);
}

 // REQUIRES: NvmMemTable has been released.
 // Note: parameter should be corrected to Interval*, which has
 // been passed outside, should be a unique ptr.
template<typename Value, class Comparator>
void IntervalSkipList<Value, Comparator>::remove(const Value& l,
                                                 const Value& r,
                                                 uint64_t timestamp) {



}







template<typename Value, class Comparator>
void IntervalSkipList<Value, Comparator>::clearIndex() {
    IntervalSLnode* cursor = head_;
    while (cursor) {
        IntervalSLnode* next = cursor->forward[0];
        delete cursor;
        cursor = next;
    }
    for (int i = 0; i < MAX_FORWARD; i++) {
        head_->forward[i] = nullptr;
    }
    maxLevel = 0;
    timestamp_ = 1;
    iCount_ = 0;
}

template<typename Value, class Comparator>
typename IntervalSkipList<Value, Comparator>::
IntervalSLnode* IntervalSkipList<Value, Comparator>::search(const Value& searchKey) const {
    IntervalSLnode* x = head_;
    for(int i = maxLevel; i >= 0; i--) {
        while (x->forward[i] != 0 && ValueCompare(x->forward[i]->key, searchKey) < 0) {
            x = x->forward[i];
        }
    }
    x = x->forward[0];
    if(x != nullptr && ValueCompare(x->key, searchKey) == 0)
        return x;
    else
        return nullptr;
}

template<typename Value, class Comparator>
void IntervalSkipList<Value, Comparator>::print(std::ostream& os) const {
    os << "\nAn Interval_skip_list"<< "("<<iCount_<<")"<<":  \n";
    IntervalSLnode* n = head_->forward[0];

    while ( n != 0 ) {
        n->print(os);
        n = n->forward[0];
    }
}

template<typename Value, class Comparator>
void IntervalSkipList<Value, Comparator>::printOrdered(std::ostream& os) const {
    IntervalSLnode* n = head_->forward[0];
    os << "values in list:  ";
    while ( n != 0 ) {
        os << n->key << " ";
        n = n->forward[0];
    }
    os << std::endl;
}

template<typename Value, class Comparator>
std::ostream& operator<<(std::ostream& os,
                         const IntervalSkipList<Value, Comparator>& isl) {
    isl.print(os);
    return os;
}

template<typename Value, class Comparator>
typename IntervalSkipList<Value, Comparator>::
IntervalSLnode* IntervalSkipList<Value, Comparator>::search(const Value& searchKey,
                                                            IntervalSLnode** update) const {
    IntervalSLnode* x = head_;
    // Find location of searchKey, building update vector indicating
    // pointers to change on insertion.
    for(int i = maxLevel; i >= 0; i--) {
        while (x->forward[i] != 0 && ValueCompare(x->forward[i]->key, searchKey) < 0) {
            x = x->forward[i];
        }
        update[i] = x;
    }
    x = x->forward[0];
    return x;
}

template<typename Value, class Comparator>
void IntervalSkipList<Value, Comparator>::insert(const Interval* I) {
    // insert end points of interval
    IntervalSLnode* left = this->insert(I->inf());
    IntervalSLnode* right = this->insert(I->sup());
    left->ownerCount++;
    left->startMarker->insert(I);
    right->ownerCount++;
    right->endMarker->insert(I);

    // place markers on interval
    this->placeMarkers(left, right, I);
    iCount_++;
}

template<typename Value, class Comparator>
typename IntervalSkipList<Value, Comparator>::
IntervalSLnode* IntervalSkipList<Value, Comparator>::insert(const Value& searchKey) {
    // array for maintaining update pointers
    IntervalSLnode* update[MAX_FORWARD];
    IntervalSLnode* x;
    int i;

    // Find location of searchKey, building update vector indicating
    // pointers to change on insertion.
    x = search(searchKey, update);
    if(x == 0 || ValueCompare(x->key, searchKey) != 0 ) {
        // put a new node in the list for this searchKey
        int newLevel = randomLevel();
        if (newLevel > maxLevel) {
            for(i = maxLevel + 1; i <= newLevel; i++){
                update[i] = head_;
            }
            maxLevel = newLevel;
        }
        x = new IntervalSLnode(searchKey, newLevel);

        // add x to the list
        for(i = 0; i <= newLevel; i++) {
            x->forward[i] = update[i]->forward[i];
            update[i]->forward[i] = x;
        }

        // adjust markers to maintain marker invariant
        this->adjustMarkersOnInsert(x, update);
    }
    // else, the searchKey is in the list already, and x points to it.
    return x;
}

    // Adjust markers on this IS-list to maintain marker invariant now that
    // node x has just been inserted, with update vector `update.'

template<typename Value, class Comparator>
void IntervalSkipList<Value, Comparator>::adjustMarkersOnInsert(IntervalSLnode* x,
                                                                IntervalSLnode** update) {
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

template<typename Value, class Comparator>
void IntervalSkipList<Value, Comparator>::removeMarkFromLevel(const Interval* m, int i,
                                                              IntervalSLnode* l,
                                                              IntervalSLnode* r) {
    IntervalSLnode *x;
    for(x = l; x != 0 && x != r; x = x->forward[i]) {
        x->markers[i]->remove(m);
        x->eqMarkers->remove(m);
    }
    if(x != 0) x->eqMarkers->remove(m);
}

template<typename Value, class Comparator>
void IntervalSkipList<Value, Comparator>::placeMarkers(IntervalSLnode* left,
                                                       IntervalSLnode* right,
                                                       const Interval* I)
{
    // Place markers for the interval I.  left is the left endpoint
    // of I and right is the right endpoint of I, so it isn't necessary
    // to search to find the endpoints.

    IntervalSLnode* x = left;
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
    while (ValueCompare(x->key, right->key) != 0) {
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



// REQUIRES: Interval exists.
template<typename Value, class Comparator>
bool IntervalSkipList<Value, Comparator>::remove(const Interval* I) {
    // arrays for maintaining update pointers
    IntervalSLnode* update[MAX_FORWARD];

    IntervalSLnode* left = search(I->inf(), update);
    if(left == 0 || left->ownerCount <= 0) {
        return false;
    }

    Interval* ih = removeMarkers(left, I);

    left->startMarker->remove(I);
    left->ownerCount--;
    if(left->ownerCount == 0) remove(left, update);

    // Note:  we search for right after removing left since some
    // of left's forward pointers may point to right.  We don't
    // want any pointers of update vector pointing to a node that is gone.

    IntervalSLnode* right = search(I->sup(), update);
    if(right == 0 || right->ownerCount <= 0) {
        return false;
    }
    right->endMarker->remove(I);
    right->ownerCount--;
    if(right->ownerCount == 0) remove(right, update);
    I->Unref();
    iCount_--;
    return true;
}

template<typename Value, class Comparator>
typename IntervalSkipList<Value, Comparator>::Interval*
IntervalSkipList<Value, Comparator>::removeMarkers(IntervalSLnode* left,
                                                   const Interval* I) {
    // Remove markers for interval I, which has left as it's left
    // endpoint,  following a staircase pattern.

    // Interval_handle res=0, tmp=0; // af: assignment not possible with std::list
    Interval* res, tmp;
    // remove marks from ascending path
    IntervalSLnode* x = left;
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
    while (ValueCompare(x->key, I->sup()) != 0) {
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

template<typename Value, class Comparator>
void IntervalSkipList<Value, Comparator>::adjustMarkersOnDelete(IntervalSLnode* x,
                                                            IntervalSLnode** update) {
    // x is node being deleted.  It is still in the list.
    // update is the update vector for x.
    IntervalList demoted;
    IntervalList newDemoted;
    IntervalList tempRemoved;
    ILE_handle m;
    int i;
    IntervalSLnode *y;

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


template<typename Value, class Comparator>
void IntervalSkipList<Value, Comparator>::remove(IntervalSLnode* x,
                                                 IntervalSLnode** update) {
    // Remove interval skip list node x.  The markers that the interval
    // x belongs to have already been removed.

    adjustMarkersOnDelete(x, update);

    // now splice out x.
    for(int i = 0; i <= x->level() - 1; i++)
        update[i]->forward[i] = x->forward[i];

    // and finally deallocate it
    delete x;
}

template<typename Value, class Comparator>
int IntervalSkipList<Value, Comparator>::randomLevel() {
    //Increase height with probability 1 in kBranching
    static const unsigned int kBranching = 4;
    int height = 1;
    while (height < MAX_FORWARD && ((random.Next() % kBranching) == 0)){
        height++;
    }
    return height;
}



// class IntervalSLnode
template<typename Value, class Comparator>
class IntervalSkipList<Value, Comparator>::IntervalSLnode {

private:
    friend class IntervalSkipList;
    const Value key;
    const int topLevel;   // top level of forward pointers in this node
    // Levels are numbered 0..topLevel.
    const bool is_header;
    IntervalSLnode** forward;   // array of forward pointers
    IntervalList** markers;    // array of interval markers, one for each pointer
    IntervalList* eqMarkers;  // See comment of find_intervals
    IntervalList* startMarker;  // Any intervals start at this node will put its mark on it
    IntervalList* endMarker; // Any intervals end at this node will put its mark on it
    int ownerCount; // number of interval end points with value equal to key

public:
    explicit IntervalSLnode(const Value &searchKey, int levels);
    explicit IntervalSLnode(int levels);    // constructor for the head_

    ~IntervalSLnode();

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
    IntervalSLnode(const IntervalSLnode&);
    void operator=(const IntervalSLnode);

};

template<typename Value, class Comparator>
IntervalSkipList<Value, Comparator>::
IntervalSLnode::IntervalSLnode(const Value &searchKey, int levels)
        : is_header(false), key(searchKey), topLevel(levels), ownerCount(0) {
    // levels is actually one less than the real number of levels
    forward = new IntervalSLnode*[levels+1];
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

template<typename Value, class Comparator>
IntervalSkipList<Value, Comparator>::
IntervalSLnode::IntervalSLnode(int levels)
        : is_header(true), key(0), topLevel(levels), ownerCount(0) {
    forward = new IntervalSLnode*[levels+1];
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

template<typename Value, class Comparator>
IntervalSkipList<Value, Comparator>::
IntervalSLnode::~IntervalSLnode() {
    for (int i = 0; i <= topLevel; i++) {
        delete markers[i];
    }
    delete[] forward;
    delete[] markers;
    delete eqMarkers;
    delete startMarker;
    delete endMarker;
}

template<typename Value, class Comparator>
void IntervalSkipList<Value, Comparator>::
IntervalSLnode::print(std::ostream &os) const {
    int i;
    os << "IntervalSLnode key:  ";
    if (is_header) {
        os << "HEADER";
    } else {
        // Raw data
        os << key;
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
            os << forward[i]->key;
        } else {
            os << "nullptr";
        }
        os << std::endl;
    }
    os << "markers:\n";
    for(i=0; i<=topLevel; i++)
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
template<typename Value, class Comparator>
class IntervalSkipList<Value, Comparator>::Interval {
private:
    friend class IntervalSkipList;
    Value inf_;
    Value sup_;
    const uint64_t stamp_;  // differentiate intervals
    NvmMemTable* table_;
    IntervalSLnode* start_;
    IntervalSLnode* end_;
    int refs_;

    Interval(const Interval&);
    Interval operator=(const Interval);

    ~Interval() {}

    // Only index can generate Interval.
    explicit Interval(const Value& inf, const Value& sup,
                      const uint64_t& stamp, NvmMemTable* table = nullptr);


    inline void set_start(IntervalSLnode* start) { start_ = start; }

    inline IntervalSLnode* get_start() { return start_; }

    inline void set_end(IntervalSLnode* end) { end_ = end; }

    inline IntervalSLnode* get_end() { return end_; }

    // No need to do ValueCompare(l.inf(), r.inf()) == 0
    // && ValueCompare(l.sup(), r.sup()) == 0, as stamp_ is unique.
    bool operator==(const Interval* I) const {
        return stamp_ == I->stamp_;
    }

    bool operator!=(const Interval* I) const {
        return ! (*this == I);
    }


    void print(std::ostream &os) const;

public:
    // Below methods to be called outside index.
    inline const Value& inf() const { return inf_; }

    inline const Value& sup() const { return sup_; }

    inline const uint64_t stamp() const { return stamp_; }

    inline NvmMemTable* get_table() { return table_; }

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

template<typename Value, class Comparator>
IntervalSkipList<Value, Comparator>::
Interval::Interval(const Value& inf,
                   const Value& sup,
                   const uint64_t& stamp,
                   NvmMemTable* table)
                  : inf_(inf), sup_(sup), stamp_(stamp), table_(table), refs_(0) {
    //assert( !(inf_ > sup_) );
    assert( table_ != nullptr);
}

template<typename Value, class Comparator>
void IntervalSkipList<Value, Comparator>::
Interval::print(std::ostream& os) const {
    os << "[" << inf_ << ", " << sup_ << "]" << " {timestamp: "<< stamp_ <<"} ";
}

template<typename Value, class Comparator>
bool IntervalSkipList<Value, Comparator>::contains(const Interval* I,
                                                   const Value& V) const {
    return ValueCompare(I->inf(), V) <= 0 && ValueCompare(V, I->sup()) <= 0;
}

template<typename Value, class Comparator>
bool IntervalSkipList<Value, Comparator>::contains_interval(const Interval* I,
                                                            const Value& i,
                                                            const Value& s) const {
    return ValueCompare(I->inf(), i) <= 0 && ValueCompare(s, I->sup()) <= 0;
}


// class IntervalListElt
template<typename Value, class Comparator>
class IntervalSkipList<Value, Comparator>::IntervalListElt {
private:
    friend class IntervalList;
    typedef IntervalListElt* ILE_handle;
    const Interval* I;
    ILE_handle next;

    // No copying allowed
    IntervalListElt(const IntervalListElt&);
    void operator=(const IntervalListElt);
public:
    explicit IntervalListElt();
    explicit IntervalListElt(const Interval* I);
    ~IntervalListElt() { }

    inline void set_next(ILE_handle nextElt) { next = nextElt; }

    inline ILE_handle get_next() const { return next; }

    inline const Interval* getInterval() { return I; }

    bool operator==(const IntervalListElt& e) const {
        return I == e.I && next == e.next;
    }

    void print(std::ostream& os) const { I->print(os); }

};

template<typename Value, class Comparator>
IntervalSkipList<Value, Comparator>::IntervalListElt::IntervalListElt()
        : next(nullptr) { }

template<typename Value, class Comparator>
IntervalSkipList<Value, Comparator>::
        IntervalListElt::IntervalListElt(const Interval* interval)
        : I(interval), next(nullptr) { }


// class IntervalList
template<typename Value, class Comparator>
class IntervalSkipList<Value, Comparator>::IntervalList {
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

    bool remove(const Interval* I, Interval* res);

    void remove(const Interval* I);

    void removeAll(IntervalList* l);

    // tips: Allocator may fit this better
    inline ILE_handle create_list_element(const Interval* I) {
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
    copy(OutputIterator out) const
    {
        ILE_handle e = first_;
        while (e != nullptr) {
            out = const_cast<Interval*>(e->I);;
            ++out;
            e = e->next;
        }
        return out;
    }

    bool contains(const Interval* I) const;

    void clear();  // remove elements of self to make self an empty list.

    void print(std::ostream& os) const;

};

template<typename Value, class Comparator>
IntervalSkipList<Value, Comparator>::IntervalList::IntervalList()
        : first_(nullptr), count(0) { }

template<typename Value, class Comparator>
IntervalSkipList<Value, Comparator>::IntervalList::~IntervalList() {
    this->clear();
}

template<typename Value, class Comparator>
inline void IntervalSkipList<Value, Comparator>::IntervalList::insert(const Interval* I) {
    ILE_handle temp = create_list_element(I);
    temp->set_next(first_);
    first_ = temp;
}


template<typename Value, class Comparator>
bool IntervalSkipList<Value, Comparator>::IntervalList::remove(const Interval* I,
                                                               Interval* res) {
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
        res = x->getInterval();
        erase_list_element(x);
    } else {
        last->set_next(x->get_next());
        res = x->getInterval();
        erase_list_element(x);
    }
    return true;
}

template<typename Value, class Comparator>
void IntervalSkipList<Value, Comparator>::IntervalList::remove(const Interval* I) {
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

template<typename Value, class Comparator>
void IntervalSkipList<Value, Comparator>::IntervalList::removeAll(IntervalList *l) {
    ILE_handle x;
    for (x = l->get_first(); x != nullptr; x = x->get_next()) {
        this->remove(x->getInterval());
    }
}


template<typename Value, class Comparator>
void IntervalSkipList<Value, Comparator>::IntervalList::copy(IntervalList* from) {
    ILE_handle e = from->get_first();
    while (e != nullptr) {
        insert(e->getInterval());
        e = e->get_next();
    }
}


template<typename Value, class Comparator>
bool IntervalSkipList<Value, Comparator>::IntervalList::contains(const Interval* I) const {
    ILE_handle x = first_;
    while (x != 0 && I != x->getInterval()) {
        x = x->get_next();
    }
    return x != nullptr;
}

template<typename Value, class Comparator>
void IntervalSkipList<Value, Comparator>::IntervalList::clear() {
    ILE_handle x = first_;
    ILE_handle y;
    while (x != nullptr) { // was 0
        y = x;
        x = x->get_next();
        erase_list_element(y);
    }
    first_ = nullptr;
}

template<typename Value, class Comparator>
void IntervalSkipList<Value, Comparator>::IntervalList::print(std::ostream &os) const {
    ILE_handle e = first_;
    while (e != nullptr) {
        e->print(os);
        e = e->get_next();
    }
}



}

#endif //SOFTDB_NVM_INDEX_H
