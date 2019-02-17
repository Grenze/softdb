//
// Created by lingo on 19-2-17.
//

#ifndef SOFTDB_NVM_INDEX_H
#define SOFTDB_NVM_INDEX_H

#include "dbformat.h"
#include "random.h"
#include "nvm_memtable.h"
//#include <list>

namespace softdb {


//const int MAX_FORWARD = 48; 	// Maximum number of forward pointers

template<typename Value, class Comparator>
class IntervalSkipList {
private:
    class IntervalSLnode;

    class Interval;

    class IntervalListElt;

    class IntervalList;

    // Maximum number of forward pointers
    enum { MAX_FORWARD = 32 };

    int maxLevel;

    Random random;

    IntervalSLnode* const head_;

    Comparator comparator_;

    //uint64_t interval_count_;

    // Is there any need?
    //Interval container;

    typedef IntervalListElt *ILE_handle;

    int randomLevel();  // choose a new node level at random

    // Three-way comparison.  Returns value:
    //   <  0 iff "a" <  "b",
    //   == 0 iff "a" == "b",
    //   >  0 iff "a" >  "b"
    int ValueCompare(const Value &a, const Value &b) const {
        return comparator_(a, b);
    }

    bool contains(const Interval& I, Value& V) const;

    bool contains_interval(const Interval& I, const Value& i, const Value& s) const;

    bool equal_interval(const Interval& l, const Interval& r) const;

    bool equal_intervalListElt(const IntervalListElt& l, const IntervalListElt& r) const;

    bool contains(const IntervalList& l, const Interval& I) const;

    bool remove(const IntervalList& l, const Interval& I, Interval& res);

    void remove(const IntervalList& l, const Interval& I);

    void removeAll(const IntervalList& r, const IntervalList& l);




    // Search for search key, and return a pointer to the
    // intervalSLnode x found, as well as setting the update vector
    // showing pointers into x.
    IntervalSLnode *search(const Value& searchKey,
                           IntervalSLnode** update) const;

    // insert a new single value
    // into list, returning a pointer to its location.
    IntervalSLnode *insert(const Value& searchKey);



    // remove markers for Interval I
    // modified by Grenze.
    void removeMarkers(const Interval& I);

    // adjust markers after insertion of x with update vector "update"
    void adjustMarkersOnInsert(IntervalSLnode* x,
                               IntervalSLnode** update);

    // Remove markers for interval m from the edges and nodes on the
    // level i path from l to r.
    void removeMarkFromLevel(const Interval& m, int i,
                             IntervalSLnode* l,
                             IntervalSLnode* r);

    // place markers for Interval I.  I must have been inserted in the list.
    // left is the left endpoint of I and right is the right endpoint if I.
    // *** needs to be fixed:
    // modified by Grenze.
    void placeMarkers(IntervalSLnode* left,
                      IntervalSLnode* right,
                      const Interval& I);

    // remove markers for Interval I starting at left, the left endpoint
    // of I, and and stopping at the right endpoint of I.
    // modified by Grenze.
    Interval *removeMarkers(IntervalSLnode* left,
                            const Interval& I);

    // adjust markers to prepare for deletion of x, which has update vector
    // "update"
    void adjustMarkersOnDelete(IntervalSLnode* x,
                               IntervalSLnode** update);

    // remove node x, which has updated vector update.
    void remove(IntervalSLnode* x,
                IntervalSLnode** update);

public:
    explicit IntervalSkipList();

    ~IntervalSkipList();

    template<class InputIterator>
    IntervalSkipList(InputIterator b, InputIterator e) : random(0xdeadbeef) {
        maxLevel = 0;
        head_ = new IntervalSLnode(MAX_FORWARD);
        for (int i = 0; i < MAX_FORWARD; i++) {
            head_->forward[i] = 0;
        }
        for (; b != e; ++b) {
            insert(*b);
        }
    }

    void clear();

    int size() const;   //number of intervals


    // return node containing Value if found, otherwise nullptr
    IntervalSLnode *search(const Value &searchKey);

    // It is assumed that when a marker is placed on an edge,
    // it will be placed in the eqMarkers sets of a node on either
    // end of the edge if the interval for the marker covers the node.
    // Similarly, when a marker is removed from an edge markers will
    // be removed from eqMarkers sets on nodes adjacent to the edge.
    template<class OutputIterator>
    OutputIterator
    find_intervals(const Value &searchKey, OutputIterator out) {
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
        return out;
    }

    bool
    is_contained(const Value &searchKey) const {
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

    // insert an interval into list
    // modified by Grenze.
    void insert(const Interval& I);

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

    bool remove(const Interval& I);  // delete an interval from list

    //print every nodes' information
    void print(std::ostream& os) const;
    //print every nodes' values in order
    void printOrdered(std::ostream& os) const;
};

template<typename Value, class Comparator>
IntervalSkipList<Value, Comparator>::IntervalSkipList() : random(0xdeadbeef) {

    maxLevel = 0;
    head_ = new IntervalSLnode(MAX_FORWARD);
    for (int i = 0; i < MAX_FORWARD; i++) {
        head_->forward[i] = 0;
    }
}

template<typename Value, class Comparator>
IntervalSkipList<Value, Comparator>::~IntervalSkipList() {
    while(head_ != 0){
        IntervalSLnode* next = head_->get_next();
        delete head_;
        head_ = next;
    }
}

template<typename Value, class Comparator>
void IntervalSkipList<Value, Comparator>::clear() {
    while(head_ != 0){
        IntervalSLnode* next = head_->get_next();
        delete head_;
        head_ = next;
    }
    head_ = new IntervalSLnode(MAX_FORWARD);
    for (int i = 0; i < MAX_FORWARD; i++) {
        head_->forward[i] = 0;
    }
    maxLevel = 0;
}

template<typename Value, class Comparator>
typename IntervalSkipList<Value, Comparator>::
IntervalSLnode* IntervalSkipList<Value, Comparator>::search(const Value& searchKey) {
    IntervalSLnode* x = head_;
    for(int i = maxLevel; i >= 0; i--) {
        while (x->forward[i] != 0 && ValueCompare(x->forward[i]->key, searchKey) < 0) {
            x = x->forward[i];
        }
    }
    x = x->forward[0];
    if(x != nullptr && ValueCompare(x->key, searchKey) == 0)
        return(x);
    else
        return(nullptr);
}

template<typename Value, class Comparator>
void IntervalSkipList<Value, Comparator>::print(std::ostream& os) const {
    IntervalSLnode* n = head_->get_next();
    os << "values in list:  ";
    while( n != 0 ) {
        os << n->key << " ";
        n = n->get_next();
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
    return(x);
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
    if(x==0 || ValueCompare(x->key, searchKey) != 0 ) {
        // put a new node in the list for this searchKey
        int newLevel = randomLevel();
        if (newLevel > maxLevel){
            for(i = maxLevel + 1; i <= newLevel; i++){
                update[i] = head_;
                head_->markers[i]->clear();
            }
            maxLevel = newLevel;
        }
        x = new IntervalSLnode(searchKey, newLevel);

        // add x to the list
        for(i=0; i <= newLevel; i++) {
            x->forward[i] = update[i]->forward[i];
            update[i]->forward[i] = x;
        }

        // adjust markers to maintain marker invariant
        this->adjustMarkersOnInsert(x,update);
    }
    // else, the searchKey is in the list already, and x points to it.
    return(x);
}


template<typename Value, class Comparator>
void IntervalSkipList<Value, Comparator>::insert(const Interval& I) {
    // insert end points of interval
    IntervalSLnode* left = this->insert(I->inf());
    IntervalSLnode* right = this->insert(I->sup());
    left->ownerCount++;
    right->ownerCount++;

    // place markers on interval
    this->placeMarkers(left,right,I);
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

    for(i=0; (i <= x->level() - 2) && x->forward[i+1] != 0; i++) {
        IntervalList* markList = update[i]->markers[i];
        for(m = markList->get_first(); m != nullptr ; m = markList->get_next(m)) {
            if(contains_interval(m->getInterval(), x->key, x->forward[i+1]->key)) {
                // promote m

                // remove m from level i path from x->forward[i] to x->forward[i+1]
                removeMarkFromLevel(*m->getInterval(),
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

        for(m = promoted.get_first(); m != nullptr; m = promoted.get_next(m)) {
            if(!contains_interval(m->getInterval(), x->key, x->forward[i+1]->key)) {
                // Then m does not need to be promoted higher.
                // Place m on the level i edge out of x and remove m from promoted.
                x->markers[i]->insert(m->getInterval());
                // mark x->forward[i] if needed
                if(m->getInterval()->contains(x->forward[i]->key))
                    x->forward[i]->eqMarkers->insert(m->getInterval());
                removePromoted.insert(m->getInterval());
            } else {
                // continue to promote m
                // Remove m from the level i path from x->forward[i]
                // to x->forward[i+1].
                removeMarkFromLevel(*(m->getInterval()),
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
    for(m=promoted.get_first(); m!=nullptr; m=promoted.get_next(m))
        if(m->getInterval()->contains(x->forward[i]->key))
            x->forward[i]->eqMarkers->insert(m->getInterval());

    // Phase 2:  place markers on edges leading into x as needed.

    // Markers on edges leading into x may need to be promoted as high as
    // the top edge coming into x, but never higher.

    promoted.clear();

    for (i=0; (i <= x->level() - 2) && !update[i+1]->isHeader(); i++) {
        tempMarkList.copy(update[i]->markers[i]);
        for(m = tempMarkList.get_first();
            m != nullptr;
            m = tempMarkList.get_next(m)){
            if(m->getInterval()->contains_interval(update[i+1]->key,x->key)) {
                // m needs to be promoted
                // add m to newPromoted
                newPromoted.insert(m->getInterval());

                // Remove m from the path of level i edges between updated[i+1]
                // and x (it will be on all those edges or else the invariant
                // would have previously been violated.
                removeMarkFromLevel(*(m->getInterval()),i,update[i+1],x);
            }
        }
        tempMarkList.clear();  // reclaim storage

        for(m = promoted.get_first(); m != nullptr; m = promoted.get_next(m)) {
            if (!update[i]->isHeader() &&
                m->getInterval()->contains_interval(update[i]->key,x->key) &&
                !update[i+1]->isHeader() &&
                ! m->getInterval()->contains_interval(update[i+1]->key,x->key) ) {
                // Place m on the level i edge between update[i] and x, and
                // remove m from promoted.
                update[i]->markers[i]->insert(m->getInterval());
                // mark update[i] if needed
                if(m->getInterval()->contains(update[i]->key))
                    update[i]->eqMarkers->insert(m->getInterval());
                removePromoted.insert(m->getInterval());
            } else {
                // Strip m from the level i path from update[i+1] to x.
                removeMarkFromLevel(*(m->getInterval()),i,update[i+1],x);
            }

        }
        // remove non-promoted marks from promoted
        removeAll(promoted, &removePromoted);
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
    for(m = promoted.get_first(); m != nullptr; m = promoted.get_next(m))
        if(contains(m->getInterval(), contains(update[i]->key)))
            update[i]->eqMarkers->insert(m->getInterval());

    // Place markers on x for all intervals the cross x.
    // (Since x is a new node, every marker comming into x must also leave x).
    for(i=0; i<x->level(); i++)
        x->eqMarkers->copy(x->markers[i]);

    promoted.clear(); // reclaim storage

} // end adjustMarkersOnInsert





// class IntervalSLnode
template<typename Value, class Comparator>
class IntervalSkipList<Value, Comparator>::IntervalSLnode {
private:
    Value const key;
    int topLevel;   // top level of forward pointers in this node
    bool is_header;
    // Levels are numbered 0..topLevel.
    IntervalSLnode **forward;   // array of forward pointers
    IntervalList **markers;    // array of interval markers, one for each pointer
    IntervalList *eqMarkers;  // See comment of find_intervals
    IntervalList *ownMarkers;
    int ownerCount; // number of interval end points with value equal to key

public:
    explicit IntervalSLnode(const Value &searchKey, int levels);
    explicit IntervalSLnode(int levels);

    ~IntervalSLnode();

    IntervalSLnode *get_next() const;

    const Value& getValue() const {
        return key;
    }

    // number of levels of this node
    int level() const {
        return (topLevel + 1);
    }

    bool isHeader() const {
        return is_header;
    }

    void print(std::ostream &os) const;

};

template<typename Value, class Comparator>
IntervalSkipList<Value, Comparator>::
IntervalSLnode::IntervalSLnode(const Value &searchKey, int levels)
        : is_header(false), key(searchKey), topLevel(levels), ownerCount(0) {
    // levels is actually one less than the real number of levels
    forward = new IntervalSLnode*[levels+1];
    markers = new IntervalList*[levels+1];
    eqMarkers = new IntervalList();
    ownMarkers = new IntervalList();
    for (int i = 0; i <= levels; i++) {
        forward[i] = 0;
        // initialize an empty interval list
        markers[i] = new IntervalList();
    }
}

template<typename Value, class Comparator>
IntervalSkipList<Value, Comparator>::
IntervalSLnode::IntervalSLnode(int levels)
        : is_header(true), key(nullptr), topLevel(levels), ownerCount(0) {
    forward = new IntervalSLnode*[levels+1];
    markers = new IntervalList*[levels+1];
    eqMarkers = new IntervalList();
    ownMarkers = new IntervalList();
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
    delete ownMarkers;
}

template<typename Value, class Comparator>
typename IntervalSkipList<Value, Comparator>::
IntervalSLnode* IntervalSkipList<Value, Comparator>::IntervalSLnode::get_next() const {
    return forward[0];
}

template<typename Value, class Comparator>
void IntervalSkipList<Value, Comparator>::
IntervalSLnode::print(std::ostream &os) const {
    int i;
    os << "IntervalSLnode key:  ";
    if (key == 0) {
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
    for(i=0; i<=topLevel; i++)
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
            os << " ("<<markers[i]->count<<")";
        } else {
            os << "nullptr";
        }
        os << "\n";
    }
    os << "EQ markers:  ";
    eqMarkers->print(os);
    os << " ("<<eqMarkers->count<<")";
    os << std::endl << std::endl;
}

// class Interval
template<typename Value, class Comparator>
class IntervalSkipList<Value, Comparator>::Interval {
private:
    Value inf_;
    Value sup_;
    const uint64_t stamp_;  // differentiate intervals
    const NvmMemTable*  table_;
    IntervalSLnode* start_;
    IntervalSLnode* end_;
public:
    explicit Interval(const Value& inf, const Value& sup, const NvmMemTable* table);

    const Value& inf() const { return inf_; }

    const Value& sup() const { return sup_; }

    const uint64_t stamp() const { return stamp_; }

    void print(std::ostream &os) const;

};

template<typename Value, class Comparator>
inline IntervalSkipList<Value, Comparator>::
Interval::Interval(const Value& inf,
                   const Value& sup,
                   const NvmMemTable* table)
                  : inf_(inf), sup_(sup), table_(table) {
    //assert( !(inf_ > sup_) );
    assert( table_ != nullptr);
}

template<typename Value, class Comparator>
void IntervalSkipList<Value, Comparator>::
Interval::print(std::ostream& os) const {
    os << "[" << inf_ << ", " << sup_ << "]";
}

template<typename Value, class Comparator>
bool IntervalSkipList<Value, Comparator>::contains(const Interval& I,
                                                   Value& V) const {
    return ValueCompare(I.inf(), V) <= 0 && ValueCompare(V, I.sup()) <= 0;

}

template<typename Value, class Comparator>
bool IntervalSkipList<Value, Comparator>::contains_interval(const Interval &I,
                                                            const Value &i,
                                                            const Value &s) const {
    return ValueCompare(I.inf(), i) <= 0 && ValueCompare(s, I.sup()) <= 0;
}

template<typename Value, class Comparator>
bool IntervalSkipList<Value, Comparator>::equal_interval(const Interval &l,
                                                         const Interval &r) const {
    return ValueCompare(l.inf(), r.inf()) == 0 &&
            ValueCompare(l.sup(), r.sup()) == 0 &&
            l.stamp() == r.stamp();
}


// class IntervalListElt
template<typename Value, class Comparator>
class IntervalSkipList<Value, Comparator>::IntervalListElt {
private:
    typedef IntervalListElt *ILE_handle;
    Interval* I;
    ILE_handle next;
public:
    explicit IntervalListElt();
    explicit IntervalListElt(const Interval& I);
    ~IntervalListElt();

    void set_next(ILE_handle nextElt) { next = nextElt; }

    IntervalListElt get_next() { return next; }

    Interval* getInterval() { return I; }

    void print(std::ostream& os) const;


};

template<typename Value, class Comparator>
inline IntervalSkipList<Value, Comparator>::IntervalListElt::IntervalListElt()
        : next(nullptr) { }

template<typename Value, class Comparator>
inline IntervalSkipList<Value, Comparator>::
        IntervalListElt::IntervalListElt(const Interval& interval)
        : I(interval), next(nullptr) { }

template<typename Value, class Comparator>
IntervalSkipList<Value, Comparator>::IntervalListElt::~IntervalListElt() { }

template<typename Value, class Comparator>
void IntervalSkipList<Value, Comparator>::IntervalListElt::print(std::ostream& os) const {
    I->print(os);
}

template<typename Value, class Comparator>
bool IntervalSkipList<Value, Comparator>::equal_intervalListElt(
                                        const IntervalListElt &l,
                                        const IntervalListElt &r) const {
    return equal_interval(l.getInterval(), r.getInterval()) && l.get_next() == r.get_next();
}


// class IntervalList
template<typename Value, class Comparator>
class IntervalSkipList<Value, Comparator>::IntervalList {
private:
    typedef IntervalListElt *ILE_handle;
    ILE_handle first_;
public:
    int count;

    explicit IntervalList();
    ~IntervalList();

    void insert(const Interval& I);

    ILE_handle get_first();

    ILE_handle create_list_element(const Interval& I) {
        IntervalListElt* elt_ptr = new IntervalListElt(I);
        count++;
        return elt_ptr;
    }

    void erase_list_element(ILE_handle I) {
        delete I;
        count--;
    }

    void set_first(ILE_handle I) { first_ = I; }

    void copy(IntervalList* from) const; // add contents of "from" to self

    template <class OutputIterator>
    OutputIterator
    copy(OutputIterator out) const
    {
        ILE_handle e = first_;
        while(e != nullptr) {
            out = *(e->I);
            ++out;
            e = e->next;
        }
        return out;
    }

    void clear();  // delete elements of self to make self an empty list.

    void print(std::ostream& os) const;

};

template<typename Value, class Comparator>
inline IntervalSkipList<Value, Comparator>::IntervalList::IntervalList()
        : first_(nullptr), count(0) { }

template<typename Value, class Comparator>
inline IntervalSkipList<Value, Comparator>::IntervalList::~IntervalList() {
    this->clear();
}

template<typename Value, class Comparator>
void IntervalSkipList<Value, Comparator>::IntervalList::insert(const Interval& I) {
    ILE_handle temp = create_list_element(I);
    temp->set_next(first_);
    first_ = temp;
}


template<typename Value, class Comparator>
bool IntervalSkipList<Value, Comparator>::remove(const IntervalList& l,
                                                 const Interval& I,
                                                 Interval& res) {
    ILE_handle x, last;
    x = l.get_first();
    last = nullptr;
    while (x != nullptr && !equal_interval(I, x->I)) {
        last = x;
         x = x->get_next();
    }
    // after the tail | at the head | in the between
    if(x == nullptr) {
        return false;
    } else if (last == nullptr) {
        l.set_first(x->get_next());
        res = x->getInterval();
        l->erase_list_element(x);
    } else {
        last->next = x->get_next();
        res = x->getInterval();
        l->erase_list_element(x);
    }
    return true;
}

template<typename Value, class Comparator>
void IntervalSkipList<Value, Comparator>::remove(const IntervalList& l,
                                                 const Interval& I) {
    ILE_handle x, last;
    x = l.get_first();
    last = nullptr;
    while (x != nullptr && !equal_interval(I, x->I)) {
        last = x;
        x = x->get_next();
    }
    // after the tail | at the head | in the between
    if(x == nullptr) {
        return;
    } else if (last == nullptr) {
        l.set_first(x->get_next());
        erase_list_element(x);
    } else {
        last->next = x->get_next();
        erase_list_element(x);
    }
}

template<typename Value, class Comparator>
void IntervalSkipList<Value, Comparator>::removeAll(const IntervalList& r, const IntervalList& l) {
    ILE_handle x;
    for (x = l.get_first(); x != nullptr; x = x->get_next()) {
        remove(r, x->getInterval());
    }
}


template<typename Value, class Comparator>
inline typename IntervalSkipList<Value, Comparator>::ILE_handle
IntervalSkipList<Value, Comparator>::IntervalList::get_first() {
    return first_;
}


template<typename Value, class Comparator>
void IntervalSkipList<Value, Comparator>::IntervalList::copy(IntervalList* from) const {
    ILE_handle e = from->get_first();
    while(e != nullptr) {
        insert(e->getInterval());
        e = e->get_next();
    }
}


template<typename Value, class Comparator>
bool IntervalSkipList<Value, Comparator>::contains(const IntervalList& l,
                                                   const Interval& I) const {
    ILE_handle x = l.get_first();
    while(x != 0 && !equal_interval(I, x->I)) {
        x = x->get_next();
    }
    return x != nullptr;
}

template<typename Value, class Comparator>
void IntervalSkipList<Value, Comparator>::IntervalList::print(std::ostream &os) const {
    ILE_handle e = first_;
    while(e != nullptr) {
        e->print(os);
        e = e->get_next();
    }
}



}

#endif //SOFTDB_NVM_INDEX_H
