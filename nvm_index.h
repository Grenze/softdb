//
// Created by lingo on 19-2-17.
//

#ifndef SOFTDB_NVM_INDEX_H
#define SOFTDB_NVM_INDEX_H

#include "dbformat.h"
#include "random.h"
//#include <list>

namespace softdb {


//const int MAX_FORWARD = 48; 	// Maximum number of forward pointers

// tips: Merge the interval class.
template<typename Value, class Comparator>
class IntervalSkipList {
private:
    class IntervalSLnode;
    class Interval;
    class IntervalList;
    class IntervalListElt;

    enum { MAX_FORWARD = 32 }; // Maximum number of forward pointers

    int maxLevel;

    Random random;

    IntervalSLnode* header;

    Comparator comparator_;

    //uint64_t interval_count_;

    // Is there any need?
    //std::list<Interval> container;

    typedef IntervalListElt* ILE_handle;

    int randomLevel();  // choose a new node level at random

    int Compare(const Value& v1, Value& v2) const {
        return compare_(v1, v2);
    }

    // Search for search key, and return a pointer to the
    // intervalSLnode x found, as well as setting the update vector
    // showing pointers into x.
    IntervalSLnode* search(const Value& searchKey,
                           IntervalSLnode** update);

    // insert a new single value
    // into list, returning a pointer to its location.
    IntervalSLnode* insert(const Value& searchKey);

    // insert an interval into list
    // modified by Grenze.
    void insert(const Interval& I);


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
    Interval* removeMarkers(IntervalSLnode* left,
                                  const Interval& I);

    // adjust markers to prepare for deletion of x, which has update vector
    // "update"
    void adjustMarkersOnDelete(IntervalSLnode* x,
                               IntervalSLnode** update);

    // remove node x, which has updated vector update.
    void remove(IntervalSLnode* x,
                IntervalSLnode** update);

public:
    IntervalSkipList();
    ~IntervalSkipList();

    template <class InputIterator>
    IntervalSkipList(InputIterator b, InputIterator e) : random(0xdeadbeef)
    {
        maxLevel = 0;
        header = new IntervalSLnode(MAX_FORWARD);
        for (int i = 0; i< MAX_FORWARD; i++) {
            header->forward[i] = 0;
        }
        for(; b!= e; ++b){
            insert(*b);
        }
    }

    void clear();

    int size() const;   //number of intervals


    // return node containing Value if found, otherwise null
    IntervalSLnode* search(const Value& searchKey);

    // It is assumed that when a marker is placed on an edge,
    // it will be placed in the eqMarkers sets of a node on either
    // end of the edge if the interval for the marker covers the node.
    // Similarly, when a marker is removed from an edge markers will
    // be removed from eqMarkers sets on nodes adjacent to the edge.
    template <class OutputIterator>
    OutputIterator
    find_intervals(const Value& searchKey, OutputIterator out )
    {
        IntervalSLnode* x = header;
        for(int i=maxLevel;
            i >= 0 && (x->isHeader() || (x->key != searchKey)); i--) {
            while (x->forward[i] != 0 && (searchKey >= x->forward[i]->key)) {
                x = x->forward[i];
            }
            // Pick up markers on edge as you drop down a level, unless you are at
            // the searchKey node already, in which case you pick up the
            // eqMarkers just prior to exiting loop.
            if(!x->isHeader() && (x->key != searchKey)) {
                out = x->markers[i]->copy(out);
            } else if (!x->isHeader()) { // we're at searchKey
                out = x->eqMarkers->copy(out);
            }
        }
        return out;
    }

    bool
    is_contained(const Value& searchKey) const
    {
        IntervalSLnode* x = header;
        for(int i=maxLevel;
            i >= 0 && (x->isHeader() || (x->key != searchKey)); i--) {
            while (x->forward[i] != 0 && (searchKey >= x->forward[i]->key)) {
                x = x->forward[i];
            }
            // Pick up markers on edge as you drop down a level, unless you are at
            // the searchKey node already, in which case you pick up the
            // eqMarkers just prior to exiting loop.
            if(!x->isHeader() && (x->key != searchKey)) {
                return true;
            } else if (!x->isHeader()) { // we're at searchKey
                return true;
            }
        }
        return false;
    }





};



}

#endif //SOFTDB_NVM_INDEX_H
