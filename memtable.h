//
// Created by lingo on 19-1-16.
//

#ifndef SOFTDB_MEMTABLE_H
#define SOFTDB_MEMTABLE_H


#include <string>
#include <iostream>
#include "db.h"
#include "dbformat.h"
#include "skiplist.h"
#include "arena.h"
#include "iterator.h"

namespace softdb {

    class InternalKeyComparator;
    class MemTableIterator;

    class MemTable {
    public:
        // MemTables are reference counted.  The initial reference count
        // is zero and the caller must call Ref() at least once.
        explicit MemTable(const InternalKeyComparator& comparator);

        // Increase reference count.
        void Ref() { ++refs_; }

        // Drafted by Grenze. Increase entry count.
        void Count(int insert) { num_ += insert; }

        // Drop reference count.  Delete if no more references exist.
        void Unref() {
            --refs_;
            assert(refs_ >= 0);
            if (refs_ <= 0) {
                delete this;
            }
        }

        // Returns an estimate of the number of bytes of data in use by this
        // data structure. It is safe to call when MemTable is being modified.
        size_t ApproximateMemoryUsage();

        // Return an iterator that yields the contents of the memtable.
        //
        // The caller must ensure that the underlying MemTable remains live
        // while the returned iterator is live.  The keys returned by this
        // iterator are internal keys encoded by AppendInternalKey in the
        // db/format.{h,cc} module.
        Iterator* NewIterator();

        // Add an entry into memtable that maps key to value at the
        // specified sequence number and with the specified type.
        // Typically value will be empty if type==kTypeDeletion.
        void Add(SequenceNumber seq, ValueType type,
                 const Slice& key,
                 const Slice& value);

        // If memtable contains a value for key, store it in *value and return true.
        // If memtable contains a deletion for key, store a NotFound() error
        // in *status and return true.
        // Else, return false.
        bool Get(const LookupKey& key, std::string* value, Status* s);

        void Info() const {std::cout<< "num_:"<<num_<<std::endl;}

    private:
        ~MemTable();  // Private since only Unref() should be used to delete it

        struct KeyComparator {
            const InternalKeyComparator comparator;
            //initialize the InternalKeyComparator
            explicit KeyComparator(const InternalKeyComparator& c) : comparator(c) { }
            //this operator() finally call the InternalKeyComparator comparator's operator() function
            int operator()(const char* a, const char* b) const;
        };
        friend class MemTableIterator;
        friend class MemTableBackwardIterator;

        //skiplist's entried are in form of char*
        typedef SkipList<const char*, KeyComparator> Table;

        KeyComparator comparator_;
        int refs_;

        // Drafted by Grenze
        int num_; // count of keys
        Arena arena_;
        Table table_;

        // No copying allowed
        MemTable(const MemTable&);
        void operator=(const MemTable&);
    };

}  // namespace softdb

#endif //SOFTDB_MEMTABLE_H
