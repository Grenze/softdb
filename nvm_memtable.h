//
// Created by lingo on 19-2-13.
//

#ifndef SOFTDB_NVM_MEMTABLE_H
#define SOFTDB_NVM_MEMTABLE_H


#include "dbformat.h"
#include "nvm_skiplist.h"
#include "iterator.h"

namespace softdb {

class InternalKeyComparator;
class NvmMemTableIterator;

class NvmMemTable {
public:
    // NvmMemTables are reference counted. The initial reference count
    // is zero and the caller must call Ref() at least once.
    explicit NvmMemTable(const InternalKeyComparator& comparator, int num);

    // Increase reference count.
    void Ref() { ++ refs_; }

    // Drop reference count. Delete if no more references exist.
    void Unref() {
        --refs_;
        assert(refs_ >= 0);
        if (refs_ == 0) {
            delete this;
        }
    }

    // Return an iterator that yields the contents of the nvmmemtable.
    //
    // The caller must ensure that the underlying NvmMemTable remains live
    // while the returned iterator is live. The keys returned by this
    // iterator are internal keys encoded by AppendInternalKey in the
    // db/format.{h,cc} module.
    Iterator* NewIterator();

    // In situations:
    // 1. Compact imm_, copy all the data from memtable to nvmmemtable.
    // 2. Merge two nvm_imm_, copy some data(count num)
    //    from two nvmmemtable to a new nvmmemtable.
    void Transport(Iterator* iter, int num);

    bool Empty() const { return table_.Empty(); }

    // If memtable contains a value for key, store it in *value and return true.
    // If memtable contains a deletion for key, store a NotFound() error
    // in *status and return true.
    // Else, return false.
    bool Get(const LookupKey& key, std::string* value, Status* s);



private:
    ~NvmMemTable(); //Private since only Unref() should be used to delete it.

    struct KeyComparator {
        const InternalKeyComparator comparator;
        // initialize the InternalKeyComparator
        explicit KeyComparator(const InternalKeyComparator& c) : comparator(c) { }
        // to use InternalKeyComparator, we need to extract internal key from entry.
        int operator()(const char*a, const char* b) const;
    };
    friend class NvmMemTableIterator;

    typedef NvmSkipList<const char*, KeyComparator> Table;

    KeyComparator comparator_;
    int refs_;

    int num_;
    Table table_;

    // No copying allowed
    NvmMemTable(const NvmMemTable&);
    void operator=(const NvmMemTable&);

};

}   // namespace softdb



#endif //SOFTDB_NVM_MEMTABLE_H
