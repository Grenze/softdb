//
// Created by lingo on 19-2-13.
//

#ifndef SOFTDB_NVM_MEMTABLE_H
#define SOFTDB_NVM_MEMTABLE_H

#include "dbformat.h"
#include "nvm_skiplist.h"
#include "softdb/iterator.h"
#include "util/hashtable.h"
#include "util/cuckoofilter.h"

namespace softdb {

class InternalKeyComparator;
class NvmMemTableIterator;

class NvmMemTable {
public:
    // NvmMemTables are reference counted. The initial reference count
    // is zero and the caller must call Ref() at least once.
    // Whether use cuckoo hash to assist, it's an option.
    explicit NvmMemTable(const InternalKeyComparator& comparator, int num, bool assist);

    // Return an iterator that yields the contents of the nvm_imm_.
    //
    // The caller must ensure that the underlying NvmMemTable remains live
    // while the returned iterator is live. The keys returned by this
    // iterator are internal keys encoded by AppendInternalKey in the
    // db/format.{h,cc} module.
    Iterator* NewIterator();

    // iter is constructed from imm_ or some nvm_imm_s
    void Transport(Iterator* iter, bool compact);

    inline int GetCount() const { return table_.GetCount(); }

    // If memtable contains a value for key, store it in *value and return true.
    // If memtable contains a deletion for key, store a NotFound() error
    // in *status and return true.
    // Else, return false.
    bool Get(const LookupKey& key, std::string* value, Status* s, const char*& HotKey);

    ~NvmMemTable();

private:

    struct KeyComparator {
        const InternalKeyComparator comparator;
        // initialize the InternalKeyComparator
        explicit KeyComparator(const InternalKeyComparator& c) : comparator(c) { }
        // to use InternalKeyComparator, we need to extract internal key from entry.
        int operator()(const char*a, const char* b) const;
    };

    friend class NvmMemTableIterator;

    typedef NvmSkipList<const char*, KeyComparator> Table;

    // prepared for skipList iterator.
    bool IteratorJump(Table::Iterator& iter, const Slice& ukey, const char* memkey) const;

    // TODO: the faster hash insert proceeds, the faster to form nvm_imm_.
    // Maybe a better hash function matters.
    typedef CuckooHash::HashTable<32, 64> Hash;

    typedef CuckooHash::CuckooFilter<32> Filter;

    KeyComparator comparator_;

    const int capacity_;
    Table table_;
    Hash* hash_;

    Filter* filter_;

    // No copying allowed
    NvmMemTable(const NvmMemTable&);
    void operator=(const NvmMemTable&);

};

}   // namespace softdb



#endif //SOFTDB_NVM_MEMTABLE_H
