//
// Created by lingo on 19-2-13.
//

#include "nvm_memtable.h"


namespace softdb {

static Slice GetLengthPrefixedSlice(const char* data) {
    uint32_t len;
    const char* p = data;
    p = GetVarint32Ptr(p, p + 5, &len);  // +5: we assume "p" is not corrupted
    return Slice(p, len);
}

// If num = 0, it's caller's duty to delete it.
NvmMemTable::NvmMemTable(const InternalKeyComparator& cmp, int num)
           : comparator_(cmp),
             refs_(0),
             num_(num),
             table_(comparator_, num_) {

}

NvmMemTable::~NvmMemTable() {
    assert(refs_ == 0);
}

//  GetLengthPrefixedSlice gets the Internal keys from char*
int NvmMemTable::KeyComparator::operator()(const char* aptr, const char* bptr)
const {
    // Internal keys are encoded as length-prefixed strings.
    Slice a = GetLengthPrefixedSlice(aptr);
    Slice b = GetLengthPrefixedSlice(bptr);
    return comparator.Compare(a, b);
}

// Encode a suitable internal key target for "target" and return it.
// Uses *scratch as scratch space, and the returned pointer will point
// into this scratch space.
static const char* EncodeKey(std::string* scratch, const Slice& target) {
    scratch->clear();
    PutVarint32(scratch, target.size());
    scratch->append(target.data(), target.size());
    return scratch->data();
}

class NvmMemTableIterator: public Iterator {
public:
    explicit NvmMemTableIterator(NvmMemTable::Table* table) : iter_(table) { }

    virtual bool Valid() const { return iter_.Valid(); }
    // use cuckoo hash to assist.shift to the user key with largest sequence,
    // then Next() until Key() == k.
    // Cost to call Next()s must be less than Seek's cost.
    // The less duplicate, the fast to use cuckoo hash.
    virtual void Seek(const Slice& k) { iter_.Seek(EncodeKey(&tmp_, k)); }
    virtual void SeekToFirst() { iter_.SeekToFirst(); }
    virtual void SeekToLast() { iter_.SeekToLast(); }
    virtual void Next() { iter_.Next(); }
    virtual void Prev() { iter_.Prev(); }
    virtual Slice key() const { return GetLengthPrefixedSlice(iter_.key()); }
    virtual Slice value() const {
        Slice key_slice = GetLengthPrefixedSlice(iter_.key());
        return GetLengthPrefixedSlice(key_slice.data() + key_slice.size());
    }

    virtual Slice Raw() const { return iter_.key(); }


    virtual Status status() const { return Status::OK(); }

private:
    NvmMemTable::Table::Iterator iter_;
    std::string tmp_;          // For passing to EncodeKey;

    // No copying allowed
    NvmMemTableIterator(const NvmMemTableIterator&);
    void operator=(const NvmMemTableIterator&);
};

Iterator* NvmMemTable::NewIterator() {
    return new NvmMemTableIterator(&table_);
}

// REQUIRES: iter is valid.
void NvmMemTable::Transport(Iterator* iter) {
    assert(iter->Valid());
    Table::Worker ins = Table::Worker(&table_);
    Slice raw;
    char* buf;
    do {
        // Raw data from imm_ or nvm_imm_
        raw = iter->Raw();
        // After make_persistent, only delete the obsolete data(char*).
        buf = new char[raw.size()];
        memcpy(buf, raw.data(), raw.size());
        iter->Next();
    } while (iter->Valid() && ins.Insert(buf));
    ins.Finish();
}




}   // namespace softdb