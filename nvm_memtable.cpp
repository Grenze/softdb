//
// Created by lingo on 19-2-13.
//

#include <iostream>
#include "nvm_memtable.h"


namespace softdb {

static Slice GetLengthPrefixedSlice(const char* data) {
    uint32_t len;
    const char* p = data;
    p = GetVarint32Ptr(p, p + 5, &len);  // +5: we assume "p" is not corrupted
    return Slice(p, len);
}

// If num = 0, it's caller's duty to delete it.
NvmMemTable::NvmMemTable(const InternalKeyComparator& cmp, int num, bool assist)
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
// Once called, never again.
void NvmMemTable::Transport(Iterator* iter) {
    assert(iter->Valid());
    int watch = 0;
    Table::Worker ins = Table::Worker(&table_);
    Slice raw;
    char* buf;
    while (iter->Valid()) {
        watch++;
        // Raw data from imm_ or nvm_imm_
        raw = iter->Raw();
        // After make_persistent, only delete the obsolete data(char*).
        // So there is only space amplification.
        // Also better for wear-leveling.
        // Read amplification normally doesn't reach
        // the number of overlapped intervals.
        assert(raw.size() > 0);
        //std::cout << ExtractUserKey(iter->key()).ToString() <<std::endl;
        buf = new char[raw.size()];
        memcpy(buf, raw.data(), raw.size());
        if (!ins.Insert(buf)) {
            ins.Finish();
            return;
        }
        iter->Next();
    }
}

// use cuckoo hash to assist Get,
// so use NvmMemTableIterator instead of Table::Iterator.
bool NvmMemTable::Get(const softdb::LookupKey &key, std::string *value, softdb::Status *s) {
    Slice memkey = key.memtable_key();
    Table::Iterator iter(&table_);
    iter.Seek(memkey.data());
    if (iter.Valid()) {
        // entry format is:
        //    klength  varint32
        //    userkey  char[klength]
        //    tag      uint64
        //    vlength  varint32
        //    value    char[vlength]
        // Check that it belongs to same user key.  We do not check the
        // sequence number since the Seek() call above should have skipped
        // all entries with overly large sequence numbers.
        const char* entry = iter.key();
        uint32_t key_length;
        const char* key_ptr = GetVarint32Ptr(entry, entry+5, &key_length);
        if (comparator_.comparator.user_comparator()->Compare(
                Slice(key_ptr, key_length - 8),
                key.user_key()) == 0) {
            // Correct user key
            const uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);
            switch (static_cast<ValueType>(tag & 0xff)) {
                case kTypeValue: {
                    Slice v = GetLengthPrefixedSlice(key_ptr + key_length);
                    value->assign(v.data(), v.size());
                    return true;
                }
                case kTypeDeletion:
                    *s = Status::NotFound(Slice());
                    return true;
            }
        }
    }
    return false;
}



}   // namespace softdb