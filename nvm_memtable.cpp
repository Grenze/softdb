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

static Slice GetRaw(const char* data) {
    uint32_t len;
    const char* p = data;   // start of data.
    p = GetVarint32Ptr(p, p + 5, &len);
    p += len;
    p = GetVarint32Ptr(p, p + 5, &len);
    p += len;   // Now p reaches end of data.
    return Slice(data, p - data);
}

static Slice GetRawKey(const char* data) {
    uint32_t len;
    const char* p = data;   // start of data.
    p = GetVarint32Ptr(p, p + 5, &len);
    p += len;
    return Slice(data, p - data);
}

// If num = 0, it's caller's duty to delete it.
NvmMemTable::NvmMemTable(const InternalKeyComparator& cmp, int num, bool assist)
           : comparator_(cmp),
             refs_(0),
             num_(num),
             table_(comparator_, num_) {
    // tips: Is reasonable to use cuckoo hash if num_ is small? Assume currently: sure.
    hash_ = (assist) ? new Hash(num_) : nullptr;
}

// Attention: Only merge procedure can decide whether kept or gone.
// Use NvmMemTableIterator's Raw() to get slice.data, and delete it.
NvmMemTable::~NvmMemTable() {
    NvmMemTable::Table::Iterator iter_ = NvmMemTable::Table::Iterator(&table_);
    iter_.SeekToFirst();
    while (iter_.Valid()) {
        delete iter_.key();
        iter_.Next();
    }
    assert(refs_ == 0);
}

//  GetLengthPrefixedSlice gets the Internal keys from char*
//  To be used for prefixed internal key compare.
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
    explicit NvmMemTableIterator(NvmMemTable::Table* table, NvmMemTable* nvmimm) : iter_(table), nvmimm_(nvmimm) { }

    virtual bool Valid() const { return iter_.Valid(); }
    // use cuckoo hash to assist.shift to the user key with largest sequence,
    // then Next() until Key() >= k.
    // Cost to call Next()s must be less than Seek's cost.
    // The less duplicate, the faster to use cuckoo hash.
    // EncodeKey prefix k.size to k.(k is internal key)
    virtual void Seek(const Slice& k) {
        uint32_t pos = 0;
        if (nvmimm_->hash_ != nullptr) {
            nvmimm_->IteratorJump(iter_, ExtractUserKey(k), EncodeKey(&tmp_, k), pos);
            if (pos == 0 || pos == UINT32_MAX) {
                iter_.Seek(EncodeKey(&tmp_, k));
            }
        } else {
            iter_.Seek(EncodeKey(&tmp_, k));
        }
    }
    virtual void SeekToFirst() { iter_.SeekToFirst(); }
    virtual void SeekToLast() { iter_.SeekToLast(); }
    virtual void Next() { iter_.Next(); }
    virtual void Prev() { iter_.Prev(); }
    virtual Slice key() const { return GetLengthPrefixedSlice(iter_.key()); }
    virtual Slice value() const {
        Slice key_slice = GetLengthPrefixedSlice(iter_.key());
        return GetLengthPrefixedSlice(key_slice.data() + key_slice.size());
    }

    virtual Slice Raw() const { return GetRaw(iter_.key()); }

    virtual Slice RawKey() const { return GetRawKey(iter_.key()); }

    virtual Status status() const { return Status::OK(); }

private:
    NvmMemTable::Table::Iterator iter_;
    std::string tmp_;          // For passing to EncodeKey;
    NvmMemTable* nvmimm_;

    // No copying allowed
    NvmMemTableIterator(const NvmMemTableIterator&);
    void operator=(const NvmMemTableIterator&);
};

Iterator* NvmMemTable::NewIterator() {
    return new NvmMemTableIterator(&table_, this);
}

// REQUIRES: iter is valid.
// Once called, never again.
void NvmMemTable::Transport(Iterator* iter) {
    assert(iter->Valid());
    // pos from 1 to num_
    uint32_t pos = 0;
    int repeat = 0;
    // tips: Need to be adjusted according to experiment.
    int threshold = 11;//(num_>8192) ? num_>>13 : 1;
    Table::Worker ins = Table::Worker(&table_);
    //get the first user key
    Slice current_user_key = ExtractUserKey(iter->key());
    uint32_t current_pos = 1;
    Slice tmp;
    while (iter->Valid()) {
        if (hash_ != nullptr) {
            // REQUIRES: no duplicate internal key
            pos++;
            tmp = ExtractUserKey(iter->key());
            if (comparator_.comparator.user_comparator()->Compare(
                    tmp, current_user_key) != 0) {
                if (repeat <= threshold) {
                    assert(hash_->Add(current_user_key, current_pos));
                } else {
                    // Indicate do not use cuckoo hash
                    // as there is too much data duplicated on this key.
                    assert(hash_->Add(current_user_key, UINT32_MAX));
                }
                current_user_key = tmp;
                current_pos = pos;
                repeat = 1;
            } else {
                repeat++;
            }
        }

        // Raw data from imm_ or nvm_imm_
        Slice raw = iter->Raw();
        // After make_persistent, only delete the obsolete data(char*).
        // So there is only space amplification.
        // Also better for wear-leveling.
        // Read amplification normally doesn't reach
        // the number of overlapped intervals.
        //std::cout<<"imm_iter: "<<iter->value().ToString()<<std::endl;
        char* buf = new char[raw.size()];
        memcpy(buf, raw.data(), raw.size());
        if (!ins.Insert(buf)) { break; }
        iter->Next();
    }
    if (hash_ != nullptr) {
        if (repeat <= threshold) {
            assert(hash_->Add(current_user_key, current_pos));
        } else {
            // Indicate do not use cuckoo hash
            // as there is too much data duplicated on this key.
            assert(hash_->Add(current_user_key, UINT32_MAX));
        }
    }
    // iter not valid or no room to insert.
    ins.Finish();
}

// REQUIRES: Use cuckoo hash to assist search.
// pos == UINT32_MAX indicates key exists but too much duplicate, use skipList search instead.
// pos == 0 indicates key does not exist.
// pos != 0 && pos != UINT32_MAX indicates cuckoo hash search succeeded.
// Bug: If there are two different key with same hash value(tag + loc),
// the key inserted before will never be accessed.
// Assume: When we insert key into cuckoo hash, the situation mentioned above never happened,
// but is's still important to check whether user key is correct as the key to search is unpredictable.
bool NvmMemTable::IteratorJump(Table::Iterator &iter, Slice ukey, const char* memkey, uint32_t& pos) const {
    assert(hash_ != nullptr);
    if (hash_->Find(ukey, &pos)) {
        if (pos != UINT32_MAX) {
            // It's still uncertain whether user key with largest sequence we found is correct
            iter.Jump(pos);
            const char* entry = iter.key();
            uint32_t key_length;
            const char* key_ptr = GetVarint32Ptr(entry, entry+5, &key_length);
            if (comparator_.comparator.user_comparator()->Compare(
                    Slice(key_ptr, key_length - 8), ukey) == 0) {
                // Correct user key
                while(iter.Valid() && comparator_(iter.key(), memkey) < 0) {
                    // move few steps forward on the same key with different sequence
                    // typically faster than skipList.
                    iter.Next();
                }
                return true;
            }
            // it's a different key with the same hash value.
            pos = 0;
        }
    }
    return false;
}


bool NvmMemTable::Get(const LookupKey &key, std::string *value, Status *s) {
    Table::Iterator iter(&table_);
    Slice memkey = key.memtable_key();
    Slice ukey = key.user_key();
    bool ukey_exist = false;

    if (hash_ != nullptr) {
        uint32_t pos = 0;
        ukey_exist = IteratorJump(iter, ukey, memkey.data(), pos);
        if (pos == 0) {
            return false;
        }
        if (pos == UINT32_MAX) {
            iter.Seek(memkey.data());
        }
    } else {
        iter.Seek(memkey.data());
    }


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
        // use ukey_exist, less key compare operation.
        if (ukey_exist || comparator_.comparator.user_comparator()->Compare(
                Slice(key_ptr, key_length - 8), ukey) == 0) {
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