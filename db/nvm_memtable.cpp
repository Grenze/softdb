//
// Created by lingo on 19-2-13.
//

#include <iostream>
#include "nvm_memtable.h"
//#include <vector>


namespace softdb {

static uint32_t GetRawLength(const char* data) {
    uint32_t len;
    const char* p = data;   // start of data.
    p = GetVarint32Ptr(p, p + 5, &len);
    p += len;
    p = GetVarint32Ptr(p, p + 5, &len);
    p += len;   // Now p reaches end of data.
    return p - data;
}

// If num = 0, it's caller's duty to delete it.
NvmMemTable::NvmMemTable(const InternalKeyComparator& cmp, const int cap, const bool assist)
           : comparator_(cmp),
             capacity_(cap),
             table_(comparator_, capacity_),
             hash_((assist) ? new Hash(capacity_) : nullptr),
             filter_((assist) ? nullptr : new Filter(capacity_)) {

}

// Attention: Only merge procedure can decide whether kept or gone.
// Use NvmMemTableIterator's Raw() to get slice.data, and delete it.
// Set bool[num_] to delete the obsolete key,
// keep the others which will be pointed by a new nvm_imm_
NvmMemTable::~NvmMemTable() {
    delete hash_;
    delete filter_;
    // TODO: use bool[] to drop old keys
    /*
    // if bool[] != nullptr;
    NvmMemTable::Table::Iterator iter_ = NvmMemTable::Table::Iterator(&table_);
    iter_.SeekToFirst();
    while (iter_.Valid()) {
        delete iter_.key();
        iter_.Next();
    }*/
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
    // The less duplicate, the faster to use cuckoo hash.
    // EncodeKey prefix k.size to k.(k is internal key)
    virtual void Seek(const Slice& k) {
        if (nvmimm_->hash_ != nullptr) {
            if (nvmimm_->IteratorJump(iter_, ExtractUserKey(k), EncodeKey(&tmp_, k))) {
                return;
            }
        }
        iter_.Seek(EncodeKey(&tmp_, k));
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

    virtual const char* Raw() const { return iter_.key(); }

    virtual Status status() const { return Status::OK(); }

private:
    NvmMemTable::Table::Iterator iter_;
    NvmMemTable* nvmimm_;
    std::string tmp_;          // For passing to EncodeKey;

    // No copying allowed
    NvmMemTableIterator(const NvmMemTableIterator&);
    void operator=(const NvmMemTableIterator&);
};

Iterator* NvmMemTable::NewIterator() {
    return new NvmMemTableIterator(&table_, this);
}

// REQUIRES: iter is valid.
// Once called, never again.
void NvmMemTable::Transport(Iterator* iter, bool compact) {
    assert(iter->Valid());
    // pos from 1 to num_
    uint32_t pos = 0;
    Table::Worker ins = Table::Worker(&table_);
    //get the first user key
    Slice current_user_key = ExtractUserKey(iter->key());
    uint32_t current_pos = 1;
    Slice tmp;
    const char* raw;
    char* buf;
    //const char* b1 = nullptr;
    //const char* b2 = nullptr;
    while (iter->Valid()) {
        //b1 = b2;
        //b2 = iter->Raw();
        //if (b1 != nullptr) {
        //    assert(comparator_(b2, b1) > 0);
        //}
        if (hash_ != nullptr) {
            pos++;
            tmp = ExtractUserKey(iter->key());
            if (comparator_.comparator.user_comparator()->Compare(tmp, current_user_key) != 0) {
                hash_->Add(current_user_key, current_pos);
                current_user_key = tmp;
                current_pos = pos;
            }
        } else {
            tmp = ExtractUserKey(iter->key());
            if (comparator_.comparator.user_comparator()->Compare(tmp, current_user_key) != 0) {
                filter_->Add(current_user_key);
                current_user_key = tmp;
            }
        }

        // Raw data from imm_ or nvm_imm_
        raw = iter->Raw();
        // After make_persistent, only delete the obsolete data(char*).
        // So there is only space amplification (no need to write key-value pair twice).
        // Also better for wear-leveling.
        // Read amplification normally doesn't reach
        // the number of overlapped intervals.
        if (compact) {
            buf = const_cast<char*>(raw);
        } else {
            uint32_t len = GetRawLength(raw);
            buf = new char[len];
            memcpy(buf, raw, len);
        }
        if (!ins.Insert(buf)) { break; }
        iter->Next();
    }
    if (hash_ != nullptr) {
        hash_->Add(current_user_key, current_pos);
    } else {
        filter_->Add(current_user_key);
    }
    // iter not valid or no room to insert.
    ins.Finish();
    // If not do this, this interval's last key will
    // have the same internal key with next interval.
    if (compact && iter->Valid()) {
        iter->Next();
    }
}

// REQUIRES: Use cuckoo hash to assist search.
// Return true iff user key exists in nvm_imm_.
// Bug: If there are two different key with same hash value(tag + loc),
// the key inserted after will never be accessed.
// Assume: When we insert key into cuckoo hash, the situation mentioned above never happened,
// but is's still important to check whether user key is correct as the key to search is unpredictable.
bool NvmMemTable::IteratorJump(Table::Iterator &iter, const Slice& ukey, const char* memkey) const {
    assert(hash_ != nullptr);
    //std::vector<uint32_t> positions;
    uint32_t pos = 0; // 0 for head_
    if (hash_->Find(ukey, pos)) {
        //for (auto &pos : positions) {
            //assert(positions.size() == 1);
            assert(pos > 0);
            iter.Jump(pos);
            const char* entry = iter.key();
            uint32_t key_length;
            const char* key_ptr = GetVarint32Ptr(entry, entry+5, &key_length);
            if (comparator_.comparator.user_comparator()->Compare(
                    Slice(key_ptr, key_length - 8), ukey) == 0) {
                // Correct user key
                iter.WaveSearch(memkey);
                return true;
            }
        //}
    }
    // No such user key
    return false;
}


bool NvmMemTable::Get(const LookupKey &key, std::string *value, Status *s, const char*& HotKey) {
    Slice memkey = key.memtable_key();
    Slice ukey = key.user_key();
    Table::Iterator iter(&table_);
    // correct user key
    bool user_key = false;

    if (hash_ != nullptr) {
        user_key = IteratorJump(iter, ukey, memkey.data());
        if (!user_key) {
            return false;
        }
    } else {
        if (!filter_->Contain(ukey)) {
            return false;
        }
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

        HotKey = entry; // used by nvm data compaction procedure

        uint32_t key_length;
        const char* key_ptr = GetVarint32Ptr(entry, entry+5, &key_length);
        if (user_key || comparator_.comparator.user_comparator()->Compare(
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