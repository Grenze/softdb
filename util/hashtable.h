//
// Created by lingo on 19-2-24.
//

#ifndef SOFTDB_HASHTABLE_H
#define SOFTDB_HASHTABLE_H



#include <cstring>
#include <assert.h>

#include "basetable.h"
#include "hashutil.h"
#include "softdb/slice.h"


namespace CuckooHash{

// Hash Table providing methods of Add, Delete, Find.
// It takes three template parameters:
// TableType: the storage of table, BaseTable by default.
template <size_t bits_per_tag, size_t bits_per_slot,
        template <size_t, size_t, size_t> class TableType = BaseTable>
class HashTable {
private:

    typedef typename softdb::Slice Slice;

    // maximum number of cuckoo kicks before claiming failure
    static const size_t kMaxCuckooKickCount = 500;

    size_t maxKickRecorded = 0;
    static const size_t SlotTagShift = (bits_per_slot - bits_per_tag);
    // assoc = slotsPerBucket
    static const size_t assoc = 4;

    // typically 32 bits tag, 32 bits location
    TableType<bits_per_tag, bits_per_slot, assoc> *table_;

    // number of keys stored
    size_t num_items_;

    typedef struct {
        size_t index;
        uint64_t slot;
        bool used;
    } VictimItem;

    VictimItem victim_;

    inline size_t IndexHash(uint32_t hv) const {
        // table_->num_buckets is always a power of two,
        // so modulo can be replaced with bitwise-and
        return hv & (table_->NumBuckets() - 1);
    }

    inline uint32_t TagHash(uint32_t hv) const {
        uint32_t tag;
        tag = hv & (table_->TagMask());
        tag += (tag == 0);
        return tag;
    }

    static const uint32_t cuckooMurmurSeedMultiplier = 816922183;

    inline void GenerateIndexTagHash(const Slice& key, size_t* index,
                                     uint32_t *tag) const {
        const uint64_t hash = MurmurHash64A(key.data(), static_cast<int>(key.size()), cuckooMurmurSeedMultiplier);
        *index = IndexHash(static_cast<uint32_t>(hash >> 32));
        *tag = TagHash((uint32_t) hash);
    }

    inline size_t AltIndex(const size_t index, const uint32_t tag) const {
        return IndexHash((uint32_t) (index ^ (tag * 0x5bd1e995)));;
    }

    //Status AddImpl(size_t i, uint32_t tag, uint32_t location);
    bool AddImpl(size_t i, uint32_t tag, uint32_t location);

    // number of current inserted items;
    size_t Size() const { return num_items_; }

    // load factor is the fraction of occupancy
    double LoadFactor() const { return 1.0 * Size() / table_->SizeInSlots(); }

    double BitsPerItem() const { return 8.0 * table_->SizeInBytes() / Size(); }

    // No copying allowed
    HashTable(const HashTable&);
    void operator=(const HashTable&);

public:

    inline bool hasVictim() const { return victim_.used; }
    explicit HashTable(const int max_num_keys) : num_items_(0), victim_() {

        // table_->num_buckets is always a power of two greater than max_num_keys
        size_t num_buckets = upperpower2(std::max<uint64_t>(1, max_num_keys / assoc));
        // check if max_num_keys/(num_buckets*assoc) <= 0.96
        // if not double num_buckets
        double frac = (double) max_num_keys / num_buckets / assoc;
        if (frac > 0.96) {
            num_buckets <<= 1;
        }
        victim_.used = false;
        table_ = new TableType<bits_per_tag, bits_per_slot, assoc>(num_buckets);
    }

    ~HashTable() { delete table_; }

    // Add an item to the filter.
    //Status Add(const Slice& item, uint32_t location);
    bool Add(const Slice& item, uint32_t location);

    // Report if the item is inserted, with false positive rate.
    //Status Find(const Slice& key, uint32_t *location) const;
    bool Find(const Slice& key, /*std::vector<*/uint32_t/*>*/& location) const;

    // Delete an key from the filter
    //Status Delete(const Slice& item);
    bool Delete(const Slice& item);

    /* methods for providing stats  */
    // summary information
    std::string Info() const;

    // size of the filter in bytes.
    size_t SizeInBytes() const { return table_->SizeInBytes(); }
};


template <size_t bits_per_tag, size_t bits_per_slot,
        template <size_t, size_t, size_t > class TableType>
bool HashTable<bits_per_tag, bits_per_slot, TableType>::Add(
        const Slice& item, const uint32_t location) {
    size_t i;
    uint32_t tag;

    if (victim_.used) {
        return false;
    }

    GenerateIndexTagHash(item, &i, &tag);
    return AddImpl(i, tag, location);
}

template <size_t bits_per_tag, size_t bits_per_slot,
        template <size_t, size_t, size_t > class TableType>
bool HashTable<bits_per_tag, bits_per_slot, TableType>::AddImpl(
        const size_t i, const uint32_t tag, const uint32_t location) {
    size_t curindex = i;
    uint64_t curslot = tag;
    curslot = (curslot << SlotTagShift) + location;
    uint64_t oldslot;

    for (uint32_t count = 0; count < kMaxCuckooKickCount; count++) {
        bool kickout = count > 0;
        oldslot = 0;
        if (table_->InsertSlotToBucket(curindex, curslot, kickout, oldslot)) {
            num_items_++;
            maxKickRecorded = (count > maxKickRecorded) ? count:maxKickRecorded;
            //return Ok;
            return true;
        }
        if (kickout) {
            curslot = oldslot;
        }
        // begin kick out after both index tried
        curindex = AltIndex(curindex, static_cast<const uint32_t>(curslot >> SlotTagShift));
    }

    maxKickRecorded = kMaxCuckooKickCount;
    victim_.index = curindex;
    victim_.slot = curslot;
    victim_.used = true;
    num_items_++;
    //return Ok;
    return true;
}

// REQUIRES: location passed in with value 0
template <size_t bits_per_tag, size_t bits_per_slot,
        template <size_t, size_t, size_t > class TableType>
bool HashTable<bits_per_tag, bits_per_slot, TableType>::Find(
        const Slice& key, /*std::vector<*/uint32_t/*>*/& location) const {
    bool found = false;
    size_t i1, i2;
    uint32_t tag;

    GenerateIndexTagHash(key, &i1, &tag);
    i2 = AltIndex(i1, tag);

    assert(i1 == AltIndex(i2, tag));

    uint64_t slot = 0;
    slot = victim_.slot;
    found = victim_.used && (tag == slot >> SlotTagShift) &&
            (i1 == victim_.index || i2 == victim_.index);

    if (found) {
        location = static_cast<uint32_t>(slot);
        //location.push_back(static_cast<uint32_t>(slot));
    } else {
        table_->FindSlotInBuckets(i1, i2, tag, location);
    }

    //if (!location.empty()) {
        //assert(location.size() <= 1);
    return location != 0;
}

template <size_t bits_per_tag, size_t bits_per_slot,
        template <size_t, size_t, size_t > class TableType>
bool HashTable<bits_per_tag, bits_per_slot, TableType>::Delete(
        const Slice& key) {
    size_t i1, i2;
    uint32_t tag;

    GenerateIndexTagHash(key, &i1, &tag);
    i2 = AltIndex(i1, tag);

    if (table_->DeleteSlotFromBucket(i1, tag) || table_->DeleteSlotFromBucket(i2, tag)) {
        num_items_--;
        // TryEliminateVictim
        if (victim_.used) {
            num_items_--;
            victim_.used = false;
            size_t i = victim_.index;
            uint64_t slot = victim_.slot;
            auto tag1 = static_cast<uint32_t>(slot >> SlotTagShift);
            auto position = static_cast<uint32_t>(slot);
            AddImpl(i, tag1, position);
        }
        return true;
    } else if (victim_.used && (tag == victim_.slot >> SlotTagShift) &&
               (i1 == victim_.index || i2 == victim_.index)) {
        num_items_--;
        victim_.used = false;
        return true;
    } else {
        return false;
    }
}

template <size_t bits_per_tag, size_t bits_per_slot,
        template <size_t, size_t, size_t > class TableType>
std::string HashTable<bits_per_tag, bits_per_slot, TableType>::Info() const {
    std::stringstream ss;
    ss << "Hashtable Status:\n"
       << "\t\t" << table_->Info() << "\n"
       << "\t\tKeys stored: " << Size() << "\n"
       << "\t\tLoad factor: " << LoadFactor() << "\n"
       << "\t\tHashtable size: " << (table_->SizeInBytes() >> 10) << " KB\n";
    if (Size() > 0) {
        ss << "\t\tbit/key:   " << BitsPerItem() << "\n";
    } else {
        ss << "\t\tbit/key:   N/A\n";
    }
    if (hasVictim()) {
        ss << "\t\tHashtable has one victim" << "\n";
    } else {
        ss << "\t\tHashtable has no victim" << "\n";
    }
    ss << "\t\tMaxKickCount up to: "<< maxKickRecorded <<"\n";
    return ss.str();
}
};  // namespace CuckooHash

#endif //SOFTDB_HASHTABLE_H
