//
// Created by lingo on 19-4-11.
//

#ifndef SOFTDB_CUCKOOFILTER_H
#define SOFTDB_CUCKOOFILTER_H

#include <cassert>
#include <algorithm>
#include <iostream>

#include "hashutil.h"
#include "singletable.h"
#include "softdb/slice.h"

namespace CuckooHash {


template <size_t bits_per_item,
        template <size_t> class TableType = SingleTable>
class CuckooFilter {

    typedef typename softdb::Slice Slice;

    // maximum number of cuckoo kicks before claiming failure
    static const size_t kMaxCuckooKickCount = 500;

    size_t maxKickRecorded = 0;

    //assoc = kTagsPerBucket
    static const size_t assoc = 4;

    // Storage of items
    TableType<bits_per_item> *table_;

    // Number of items stored
    size_t num_items_;

    typedef struct {
        size_t index;
        uint32_t tag;
        bool used;
    } VictimCache;

    VictimCache victim_;

    inline size_t IndexHash(uint32_t hv) const {
        // table_->num_buckets is always a power of two, so modulo can be replaced
        // with
        // bitwise-and:
        return hv & (table_->NumBuckets() - 1);
    }

    inline uint32_t TagHash(uint32_t hv) const {
        uint32_t tag;
        tag = hv & (table_->TagMask());
        tag += (tag == 0);
        return tag;
    }

    const uint32_t kCuckooMurmurSeedMultiplier = 816922183;

    inline void GenerateIndexTagHash(const Slice& key, size_t* index,
                                     uint32_t* tag) const {

        const uint64_t hash = MurmurHash64A(key.data(), static_cast<int>(key.size()), kCuckooMurmurSeedMultiplier);
        *index = IndexHash(hash>>32);
        *tag = TagHash(hash);
    }

    inline size_t AltIndex(const size_t index, const uint32_t tag) const {
        return IndexHash((uint32_t)(index ^ (tag * 0x5bd1e995)));
    }

    bool AddImpl(const size_t i, const uint32_t tag);

    // load factor is the fraction of occupancy
    double LoadFactor() const { return 1.0 * Size() / table_->SizeInTags(); }

    double BitsPerItem() const { return 8.0 * table_->SizeInBytes() / Size(); }

    // No copying allowed
    CuckooFilter(const CuckooFilter&);
    void operator=(const CuckooFilter&);

public:
    explicit CuckooFilter(const size_t max_num_keys) : num_items_(0), victim_() {

        //table_->num_buckets is always a power of two greater than max_num_keys
        size_t num_buckets = upperpower2(std::max<uint64_t>(1, max_num_keys / assoc));
        //check if max_num_keys/(num_buckets*assoc) <= 0.96
        //if not double num_buckets
        double frac = (double)max_num_keys / num_buckets / assoc;
        if (frac > 0.96) {
            num_buckets <<= 1;
        }
        victim_.used = false;
        table_ = new TableType<bits_per_item>(num_buckets);
    }

    ~CuckooFilter() { delete table_; }

    // Add an item to the filter.
    bool Add(const Slice& item);

    // Report if the item is inserted, with false positive rate.
    bool Contain(const Slice& key) const;

    // Delete an key from the filter
    bool Delete(const Slice& item);

    /* methods for providing stats  */
    // summary information
    std::string Info() const;

    // number of current inserted items;
    size_t Size() const { return num_items_; }

    // size of the filter in bytes.
    size_t SizeInBytes() const { return table_->SizeInBytes(); }
};

template <size_t bits_per_item,
        template <size_t> class TableType>
bool CuckooFilter<bits_per_item, TableType>::Add(
        const Slice& item) {
    size_t i;
    uint32_t tag;

    if (victim_.used) {
        return false;
    }

    GenerateIndexTagHash(item, &i, &tag);
    return AddImpl(i, tag);
}

template <size_t bits_per_item,
        template <size_t> class TableType>
bool CuckooFilter<bits_per_item, TableType>::AddImpl(
        const size_t i, const uint32_t tag) {
    size_t curindex = i;
    uint32_t curtag = tag;
    uint32_t oldtag;

    for (uint32_t count = 0; count < kMaxCuckooKickCount; count++) {
        bool kickout = count > 0;
        oldtag = 0;
        if (table_->InsertTagToBucket(curindex, curtag, kickout, oldtag)) {
            num_items_++;
            maxKickRecorded = (count > maxKickRecorded)? count : maxKickRecorded;
            return true;
        }
        if (kickout) {
            curtag = oldtag;
        }
        //beign kick out after both index tried
        curindex = AltIndex(curindex, curtag);
    }
    maxKickRecorded = kMaxCuckooKickCount;
    victim_.index = curindex;
    victim_.tag = curtag;
    victim_.used = true;
    return true;
}

template <size_t bits_per_item,
        template <size_t> class TableType>
bool CuckooFilter<bits_per_item, TableType>::Contain(
        const Slice& key) const {
    bool found = false;
    size_t i1, i2;
    uint32_t tag;

    GenerateIndexTagHash(key, &i1, &tag);
    i2 = AltIndex(i1, tag);

    assert(i1 == AltIndex(i2, tag));

    found = victim_.used && (tag == victim_.tag) &&
            (i1 == victim_.index || i2 == victim_.index);

    if (found || table_->FindTagInBuckets(i1, i2, tag)) {
        return true;
    } else {
        return false;
    }
}

template <size_t bits_per_item,
        template <size_t> class TableType>
bool CuckooFilter<bits_per_item, TableType>::Delete(
        const Slice& key) {
    size_t i1, i2;
    uint32_t tag;

    GenerateIndexTagHash(key, &i1, &tag);
    i2 = AltIndex(i1, tag);

    if (table_->DeleteTagFromBucket(i1, tag) || table_->DeleteTagFromBucket(i2, tag)) {
        num_items_--;
        // TryEliminateVictim
        if (victim_.used) {
            num_items_--;
            victim_.used = false;
            size_t i = victim_.index;
            uint32_t tag = victim_.tag;
            AddImpl(i, tag);
        }
        return true;
    } else if (victim_.used && tag == victim_.tag &&
               (i1 == victim_.index || i2 == victim_.index)) {
        num_items_--;
        victim_.used = false;
        return true;
    } else {
        return false;
    }
}

template <size_t bits_per_item,
        template <size_t> class TableType>
std::string CuckooFilter<bits_per_item, TableType>::Info() const {
    std::stringstream ss;
    ss << "CuckooFilter Status:\n"
       << "\t\t" << table_->Info() << "\n"
       << "\t\tKeys stored: " << Size() << "\n"
       << "\t\tLoad factor: " << LoadFactor() << "\n"
       << "\t\tHashtable size: " << (table_->SizeInBytes() >> 10) << " KB\n";
    if (Size() > 0) {
        ss << "\t\tbit/key:   " << BitsPerItem() << "\n";
    } else {
        ss << "\t\tbit/key:   N/A\n";
    }
    ss << "\t\tmaxKickRecorded: " << maxKickRecorded <<"\n";
    return ss.str();
}
}  // namespace CuckooHash
#endif //SOFTDB_CUCKOOFILTER_H
