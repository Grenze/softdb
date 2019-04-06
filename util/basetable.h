//
// Created by lingo on 19-2-24.
//

#ifndef SOFTDB_BASETABLE_H
#define SOFTDB_BASETABLE_H



#include <cstring>
#include <cstdint>
#include <sstream>
#include <vector>

#include "bitsutil.h"

// Generally every bucket contains slots with the number of associativity,
// slot contains key's hash tag and location of key-value pair

namespace CuckooHash {

template <size_t bits_per_tag, size_t bits_per_slot, size_t associativity>
class BaseTable {
    static const size_t slotsPerBucket = associativity;
    // plus 7 to make sure there is enough space per Bucket to store slots
    // when bits_per_slot = 1
    static const size_t bytesPerBucket =
            (bits_per_slot * slotsPerBucket + 7) >> 3;
    static const uint32_t tagMask = static_cast<const uint32_t>((1ULL << bits_per_tag) - 1);
    static const size_t SlotTagShift = (bits_per_slot - bits_per_tag);
    static const size_t paddingBuckets =
            ((((bytesPerBucket + 7) / 8) * 8) - 1) / bytesPerBucket;

    struct Bucket {
        char bits_[bytesPerBucket];
    } __attribute__((__packed__));

    // using a pointer adds one more indirection
    size_t num_buckets_;
    Bucket *buckets_;

public:
    explicit BaseTable(const size_t num) : num_buckets_(num) {
        buckets_ = new Bucket[num_buckets_ + paddingBuckets];
        // Attention: Do not forget to memset it.
        memset(buckets_, 0, bytesPerBucket * (num_buckets_ + paddingBuckets));
    }

    ~BaseTable() {
        delete[] buckets_;
    }

    size_t NumBuckets() const {
        return num_buckets_;
    }

    uint32_t TagMask() const{
        return tagMask;
    }

    size_t SizeInBytes() const {
        return bytesPerBucket * num_buckets_;
    }

    size_t SizeInSlots() const {
        return slotsPerBucket * num_buckets_;
    }

    std::string Info() const {
        std::stringstream ss;
        ss << "BaseTable with tag size and slot size: " << bits_per_tag << " / " << bits_per_slot << " bits \n";
        ss << "\t\tAssociativity: " << slotsPerBucket << "\n";
        ss << "\t\tTotal # of rows: " << num_buckets_ << "\n";
        ss << "\t\tTotal # slots: " << SizeInSlots() << "\n";
        return ss.str();
    }

    // read slot from pos(i,j)
    inline uint64_t ReadSlot(const size_t i, const size_t j) const {
        const char *p = buckets_[i].bits_;
        uint64_t slot = 0;
        if (bits_per_slot == 64) {
            slot = ((uint64_t *)p)[j];
        }
        return slot;
    }

    inline uint64_t ReadTag(const size_t i, const size_t j) const {
        return ReadSlot(i, j) >> SlotTagShift;
    }

    // write slot to pos(i,j)
    inline void WriteSlot(const size_t i, const size_t j, const uint64_t s) {
        char *p = buckets_[i].bits_;
        uint64_t slot = s;
        if (bits_per_slot == 64){
            ((uint64_t *)p)[j] = slot;
        }
    }

    // find slot with specific tag in buckets
    inline void FindSlotInBuckets(const size_t i1, const size_t i2,
                                      const uint32_t tag, /*std::vector<*/uint32_t/*>*/& locations) const {
        uint64_t slot1 = 0;
        for (size_t j = 0; j < slotsPerBucket; j++) {
            slot1 = ReadSlot(i1, j);
            if (slot1 != 0 && slot1 >> SlotTagShift == tag){
                //locations.push_back(static_cast<uint32_t>(slot1));
                locations = static_cast<uint32_t>(slot1);
                return;
            }
            if (i2 != i1) {
                slot1 = ReadSlot(i2, j);
                if(slot1 != 0 && slot1 >> SlotTagShift == tag) {
                    //locations.push_back(static_cast<uint32_t>(slot1));
                    locations = static_cast<uint32_t>(slot1);
                    return;
                }
            }
        }
    }

    // delete slot with specific tag from bucket
    inline bool DeleteSlotFromBucket(const size_t i, const uint32_t tag) {
        for (size_t j = 0; j < slotsPerBucket; j++) {
            if (ReadTag(i, j) == tag) {
                WriteSlot(i, j, 0);
                return true;
            }
        }
        return false;
    }

    inline bool InsertSlotToBucket(const size_t i, const uint64_t slot,
                                   const bool kickout, uint64_t &oldslot) {
        for (size_t j = 0; j < slotsPerBucket; j++) {
            if (ReadSlot(i, j) == 0) {
                WriteSlot(i, j, slot);
                return true;
            }
        }
        if (kickout) {
            size_t r = rand() & (slotsPerBucket - 1);
            oldslot = ReadSlot(i, r);
            WriteSlot(i, r, slot);
        }
        return false;
    }

    inline size_t NumSlotsInBucket(const size_t i) const {
        size_t num = 0;
        for (size_t j = 0; j < slotsPerBucket; j++) {
            if (ReadSlot(i, j) != 0) {
                num++;
            }
        }
        return num;
    }
};
}   // namespace CuckooHash

#endif //SOFTDB_BASETABLE_H
