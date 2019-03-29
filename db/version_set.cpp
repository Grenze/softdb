//
// Created by lingo on 19-1-17.
//

#include <iostream>
#include "version_set.h"


namespace softdb {


void VersionSet::MarkFileNumberUsed(uint64_t number) {
    if (next_file_number_ <= number) {
        next_file_number_ = number + 1;
    }
}

VersionSet::~VersionSet(){

}

VersionSet::VersionSet(const std::string& dbname,
                       const Options* options,
                       /*TableCache* table_cache,*/
                       const InternalKeyComparator* cmp)
        : //env_(options->env),
          dbname_(dbname),
          options_(options),
          //table_cache_(table_cache),
          icmp_(*cmp),
          next_file_number_(2),
          //manifest_file_number_(0),  // Filled by Recover()
          last_sequence_(0),
          log_number_(0),
          prev_log_number_(0),
          nvm_compaction_scheduled_(false),
          index_cmp_(*cmp),
          index_(index_cmp_)
          //descriptor_file_(nullptr),
          //descriptor_log_(nullptr),
          //dummy_versions_(this),
          //current_(nullptr) {
    //AppendVersion(new Version(this));
{   }

static Slice GetLengthPrefixedSlice(const char* data) {
    uint32_t len;
    const char* p = data;
    p = GetVarint32Ptr(p, p + 5, &len);  // +5: we assume "p" is not corrupted
    return Slice(p, len);
}

// Compare internal key or user key.
int VersionSet::KeyComparator::operator()(const char *aptr, const char *bptr, bool ukey) const {
    Slice akey = GetLengthPrefixedSlice(aptr);
    Slice bkey = GetLengthPrefixedSlice(bptr);

    return (ukey) ?
            comparator.user_comparator()->Compare(ExtractUserKey(akey), ExtractUserKey(bkey)) :
            comparator.Compare(akey, bkey);
}



// Called by WriteLevel0Table or DoCompactionWork.
// iter is constructed from imm_ or two nvm_imm_.
// If modify versions_ here, use mutex_ in to protect versions_.
// REQUIRES: iter->Valid().
Status VersionSet::BuildTable(Iterator *iter, TableMetaData *meta) {


    Status s = Status::OK();

    assert(iter->Valid());

    //Slice start = iter->key();


    NvmMemTable *table = new NvmMemTable(icmp_, meta->count, options_->use_cuckoo);
    table->Transport(iter);
    assert(!table->Empty());

    // Verify that the table is usable
    Iterator *table_iter = table->NewIterator();

    table_iter->SeekToFirst();  // O(1)
    Slice lRawKey = table_iter->RawKey();
    // tips: Where to delete them?
    char* buf1 = new char[lRawKey.size()];
    memcpy(buf1, lRawKey.data(), lRawKey.size());
    //meta->smallest = Slice(buf1, lRawKey.size());

    table_iter->SeekToLast();   // O(1)
    Slice rRawKey = table_iter->RawKey();
    char* buf2 = new char[rRawKey.size()];
    memcpy(buf2, rRawKey.data(), rRawKey.size());
    //meta->largest = Slice(buf2, rRawKey.size());

    /*
    Status stest = Status::OK();
    iter->Seek(start);
    table_iter->SeekToFirst();
    while (table_iter->Valid()) {
        assert(table_iter->key().ToString() == iter->key().ToString() &&
               table_iter->value().ToString() == iter->value().ToString());
        table_iter->Next();
        iter->Next();
    }


    iter->Seek(start);
    for (; iter->Valid(); iter->Next()) {
        table_iter->Seek(iter->key());

        LookupKey lkey(ExtractUserKey(iter->key()), kMaxSequenceNumber);
        std::string value;
        table->Get(lkey, &value, &stest);

        assert(table_iter->value().ToString() == value);
        if (value != "") {
            //std::cout << "Get: "<<value <<std::endl;
        }
    }
     */

    delete table_iter;

    // Hook table to ISL to get indexed.
    index_.WriteLock();
    index_.insert(buf1, buf2, table);   // awesome fast
    index_.WriteUnlock();

    //TODO: MaybeScheduleNvmCompaction()

    // Check for input iterator errors
    if (!iter->status().ok()) {
        s = iter->status();
    }

    return s;
}



void VersionSet::Get(const LookupKey &key, std::string *value, Status *s, port::Mutex* mu) {
    Slice memkey = key.memtable_key();
    std::vector<interval*> intervals;
    index_.ReadLock();
    index_.search(memkey.data(), intervals);
    index_.ReadUnlock();
    bool found = false;
    //std::cout<<intervals.size()<<std::endl;
    for (auto &interval : intervals) {
        //std::cout<<interval->stamp()<<" ";
        if (!found) {
            found = interval->get_table()->Get(key, value, s);
        }
        interval->Unref();
    }
    //std::cout<<std::endl;
    if (!found) {
        *s = Status::NotFound(Slice());
    }

    // Iff overlaps > threshold, trigger a nvm data compaction.
    if (intervals.size() >= 1000) {
        mu->Lock();
        if (nvm_compaction_scheduled_) {
            mu->Unlock();
        } else {
            nvm_compaction_scheduled_ = true;
            mu->Unlock();
            DoCompaction(memkey.data());
            // Is there need to lock?
            mu->Lock();
            nvm_compaction_scheduled_ = false;
            mu->Unlock();
        }
    }
    //TODO: MaybeScheduleNvmCompaction()

}

// Only one nvm data compaction thread
void VersionSet::DoCompaction(const char *HotKey) {
    std::vector<interval*> intervals;
    const char* left = HotKey;
    const char* right = HotKey;
    index_.ReadLock();
    index_.search(HotKey, intervals, false);
    for (auto &interval : intervals) {
        if (icmp_.Compare(GetLengthPrefixedSlice(interval->inf()),
                          GetLengthPrefixedSlice(left)) < 0) {
            left = interval->inf();
        }
        if (icmp_.Compare(GetLengthPrefixedSlice(interval->sup()),
                          GetLengthPrefixedSlice(right)) > 0) {
            right = interval->sup();
        }
    }
    while (true) {
        intervals.clear();
        index_.search(left, intervals, false);
        // internal key is unique
        if (intervals.size() == 1) {
            break;
        }
        for (auto &interval : intervals) {
            if (icmp_.Compare(GetLengthPrefixedSlice(interval->inf()),
                              GetLengthPrefixedSlice(left)) < 0) {
                left = interval->inf();
            }
        }
    }
    while (true) {
        intervals.clear();
        index_.search(right, intervals, false);
        // internal key is unique
        if (intervals.size() == 1) {
            break;
        }
        for (auto &interval: intervals) {
            if (icmp_.Compare(GetLengthPrefixedSlice(interval->sup()),
                              GetLengthPrefixedSlice(right)) > 0) {
                right = interval->sup();
            }
        }
    }
    index_.ReadUnlock();



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


class CompactIterator {


private:
    typedef VersionSet::Index::Interval interval;
    const InternalKeyComparator iter_icmp;

    VersionSet::Index::IteratorHelper helper_;

    const char* left;
    const char* right;
    std::vector<interval*> old_intervals;
    std::vector<interval*> new_intervals;
};

class NvmIterator: public Iterator {
public:
    explicit NvmIterator(const InternalKeyComparator& cmp, VersionSet::Index* index)
                        : iter_icmp(cmp),
                          helper_(index),
                          left(nullptr),
                          right(nullptr),
                          time_border(index->NextTimestamp()) {

    }

    ~NvmIterator() {
        Release();
    }

    virtual bool Valid() const {
        // Never call the Seek* function or no interval
        if (left == nullptr && right == nullptr) {
            return false;
        }
        // There is no data in current interval
        if (merge_iter == nullptr) {
            return false;
        }
        return merge_iter->Valid();
    }

    // k is internal key
    virtual void Seek(const Slice& k) {
        if (left == nullptr && right == nullptr) {
            HelpSeek(k);
        } else if (left != nullptr && iter_icmp.Compare(k, GetLengthPrefixedSlice(left)) <= 0) {
            HelpSeek(k);
        } else if (right != nullptr && iter_icmp.Compare(k, GetLengthPrefixedSlice(right)) >= 0) {
            HelpSeek(k);
        }
        // now we at the interval which include the data, or there is no such interval.
        if (merge_iter != nullptr) {
            merge_iter->Seek(k);
        }
    }

    virtual void SeekToFirst() {
        HelpSeekToFirst();
        if (merge_iter != nullptr) {
            merge_iter->SeekToFirst();
        }
    }

    virtual void SeekToLast() {
        HelpSeekToLast();
        if (merge_iter != nullptr) {
            merge_iter->SeekToLast();
        }
    }

    virtual void Next() {
        assert(Valid());
        merge_iter->Next();

        // we are after the last key
        if (right == nullptr) return;

        Slice ikey = GetLengthPrefixedSlice(right);
        if (merge_iter->Valid()) {
            // reach the border and trigger a seek
            if (iter_icmp.Compare(merge_iter->key(), ikey) == 0) {
                Seek(ikey);
            }
        } else {
            Seek(ikey);
        }
    }

    virtual void Prev() {
        assert(Valid());
        merge_iter->Prev();

        // we are before the first node
        if (left == nullptr) return;

        Slice ikey = GetLengthPrefixedSlice(left);
        if (merge_iter->Valid()) {
            // reach the border and trigger a seek
            if (iter_icmp.Compare(merge_iter->key(), ikey) == 0) {
                Seek(ikey);
            }
        } else {
            Seek(ikey);
        }
    }

    virtual Slice key() const {
        assert(Valid());
        return merge_iter->key();
    }

    virtual Slice value() const {
        assert(Valid());
        return merge_iter->value();
    }

    virtual Slice Raw() const {}
    virtual Slice RawKey() const {}

    virtual Status status() const {
        return merge_iter->status();
    }

private:

    // target is internal key
    void HelpSeek(const Slice& k) {
        ClearIterator();
        //std::cout<<"target: "<<ExtractUserKey(k).ToString()<<std::endl;

        helper_.ReadLock();
        helper_.Seek(EncodeKey(&tmp_, k), intervals, left, right);
        helper_.ReadUnlock();
/*
        if (left != nullptr) {
            std::cout<<"left: "<<ExtractUserKey(GetLengthPrefixedSlice(left)).ToString()<<std::endl;
        } else {
            std::cout<<"left: nullptr"<<std::endl;
        }
        if (right != nullptr) {
            std::cout<<"right: "<<ExtractUserKey(GetLengthPrefixedSlice(right)).ToString()<<std::endl;
        } else {
            std::cout<<"right: nullptr"<<std::endl;
        }*/
        //TODO: MaybeScheduleNvmCompaction()

        InitIterator();
    }

    void HelpSeekToFirst() {
        ClearIterator();
        helper_.ReadLock();
        helper_.SeekToFirst(intervals, left, right);
        helper_.ReadUnlock();
/*
        if (left != nullptr) {
            std::cout<<"left: "<<ExtractUserKey(GetLengthPrefixedSlice(left)).ToString()<<std::endl;
        } else {
            std::cout<<"left: nullptr"<<std::endl;
        }
        if (right != nullptr) {
            std::cout<<"right: "<<ExtractUserKey(GetLengthPrefixedSlice(right)).ToString()<<std::endl;
        } else {
            std::cout<<"right: nullptr"<<std::endl;
        }*/

        InitIterator();
    }

    void HelpSeekToLast() {
        ClearIterator();
        helper_.ReadLock();
        helper_.SeekToLast(intervals, left, right);
        helper_.ReadUnlock();
        InitIterator();
    }


    void Release() {
        // release the intervals in last search
        for (auto &interval : intervals) {
            if (interval->stamp() < time_border) {
                interval->Unref();
            }
        }
    }


    void ClearIterator() {
        Release();
        intervals.clear();
        iterators.clear();
    }

    void InitIterator() {
        for (auto &interval : intervals) {
            if (interval->stamp() < time_border) {
                interval->Ref();
                //std::cout<<"inf: "<<interval->inf()<<"sup: "<< interval->sup();
                iterators.push_back(interval->get_table()->NewIterator());
            }
        }
        //std::cout<<std::endl;
        merge_iter = (iterators.empty()) ?
                nullptr : NewMergingIterator(&iter_icmp, &iterators[0], iterators.size());
    }

    typedef VersionSet::Index::Interval interval;

    const InternalKeyComparator iter_icmp;

    VersionSet::Index::IteratorHelper helper_;

    // updated by nvmSkipList's IterateHelper
    const char* left;   // nullptr indicates head_
    const char* right;  // nullptr indicates tail_
    // upper bound, prevent further generated interval's iterator from being added.
    // We are interested at the intervals whose timestamp < time_border.
    const uint64_t time_border;
    std::vector<interval*> intervals;
    std::vector<Iterator*> iterators;
    Iterator* merge_iter;

    std::string tmp_;       // For passing to EncodeKey


    // No copying allowed
    NvmIterator(const NvmIterator&);
    void operator=(const NvmIterator&);
};


Iterator* VersionSet::NewIterator() {
    return new NvmIterator(icmp_, &index_);
}





}  // namespace softdb

