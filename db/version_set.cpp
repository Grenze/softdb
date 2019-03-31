//
// Created by lingo on 19-1-17.
//

#include <iostream>
#include "version_set.h"
#include <unordered_set>

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


// Called by WriteLevel0Table(timestamp == 0) or DoCompactionWork(timestamp != 0).
// Interval's timestamp starts from 1.
// iter is constructed from imm_ or two nvm_imm_.
// If modify versions_ here, use mutex_ in to protect versions_.
// REQUIRES: iter->Valid().
Status VersionSet::BuildTable(Iterator *iter, const int count, uint64_t timestamp) {


    Status s = Status::OK();

    assert(iter->Valid());

    //Slice start = iter->key();

    NvmMemTable *table = new NvmMemTable(icmp_, count, options_->use_cuckoo);
    table->Transport(iter, timestamp != 0);
    assert(!table->Empty());

    // Verify that the table is usable
    Iterator *table_iter = table->NewIterator();

    table_iter->SeekToFirst();  // O(1)
    const char* lRaw = table_iter->Raw();

    table_iter->SeekToLast();   // O(1)
    const char* rRaw = table_iter->Raw();

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
    index_.insert(lRaw, rRaw, table, timestamp);   // awesome fast
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
    //std::cout<<"Want: "<<key.user_key().ToString()<<std::endl;
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
    if (intervals.size() >= 3) {
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

}



// Only used in nvm data compaction, neither l or r is nullptr.
class CompactIterator : public Iterator {
public:

    typedef VersionSet::Index::Interval interval;

    explicit CompactIterator(const InternalKeyComparator& cmp,
            VersionSet::Index* index,
            const char* l,
            const char* r,
            uint64_t t,
            std::vector<interval*>& inters)
            : iter_icmp(cmp),
              helper_(index),
              left_border(l),
              right_border(r),
              left(nullptr),
              right(nullptr),
              time_border(t + 1),
              finished(false),
              old_intervals(inters),
              merge_iter(nullptr)
              {
        /*
        std::cout<<"left_border: "<<GetLengthPrefixedSlice(left_border).ToString()
        <<"right_border: "<<GetLengthPrefixedSlice(right_border).ToString()<<std::endl;
         */
    }

    ~CompactIterator() {
        Release();
        assert(finished);
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
        if (finished) {
            return false;
        }
        return merge_iter->Valid();
    }

    // k is internal key
    virtual void Seek(const Slice& k) {

    }

    // Only the keys in compaction range appeals to us.
    virtual void SeekToFirst() {
        HelpSeek(left_border);
    }

    virtual void SeekToLast() {

    }

    virtual void Next() {
        assert(Valid());

        if (merge_iter->Raw() == right_border) {
            finished = true;
        }
        merge_iter->Next();

        // we are after the last key
        if (right == nullptr) return;

        if (merge_iter->Valid()) {
            // reach the border and trigger a seek
            if (merge_iter->Raw() == right) {
                HelpSeek(right);
            }
        } else {
            HelpSeek(right);
        }
    }

    virtual void Prev() {

    }

    // keep key() value() function to test.
    virtual Slice key() const {
        assert(Valid());
        return merge_iter->key();
    }

    virtual Slice value() const {
        assert(Valid());
        return merge_iter->value();
    }

    // transport keys between old intervals and new intervals.
    virtual const char* Raw() const {
        assert(Valid());
        return merge_iter->Raw();
    }

    virtual Status status() const {
        return merge_iter->status();
    }


private:

    void HelpSeek(const char* k) {
        assert(k != nullptr);
        ClearIterator();
        /*
        std::cout<<"target: "<<ExtractUserKey(GetLengthPrefixedSlice(k)).ToString()<<std::endl;
        if (left != nullptr) {
            std::cout<<"left: "<<ExtractUserKey(GetLengthPrefixedSlice(left)).ToString()<<" ";
        } else {
            std::cout<<"left: nullptr"<<" ";
        }
        if (right != nullptr) {
            std::cout<<"right: "<<ExtractUserKey(GetLengthPrefixedSlice(right)).ToString()<<" ";
        } else {
            std::cout<<"right: nullptr"<<" ";
        }
        std::cout<<std::endl;
        if (merge_iter != nullptr && merge_iter->Valid()) {
            std::cout<<"current pos: "<<ExtractUserKey(merge_iter->key()).ToString()<<std::endl;
        }
         */

        helper_.ReadLock();
        helper_.Seek(k, intervals, left, right);
        InitIterator();
        helper_.ReadUnlock();
        if (merge_iter != nullptr) {
            merge_iter->Seek(GetLengthPrefixedSlice(k));
        }
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
                if (filter.find(interval) == filter.end()) {
                    if (iter_icmp.Compare(GetLengthPrefixedSlice(interval->sup()),
                            GetLengthPrefixedSlice(left_border)) < 0 ||
                        iter_icmp.Compare(GetLengthPrefixedSlice(interval->inf()),
                                GetLengthPrefixedSlice(right_border)) > 0 ) {
                        // skip irrelevant intervals
                    } else {
                        filter.insert(interval);
                        old_intervals.push_back(interval);
                    }
                }
                //std::cout<<"inf: "<<interval->inf()<<"sup: "<< interval->sup();
                iterators.push_back(interval->get_table()->NewIterator());
            }
        }
        //std::cout<<std::endl;
        merge_iter = (iterators.empty()) ?
                     nullptr : NewMergingIterator(&iter_icmp, &iterators[0], iterators.size());
    }



    const InternalKeyComparator iter_icmp;

    VersionSet::Index::IteratorHelper helper_;

    const char* const left_border;
    const char* const right_border;
    const char* left;
    const char* right;

    const uint64_t  time_border;
    bool finished;
    std::unordered_set<interval*> filter;
    std::vector<interval*>& old_intervals;
    std::vector<interval*> intervals;
    std::vector<Iterator*> iterators;
    Iterator* merge_iter;

    // No copying allowed
    CompactIterator(const CompactIterator&);
    void operator=(const CompactIterator&);
};


// Only one nvm data compaction thread
void VersionSet::DoCompaction(const char *HotKey) {
    // avg_count may be mis-calculated a little larger than real value under multi-thread.
    // But this doesn't matter.
    uint64_t avg_count = last_sequence_/index_.size();
    assert(avg_count > 0);
    std::vector<interval*> intervals;
    const char* left = HotKey;
    const char* right = HotKey;

    index_.WriteLock();
    // use available timestamp for new intervals generated from compaction
    uint64_t merge_time_line = index_.NextTimestamp();
    // currently max timestamp
    uint64_t time_border = merge_time_line - 1;
    // increase timestamp
    index_.IncTimestamp();
    index_.WriteUnlock();

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

    // expand interval set to leftmost overlapped interval
    bool flag = true;
    while (flag) {
        flag = false;
        intervals.clear();
        index_.search(left, intervals, false);
        //std::cout<<"left: "<<ExtractUserKey(GetLengthPrefixedSlice(left)).ToString()<<std::endl;
        for (auto &interval : intervals) {
            if (icmp_.Compare(GetLengthPrefixedSlice(interval->inf()),
                              GetLengthPrefixedSlice(left)) < 0) {
                left = interval->inf();
                flag = true;
            }
        }
    }

    // expand interval set to rightmost overlapped interval
    flag = true;
    while (flag) {
        flag = false;
        intervals.clear();
        index_.search(right, intervals, false);
        //std::cout<<"right: "<<ExtractUserKey(GetLengthPrefixedSlice(right)).ToString()<<std::endl;
        for (auto &interval: intervals) {
            if (icmp_.Compare(GetLengthPrefixedSlice(interval->sup()),
                              GetLengthPrefixedSlice(right)) > 0) {
                right = interval->sup();
                flag = true;
            }
        }
    }
    index_.ReadUnlock();

    intervals.clear();
    Iterator* iter = new CompactIterator(icmp_, &index_, left, right, time_border, intervals);
    iter->SeekToFirst();
    //std::cout<<"old intervals: "<<index_.size()<<std::endl;
    //ShowIndex();
    while (iter->Valid()) {
        BuildTable(iter, avg_count, merge_time_line);
    }
    delete iter;
    // Before delete the old intervals,
    // there are two same internal key in old interval and new interval,
    // as new interval's timestamp is greater than old ones,
    // it doesn't degrade the performance of Get operation.
    // But as there are redundant intervals, iterator can be slightly slow.
    index_.WriteLock();
    //std::cout<<"old intervals' timestamp: "<<std::endl;
    for (auto &interval: intervals) {
        index_.remove(interval);
        //std::cout<<interval->stamp()<<std::endl;
        //interval->Unref();  // call Unref() to delete interval.
    }
    //std::cout<<"new intervals: "<<index_.size()<<std::endl;
    index_.WriteUnlock();
    //ShowIndex();

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

class NvmIterator: public Iterator {
public:
    explicit NvmIterator(const InternalKeyComparator& cmp, VersionSet::Index* index)
                        : iter_icmp(cmp),
                          helper_(index),
                          left(nullptr),
                          right(nullptr),
                          time_border(index->NextTimestamp()),
                          merge_iter(nullptr) {

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
        if ((left == nullptr && right == nullptr) ||
            (left != nullptr && iter_icmp.Compare(k, GetLengthPrefixedSlice(left)) <= 0) ||
            (right != nullptr && iter_icmp.Compare(k, GetLengthPrefixedSlice(right)) >= 0)) {
            HelpSeek(EncodeKey(&tmp_, k));
        }
        else {
            // Currently we are at the interval which include the data,
            // or there is no such interval.
            if (merge_iter != nullptr) {
                merge_iter->Seek(k);
            }
        }

    }

    virtual void SeekToFirst() {
        HelpSeekToFirst();
    }

    virtual void SeekToLast() {
        HelpSeekToLast();
    }

    virtual void Next() {
        assert(Valid());
        merge_iter->Next();

        // we are after the last key
        if (right == nullptr) return;

        if (merge_iter->Valid()) {
            // reach the border and trigger a seek
            if (merge_iter->Raw() == right) {
                HelpSeek(right);
            }
        } else {
            HelpSeek(right);
        }
    }

    virtual void Prev() {
        assert(Valid());
        merge_iter->Prev();

        // we are before the first node
        if (left == nullptr) return;

        if (merge_iter->Valid()) {
            // reach the border and trigger a seek
            if (merge_iter->Raw() == left) {
                HelpSeek(left);
            }
        } else {
            HelpSeek(left);
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

    virtual const char* Raw() const {
        assert(Valid());
        return merge_iter->Raw();
    }

    virtual Status status() const {
        return merge_iter->status();
    }

private:

    // target is internal key
    void HelpSeek(const char* k) {
        assert(k != nullptr);
        ClearIterator();

        /*std::cout<<"target: "<<ExtractUserKey(GetLengthPrefixedSlice(k)).ToString()<<std::endl;
        if (left != nullptr) {
            std::cout<<"left: "<<ExtractUserKey(GetLengthPrefixedSlice(left)).ToString()<<" ";
        } else {
            std::cout<<"left: nullptr"<<" ";
        }
        if (right != nullptr) {
            std::cout<<"right: "<<ExtractUserKey(GetLengthPrefixedSlice(right)).ToString()<<" ";
        } else {
            std::cout<<"right: nullptr"<<" ";
        }
        std::cout<<std::endl;
        if (merge_iter != nullptr && merge_iter->Valid()) {
            std::cout<<"current pos: "<<ExtractUserKey(merge_iter->key()).ToString()<<std::endl;
        }*/

        helper_.ReadLock();
        helper_.Seek(k, intervals, left, right);
        InitIterator();
        helper_.ReadUnlock();

        if (merge_iter != nullptr) {
            merge_iter->Seek(GetLengthPrefixedSlice(k));
        }
        //TODO: MaybeScheduleNvmCompaction()

    }

    void HelpSeekToFirst() {
        ClearIterator();
        helper_.ReadLock();
        helper_.SeekToFirst(intervals, left, right);
        InitIterator();
        helper_.ReadUnlock();

        if (merge_iter != nullptr) {
            merge_iter->SeekToFirst();
        }
    }

    void HelpSeekToLast() {
        ClearIterator();
        helper_.ReadLock();
        helper_.SeekToLast(intervals, left, right);
        InitIterator();
        helper_.ReadUnlock();
        if (merge_iter != nullptr) {
            merge_iter->SeekToLast();
        }
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

