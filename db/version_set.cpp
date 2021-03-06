//
// Created by lingo on 19-1-17.
//

#include <iostream>
#include "version_set.h"
#include <unordered_set>
#include <util/mutexlock.h>

//#define compact_debug
//#define mem_dump_debug

namespace softdb {

// REQUIRES: only one merge thread.
#if defined(compact_debug)
static long total_count = 0;
static long merge_count = 0;
static long new_table_count = 0;
static long abandon_count = 0;
#endif

void VersionSet::MarkFileNumberUsed(uint64_t number) {
    if (next_file_number_ <= number) {
        next_file_number_ = number + 1;
    }
}

VersionSet::~VersionSet(){
    assert(writes_ - drops_ == index_.CountKVs());
    //std::cout << "Intervals(/KVs): "<< index_.SizeInBytes() << " Bytes" << std::endl;
}

VersionSet::VersionSet(const std::string& dbname,
                       const Options* options,
                       /*TableCache* table_cache,*/
                       const InternalKeyComparator* cmp,
                       port::Mutex& mu,
                       port::AtomicPointer& shutdown,
                       SnapshotList& snapshots,
                       Status& bg_error,
                       bool& nvm_compaction_scheduled,
                       port::CondVar& nvm_signal)
        : env_(options->env),
          mutex_(mu),
          shutting_down_(shutdown),
          snapshots_(snapshots),
          bg_error_(bg_error),
          dbname_(dbname),
          options_(options),
          //table_cache_(table_cache),
          icmp_(*cmp),
          next_file_number_(2),
          //manifest_file_number_(0),  // Filled by Recover()
          last_sequence_(0),
          writes_(0),
          build_tables_(0),
          drops_(0),
          peak_height_(0),
          merges_(0),
          merge_latency_(0),
          log_number_(0),
          prev_log_number_(0),
          nvm_compaction_scheduled_(nvm_compaction_scheduled),
          nvm_signal(nvm_signal),
          index_cmp_(*cmp),
          index_(index_cmp_)
          //descriptor_file_(nullptr),
          //descriptor_log_(nullptr),
          //dummy_versions_(this),
          //current_(nullptr) {
    //AppendVersion(new Version(this));
{   }


// Compare internal key or user key.
inline int VersionSet::KeyComparator::operator()(const char *aptr, const char *bptr, bool ukey) const {
    Slice akey = GetLengthPrefixedSlice(aptr);
    Slice bkey = GetLengthPrefixedSlice(bptr);

    return (ukey) ?
            comparator.user_comparator()->Compare(ExtractUserKey(akey), ExtractUserKey(bkey)) :
            comparator.Compare(akey, bkey);
}

Status VersionSet::BuildTable(Iterator *iter, const int count) {

    Status s = Status::OK();

    interval* new_interval = BuildInterval(iter, count, &s);

    if (new_interval == nullptr) { return s; }

    // Get table indexed in nvm.
    index_.WriteLock();
    index_.insert(new_interval);   // log^2(n)
    index_.WriteUnlock();

    Iterator* table_iter = new_interval->get_table()->NewIterator();
    table_iter->SeekToFirst();  // O(1)
    const char* lRaw = table_iter->Raw();
    table_iter->SeekToLast();   // O(1)
    const char* rRaw = table_iter->Raw();
    delete table_iter;

    // convert imm to nvm imm might trigger a compaction
    // by stabbing the intervals overlap its end points.
    //ShowIndex();
    index_.ReadLock();
    int lCount = index_.stab(lRaw);
    int rCount = index_.stab(rRaw);
    index_.ReadUnlock();
    //std::cout<<"lCount: "<<lCount<<" rCount: "<<rCount<<std::endl;
    if (lCount >= rCount) {
        MaybeScheduleCompaction(lRaw, lCount);
    } else {
        MaybeScheduleCompaction(rRaw, rCount);
    }
    //ShowIndex();

    return s;
}

std::uint64_t NowNanos() {
    return static_cast<uint64_t>(::std::chrono::duration_cast<::std::chrono::nanoseconds>(
            ::std::chrono::steady_clock::now().time_since_epoch())
            .count());
}

// Called by BuildTable(timestamp == 0) or DoCompactionWork(timestamp != 0).
// Interval's timestamp starts from 1.
// iter is constructed from imm_ or some nvm_imm_.
// If modify versions_, use mutex_ in to protect versions_.
// REQUIRES: iter->Valid().
VersionSet::interval* VersionSet::BuildInterval(Iterator *iter, int count, Status *s, uint64_t timestamp) {

    *s = Status::OK();
    assert(iter->Valid());

    assert(count >= 0);
    uint64_t start = 0;
    if (timestamp != 0) {
        start = NowNanos();
    }
    NvmMemTable *table = new NvmMemTable(icmp_, count, options_->use_cuckoo);
    table->Transport(iter, timestamp != 0);
    if (timestamp != 0) {
        uint64_t period = NowNanos() - start;
        merges_ += table->GetCount();
        merge_latency_ += period;
    } else {
        writes_ += table->GetCount();
        build_tables_ ++;
    }

    // Check for input iterator errors
    if (!iter->status().ok()) {
        *s = iter->status();
    }
    // an empty table, just delete it.
    if (table->GetCount() == 0) {
        table->Destroy(false);
        return nullptr;
    }

    // Verify that the table is usable
    Iterator *table_iter = table->NewIterator();

#if defined(compact_debug)
    if (timestamp != 0) {
        new_table_count += table->GetCount();
    }
#endif

#if defined(mem_dump_debug)
    if (timestamp == 0) {
        iter->SeekToFirst();
        Slice start = iter->key();
        Status stest = Status::OK();
        iter->Seek(start);
        table_iter->SeekToFirst();
        while (table_iter->Valid()) {
            assert(table_iter->key().ToString() == iter->key().ToString() &&
                   table_iter->value().ToString() == iter->value().ToString());
            table_iter->Next();
            iter->Next();
        }
    }
#endif

    table_iter->SeekToFirst();  // O(1)
    const char* lRaw = table_iter->Raw();
    table_iter->SeekToLast();   // O(1)
    const char* rRaw = table_iter->Raw();
    delete table_iter;
    assert(index_cmp_(lRaw, rRaw) <= 0);

    return index_.generate(lRaw, rRaw, table, timestamp);
}


void VersionSet::Get(const LookupKey &key, std::string *value, Status *s) {
    Slice memkey = key.memtable_key();
    std::vector<interval*> intervals;
    const char* HotKey = nullptr;
    int overlaps = 0;

    index_.ReadLock();
    // we are interested in user key.
    index_.search(memkey.data(), intervals, overlaps);
    for (auto &interval : intervals) {
        //interval->print(std::cout);
        // interval will exist before we release it.
        interval->Ref();
    }
    index_.ReadUnlock();

    bool found = false;
    //std::cout<<"Want: ";
    //Decode(key.memtable_key().data(), std::cout);
    //std::cout<<std::endl;
    for (auto &interval : intervals) {
        if (!found) {
            found = interval->get_table()->Get(key, value, s, HotKey);
        }
        interval->Unref();
    }
    if (!found) {
        *s = Status::NotFound(Slice());
    }

    // memkey is stored in LookupKey, highly possible be freed under multi threads.
    if (HotKey != nullptr) {
        // If interval i1 ends with the same user key as interval i2 starts,
        // and we set max_overlap to 2, when we get this key, a compaction of i1 and i2
        // will be triggered which is unnecessary.
        // So do not directly use intervals.size().
        MaybeScheduleCompaction(HotKey, overlaps);
    }

}

void VersionSet::MaybeScheduleCompaction(const char* HotKey, const int overlaps) {
    assert(HotKey != nullptr);
    if (nvm_compaction_scheduled_ || overlaps < options_->max_overlap) return;

    MutexLock l(&mutex_);
    if (nvm_compaction_scheduled_) {
        // Already scheduled
    } else if (shutting_down_.Acquire_Load()) {
        // DB is being deleted, no more background compactions
    } else if (!bg_error_.ok()) {
        // Already got an error; no more changes
    } else if (overlaps < options_->max_overlap) {
        // No work to be done
    } else {
        //std::cout<<"compact start"<<std::endl;
        nvm_compaction_scheduled_ = true;
        env_->NvmSchedule(&VersionSet::BGWork, this, (void*)HotKey);
    }
}

void VersionSet::BGWork(void* vs, void* hk) {
    reinterpret_cast<VersionSet*>(vs)->BackgroundCall(static_cast<const char*>(hk));
}

void VersionSet::BackgroundCall(const char* HotKey) {
    MutexLock l(&mutex_);
    assert(nvm_compaction_scheduled_);
    if (shutting_down_.Acquire_Load()) {
        // No more background work when shutting down.
    } else if (!bg_error_.ok()) {
        // No more background work after a background error.
    } else {
        BackgroundCompaction(HotKey);
    }
    nvm_compaction_scheduled_ = false;
    nvm_signal.Signal();    // Only delete DB operation will wait at this signal
}

void VersionSet::BackgroundCompaction(const char* HotKey) {
    mutex_.AssertHeld();
    mutex_.Unlock();
    DoCompactionWork(HotKey);
    mutex_.Lock();
    // lock and modify, prevent divide zero error.
    peak_height_ = 0;
    merges_ = 0;
    merge_latency_ = 0;
}


// Only used in nvm data compaction, neither l or r is nullptr.
class CompactIterator : public Iterator {
public:

    typedef VersionSet::Index::Interval interval;

    explicit CompactIterator(const InternalKeyComparator& cmp,
            VersionSet::Index* index,
            const char* l,
            const char* r,
            const uint64_t t1,
            const uint64_t s,
            std::vector<interval*>& inters)
            : iter_icmp(cmp),
              helper_(index),
              left_border(l),
              right_border(r),
              right(nullptr),
              time_up(t1),
              smallest_snapshot(s),
              drops(0),
              old_intervals(inters),
              merge_iter(nullptr),
              has_current_user_key(false),
              last_sequence_for_key(kMaxSequenceNumber)
              {
        assert(l != nullptr && r != nullptr);
        // no single point interval as triggering overlap > 1.
        assert(iter_icmp.Compare(GetLengthPrefixedSlice(left_border),
                               GetLengthPrefixedSlice(right_border)) < 0);
#if defined(compact_debug)
        total_count = 0;
        merge_count = 0;
        new_table_count = 0;
        abandon_count = 0;
#endif

/*
        std::cout<<"left_border: ";
        Decode(left_border, std::cout);
        std::cout<<" right_border: ";
        Decode(right_border, std::cout);
        std::cout<<std::endl;
*/
    }

    ~CompactIterator() {
        // only need to clear iterator.
        delete merge_iter;
    }

    virtual bool Valid() const {
        return merge_iter->Valid();
    }

    virtual void Seek(const Slice& k) { }

    // Start compact(iterate) from left_border.
    virtual void SeekToFirst() {
        HelpSeek(left_border);
        assert(Valid());
        while (Valid() && SkipObsoleteKeys()) {
            HelpNext();
        }
    }

    virtual void SeekToLast() { }

    virtual void Next() {
        assert(Valid());

        // move forward and trigger skip procedure
        HelpNext();
        while (Valid() && SkipObsoleteKeys()) {
            HelpNext();
        }
    }

    virtual void Prev() { }

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
        //assert(iter_icmp.Compare(GetLengthPrefixedSlice(merge_iter->Raw()),
        //                         GetLengthPrefixedSlice(right_border)) <= 0);
        return merge_iter->Raw();
    }

    virtual Status status() const {
        return merge_iter->status();
    }

    virtual void Abandon() { }

    inline uint64_t DropCount() { return drops; }

private:

    bool SkipObsoleteKeys() {
        Slice key = merge_iter->key();
        // Handle key/value, add to state, etc.
        bool drop = false;
        if (!ParseInternalKey(key, &ikey)) {
            // Do not hide error keys
            current_user_key.clear();
            has_current_user_key = false;
            last_sequence_for_key = kMaxSequenceNumber;
        } else {
            if (!has_current_user_key ||
                iter_icmp.user_comparator()->Compare(ikey.user_key,
                                                     Slice(current_user_key)) != 0) {
                // First occurrence of this user key
                current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
                has_current_user_key = true;
                last_sequence_for_key = kMaxSequenceNumber;
                // the user key you first encounter is always kept due to largest sequence.
            }

            if (last_sequence_for_key <= smallest_snapshot) {
                // Hidden by an newer entry for same user key
                drop = true;    // (A)
            } /*else if (ikey.type == kTypeDeletion &&
                       ikey.sequence <= smallest_snapshot) {
                // For this user key:
                // (1) there is no data in higher levels
                // (2) data in lower levels will have larger sequence numbers
                // (3) data in layers that are being compacted here and have
                //     smaller sequence numbers will be dropped in the next
                //     few iterations of this loop (by rule (A) above).
                // Therefore this deletion marker is obsolete and can be dropped.
                drop = true;
            }*/

            last_sequence_for_key = ikey.sequence;
        }
        if (drop) {
            merge_iter->Abandon();
            drops++;
#if defined(compact_debug)
            abandon_count++;
#endif
        /*
            Decode(merge_iter->Raw(), std::cout);
            std::cout<<" Dropped";
            std::cout<<std::endl;
            */
        }
        return drop;
    }

    void HelpNext() {
        assert(Valid());

#if defined(compact_debug)
        merge_count++;
        const char* before = merge_iter->Raw();
#endif

        merge_iter->Next();

#if defined(compact_debug)
        if (merge_iter->Valid()) {
            assert(iter_icmp.Compare(GetLengthPrefixedSlice(before),
                    GetLengthPrefixedSlice(merge_iter->Raw())) < 0);
        }
#endif

        // reach the border and trigger a seek, right set to 0 terminates it.
        if (merge_iter->Valid() && merge_iter->Raw() == right) {
            HelpSeek(right);
        }
    }

    void HelpSeek(const char* k) {
        assert(k != nullptr);
        ClearState();

/*
        helper_.ShowIndex();
        std::cout<<"target: ";
        Decode(k, std::cout);
        std::cout<<std::endl;
        std::cout<<"right: ";
        if (right == nullptr) {
            std::cout<<"nullptr";
        } else {
            Decode(right, std::cout);
        }
        std::cout<<std::endl;
        if (merge_iter != nullptr && merge_iter->Valid()) {
            std::cout<<"current pos: ";
            Decode(merge_iter->Raw(), std::cout);
            std::cout<<std::endl;
        }
*/

        helper_.ReadLock();
        helper_.Seek(k, intervals, right, right_border, time_up);
        helper_.ReadUnlock();
        InitIterator();

        assert(merge_iter != nullptr);
        merge_iter->Seek(GetLengthPrefixedSlice(k));
/*
        helper_.ShowIndex();
        std::cout<<"changed right: ";
        if (right == nullptr) {
            std::cout<<"nullptr";
        } else {
            Decode(right, std::cout);
        }
        std::cout<<std::endl;
        if (merge_iter != nullptr && merge_iter->Valid()) {
            std::cout<<"changed pos: ";
            Decode(merge_iter->Raw(), std::cout);
            std::cout<<std::endl;
        }
*/


        //if (merge_iter->Valid()) {
        //    assert(merge_iter->Raw() == k);
        //}

    }

    void ClearState() {
        delete merge_iter;
        merge_iter = nullptr;
        iterators.clear();
        intervals.clear();
    }

    void InitIterator() {
        for (auto &interval : intervals) {
            if (interval->stamp() <= time_up) {
                // no need to ref intervals here as only this thread unref intervals with write lock protected.
                if (filter.find(interval) == filter.end()) {
                    assert(iter_icmp.Compare(GetLengthPrefixedSlice(left_border), GetLengthPrefixedSlice(interval->inf())) <= 0
                           && iter_icmp.Compare(GetLengthPrefixedSlice(interval->sup()), GetLengthPrefixedSlice(right_border)) <= 0);
                    filter.insert(interval);
                    old_intervals.push_back(interval);
                }
                //interval->print(std::cout);
                iterators.push_back(interval->get_table()->NewIterator());
            }
        }
        //std::cout<<std::endl;
        merge_iter = NewMergingIterator(&iter_icmp, &iterators[0], iterators.size());
    }


    const InternalKeyComparator iter_icmp;

    VersionSet::Index::IteratorHelper helper_;

    const char* const left_border;
    const char* const right_border;
    const char* right;

    const uint64_t time_up;
    const uint64_t smallest_snapshot;
    uint64_t drops;
    std::unordered_set<interval*> filter;
    std::vector<interval*>& old_intervals;
    std::vector<interval*> intervals;
    std::vector<Iterator*> iterators;
    Iterator* merge_iter;

    ParsedInternalKey ikey;
    std::string current_user_key;
    bool has_current_user_key;
    SequenceNumber last_sequence_for_key;

    // No copying allowed
    CompactIterator(const CompactIterator&);
    void operator=(const CompactIterator&);
};


// Only one nvm data compaction thread
void VersionSet::DoCompactionWork(const char *HotKey) {
    assert(HotKey != nullptr);
    //const uint64_t avg_count = last_sequence_/index_.size();
    assert(writes_ > 0 && build_tables_ > 0);
    const uint64_t avg_count = writes_/build_tables_;
    assert(avg_count > 0);
    std::vector<interval*> old_intervals;
    std::vector<interval*> new_intervals;
    const char* left = HotKey;
    const char* right = HotKey;
    uint64_t time_up = 0;
    Status s = Status::OK();
    bool break_it = false;

    index_.ReadLock();
    index_.search(HotKey, old_intervals, true);
    index_.ReadUnlock();
    // Although hardly, it might happen.
    if (old_intervals.size() <= 1) return;
    assert(old_intervals.size() > 1);
    peak_height_ = old_intervals.size();
    time_up = old_intervals[0]->stamp();
    assert(time_up > old_intervals[1]->stamp());
    for (auto &interval : old_intervals) {
        if (index_cmp_(interval->inf(), left) < 0) {
            left = interval->inf();
        }
        if (index_cmp_(interval->sup(), right) > 0) {
            right = interval->sup();
        }
    }

    // expand interval set to leftmost overlapped interval
    while (!break_it) {
        break_it = true;
        old_intervals.clear();
        index_.ReadLock();
        index_.search(left, old_intervals);
        index_.ReadUnlock();
        //Decode(left, std::cout);
        //std::cout<<std::endl;
        for (auto &interval : old_intervals) {
            if (interval->stamp() <= time_up && index_cmp_(interval->inf(), left) < 0) {
                left = interval->inf();
                break_it = false;
            }
        }
    }
    assert(old_intervals[0]->inf() == left);

    break_it = false;
    // expand interval set to rightmost overlapped interval
    while (!break_it) {
        break_it = true;
        old_intervals.clear();
        index_.ReadLock();
        index_.search(right, old_intervals);
        index_.ReadUnlock();
        //Decode(right, std::cout);
        //std::cout<<std::endl;
        for (auto &interval: old_intervals) {
            if (interval->stamp() <= time_up && index_cmp_(interval->sup(), right) > 0) {
                right = interval->sup();
                break_it = false;
            }
        }
    }

    assert(old_intervals[0]->sup() == right);
    assert(index_cmp_(left, right) < 0);

    old_intervals.clear();

    // internal key ranged in [left, right]
    // with timestamp <= merge_line - 1 will be compacted,
    // produced intervals with merge_line and no overlap.
    uint64_t smallest_snapshot = last_sequence_;
    if (!snapshots_.empty()) {
        smallest_snapshot = snapshots_.oldest()->sequence_number();
    }
    Iterator* iter = new CompactIterator(icmp_, &index_, left, right, time_up, smallest_snapshot, old_intervals);
    //ShowIndex();
    iter->SeekToFirst();
    assert(iter->Valid());
    while (iter->Valid()) {
        new_intervals.push_back(BuildInterval(iter, avg_count, &s, time_up));
        assert(s.ok());
    }
    drops_ += dynamic_cast<CompactIterator*>(iter)->DropCount();
    delete iter;

    // Data consistency accross failure.
    //ShowIndex();
    //std::cout<<"Insert new intervals: ";
    for (auto &interval : new_intervals) {
        //interval->print(std::cout);
        index_.WriteLock();
        index_.insert(interval);
        index_.WriteUnlock();
    }
    //ShowIndex();
    //std::cout<<"Removed old intervals: ";
    for (auto &interval: old_intervals) {
        //interval->print(std::cout);
        index_.WriteLock();
        // From the moment we remove it from nvm index, no more thread will access it.
        // Ref() is protected by read lock.
        index_.remove(interval);
        index_.WriteUnlock();
        interval->Unref();  // delete interval.
#if defined(compact_debug)
        total_count += interval->get_table()->GetCount();
#endif
    }
    //ShowIndex();
#if defined(compact_debug)
    assert(merge_count == total_count && merge_count == new_table_count + abandon_count);
#endif
    /*std::cout << "total_count: " << total_count << "\tmerge_count: "
    << merge_count << "\tnew_table_count: " << new_table_count <<
    "\tabandon_count: " << abandon_count << std::endl;*/
    //std::cout<<std::endl;
}



class NvmIterator: public Iterator {
public:
    explicit NvmIterator(const InternalKeyComparator& cmp,
                         VersionSet::Index* const index,
                         VersionSet* const vs)
                        : iter_icmp(cmp),
                          helper_(index),
                          left(nullptr),
                          right(nullptr),
                          merge_iter(nullptr),
                          versions_(vs),
                          overlaps(0) {
    }

    ~NvmIterator() {
        ReleaseAndClear();
    }

    virtual bool Valid() const {
        // never called Seek*.
        if (merge_iter == nullptr) {
            return false;
        }
        // both left and right set nullptr only in two situations:
        // (1) ReleaseAndClear()
        // (2) there are some intervals but find_intervals set left to 0 and right to 0
        return merge_iter->Valid();
    }

    // k is internal key
    virtual void Seek(const Slice& k) {
        if (merge_iter != nullptr &&
        (left == nullptr || iter_icmp.Compare(k, GetLengthPrefixedSlice(left)) >= 0) &&
        (right == nullptr || iter_icmp.Compare(k, GetLengthPrefixedSlice(right)) <= 0)) {
            merge_iter->Seek(k);
        } else {
            HelpSeek(EncodeKey(&tmp_, k), IterSeek);
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

        //const char* before = merge_iter->Raw();

        merge_iter->Next();

        /*if (merge_iter->Valid()) {
            assert(iter_icmp.Compare(GetLengthPrefixedSlice(before),
                                     GetLengthPrefixedSlice(merge_iter->Raw())) < 0);
        }*/

        // reach the border and trigger a seek
        if (merge_iter->Valid() && merge_iter->Raw() == right) {
            HelpSeek(right, IterNext);
        }
    }

    virtual void Prev() {
        assert(Valid());
        merge_iter->Prev();

        // reach the border and trigger a seek
        if (merge_iter->Valid() && merge_iter->Raw() == left) {
            HelpSeek(left, IterPrev);
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

    virtual void Abandon() {}

private:

    // target is internal key
    void HelpSeek(const char* k, const int iter_move) {
        assert(k != nullptr);
        ReleaseAndClear();
/*
        std::cout<<"target: ";
        Decode(k, std::cout);
        std::cout<<std::endl;
        std::cout<<"left: ";
        if (left == nullptr) {
            std::cout<<"nullptr";
        } else {
            Decode(left, std::cout);
        }
        std::cout<<" ";
        std::cout<<"right: ";
        if (right == nullptr) {
            std::cout<<"nullptr";
        } else {
            Decode(right, std::cout);
        }
        std::cout<<std::endl;
        if (merge_iter != nullptr && merge_iter->Valid()) {
            std::cout<<"current pos: ";
            Decode(merge_iter->Raw(), std::cout);
            std::cout<<std::endl;
        }
*/
        helper_.ReadLock();
        helper_.Seek(k, intervals, left, right, overlaps, iter_move);
        for (auto &interval : intervals) {
            interval->Ref();
        }
        helper_.ReadUnlock();
        InitIterator();

        merge_iter->Seek(GetLengthPrefixedSlice(k));
        if (merge_iter->Valid()) {
            versions_->MaybeScheduleCompaction(merge_iter->Raw(), overlaps);
        }
    }

    void HelpSeekToFirst() {
        ReleaseAndClear();
        helper_.ReadLock();
        helper_.SeekToFirst(intervals, left, right);
        for (auto &interval : intervals) {
            interval->Ref();
        }
        helper_.ReadUnlock();
        InitIterator();
        merge_iter->SeekToFirst();
        // no reason to schedule compaction here.
    }

    void HelpSeekToLast() {
        ReleaseAndClear();
        helper_.ReadLock();
        helper_.SeekToLast(intervals, left, right);
        for (auto &interval : intervals) {
            interval->Ref();
        }
        helper_.ReadUnlock();
        InitIterator();
        merge_iter->SeekToLast();
        //no reason to schedule compaction here.
    }

    void ReleaseAndClear() {
        delete merge_iter;
        merge_iter = nullptr;
        left = nullptr;
        right = nullptr;
        iterators.clear();
        // release the intervals in last search
        for (auto &interval : intervals) {
            interval->Unref();
        }
        intervals.clear();
    }

    void InitIterator() {
        for (auto &interval : intervals) {
            //interval->print(std::cout);
            iterators.push_back(interval->get_table()->NewIterator());
        }
        //std::cout<<std::endl;

        // no interval in index will make an EmptyIterator, Valid() returns false forever.
        merge_iter = NewMergingIterator(&iter_icmp, &iterators[0], iterators.size());
    }

    typedef VersionSet::Index::Interval interval;

    const InternalKeyComparator iter_icmp;

    VersionSet::Index::IteratorHelper helper_;

    // updated by nvmSkipList's IterateHelper
    const char* left;   // nullptr indicates head_
    const char* right;  // nullptr indicates tail_
    std::vector<interval*> intervals;
    std::vector<Iterator*> iterators;
    Iterator* merge_iter;

    VersionSet* const versions_;

    int overlaps;   // once fetch intervals, check if too much overlaps

    std::string tmp_;       // For passing to EncodeKey


    // No copying allowed
    NvmIterator(const NvmIterator&);
    void operator=(const NvmIterator&);
};


Iterator* VersionSet::NewIterator() {
    return new NvmIterator(icmp_, &index_, this);
}


}  // namespace softdb

