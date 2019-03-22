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
Status VersionSet::BuildTable(Iterator *iter, TableMetaData *meta, port::Mutex* mu) {


    Status s = Status::OK();
    meta->file_size = 0;

    assert(iter->Valid());

    Slice start = iter->key();


    NvmMemTable *table = new NvmMemTable(icmp_, meta->count, options_->use_cuckoo);
    table->Transport(iter);
    // no data.
    if (table->Empty()) {
        delete table;
        return s;
    }

    // Verify that the table is usable
    Iterator *table_iter = table->NewIterator();

    table_iter->SeekToFirst();  // O(1)
    Slice lRawKey = table_iter->RawKey();
    // tips: Where to delete them?
    char* buf1 = new char[lRawKey.size()];
    memcpy(buf1, lRawKey.data(), lRawKey.size());
    meta->smallest = Slice(buf1, lRawKey.size());

    table_iter->SeekToLast();   // O(1)
    Slice rRawKey = table_iter->RawKey();
    char* buf2 = new char[rRawKey.size()];
    memcpy(buf2, rRawKey.data(), rRawKey.size());
    meta->largest = Slice(buf2, rRawKey.size());



    //TODO: hook table to ISL to get indexed.
    mu->Lock();
    index_.insert(buf1, buf2, table);   // awesome fast
    mu->Unlock();


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


    //delete table;



    // Check for input iterator errors
    if (!iter->status().ok()) {
        s = iter->status();
    }

    return s;
}



void VersionSet::Get(const LookupKey &key, std::string *value, Status *s) {
    Slice memkey = key.memtable_key();
    std::vector<interval*> intervals;
    index_.search(memkey.data(), intervals);
    bool notfound = true;
    //std::cout<<intervals.size()<<std::endl;
    for (auto &interval : intervals) {
        //std::cout<<interval->stamp()<<" ";
        if (notfound && interval->get_table()->Get(key, value, s)) {
            notfound = false;
        }
        interval->Unref();
    }
    //std::cout<<std::endl;
    if (notfound) {
        *s = Status::NotFound(Slice());
    }

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
                          right(nullptr) {

    }

    ~NvmIterator() {
        //std::cout<<"count: "<<count<<std::endl;
        // release the intervals in last search
        helper_.Release();
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
        //count++;
        merge_iter->Next();

        // we are before the last node
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
        //count++;
        merge_iter->Prev();

        // we are after the first node
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

        helper_.Seek(EncodeKey(&tmp_, k), iterators, left, right);
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

    void HelpSeekToFirst() {
        ClearIterator();

        helper_.SeekToFirst(iterators, left, right);
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
        helper_.SeekToLast(iterators, left, right);
        InitIterator();
    }

    void ClearIterator() {
        iterators.clear();
    }

    void InitIterator() {
        //std::cout<<"intervals: "<<iterators.size()<<std::endl;
        merge_iter = (iterators.empty()) ?
                nullptr : NewMergingIterator(&iter_icmp, &iterators[0], iterators.size());
    }

    const InternalKeyComparator iter_icmp;



    // updated by nvmSkipList's IterateHelper
    const char* left;   // nullptr indicates head_
    const char* right;  // nullptr indicates tail_
    //std::vector<interval*> intervals;
    std::vector<Iterator*> iterators;
    VersionSet::Index::IteratorHelper helper_;
    Iterator* merge_iter;

    std::string tmp_;       // For passing to EncodeKey

    //long count;

    // No copying allowed
    NvmIterator(const NvmIterator&);
    void operator=(const NvmIterator&);
};


Iterator* VersionSet::NewIterator() {
    return new NvmIterator(icmp_, &index_);
}





}  // namespace softdb

