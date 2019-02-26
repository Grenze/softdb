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

// Compare user key.
int VersionSet::KeyComparator::operator()(const char *aptr, const char *bptr) const {
    Slice akey = GetLengthPrefixedSlice(aptr);
    Slice bkey = GetLengthPrefixedSlice(bptr);
    return comparator.user_comparator()->Compare(ExtractUserKey(akey), ExtractUserKey(bkey));
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
    while(table_iter->Valid()) {
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
    //std::cout<<intervals.size()<<std::endl; // 2 3 5 6 1-1000000 2<<20 2<<10
    for (std::vector<interval*>::iterator
         it = intervals.begin(); it != intervals.end(); it++) {
        //std::cout<<(*it)->stamp()<<" ";
        if ((*it)->get_table()->Get(key, value, s)) {
            //std::cout<<std::endl;
            return;
        }
    }
    *s = Status::NotFound(Slice());

}

class NvmIterator: public Iterator {
public:
    explicit NvmIterator() {

    }
};







Iterator* VersionSet::NewIterator() {

}



}  // namespace softdb

