//
// Created by lingo on 19-1-17.
//

#include <iostream>
#include "version_set.h"
#include "iterator.h"
#include "nvm_memtable.h"


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
          timestamp_(0),
          log_number_(0),
          prev_log_number_(0)
          //descriptor_file_(nullptr),
          //descriptor_log_(nullptr),
          //dummy_versions_(this),
          //current_(nullptr) {
    //AppendVersion(new Version(this));
{   }

// Called by WriteLevel0Table or DoCompactionWork.
// iter is constructed from imm_ or two nvm_imm_.
Status VersionSet::BuildTable(Iterator *iter, FileMetaData *meta) {
    Status s = Status::OK();
    meta->file_size = 0;
    iter->SeekToFirst();
    if (iter->Valid()) {
        NvmMemTable *table = new NvmMemTable(icmp_, meta->count, options_->use_cuckoo);
        table->Ref();
        table->Transport(iter);

        // Verify that the table is usable
        //table->Ref();
        Iterator *it = table->NewIterator();
        it->SeekToFirst();  // O(1)
        meta->smallest.DecodeFrom(it->key());
        it->SeekToLast();   // O(1)
        meta->largest.DecodeFrom(it->key());
        s = it->status();


        iter->SeekToFirst();
        it->SeekToFirst();
        while(it->Valid()) {
            assert(it->key().ToString() == iter->key().ToString() &&
                   it->value().ToString() == iter->value().ToString());
            it->Next();
            iter->Next();
        }


        Slice sl;
        iter->SeekToFirst();
        for (; iter->Valid(); iter->Next()) {
            it->Seek(iter->key());
            LookupKey lkey(ExtractUserKey(iter->key()), kMaxSequenceNumber);
            std::string value;
            table->Get(lkey, &value, &s);
            assert(it->value().ToString() == value);
            if (value != "") {
                //std::cout << "Get: "<<value <<std::endl;
            }
            //assert(value == std::to_string(ll));
        }



        delete it;
        table->Unref();

        //TODO: hook it to ISL to get indexed.
        //TimeSeq

    }


    // Check for input iterator errors
    if (!iter->status().ok()) {
        s = iter->status();
    }

    return s;
}

}  // namespace softdb

