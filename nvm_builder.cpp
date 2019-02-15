//
// Created by lingo on 19-2-13.
//

#include <iostream>
#include "nvm_builder.h"
#include "version_set.h"
#include "iterator.h"
#include "nvm_memtable.h"


namespace softdb {


// Called by WriteLevel0Table or DoCompactionWork.
Status BuildTable(const Options& options,
                  const InternalKeyComparator& comparator,
                  Iterator* iter,
                  FileMetaData* meta) {
    Status s = Status::OK();
    meta->file_size = 0;
    iter->SeekToFirst();
    if (iter->Valid()) {
        NvmMemTable *table = new NvmMemTable(comparator, meta->count, options.use_cuckoo);
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
        //table->Unref();

        //TODO: hook it to ISL to get indexed.
        //TimeSeq

    }


    // Check for input iterator errors
    if (!iter->status().ok()) {
        s = iter->status();
    }

    return s;

}


}