//
// Created by lingo on 19-2-13.
//

#include "nvm_builder.h"
#include "version_set.h"
#include "iterator.h"
#include "nvm_memtable.h"


namespace softdb {

Status BuildTable(const Options& options,
                  const InternalKeyComparator& comparator,
                  Iterator* iter,
                  FileMetaData* meta) {
    Status s = Status::OK();
    meta->file_size = 0;
    iter->SeekToFirst();
    NvmMemTable* table;
    if (iter->Valid()) {
        table = new NvmMemTable(comparator, meta->count, options.use_cuckoo);
        table->Ref();
        table->Transport(iter);
    }

    if (s.ok()) {
        // Verify that the table is usable
        Iterator* it = table->NewIterator();
        it->SeekToFirst();  // O(1)
        meta->smallest.DecodeFrom(it->key());
        it->SeekToLast();   // O(1)
        meta->largest.DecodeFrom(it->key());
        s = it->status();
        delete it;
    }

    //TODO: hook it to ISL to get indexed.
    //TimeSeq
    table->Unref();

    // Check for input iterator errors
    if (!iter->status().ok()) {
        s = iter->status();
    }
    return s;

}


}