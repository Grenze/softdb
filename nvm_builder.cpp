//
// Created by lingo on 19-2-13.
//

#include "nvm_builder.h"
#include "version_set.h"
#include "iterator.h"
#include "nvm_memtable.h"


namespace softdb {

Status BuildTable(const Options& options,
                  Iterator* iter,
                  FileMetaData* meta) {
    Status s;
    meta->file_size = 0;
    iter->SeekToFirst();

    if (iter->Valid()) {

    }


}


}