//
// Created by lingo on 19-2-17.
//

#ifndef SOFTDB_NVM_INDEX_H
#define SOFTDB_NVM_INDEX_H

#include "dbformat.h"

namespace softdb {

// Pass this as parameter neither insert interval or delete interval,
// when insert interval, you are inside index
// and keep or delete the data newed outside,
// when delete interval, you are outside index
// and keep or delete the data obsolete inside.
struct TableMetaData {
    int refs;
    int allowed_seeks;          // Seeks allowed until compaction
    int count;                  // Number of keys to insert
    uint64_t number;
    uint64_t file_size;         // File size in bytes
    Slice smallest;       // Smallest internal key served by table
    bool delete_smallest;        // If true, delete smallest.data()
    Slice largest;        // Largest internal key served by table
    bool delete_largest;        //If true, delete largest.data()

    TableMetaData() : refs(0), allowed_seeks(1 << 30), file_size(0) { }
};



}

#endif //SOFTDB_NVM_INDEX_H
