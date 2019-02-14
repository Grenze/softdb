//
// Created by lingo on 19-2-13.
//

#ifndef SOFTDB_NVM_BUILDER_H
#define SOFTDB_NVM_BUILDER_H

#include "status.h"

namespace softdb {

struct Options;
struct FileMetaData;

class Iterator;

// Build a Nvm Table from the contents of *iter. The generated table
// will be marked according to meta->timeSeq. On success, the rest of
// *meta will be filled with metadata about the generated table.
// If no data is present in *iter, meta->file_size will be set to
// zero, and no Table will be produced.
Status BuildTable(const Options& options,
                  Iterator* iter,
                  FileMetaData* meta);
}   // namespace softdb

#endif //SOFTDB_NVM_BUILDER_H
