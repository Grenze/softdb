//
// Created by lingo on 19-2-26.
//

#ifndef SOFTDB_DB_ITER_H
#define SOFTDB_DB_ITER_H

#include <stdint.h>
#include "softdb/db.h"
#include "dbformat.h"

namespace softdb {

class DBImpl;

// Return a new iterator that converts internal keys (yielded by
// "*internal_iter") that were live at the specified "sequence" number
// into appropriate user keys.
Iterator* NewDBIterator(DBImpl* db,
                        const Comparator* user_key_comparator,
                        Iterator* internal_iter,
                        SequenceNumber sequence/*,
                        uint32_t seed*/);
}   // namespace softdb


#endif //SOFTDB_DB_ITER_H
