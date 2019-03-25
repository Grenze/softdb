//
// Created by lingo on 19-1-17.
//

#ifndef SOFTDB_VERSION_SET_H
#define SOFTDB_VERSION_SET_H


#include "port/port.h"
#include "dbformat.h"
//#include "nvm_interval.h"
//#include "nvm_ISL.h"
#include "include/softdb/iterator.h"
#include "nvm_index.h"

namespace softdb {


// Pass smallest.data() and largest.data() with two bool
// as parameter neither insert interval or delete interval,
// when insert interval, you are inside index
// and keep or delete the data newed outside,
// when delete interval, you are outside index
// and keep or delete the data obsolete inside.
struct TableMetaData {
    int refs;
    int allowed_seeks;          // Seeks allowed until compaction
    int count;                  // Number of keys to insert
    //uint64_t number;
    uint64_t timestamp;
    uint64_t file_size;         // File size in bytes
    Slice smallest;       // Smallest internal key served by table
    bool remove_smallest;        // If true, delete smallest.data()
    Slice largest;        // Largest internal key served by table
    bool remove_largest;        //If true, delete largest.data()

    TableMetaData()
                : refs(0),
                  allowed_seeks(1 << 30),
                  file_size(0),
                  remove_smallest(false),
                  remove_largest(false) { }
};



class VersionSet {
public:
    VersionSet(const std::string& dbname,
            const Options* options,
            /*TableCache* table_cache,*/
            const InternalKeyComparator* cmp);

    VersionSet();
    ~VersionSet();

    // Allocate and return a new file number
    uint64_t NewFileNumber() { return next_file_number_++; }

    // Arrange to reuse "file_number" unless a newer file number has
    // already been allocated.
    // REQUIRES: "file_number" was returned by a call to NewFileNumber().
    void ReuseFileNumber(uint64_t file_number) {
        if (next_file_number_ == file_number + 1) {
            next_file_number_ = file_number;
        }
    }

    // Return the last sequence number.
    uint64_t LastSequence() const { return last_sequence_; }

    // Set the last sequence number to s.
    void SetLastSequence(uint64_t s) {
        assert(s >= last_sequence_);
        last_sequence_ = s;
    }

    // Mark the specified file number as used.
    void MarkFileNumberUsed(uint64_t number);

    // Return the current log file number.
    uint64_t LogNumber() const { return log_number_; }

    // Return the log file number for the log file that is currently
    // being compacted, or zero if there is no such log file.
    uint64_t PrevLogNumber() const { return prev_log_number_; }

    void SetLogNumber(uint64_t num) { log_number_= num; }

    void SetPreLogNumber(uint64_t num) { prev_log_number_ = num; }

    // Return the last timestamp number.
    uint64_t NextTimestamp() const { return index_.NextTimestamp(); }


    // Build a Nvm Table from the contents of *iter. The generated table
    // will be marked according to timestamp_. On success, the rest of
    // *meta will be filled with metadata about the generated table.
    // If no data is present in *iter, meta->file_size will be set to
    // zero, and no Table will be produced.
    Status BuildTable(Iterator* iter, TableMetaData* meta);

    void Get(const LookupKey &key, std::string *value, Status *s);


    // Return an iterator that yields the contents of nvm immutable memtables(nvm_imm_),
    // we use intervals to take charge of nvm_imm_s.
    //
    // Similar to leveldb, the interval iterator
    // is currently referencing will not be deleted through compaction.
    //
    // The data iterator is travelling is protected by a snapshot created alongside iterator.
    // In other words, the data seen by iterator is a snapshot.
    //
    Iterator* NewIterator();

    void ShowIndex() const {
        index_.print(std::cout);
        index_.printOrdered(std::cout);
    };

private:

    const std::string dbname_;
    const Options* const options_;
    const InternalKeyComparator icmp_;
    uint64_t next_file_number_;
    uint64_t last_sequence_;
    uint64_t log_number_;
    uint64_t prev_log_number_;  // 0 or backing store for memtable being compacted

    struct KeyComparator {
        const InternalKeyComparator comparator;
        explicit KeyComparator(const InternalKeyComparator& c) : comparator(c) { }
        int operator()(const char* a, const char* b, bool ukey = false) const;
    };

    friend class NvmIterator;

    typedef IntervalSkipList<const char*, KeyComparator> Index;
    typedef Index::Interval interval;
    KeyComparator index_cmp_;
    Index index_;   // protected by mutex_

    // No copying allowed
    VersionSet(const VersionSet&);
    void operator=(const VersionSet&);


};



}  // namespace softdb

#endif //SOFTDB_VERSION_SET_H