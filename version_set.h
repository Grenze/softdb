//
// Created by lingo on 19-1-17.
//

#ifndef SOFTDB_VERSION_SET_H
#define SOFTDB_VERSION_SET_H


#include "port.h"
#include "dbformat.h"
#include "nvm_slice.h"
#include "nvm_interval.h"
#include "nvm_ISL.h"
#include "iterator.h"

namespace softdb {

struct FileMetaData {
    int refs;
    int allowed_seeks;          // Seeks allowed until compaction
    int count;                  // Number of key to insert
    uint64_t number;
    uint64_t file_size;         // File size in bytes
    InternalKey smallest;       // Smallest internal key served by table
    InternalKey largest;        // Largest internal key served by table

    FileMetaData() : refs(0), allowed_seeks(1 << 30), file_size(0) { }
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
    uint64_t LastTimestamp() const { return timestamp_; }

    // Set the last timestamp number to t.
    void SetLastTimestamp(uint64_t t) {
        assert(t >= timestamp_);
        timestamp_ = t;
    }

    // Allocate and return a new interval timestamp number.
    uint64_t NewTimestamp() { return timestamp_++; }

    // Build a Nvm Table from the contents of *iter. The generated table
    // will be marked according to timestamp_. On success, the rest of
    // *meta will be filled with metadata about the generated table.
    // If no data is present in *iter, meta->file_size will be set to
    // zero, and no Table will be produced.
    Status BuildTable(Iterator* iter, FileMetaData* meta);


private:

    const std::string dbname_;
    const Options* const options_;
    const InternalKeyComparator icmp_;
    uint64_t next_file_number_;
    uint64_t last_sequence_;
    uint64_t log_number_;
    uint64_t prev_log_number_;  // 0 or backing store for memtable being compacted
    uint64_t timestamp_;   // Mark intervals with timestamp

    // No copying allowed
    VersionSet(const VersionSet&);
    void operator=(const VersionSet&);
};

}  // namespace softdb

#endif //SOFTDB_VERSION_SET_H
