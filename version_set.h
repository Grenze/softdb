//
// Created by lingo on 19-1-17.
//

#ifndef SOFTDB_VERSION_SET_H
#define SOFTDB_VERSION_SET_H


#include "port.h"

namespace softdb {

class VersionSet {
public:
    VersionSet();
    ~VersionSet();

    // Allocate and return a new file number
    uint64_t NewFileNumber() { return next_file_number_++; }

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

private:

    uint64_t next_file_number_;
    uint64_t last_sequence_;
    uint64_t log_number_;
    uint64_t prev_log_number_;  // 0 or backing store for memtable being compacted

    // No copying allowed
    VersionSet(const VersionSet&);
    void operator=(const VersionSet&);
};

}  // namespace softdb

#endif //SOFTDB_VERSION_SET_H
