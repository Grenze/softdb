//
// Created by lingo on 19-1-16.
//

#ifndef SOFTDB_LOG_WRITER_H
#define SOFTDB_LOG_WRITER_H


#include <stdint.h>
#include "log_format.h"
#include "softdb/slice.h"
#include "softdb/status.h"

namespace softdb {

    class WritableFile;

    namespace log {

        class Writer {
        public:
            // Create a writer that will append data to "*dest".
            // "*dest" must be initially empty.
            // "*dest" must remain live while this Writer is in use.
            explicit Writer(WritableFile* dest);

            // Create a writer that will append data to "*dest".
            // "*dest" must have initial length "dest_length".
            // "*dest" must remain live while this Writer is in use.
            Writer(WritableFile* dest, uint64_t dest_length);

            ~Writer();

            Status AddRecord(const Slice& slice);

        private:
            WritableFile* dest_;
            int block_offset_;       // Current offset in block

            // crc32c values for all supported record types.  These are
            // pre-computed to reduce the overhead of computing the crc of the
            // record type stored in the header.
            uint32_t type_crc_[kMaxRecordType + 1];

            Status EmitPhysicalRecord(RecordType type, const char* ptr, size_t length);

            // No copying allowed
            Writer(const Writer&);
            void operator=(const Writer&);
        };

    }  // namespace log
}  // namespace softdb


#endif //SOFTDB_LOG_WRITER_H
