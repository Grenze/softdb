//
// Created by lingo on 19-3-22.
//

#ifndef SOFTDB_TESTUTIL_H
#define SOFTDB_TESTUTIL_H


#include "env.h"
#include "slice.h"
#include "random.h"

namespace softdb {
    namespace test {

// Store in *dst a random string of length "len" and return a Slice that
// references the generated data.
        Slice RandomString(Random* rnd, int len, std::string* dst);

// Return a random key with the specified length that may contain interesting
// characters (e.g. \x00, \xff, etc.).
        std::string RandomKey(Random* rnd, int len);

// Store in *dst a string of length "len" that will compress to
// "N*compressed_fraction" bytes and return a Slice that references
// the generated data.
        Slice CompressibleString(Random* rnd, double compressed_fraction,
                                 size_t len, std::string* dst);

// A wrapper that allows injection of errors.
        class ErrorEnv : public EnvWrapper {
        public:
            bool writable_file_error_;
            int num_writable_file_errors_;

            ErrorEnv() : EnvWrapper(Env::Default()),
                         writable_file_error_(false),
                         num_writable_file_errors_(0) { }

            virtual Status NewWritableFile(const std::string& fname,
                                           WritableFile** result) {
                if (writable_file_error_) {
                    ++num_writable_file_errors_;
                    *result = nullptr;
                    return Status::IOError(fname, "fake error");
                }
                return target()->NewWritableFile(fname, result);
            }

            virtual Status NewAppendableFile(const std::string& fname,
                                             WritableFile** result) {
                if (writable_file_error_) {
                    ++num_writable_file_errors_;
                    *result = nullptr;
                    return Status::IOError(fname, "fake error");
                }
                return target()->NewAppendableFile(fname, result);
            }
        };

    }  // namespace test
}  // namespace softdb

#endif //SOFTDB_TESTUTIL_H
