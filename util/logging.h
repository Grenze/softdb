//
// Created by lingo on 19-1-16.
//

#ifndef SOFTDB_LOGGING_H
#define SOFTDB_LOGGING_H


#include <stdio.h>
#include <stdint.h>
#include <string>
#include "port/port.h"

namespace softdb {

    class Slice;
    class WritableFile;

// Append a human-readable printout of "num" to *str
    void AppendNumberTo(std::string* str, uint64_t num);

// Append a human-readable printout of "value" to *str.
// Escapes any non-printable characters found in "value".
    void AppendEscapedStringTo(std::string* str, const Slice& value);

// Return a human-readable printout of "num"
    std::string NumberToString(uint64_t num);

// Return a human-readable version of "value".
// Escapes any non-printable characters found in "value".
    std::string EscapeString(const Slice& value);

// Parse a human-readable number from "*in" into *value.  On success,
// advances "*in" past the consumed number and sets "*val" to the
// numeric value.  Otherwise, returns false and leaves *in in an
// unspecified state.
    bool ConsumeDecimalNumber(Slice* in, uint64_t* val);

}  // namespace softdb


#endif //SOFTDB_LOGGING_H
