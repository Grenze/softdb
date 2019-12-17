//
// Created by lingo on 19-1-15.
//

#include "softdb/options.h"

#include "softdb/comparator.h"
#include "softdb/env.h"

namespace softdb {

Options::Options()
        : comparator(BytewiseComparator()),
          create_if_missing(false),
          error_if_exists(false),
          paranoid_checks(false),
          env(Env::Default()),
          info_log(nullptr),
          write_buffer_size(4<<20),
          //max_open_files(1000),
          //block_cache(nullptr),
          //block_size(4096),
          //block_restart_interval(16),
          //max_file_size(2<<20),
          //compression(kSnappyCompression),
          reuse_logs(false),
          //filter_policy(nullptr)
          use_cuckoo(true),
          max_overlap(2),
          run_in_dram(true),
          peak(100)
          {
}

}  // namespace softdb
