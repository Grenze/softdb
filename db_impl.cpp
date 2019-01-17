//
// Created by lingo on 19-1-16.
//

#include "db_impl.h"

#include <stdint.h>
#include <stdio.h>

#include <algorithm>
#include <set>
#include <string>
#include <vector>

//#include "builder.h"
//#include "db_iter.h"
#include "dbformat.h"
#include "filename.h"
#include "log_reader.h"
#include "log_writer.h"
#include "memtable.h"
//#include "table_cache.h"
//#include "version_set.h"
#include "write_batch_internal.h"
#include "db.h"
#include "env.h"
#include "status.h"
//#include "table.h"
//#include "table_builder.h"
#include "port.h"
//#include "block.h"
//#include "merger.h"
//#include "two_level_iterator.h"
#include "coding.h"
#include "logging.h"
#include "mutexlock.h"


namespace softdb {






// Fix user-supplied options to be reasonable
template <class T, class V>
static void ClipToRange(T* ptr, V minvalue, V maxvalue) {
    if (static_cast<V>(*ptr) > maxvalue) *ptr = maxvalue;
    if (static_cast<V>(*ptr) < minvalue) *ptr = minvalue;
}
Options SanitizeOptions(const std::string& dbname,
                        const InternalKeyComparator* icmp,
                        /*const InternalFilterPolicy* ipolicy,*/
                        const Options& src) {
    Options result = src;
    result.comparator = icmp;
    //result.filter_policy = (src.filter_policy != nullptr) ? ipolicy : nullptr;
    //ClipToRange(&result.max_open_files,    64 + kNumNonTableCacheFiles, 50000);
    ClipToRange(&result.write_buffer_size, 64<<10,                      1<<30);
    ClipToRange(&result.max_file_size,     1<<20,                       1<<30);
    //ClipToRange(&result.block_size,        1<<10,                       4<<20);
    if (result.info_log == nullptr) {
        // Open a log file in the same directory as the db
        src.env->CreateDir(dbname);  // In case it does not exist
        src.env->RenameFile(InfoLogFileName(dbname), OldInfoLogFileName(dbname));
        Status s = src.env->NewLogger(InfoLogFileName(dbname), &result.info_log);
        if (!s.ok()) {
            // No place suitable for logging
            result.info_log = nullptr;
        }
    }
    /*if (result.block_cache == nullptr) {
        result.block_cache = NewLRUCache(8 << 20);
    }*/
    return result;
}


DBImpl::DBImpl(const Options& raw_options, const std::string& dbname)
    : env_(raw_options.env),
    internal_comparator_(raw_options.comparator),
    //internal_filter_policy_(raw_options.filter_policy),
    options_(SanitizeOptions(dbname, &internal_comparator_,
                             /*&internal_filter_policy_, */ raw_options)),
    owns_info_log_(options_.info_log != raw_options.info_log),
    //owns_cache_(options_.block_cache != raw_options.block_cache),
    dbname_(dbname),
    //table_cache_(new TableCache(dbname_, options_, TableCacheSize(options_))),
    db_lock_(nullptr),
    shutting_down_(nullptr),
    background_work_finished_signal_(&mutex_),
    mem_(nullptr),
    imm_(nullptr),
    logfile_(nullptr),
    logfile_number_(0),
    log_(nullptr),
    seed_(0),
    tmp_batch_(new WriteBatch),
    background_compaction_scheduled_(false),
    manual_compaction_(nullptr)
    //versions_(new VersionSet(dbname_, &options_, table_cache_, &internal_comparator_))
    {
        has_imm_.Release_Store(nullptr);
    }


// Default implementations of convenience methods that subclasses of DB
// can call if they wish
Status DB::Put(const WriteOptions& opt, const Slice& key, const Slice& value) {
    WriteBatch batch;
    batch.Put(key, value);
    return Write(opt, &batch);
}

Status DB::Delete(const WriteOptions& opt, const Slice& key) {
    WriteBatch batch;
    batch.Delete(key);
    return Write(opt, &batch);
}

DB::~DB() { }

Status DB::Open(const Options& options, const std::string& dbname,
                DB** dbptr) {
    *dbptr = nullptr;

    DBImpl* impl = new DBImpl(options, dbname);
    impl->mutex_.Lock();
    //VersionEdit edit;
    // Recover handles create_if_missing, error_if_exists
    bool save_manifest = false;
    Status s = impl->Recover(&edit, &save_manifest);
    if (s.ok() && impl->mem_ == nullptr) {
        // Create new log and a corresponding memtable.
        uint64_t new_log_number = impl->versions_->NewFileNumber();
        WritableFile* lfile;
        s = options.env->NewWritableFile(LogFileName(dbname, new_log_number),
                                         &lfile);
        if (s.ok()) {
            edit.SetLogNumber(new_log_number);
            impl->logfile_ = lfile;
            impl->logfile_number_ = new_log_number;
            impl->log_ = new log::Writer(lfile);
            impl->mem_ = new MemTable(impl->internal_comparator_);
            impl->mem_->Ref();
        }
    }
    if (s.ok() && save_manifest) {
        edit.SetPrevLogNumber(0);  // No older logs needed after recovery.
        edit.SetLogNumber(impl->logfile_number_);
        s = impl->versions_->LogAndApply(&edit, &impl->mutex_);
    }
    if (s.ok()) {
        impl->DeleteObsoleteFiles();
        impl->MaybeScheduleCompaction();
    }
    impl->mutex_.Unlock();
    if (s.ok()) {
        assert(impl->mem_ != nullptr);
        *dbptr = impl;
    } else {
        delete impl;
    }
    return s;
}

}  // namespace softdb























