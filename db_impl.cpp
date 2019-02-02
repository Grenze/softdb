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
#include "version_set.h"
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


// Information kept for every waiting writer
struct DBImpl::Writer {
    Status status;
    WriteBatch* batch;
    bool sync;
    bool done;
    port::CondVar cv;

    explicit Writer(port::Mutex* mu) : cv(mu) { }
};



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
    //manual_compaction_(nullptr)l,
    //versions_(new VersionSet(dbname_, &options_, table_cache_, &internal_comparator_))
    versions_(new VersionSet())
    {
        has_imm_.Release_Store(nullptr);
    }

DBImpl::~DBImpl() {
    // Wait for background work to finish
    mutex_.Lock();
    shutting_down_.Release_Store(this);  // Any non-null value is ok
    while (background_compaction_scheduled_) {
        background_work_finished_signal_.Wait();
    }
    mutex_.Unlock();

    if (db_lock_ != nullptr) {
        env_->UnlockFile(db_lock_);
    }

    delete versions_;
    if (mem_ != nullptr) mem_->Unref();
    if (imm_ != nullptr) imm_->Unref();
    delete tmp_batch_;
    delete log_;
    delete logfile_;
    //delete table_cache_;

    if (owns_info_log_) {
        delete options_.info_log;
    }
    /*if (owns_cache_) {
        delete options_.block_cache;
    }*/
}

void DBImpl::MaybeIgnoreError(Status* s) const {
    if (s->ok() || options_.paranoid_checks) {
        // No change needed
    } else {
        Log(options_.info_log, "Ignoring error %s", s->ToString().c_str());
        *s = Status::OK();
    }
}

/**
 *  if organize the nvm freeze skiplists in many small files, then use this method to delete the obsolete files
 * */
void DBImpl::DeleteObsoleteFiles() {
    mutex_.AssertHeld();

    if (!bg_error_.ok()) {
        // After a background error, we don't know whether a new version may
        // or may not have been committed, so we cannot safely garbage collect.
        return;
    }

    // Make a set of all of the live files
    //std::set<uint64_t> live = pending_outputs_;
    //versions_->AddLiveFiles(&live);

    std::vector<std::string> filenames;
    env_->GetChildren(dbname_, &filenames);  // Ignoring errors on purpose
    uint64_t number;
    FileType type;
    for (size_t i = 0; i < filenames.size(); i++) {
        if (ParseFileName(filenames[i], &number, &type)) {
            bool keep = true;
            switch (type) {
                case kLogFile:
                    keep = ((number >= versions_->LogNumber()) ||
                            (number == versions_->PrevLogNumber()));
                    break;
                /*
                case kDescriptorFile:
                    // Keep my manifest file, and any newer incarnations'
                    // (in case there is a race that allows other incarnations)
                    keep = (number >= versions_->ManifestFileNumber());
                    break;
                case kTableFile:many
                    keep = (live.find(number) != live.end());
                    break;
                case kTempFile:
                    // Any temp files that are currently being written to must
                    // be recorded in pending_outputs_, which is inserted into "live"
                    keep = (live.find(number) != live.end());
                    break;
                */
                case kCurrentFile:
                case kDBLockFile:
                case kInfoLogFile:
                    keep = true;
                    break;
            }

            if (!keep) {
                /*if (type == kTableFile) {
                    table_cache_->Evict(number);
                }*/
                Log(options_.info_log, "Delete type=%d #%lld\n",
                    static_cast<int>(type),
                    static_cast<unsigned long long>(number));
                env_->DeleteFile(dbname_ + "/" + filenames[i]);
            }
        }
    }
}

Status DBImpl::RecoverLogFile(uint64_t log_number, bool last_log,
                              /*bool* save_manifest, VersionEdit* edit,*/
                              SequenceNumber* max_sequence) {
    struct LogReporter : public log::Reader::Reporter {
        Env* env;
        Logger* info_log;
        const char* fname;
        Status* status;  // null if options_.paranoid_checks==false
        virtual void Corruption(size_t bytes, const Status& s) {
            Log(info_log, "%s%s: dropping %d bytes; %s",
                (this->status == nullptr ? "(ignoring error) " : ""),
                fname, static_cast<int>(bytes), s.ToString().c_str());
            if (this->status != nullptr && this->status->ok()) *this->status = s;
        }
    };

    mutex_.AssertHeld();

    // Open the log file
    std::string fname = LogFileName(dbname_, log_number);
    SequentialFile* file;
    Status status = env_->NewSequentialFile(fname, &file);
    if (!status.ok()) {
        MaybeIgnoreError(&status);
        return status;
    }

    // Create the log reader.
    LogReporter reporter;
    reporter.env = env_;
    reporter.info_log = options_.info_log;
    reporter.fname = fname.c_str();
    reporter.status = (options_.paranoid_checks ? &status : nullptr);
    // We intentionally make log::Reader do checksumming even if
    // paranoid_checks==false so that corruptions cause entire commits
    // to be skipped instead of propagating bad information (like overly
    // large sequence numbers).
    log::Reader reader(file, &reporter, true/*checksum*/,
                       0/*initial_offset*/);
    Log(options_.info_log, "Recovering log #%llu",
        (unsigned long long) log_number);

    // Read all the records and add to a memtable
    std::string scratch;
    Slice record;
    WriteBatch batch;
    int compactions = 0;
    MemTable* mem = nullptr;
    while (reader.ReadRecord(&record, &scratch) &&
           status.ok()) {
        if (record.size() < 12) {
            reporter.Corruption(
                    record.size(), Status::Corruption("log record too small"));
            continue;
        }
        WriteBatchInternal::SetContents(&batch, record);

        if (mem == nullptr) {
            mem = new MemTable(internal_comparator_);
            mem->Ref();
        }
        status = WriteBatchInternal::InsertInto(&batch, mem);
        MaybeIgnoreError(&status);
        if (!status.ok()) {
            break;
        }
        const SequenceNumber last_seq =
                WriteBatchInternal::Sequence(&batch) +
                WriteBatchInternal::Count(&batch) - 1;
        if (last_seq > *max_sequence) {
            *max_sequence = last_seq;
        }

        if (mem->ApproximateMemoryUsage() > options_.write_buffer_size) {
            compactions++;
            //*save_manifest = true;
            /**
             * WriteLevel0Table should be replaced with nvm write freeze skiplist method,
             * which runs smoothly and faster
             * */
            //status = WriteLevel0Table(mem, edit, nullptr);

            mem->Unref();
            mem = nullptr;
            if (!status.ok()) {
                // Reflect errors immediately so that conditions like full
                // file-systems cause the DB::Open() to fail.
                break;
            }
        }
    }

    delete file;

    // See if we should keep reusing the last log file.
    // EXPERIMENTAL: If true, append to existing MANIFEST and log files
    // when a database is opened.  This can significantly speed up open.
    //
    // Default: currently false, but may become true later.
    if (status.ok() && options_.reuse_logs && last_log && compactions == 0) {
        assert(logfile_ == nullptr);
        assert(log_ == nullptr);
        assert(mem_ == nullptr);
        uint64_t lfile_size;
        if (env_->GetFileSize(fname, &lfile_size).ok() &&
            env_->NewAppendableFile(fname, &logfile_).ok()) {
            Log(options_.info_log, "Reusing old log %s \n", fname.c_str());
            log_ = new log::Writer(logfile_, lfile_size);
            logfile_number_ = log_number;
            if (mem != nullptr) {
                mem_ = mem;
                mem = nullptr;
            } else {
                // mem can be nullptr if lognum exists but was empty.
                mem_ = new MemTable(internal_comparator_);
                mem_->Ref();
            }
        }
    }

    if (mem != nullptr) {
        // mem did not get reused; compact it.
        if (status.ok()) {
            //*save_manifest = true;
            /**
             * WriteLevel0Table should be replaced with nvm write freeze skiplist method,
             * which runs smoothly and faster
             * */
            //status = WriteLevel0Table(mem, edit, nullptr);
        }
        mem->Unref();
    }

    return status;
}

Status DBImpl::Recover(/*VersionEdit* edit, bool *save_manifest*/) {
    mutex_.AssertHeld();

    // Ignore error from CreateDir since the creation of the DB is
    // committed only when the descriptor is created, and this directory
    // may already exist from a previous failed creation attempt.
    env_->CreateDir(dbname_);
    assert(db_lock_ == nullptr);
    Status s = env_->LockFile(LockFileName(dbname_), &db_lock_);
    if (!s.ok()) {
        return s;
    }

    if (!env_->FileExists(CurrentFileName(dbname_))) {
        if (options_.create_if_missing) {
            s = NewDB();
            if (!s.ok()) {
                return s;
            }
        } else {
            return Status::InvalidArgument(
                    dbname_, "does not exist (create_if_missing is false)");
        }
    } else {
        if (options_.error_if_exists) {
            return Status::InvalidArgument(
                    dbname_, "exists (error_if_exists is true)");
        }
    }


    /**
     * versions_'s information should be stored in nvm and recovered from nvm when softdb is re-opened,
     * implement it here
     * */
    /*
    s = versions_->Recover(save_manifest);
    if (!s.ok()) {
        return s;
    }
     */
    SequenceNumber max_sequence(0);

    // Recover from all newer log files than the ones named in the
    // descriptor (new log files may have been added by the previous
    // incarnation without registering them in the descriptor).
    //
    // Note that PrevLogNumber() is no longer used, but we pay
    // attention to it in case we are recovering a database
    // produced by an older version of softdb.
    const uint64_t min_log = versions_->LogNumber();
    const uint64_t prev_log = versions_->PrevLogNumber();
    /**
     * min_log and prev_log should be recorded to recover the recently inserted data and delete
     * the obsolete log files
     * */
    std::vector<std::string> filenames;
    s = env_->GetChildren(dbname_, &filenames);
    if (!s.ok()) {
        return s;
    }
    //std::set<uint64_t> expected;
    //versions_->AddLiveFiles(&expected);
    uint64_t number;
    FileType type;
    std::vector<uint64_t> logs;
    for (size_t i = 0; i < filenames.size(); i++) {
        if (ParseFileName(filenames[i], &number, &type)) {
            //expected.erase(number);
            if (type == kLogFile && ((number >= min_log) || (number == prev_log)))
                logs.push_back(number);
        }
    }
    /**
     *  check if any file is lost, expected files should be erased to empty
     * */
    /*if (!expected.empty()) {
        char buf[50];
        snprintf(buf, sizeof(buf), "%d missing files; e.g.",
                 static_cast<int>(expected.size()));
        return Status::Corruption(buf, TableFileName(dbname_, *(expected.begin())));
    }*/

    // Recover in the order in which the logs were generated
    std::sort(logs.begin(), logs.end());
    for (size_t i = 0; i < logs.size(); i++) {
        s = RecoverLogFile(logs[i], (i == logs.size() - 1), /*save_manifest, edit,*/
                           &max_sequence);
        if (!s.ok()) {
            return s;
        }

        // The previous incarnation may not have written any MANIFEST
        // records after allocating this log number.  So we manually
        // update the file number allocation counter in VersionSet.

        // make the next_file_number_ = max_file_number(currently in working dir) + 1
        // others type files' number have been recorded into next_file_number
        versions_->MarkFileNumberUsed(logs[i]);
        // if there is too many log files, consider move this sentence out of loop,
        // instead use versions_->MarkFileNumberUsed(logs[i-1]) whose i reaches the number of log files already.
    }

    if (versions_->LastSequence() < max_sequence) {
        versions_->SetLastSequence(max_sequence);
    }


    return Status::OK();
}

/**
 * if there is no version_edit, nothing need to do in NewDB currently.
 * version_edit which records the information of db created,
 * encoded into string and appended to manifest.
 * */
Status DBImpl::NewDB() {
    //VersionEdit new_db;
    //new_db.SetComparatorName(user_comparator()->Name());
    //new_db.SetLogNumber(0);
    //new_db.SetNextFile(2);
    //new_db.SetLastSequence(0);

    const std::string manifest = DescriptorFileName(dbname_, 1);
    WritableFile* file;
    Status s = env_->NewWritableFile(manifest, &file);
    if (!s.ok()) {
        return s;
    }
    {
        log::Writer log(file);
        std::string record;
        //new_db.EncodeTo(&record);
        s = log.AddRecord(record);
        if (s.ok()) {
            s = file->Close();
        }
    }
    delete file;
    if (s.ok()) {
        // Make "CURRENT" file that points to the new manifest file.
        s = SetCurrentFile(env_, dbname_, 1);
    } else {
        env_->DeleteFile(manifest);
    }
    return s;
}


Status DBImpl::Get(const ReadOptions& options,
                   const Slice& key,
                   std::string* value) {
    Status s;
    MutexLock l(&mutex_);
    SequenceNumber snapshot;
    //if (options.snapshot != nullptr) {
    //    snapshot =
    //            static_cast<const SnapshotImpl*>(options.snapshot)->sequence_number();
    //} else {
        snapshot = versions_->LastSequence();
    //}

    MemTable* mem = mem_;
    MemTable* imm = imm_;
    //Version* current = versions_->current();
    mem->Ref();
    if (imm != nullptr) imm->Ref();
    //current->Ref();

    bool have_stat_update = false;
    //Version::GetStats stats;

    // Unlock while reading from files and memtables
    {
        mutex_.Unlock();
        // First look in the memtable, then in the immutable memtable (if any).
        LookupKey lkey(key, snapshot);
        if (mem->Get(lkey, value, &s)) {
            // Done
        } else if (imm != nullptr && imm->Get(lkey, value, &s)) {
            // Done
        } else {
            //s = current->Get(options, lkey, value, &stats);
            have_stat_update = true;
        }
        mutex_.Lock();
    }

    /**
     *  interval skip list and its partner is responsible for MaybeScheduleCompaction()
     *  and Get(options, lkey ,value, &stats) method above.
     *
     * */
    //if (have_stat_update && current->UpdateStats(stats)) {
    //    MaybeScheduleCompaction();
    //}
    mem->Unref();
    if (imm != nullptr) imm->Unref();
    //current->Unref();
    return s;
}


// Convenience methods
Status DBImpl::Put(const WriteOptions& o, const Slice& key, const Slice& val) {
    return DB::Put(o, key, val);
}

Status DBImpl::Delete(const WriteOptions& options, const Slice& key) {
    return DB::Delete(options, key);
}

//
void DBImpl::RecordBackgroundError(const Status& s) {
    mutex_.AssertHeld();
    if (bg_error_.ok()) {
        bg_error_ = s;
        background_work_finished_signal_.SignalAll();
    }
}

// my_batch is different from batchGroup which contains sequence and count,
// in other words, my_batches are appended to the tail of batchGroup
Status DBImpl::Write(const WriteOptions& options, WriteBatch* my_batch) {
    Writer w(&mutex_);
    w.batch = my_batch;
    w.sync = options.sync;
    w.done = false;

    MutexLock l(&mutex_);
    writers_.push_back(&w);
    // only the thread in front of writers_ and its batch not dealt yet
    // will be the consumer.
    while (!w.done && &w != writers_.front()) {
        w.cv.Wait();
    }
    if (w.done) {
        return w.status;
    }

    // May temporarily unlock and wait.
    /**
     *  MakeRoomForWrite is considered to be removed due to the fast compact progress of softdb
     * */
    //Status status = MakeRoomForWrite(my_batch == nullptr);
    Status status = Status::OK();


    uint64_t last_sequence = versions_->LastSequence();
    // last_writer to be dealt in batchGroup
    Writer* last_writer = &w;
    if (status.ok() && my_batch != nullptr) {  // nullptr batch is for compactions
        WriteBatch* updates = BuildBatchGroup(&last_writer);
        WriteBatchInternal::SetSequence(updates, last_sequence + 1);
        last_sequence += WriteBatchInternal::Count(updates);

        // Add to log and apply to memtable.  We can release the lock
        // during this phase since &w is currently responsible for logging
        // and protects against concurrent loggers and concurrent writes
        // into mem_.
        {
            mutex_.Unlock();
            //record the whole content of writeBatch, see write_batch.cc's information about writeBatch's _rep,
            //log is divided in writeBatch logically and block physically,
            //therefore in skiplsit there maybe exists the same key on different insert time.
            status = log_->AddRecord(WriteBatchInternal::Contents(updates));
            bool sync_error = false;
            if (status.ok() && options.sync) {
                status = logfile_->Sync();
                if (!status.ok()) {
                    sync_error = true;
                }
            }
            //append the sequence_ and kTypeValue (total 64bits) to the end of user's key, then insert it into skiplist,
            //see it in memtable.cc's Add function and write_batch.cc's Put and Delete function.
            if (status.ok()) {
                status = WriteBatchInternal::InsertInto(updates, mem_);
            }
            mutex_.Lock();
            if (sync_error) {
                // The state of the log file is indeterminate: the log record we
                // just added may or may not show up when the DB is re-opened.
                // So we force the DB into a mode where all future writes fail.
                RecordBackgroundError(status);
            }
        }
        // in BuildBatchGroup, if result points to caller's batch, then there is need to clear tmp_batch.
        if (updates == tmp_batch_) tmp_batch_->Clear();

        versions_->SetLastSequence(last_sequence);
    }

    // notify the producer threads to return whose batch is already consumed by this consumer thread,
    // until the last_writer.
    while (true) {
        Writer* ready = writers_.front();
        writers_.pop_front();
        if (ready != &w) {
            ready->status = status;
            ready->done = true;
            ready->cv.Signal();
        }
        if (ready == last_writer) break;
    }

    // Notify new head of write queue
    if (!writers_.empty()) {
        writers_.front()->cv.Signal();
    }

    return status;
}

// REQUIRES: Writer list must be non-empty
// REQUIRES: First writer must have a non-null batch
WriteBatch* DBImpl::BuildBatchGroup(Writer** last_writer) {
    mutex_.AssertHeld();
    assert(!writers_.empty());
    Writer* first = writers_.front();
    WriteBatch* result = first->batch;
    assert(result != nullptr);

    size_t size = WriteBatchInternal::ByteSize(first->batch);

    // Allow the group to grow up to a maximum size, but if the
    // original write is small, limit the growth so we do not slow
    // down the small write too much.
    size_t max_size = 1 << 20;
    if (size <= (128<<10)) {
        max_size = size + (128<<10);
    }

    *last_writer = first;
    std::deque<Writer*>::iterator iter = writers_.begin();
    ++iter;  // Advance past "first"
    for (; iter != writers_.end(); ++iter) {
        Writer* w = *iter;
        if (w->sync && !first->sync) {
            // Do not include a sync write into a batch handled by a non-sync write.
            break;
        }

        if (w->batch != nullptr) {
            size += WriteBatchInternal::ByteSize(w->batch);
            if (size > max_size) {
                // Do not make batch too big
                break;
            }

            // Append to *result
            if (result == first->batch) {
                // Switch to temporary batch instead of disturbing caller's batch
                result = tmp_batch_;
                assert(WriteBatchInternal::Count(result) == 0);
                WriteBatchInternal::Append(result, first->batch);
            }
            WriteBatchInternal::Append(result, w->batch);
        }
        // record the last batch in writers_ to be built into a batch group,
        // the consumer thread in the front of writers_ will consume them and
        // notify the corresponding producer thread.
        *last_writer = w;
    }
    return result;
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
    //bool save_manifest = false;
    Status s = impl->Recover(/*&edit, &save_manifest*/);
    if (s.ok() && impl->mem_ == nullptr) {
        // Create new log and a corresponding memtable.
        uint64_t new_log_number = impl->versions_->NewFileNumber();
        WritableFile* lfile;
        s = options.env->NewWritableFile(LogFileName(dbname, new_log_number),
                                         &lfile);
        if (s.ok()) {
            //edit.SetLogNumber(new_log_number);
            impl->logfile_ = lfile;
            impl->logfile_number_ = new_log_number;
            impl->log_ = new log::Writer(lfile);
            impl->mem_ = new MemTable(impl->internal_comparator_);
            impl->mem_->Ref();
        }
    }
    /**
     *  log file's number should be recorded, for the convenience of garbage collect
     *
     *
     * */
    /*
    if (s.ok() && save_manifest) {
        edit.SetPrevLogNumber(0);  // No older logs needed after recovery.
        edit.SetLogNumber(impl->logfile_number_);
        s = impl->versions_->LogAndApply(&edit, &impl->mutex_);
    }
     */
    /**
     *  DeleteObsoleteFiles() is responsible for removing the unused log file, in brief it's garbage collect
     *
     * */

    if (s.ok()) {
        impl->DeleteObsoleteFiles();
        //impl->MaybeScheduleCompaction();
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























