//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "crocksdb/c.h"

#include <stdlib.h>

#include <limits>

#include "db/column_family.h"
#include "file/random_access_file_reader.h"
#include "file/sequence_file_reader.h"
#include "file/writable_file_writer.h"
#include "rocksdb/cache.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/comparator.h"
#include "rocksdb/concurrent_task_limiter.h"
#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#include "rocksdb/encryption.h"
#include "rocksdb/env.h"
#include "rocksdb/env_encryption.h"
#include "rocksdb/env_inspected.h"
#include "rocksdb/file_system.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/iostats_context.h"
#include "rocksdb/iterator.h"
#include "rocksdb/ldb_tool.h"
#include "rocksdb/listener.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/options.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/rate_limiter.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/sst_dump_tool.h"
#include "rocksdb/sst_file_reader.h"
#include "rocksdb/sst_partitioner.h"
#include "rocksdb/statistics.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "rocksdb/table_properties.h"
#include "rocksdb/types.h"
#include "rocksdb/universal_compaction.h"
#include "rocksdb/utilities/backup_engine.h"
#include "rocksdb/utilities/checkpoint.h"
#include "rocksdb/utilities/db_ttl.h"
#include "rocksdb/utilities/debug.h"
#include "rocksdb/utilities/options_util.h"
#include "rocksdb/utilities/table_properties_collectors.h"
#include "rocksdb/write_batch.h"
#include "rocksdb/write_buffer_manager.h"
#include "src/blob_format.h"
#include "table/block_based/block_based_table_factory.h"
#include "table/sst_file_writer_collectors.h"
#include "table/table_reader.h"
#include "titan/checkpoint.h"
#include "titan/db.h"
#include "titan/options.h"
#include "util/coding.h"

#if !defined(ROCKSDB_MAJOR) || !defined(ROCKSDB_MINOR) || \
    !defined(ROCKSDB_PATCH)
#error Only rocksdb 5.7.3+ is supported.
#endif

#if ROCKSDB_MAJOR * 10000 + ROCKSDB_MINOR * 100 + ROCKSDB_PATCH < 50703
#error Only rocksdb 5.7.3+ is supported.
#endif

using rocksdb::BackgroundErrorReason;
using rocksdb::BackupEngine;
using rocksdb::BackupEngineOptions;
using rocksdb::BackupInfo;
using rocksdb::BlockBasedTableOptions;
using rocksdb::BlockCipher;
using rocksdb::Cache;
using rocksdb::Checkpoint;
using rocksdb::ColumnFamilyDescriptor;
using rocksdb::ColumnFamilyHandle;
using rocksdb::ColumnFamilyOptions;
using rocksdb::CompactionFilter;
using rocksdb::CompactionFilterFactory;
using rocksdb::CompactionJobInfo;
using rocksdb::CompactionOptionsFIFO;
using rocksdb::CompactRangeOptions;
using rocksdb::Comparator;
using rocksdb::CompressionType;
using rocksdb::ConfigOptions;
using rocksdb::CuckooTableOptions;
using rocksdb::DB;
using rocksdb::DBOptions;
using rocksdb::DbPath;
using rocksdb::DBWithTTL;
using rocksdb::EncryptionProvider;
using rocksdb::EntryType;
using rocksdb::Env;
using rocksdb::EnvOptions;
using rocksdb::EventListener;
using rocksdb::ExternalFileIngestionInfo;
using rocksdb::ExternalSstFileInfo;
using rocksdb::FileLock;
using rocksdb::FileOptions;
using rocksdb::FilterBitsBuilder;
using rocksdb::FilterBitsReader;
using rocksdb::FilterBuildingContext;
using rocksdb::FilterPolicy;
using rocksdb::FlushJobInfo;
using rocksdb::FlushOptions;
using rocksdb::FSRandomAccessFile;
using rocksdb::HistogramData;
using rocksdb::HyperClockCacheOptions;
using rocksdb::InfoLogLevel;
using rocksdb::IngestExternalFileOptions;
using rocksdb::Iterator;
using rocksdb::KeyVersion;
using rocksdb::LiveFileMetaData;
using rocksdb::Logger;
using rocksdb::LRUCacheOptions;
using rocksdb::MemTableInfo;
using rocksdb::MergeInstanceOptions;
using rocksdb::MergeOperator;
using rocksdb::NewBloomFilterPolicy;
using rocksdb::NewEncryptedEnv;
using rocksdb::NewGenericRateLimiter;
using rocksdb::NewLRUCache;
using rocksdb::NewRibbonFilterPolicy;
using rocksdb::Options;
using rocksdb::PartitionerRequest;
using rocksdb::PartitionerResult;
using rocksdb::PerfFlags;
using rocksdb::PinnableSlice;
using rocksdb::PostWriteCallback;
using rocksdb::RandomAccessFile;
using rocksdb::Range;
using rocksdb::RangePtr;
using rocksdb::RateLimiter;
using rocksdb::ReadOptions;
using rocksdb::RestoreOptions;
using rocksdb::SequenceNumber;
using rocksdb::SequentialFile;
using rocksdb::Slice;
using rocksdb::SliceParts;
using rocksdb::SliceTransform;
using rocksdb::Snapshot;
using rocksdb::SstFileReader;
using rocksdb::SstFileWriter;
using rocksdb::SstPartitioner;
using rocksdb::SstPartitionerFactory;
using rocksdb::Statistics;
using rocksdb::Status;
using rocksdb::SubcompactionJobInfo;
using rocksdb::TableFileCreationReason;
using rocksdb::TableProperties;
using rocksdb::TablePropertiesCollection;
using rocksdb::TablePropertiesCollector;
using rocksdb::TablePropertiesCollectorFactory;
using rocksdb::UserCollectedProperties;
using rocksdb::WALRecoveryMode;
using rocksdb::WritableFile;
using rocksdb::WriteBatch;
using rocksdb::WriteBufferManager;
using rocksdb::WriteOptions;
using rocksdb::WriteStallCondition;
using rocksdb::WriteStallInfo;

using rocksdb::BlockBasedTableFactory;
using rocksdb::BottommostLevelCompaction;
using rocksdb::ColumnFamilyData;
using rocksdb::ColumnFamilyHandleImpl;
using rocksdb::ColumnFamilyMetaData;
using rocksdb::CompactionOptions;
using rocksdb::CompactionReason;
using rocksdb::DecodeFixed32;
using rocksdb::DecodeFixed64;
using rocksdb::ExternalSstFilePropertyNames;
using rocksdb::IOStatsContext;
using rocksdb::LDBTool;
using rocksdb::LevelMetaData;
using rocksdb::MemoryAllocator;
using rocksdb::PerfContext;
using rocksdb::PerfLevel;
using rocksdb::PutFixed64;
using rocksdb::RandomAccessFile;
using rocksdb::RandomAccessFileReader;
using rocksdb::RandomRWFile;
using rocksdb::SSTDumpTool;
using rocksdb::SstFileMetaData;
using rocksdb::TableReader;
using rocksdb::TableReaderOptions;
using rocksdb::VectorRepFactory;

using rocksdb::kMaxSequenceNumber;

using rocksdb::titandb::BlobIndex;
using rocksdb::titandb::TitanBlobRunMode;
using rocksdb::titandb::TitanCFDescriptor;
using rocksdb::titandb::TitanCFOptions;
using rocksdb::titandb::TitanDB;
using rocksdb::titandb::TitanDBOptions;
using rocksdb::titandb::TitanOptions;
using rocksdb::titandb::TitanReadOptions;
using TitanCheckpoint = rocksdb::titandb::Checkpoint;

#ifdef OPENSSL
using rocksdb::encryption::EncryptionMethod;
using rocksdb::encryption::FileEncryptionInfo;
using rocksdb::encryption::KeyManager;
using rocksdb::encryption::NewKeyManagedEncryptedEnv;
#endif

using rocksdb::FileSystemInspector;
using rocksdb::NewFileSystemInspectedEnv;
using std::shared_ptr;

using rocksdb::ConcurrentTaskLimiter;
using rocksdb::NewConcurrentTaskLimiter;

extern "C" {

const char* block_base_table_str = "BlockBasedTable";
// Global flag that controls manual compaction cancellation. When set to
// 'true', all currently in-progress manual compaction operations will be
// canceled as soon as possible.
// TODO: Refactor to make this flag DB-instance specific rather than global.
static std::atomic<bool> GLOBAL_MANUAL_COMPACTION_CANCELED_FLAG{false};

struct crocksdb_t {
  DB* rep;
};
struct crocksdb_status_ptr_t {
  Status* rep;
};
struct crocksdb_backup_engine_t {
  BackupEngine* rep;
};
struct crocksdb_backup_engine_info_t {
  std::vector<BackupInfo> rep;
};
struct crocksdb_checkpoint_t {
  Checkpoint* rep;
};
struct crocksdb_restore_options_t {
  RestoreOptions rep;
};
struct crocksdb_iterator_t {
  Iterator* rep;
};
struct crocksdb_writebatch_t {
  WriteBatch rep;
};
struct crocksdb_snapshot_t {
  const Snapshot* rep;
};
struct crocksdb_flushoptions_t {
  FlushOptions rep;
};
struct crocksdb_fifo_compaction_options_t {
  CompactionOptionsFIFO rep;
};
struct crocksdb_readoptions_t {
  ReadOptions rep;
  Slice upper_bound;  // stack variable to set pointer to in ReadOptions
  Slice lower_bound;
};
struct crocksdb_writeoptions_t {
  WriteOptions rep;
};
struct crocksdb_options_t {
  Options rep;
};
struct crocksdb_column_family_descriptor {
  ColumnFamilyDescriptor rep;
};
struct crocksdb_compactoptions_t {
  CompactRangeOptions rep;
};
struct crocksdb_block_based_table_options_t {
  BlockBasedTableOptions rep;
};
struct crocksdb_cuckoo_table_options_t {
  CuckooTableOptions rep;
};
struct crocksdb_seqfile_t {
  SequentialFile* rep;
};
struct crocksdb_randomfile_t {
  RandomAccessFile* rep;
};
struct crocksdb_writablefile_t {
  WritableFile* rep;
};
struct crocksdb_filelock_t {
  FileLock* rep;
};
struct crocksdb_logger_t {
  shared_ptr<Logger> rep;
};
struct crocksdb_logger_impl_t : public Logger {
  void* rep;

  void (*destructor_)(void*);
  void (*logv_internal_)(void* logger, uint32_t log_level, const char* log);

  void log_help_(void* logger, uint32_t log_level, const char* format,
                 va_list ap) {
    // Try twice, first with buffer on stack, second with buffer on heap.
    constexpr int kBufferSize = 500;
    char buffer[kBufferSize];
    va_list ap_copy;
    va_copy(ap_copy, ap);
    int num = vsnprintf(buffer, kBufferSize, format, ap_copy);
    va_end(ap_copy);
    if (num < kBufferSize) {
      logv_internal_(rep, log_level, buffer);
    } else {
      char* large_buffer = new char[num + 1];
      vsnprintf(large_buffer, static_cast<size_t>(num) + 1, format, ap);
      logv_internal_(rep, log_level, large_buffer);
      delete[] large_buffer;
    }
  }

  void Logv(const char* format, va_list ap) override {
    log_help_(rep, static_cast<uint32_t>(InfoLogLevel::HEADER_LEVEL), format,
              ap);
  }

  void Logv(const InfoLogLevel log_level, const char* format,
            va_list ap) override {
    log_help_(rep, static_cast<uint32_t>(log_level), format, ap);
  }

  virtual ~crocksdb_logger_impl_t() { (*destructor_)(rep); }
};
struct crocksdb_lru_cache_options_t {
  LRUCacheOptions rep;
};
struct crocksdb_hyper_clock_cache_options_t {
  HyperClockCacheOptions rep;
};
struct crocksdb_cache_t {
  shared_ptr<Cache> rep;
};
struct crocksdb_memory_allocator_t {
  shared_ptr<MemoryAllocator> rep;
};
struct crocksdb_livefiles_t {
  std::vector<LiveFileMetaData> rep;
};
struct crocksdb_column_family_handle_t {
  ColumnFamilyHandle* rep;
};
struct crocksdb_envoptions_t {
  EnvOptions rep;
};
struct crocksdb_sequential_file_t {
  SequentialFile* rep;
};
struct crocksdb_ingestexternalfileoptions_t {
  IngestExternalFileOptions rep;
};
struct crocksdb_sstfilereader_t {
  SstFileReader* rep;
};
struct crocksdb_sstfilewriter_t {
  SstFileWriter* rep;
};
struct crocksdb_externalsstfileinfo_t {
  ExternalSstFileInfo rep;
};
struct crocksdb_ratelimiter_t {
  std::shared_ptr<RateLimiter> rep;
};
struct crocksdb_write_buffer_manager_t {
  std::shared_ptr<WriteBufferManager> rep;
};
struct crocksdb_concurrent_task_limiter_t {
  std::shared_ptr<ConcurrentTaskLimiter> rep;
};
struct crocksdb_statistics_t {
  std::shared_ptr<Statistics> rep;
};
struct crocksdb_histogramdata_t {
  HistogramData rep;
};
struct crocksdb_pinnableslice_t {
  PinnableSlice rep;
};
struct crocksdb_flushjobinfo_t {
  FlushJobInfo rep;
};
struct crocksdb_writestallcondition_t {
  WriteStallCondition rep;
};
struct crocksdb_writestallinfo_t {
  WriteStallInfo rep;
};
struct crocksdb_memtableinfo_t {
  MemTableInfo rep;
};
struct crocksdb_compactionjobinfo_t {
  CompactionJobInfo rep;
};
struct crocksdb_subcompactionjobinfo_t {
  SubcompactionJobInfo rep;
};
struct crocksdb_externalfileingestioninfo_t {
  ExternalFileIngestionInfo rep;
};

struct crocksdb_keyversions_t {
  std::vector<KeyVersion> rep;
};

struct crocksdb_compactionfiltercontext_t {
  CompactionFilter::Context rep;
};

struct crocksdb_column_family_meta_data_t {
  ColumnFamilyMetaData rep;
};
struct crocksdb_level_meta_data_t {
  LevelMetaData rep;
};
struct crocksdb_sst_file_meta_data_t {
  SstFileMetaData rep;
};
struct crocksdb_compaction_options_t {
  CompactionOptions rep;
};

struct crocksdb_map_property_t {
  std::map<std::string, std::string> rep;
};

struct crocksdb_compactionfilter_t : public CompactionFilter {
  void* state_;
  void (*destructor_)(void*);
  uint32_t (*filter_)(void*, int level, const char* key, size_t key_length,
                      uint32_t value_type, const char* existing_value,
                      size_t value_length, char** new_value,
                      size_t* new_value_length, char** skip_until,
                      size_t* skip_until_length);

  const char* (*name_)(void*);

  virtual ~crocksdb_compactionfilter_t() { (*destructor_)(state_); }

  virtual Decision UnsafeFilter(int level, const Slice& key,
                                ValueType value_type,
                                const Slice& existing_value,
                                std::string* new_value,
                                std::string* skip_until) const override {
    char* c_new_value = nullptr;
    char* c_skip_until = nullptr;
    size_t new_value_length, skip_until_length = 0;

    uint32_t r =
        (*filter_)(state_, level, key.data(), key.size(),
                   static_cast<uint32_t>(value_type), existing_value.data(),
                   existing_value.size(), &c_new_value, &new_value_length,
                   &c_skip_until, &skip_until_length);
    CompactionFilter::Decision result =
        static_cast<CompactionFilter::Decision>(r);
    if (result == Decision::kChangeValue) {
      new_value->assign(c_new_value, new_value_length);
      free(c_new_value);
    } else if (result == Decision::kRemoveAndSkipUntil) {
      skip_until->assign(c_skip_until, skip_until_length);
      free(c_skip_until);
    }
    return result;
  }

  virtual const char* Name() const override { return (*name_)(state_); }
};

struct crocksdb_compactionfilterfactory_t : public CompactionFilterFactory {
  void* state_;
  void (*destructor_)(void*);
  crocksdb_compactionfilter_t* (*create_compaction_filter_)(
      void*, crocksdb_compactionfiltercontext_t* context);
  unsigned char (*should_filter_table_file_creation_)(void*, uint32_t reason);
  const char* (*name_)(void*);

  virtual ~crocksdb_compactionfilterfactory_t() { (*destructor_)(state_); }

  virtual std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& context) override {
    crocksdb_compactionfiltercontext_t ccontext;
    ccontext.rep = context;
    CompactionFilter* cf = (*create_compaction_filter_)(state_, &ccontext);
    return std::unique_ptr<CompactionFilter>(cf);
  }

  virtual bool ShouldFilterTableFileCreation(
      TableFileCreationReason reason) const override {
    uint32_t creason = static_cast<uint32_t>(reason);
    return (*should_filter_table_file_creation_)(state_, creason);
  }

  virtual const char* Name() const override { return (*name_)(state_); }
};

struct crocksdb_comparator_t : public Comparator {
  void* state_;
  void (*destructor_)(void*);
  int (*compare_)(void*, const char* a, size_t alen, const char* b,
                  size_t blen);
  const char* (*name_)(void*);

  virtual ~crocksdb_comparator_t() { (*destructor_)(state_); }

  virtual int Compare(const Slice& a, const Slice& b) const override {
    return (*compare_)(state_, a.data(), a.size(), b.data(), b.size());
  }

  virtual const char* Name() const override { return (*name_)(state_); }

  // No-ops since the C binding does not support key shortening methods.
  virtual void FindShortestSeparator(std::string*,
                                     const Slice&) const override {}
  virtual void FindShortSuccessor(std::string*) const override {}
};

struct crocksdb_filterpolicy_t : public FilterPolicy {
  void* state_;
  void (*destructor_)(void*);
  const char* (*name_)(void*);

  virtual ~crocksdb_filterpolicy_t() { (*destructor_)(state_); }

  virtual const char* Name() const override { return (*name_)(state_); }
};

struct crocksdb_mergeoperator_t : public MergeOperator {
  void* state_;
  void (*destructor_)(void*);
  const char* (*name_)(void*);
  char* (*full_merge_)(void*, const char* key, size_t key_length,
                       const char* existing_value, size_t existing_value_length,
                       const char* const* operands_list,
                       const size_t* operands_list_length, int num_operands,
                       unsigned char* success, size_t* new_value_length);
  char* (*partial_merge_)(void*, const char* key, size_t key_length,
                          const char* const* operands_list,
                          const size_t* operands_list_length, int num_operands,
                          unsigned char* success, size_t* new_value_length);
  void (*delete_value_)(void*, const char* value, size_t value_length);

  virtual ~crocksdb_mergeoperator_t() { (*destructor_)(state_); }

  virtual const char* Name() const override { return (*name_)(state_); }

  virtual bool FullMergeV2(const MergeOperationInput& merge_in,
                           MergeOperationOutput* merge_out) const override {
    size_t n = merge_in.operand_list.size();
    std::vector<const char*> operand_pointers(n);
    std::vector<size_t> operand_sizes(n);
    for (size_t i = 0; i < n; i++) {
      Slice operand(merge_in.operand_list[i]);
      operand_pointers[i] = operand.data();
      operand_sizes[i] = operand.size();
    }

    const char* existing_value_data = nullptr;
    size_t existing_value_len = 0;
    if (merge_in.existing_value != nullptr) {
      existing_value_data = merge_in.existing_value->data();
      existing_value_len = merge_in.existing_value->size();
    }

    unsigned char success;
    size_t new_value_len;
    char* tmp_new_value = (*full_merge_)(
        state_, merge_in.key.data(), merge_in.key.size(), existing_value_data,
        existing_value_len, &operand_pointers[0], &operand_sizes[0],
        static_cast<int>(n), &success, &new_value_len);
    merge_out->new_value.assign(tmp_new_value, new_value_len);

    if (delete_value_ != nullptr) {
      (*delete_value_)(state_, tmp_new_value, new_value_len);
    } else {
      free(tmp_new_value);
    }

    return success;
  }

  virtual bool PartialMergeMulti(const Slice& key,
                                 const std::deque<Slice>& operand_list,
                                 std::string* new_value,
                                 Logger*) const override {
    size_t operand_count = operand_list.size();
    std::vector<const char*> operand_pointers(operand_count);
    std::vector<size_t> operand_sizes(operand_count);
    for (size_t i = 0; i < operand_count; ++i) {
      Slice operand(operand_list[i]);
      operand_pointers[i] = operand.data();
      operand_sizes[i] = operand.size();
    }

    unsigned char success;
    size_t new_value_len;
    char* tmp_new_value = (*partial_merge_)(
        state_, key.data(), key.size(), &operand_pointers[0], &operand_sizes[0],
        static_cast<int>(operand_count), &success, &new_value_len);
    new_value->assign(tmp_new_value, new_value_len);

    if (delete_value_ != nullptr) {
      (*delete_value_)(state_, tmp_new_value, new_value_len);
    } else {
      free(tmp_new_value);
    }

    return success;
  }
};

struct crocksdb_env_t {
  Env* rep;
  bool is_default;
  std::shared_ptr<EncryptionProvider> encryption_provider;
  std::shared_ptr<BlockCipher> block_cipher;
};

struct crocksdb_slicetransform_t : public SliceTransform {
  void* state_;
  void (*destructor_)(void*);
  const char* (*name_)(void*);
  char* (*transform_)(void*, const char* key, size_t length,
                      size_t* dst_length);
  unsigned char (*in_domain_)(void*, const char* key, size_t length);
  unsigned char (*in_range_)(void*, const char* key, size_t length);

  virtual ~crocksdb_slicetransform_t() { (*destructor_)(state_); }

  virtual const char* Name() const override { return (*name_)(state_); }

  virtual Slice Transform(const Slice& src) const override {
    size_t len;
    char* dst = (*transform_)(state_, src.data(), src.size(), &len);
    return Slice(dst, len);
  }

  virtual bool InDomain(const Slice& src) const override {
    return (*in_domain_)(state_, src.data(), src.size());
  }

  virtual bool InRange(const Slice& src) const override {
    return (*in_range_)(state_, src.data(), src.size());
  }
};

struct crocksdb_universal_compaction_options_t {
  rocksdb::CompactionOptionsUniversal* rep;
};

struct crocksdb_writebatch_iterator_t {
  rocksdb::WriteBatch::Iterator* rep;
};

#ifdef OPENSSL
struct crocksdb_file_encryption_info_t {
  FileEncryptionInfo* rep;
};

struct crocksdb_encryption_key_manager_t {
  std::shared_ptr<KeyManager> rep;
};
#endif

struct crocksdb_sst_partitioner_t {
  std::unique_ptr<SstPartitioner> rep;
};

struct crocksdb_sst_partitioner_request_t {
  PartitionerRequest* rep;
  Slice prev_user_key;
  Slice current_user_key;
};

struct crocksdb_sst_partitioner_context_t {
  SstPartitioner::Context* rep;
};

struct crocksdb_sst_partitioner_factory_t {
  std::shared_ptr<SstPartitionerFactory> rep;
};

struct crocksdb_file_system_inspector_t {
  std::shared_ptr<FileSystemInspector> rep;
};

struct crocksdb_post_write_callback_t : public PostWriteCallback {
  void* state_;
  void (*on_post_write_callback)(void*, uint64_t);

  void Callback(SequenceNumber seq) override {
    on_post_write_callback(state_, seq);
  }
};

crocksdb_post_write_callback_t* crocksdb_post_write_callback_init(
    void* buf, size_t buf_len, void* state,
    on_post_write_callback_cb on_post_write_callback) {
  void* input_buf = buf;
  assert(std::align(alignof(crocksdb_post_write_callback_t),
                    sizeof(crocksdb_post_write_callback_t), buf,
                    buf_len) == input_buf);
  crocksdb_post_write_callback_t* r = new (buf) crocksdb_post_write_callback_t;
  r->state_ = state;
  r->on_post_write_callback = on_post_write_callback;
  return r;
}

static bool SaveError(char** errptr, const Status& s) {
  assert(errptr != nullptr);
  if (s.ok()) {
    return false;
  } else if (*errptr == nullptr) {
    *errptr = strdup(s.ToString().c_str());
  } else {
    // TODO(sanjay): Merge with existing error?
    // This is a bug if *errptr is not created by malloc()
    free(*errptr);
    *errptr = strdup(s.ToString().c_str());
  }
  return true;
}

static char* CopyString(const std::string& str) {
  char* result = reinterpret_cast<char*>(malloc(sizeof(char) * str.size()));
  memcpy(result, str.data(), sizeof(char) * str.size());
  return result;
}

crocksdb_t* crocksdb_open(const crocksdb_options_t* options, const char* name,
                          char** errptr) {
  DB* db;
  if (SaveError(errptr, DB::Open(options->rep, std::string(name), &db))) {
    return nullptr;
  }
  crocksdb_t* result = new crocksdb_t;
  result->rep = db;
  return result;
}

crocksdb_t* crocksdb_open_with_ttl(const crocksdb_options_t* options,
                                   const char* name, int ttl, char** errptr) {
  DBWithTTL* db;
  if (SaveError(errptr,
                DBWithTTL::Open(options->rep, std::string(name), &db, ttl))) {
    return nullptr;
  }
  crocksdb_t* result = new crocksdb_t;
  result->rep = db;
  return result;
}

crocksdb_t* crocksdb_open_for_read_only(const crocksdb_options_t* options,
                                        const char* name,
                                        unsigned char error_if_log_file_exist,
                                        char** errptr) {
  DB* db;
  if (SaveError(errptr, DB::OpenForReadOnly(options->rep, std::string(name),
                                            &db, error_if_log_file_exist))) {
    return nullptr;
  }
  crocksdb_t* result = new crocksdb_t;
  result->rep = db;
  return result;
}

void crocksdb_merge_disjoint_instances(crocksdb_t* db,
                                       unsigned char merge_memtable,
                                       unsigned char allow_source_write,
                                       int max_preload_files,
                                       crocksdb_t** instances,
                                       size_t num_instances, char** errptr) {
  MergeInstanceOptions opts;
  opts.merge_memtable = merge_memtable;
  opts.allow_source_write = allow_source_write;
  opts.max_preload_files = max_preload_files;
  std::vector<DB*> dbs;
  for (auto i = 0; i < num_instances; i++) {
    dbs.push_back(instances[i]->rep);
  }
  SaveError(errptr, db->rep->MergeDisjointInstances(opts, std::move(dbs)));
}

void crocksdb_status_ptr_get_error(crocksdb_status_ptr_t* status,
                                   char** errptr) {
  SaveError(errptr, *(status->rep));
}

void crocksdb_resume(crocksdb_t* db, char** errptr) {
  SaveError(errptr, db->rep->Resume());
}

crocksdb_checkpoint_t* crocksdb_checkpoint_object_create(crocksdb_t* db,
                                                         char** errptr) {
  Checkpoint* checkpoint;
  if (SaveError(errptr, Checkpoint::Create(db->rep, &checkpoint))) {
    return nullptr;
  }
  crocksdb_checkpoint_t* result = new crocksdb_checkpoint_t;
  result->rep = checkpoint;
  return result;
}

void crocksdb_checkpoint_create(crocksdb_checkpoint_t* checkpoint,
                                const char* checkpoint_dir,
                                uint64_t log_size_for_flush, char** errptr) {
  SaveError(errptr, checkpoint->rep->CreateCheckpoint(
                        std::string(checkpoint_dir), log_size_for_flush));
}

void crocksdb_checkpoint_object_destroy(crocksdb_checkpoint_t* checkpoint) {
  delete checkpoint->rep;
  delete checkpoint;
}

crocksdb_backup_engine_t* crocksdb_backup_engine_open(
    const crocksdb_options_t* options, const char* path, char** errptr) {
  BackupEngine* be;
  if (SaveError(errptr, BackupEngine::Open(options->rep.env,
                                           BackupEngineOptions(path), &be))) {
    return nullptr;
  }
  crocksdb_backup_engine_t* result = new crocksdb_backup_engine_t;
  result->rep = be;
  return result;
}

void crocksdb_backup_engine_create_new_backup(crocksdb_backup_engine_t* be,
                                              crocksdb_t* db, char** errptr) {
  SaveError(errptr, be->rep->CreateNewBackup(db->rep));
}

void crocksdb_backup_engine_purge_old_backups(crocksdb_backup_engine_t* be,
                                              uint32_t num_backups_to_keep,
                                              char** errptr) {
  SaveError(errptr, be->rep->PurgeOldBackups(num_backups_to_keep));
}

crocksdb_restore_options_t* crocksdb_restore_options_create() {
  return new crocksdb_restore_options_t;
}

void crocksdb_restore_options_destroy(crocksdb_restore_options_t* opt) {
  delete opt;
}

void crocksdb_restore_options_set_keep_log_files(
    crocksdb_restore_options_t* opt, int v) {
  opt->rep.keep_log_files = v;
}

void crocksdb_backup_engine_restore_db_from_latest_backup(
    crocksdb_backup_engine_t* be, const char* db_dir, const char* wal_dir,
    const crocksdb_restore_options_t* restore_options, char** errptr) {
  SaveError(errptr, be->rep->RestoreDBFromLatestBackup(std::string(db_dir),
                                                       std::string(wal_dir),
                                                       restore_options->rep));
}

const crocksdb_backup_engine_info_t* crocksdb_backup_engine_get_backup_info(
    crocksdb_backup_engine_t* be) {
  crocksdb_backup_engine_info_t* result = new crocksdb_backup_engine_info_t;
  be->rep->GetBackupInfo(&result->rep);
  return result;
}

int crocksdb_backup_engine_info_count(
    const crocksdb_backup_engine_info_t* info) {
  return static_cast<int>(info->rep.size());
}

int64_t crocksdb_backup_engine_info_timestamp(
    const crocksdb_backup_engine_info_t* info, int index) {
  return info->rep[index].timestamp;
}

uint32_t crocksdb_backup_engine_info_backup_id(
    const crocksdb_backup_engine_info_t* info, int index) {
  return info->rep[index].backup_id;
}

uint64_t crocksdb_backup_engine_info_size(
    const crocksdb_backup_engine_info_t* info, int index) {
  return info->rep[index].size;
}

uint32_t crocksdb_backup_engine_info_number_files(
    const crocksdb_backup_engine_info_t* info, int index) {
  return info->rep[index].number_files;
}

void crocksdb_backup_engine_info_destroy(
    const crocksdb_backup_engine_info_t* info) {
  delete info;
}

void crocksdb_backup_engine_close(crocksdb_backup_engine_t* be) {
  delete be->rep;
  delete be;
}

void crocksdb_close(crocksdb_t* db) {
  delete db->rep;
  delete db;
}

void crocksdb_pause_bg_work(crocksdb_t* db) { db->rep->PauseBackgroundWork(); }

void crocksdb_continue_bg_work(crocksdb_t* db) {
  db->rep->ContinueBackgroundWork();
}

void crocksdb_set_global_manual_compaction_canceled(unsigned char v) {
  GLOBAL_MANUAL_COMPACTION_CANCELED_FLAG.store(v, std::memory_order_seq_cst);
}

void crocksdb_disable_manual_compaction(crocksdb_t* db) {
  db->rep->DisableManualCompaction();
}

void crocksdb_enable_manual_compaction(crocksdb_t* db) {
  db->rep->EnableManualCompaction();
}

crocksdb_t* crocksdb_open_column_families(
    const crocksdb_options_t* db_options, const char* name,
    int num_column_families, const char** column_family_names,
    const crocksdb_options_t** column_family_options,
    crocksdb_column_family_handle_t** column_family_handles, char** errptr) {
  std::vector<ColumnFamilyDescriptor> column_families;
  for (int i = 0; i < num_column_families; i++) {
    column_families.push_back(ColumnFamilyDescriptor(
        std::string(column_family_names[i]),
        ColumnFamilyOptions(column_family_options[i]->rep)));
  }

  DB* db;
  std::vector<ColumnFamilyHandle*> handles;
  if (SaveError(errptr, DB::Open(DBOptions(db_options->rep), std::string(name),
                                 column_families, &handles, &db))) {
    return nullptr;
  }

  for (size_t i = 0; i < handles.size(); i++) {
    crocksdb_column_family_handle_t* c_handle =
        new crocksdb_column_family_handle_t;
    c_handle->rep = handles[i];
    column_family_handles[i] = c_handle;
  }
  crocksdb_t* result = new crocksdb_t;
  result->rep = db;
  return result;
}

crocksdb_t* crocksdb_open_column_families_with_ttl(
    const crocksdb_options_t* db_options, const char* name,
    int num_column_families, const char** column_family_names,
    const crocksdb_options_t** column_family_options, const int32_t* ttl_array,
    unsigned char read_only,
    crocksdb_column_family_handle_t** column_family_handles, char** errptr) {
  std::vector<ColumnFamilyDescriptor> column_families;
  std::vector<int32_t> ttls;
  for (int i = 0; i < num_column_families; i++) {
    column_families.push_back(ColumnFamilyDescriptor(
        std::string(column_family_names[i]),
        ColumnFamilyOptions(column_family_options[i]->rep)));
    ttls.push_back(ttl_array[i]);
  }

  DBWithTTL* db;
  std::vector<ColumnFamilyHandle*> handles;
  if (SaveError(errptr, DBWithTTL::Open(DBOptions(db_options->rep),
                                        std::string(name), column_families,
                                        &handles, &db, ttls, read_only))) {
    return nullptr;
  }

  for (size_t i = 0; i < handles.size(); i++) {
    crocksdb_column_family_handle_t* c_handle =
        new crocksdb_column_family_handle_t;
    c_handle->rep = handles[i];
    column_family_handles[i] = c_handle;
  }
  crocksdb_t* result = new crocksdb_t;
  result->rep = db;
  return result;
}

crocksdb_t* crocksdb_open_for_read_only_column_families(
    const crocksdb_options_t* db_options, const char* name,
    int num_column_families, const char** column_family_names,
    const crocksdb_options_t** column_family_options,
    crocksdb_column_family_handle_t** column_family_handles,
    unsigned char error_if_log_file_exist, char** errptr) {
  std::vector<ColumnFamilyDescriptor> column_families;
  for (int i = 0; i < num_column_families; i++) {
    column_families.push_back(ColumnFamilyDescriptor(
        std::string(column_family_names[i]),
        ColumnFamilyOptions(column_family_options[i]->rep)));
  }

  DB* db;
  std::vector<ColumnFamilyHandle*> handles;
  if (SaveError(errptr,
                DB::OpenForReadOnly(DBOptions(db_options->rep),
                                    std::string(name), column_families,
                                    &handles, &db, error_if_log_file_exist))) {
    return nullptr;
  }

  for (size_t i = 0; i < handles.size(); i++) {
    crocksdb_column_family_handle_t* c_handle =
        new crocksdb_column_family_handle_t;
    c_handle->rep = handles[i];
    column_family_handles[i] = c_handle;
  }
  crocksdb_t* result = new crocksdb_t;
  result->rep = db;
  return result;
}

char** crocksdb_list_column_families(const crocksdb_options_t* options,
                                     const char* name, size_t* lencfs,
                                     char** errptr) {
  std::vector<std::string> fams;
  SaveError(errptr, DB::ListColumnFamilies(DBOptions(options->rep),
                                           std::string(name), &fams));

  *lencfs = fams.size();
  char** column_families =
      static_cast<char**>(malloc(sizeof(char*) * fams.size()));
  for (size_t i = 0; i < fams.size(); i++) {
    column_families[i] = strdup(fams[i].c_str());
  }
  return column_families;
}

void crocksdb_list_column_families_destroy(char** list, size_t len) {
  for (size_t i = 0; i < len; ++i) {
    free(list[i]);
  }
  free(list);
}

crocksdb_column_family_handle_t* crocksdb_create_column_family(
    crocksdb_t* db, const crocksdb_options_t* column_family_options,
    const char* column_family_name, char** errptr) {
  crocksdb_column_family_handle_t* handle = new crocksdb_column_family_handle_t;
  SaveError(errptr, db->rep->CreateColumnFamily(
                        ColumnFamilyOptions(column_family_options->rep),
                        std::string(column_family_name), &(handle->rep)));
  return handle;
}

void crocksdb_drop_column_family(crocksdb_t* db,
                                 crocksdb_column_family_handle_t* handle,
                                 char** errptr) {
  SaveError(errptr, db->rep->DropColumnFamily(handle->rep));
}

uint32_t crocksdb_column_family_handle_id(
    crocksdb_column_family_handle_t* handle) {
  return handle->rep->GetID();
}

void crocksdb_column_family_handle_destroy(
    crocksdb_column_family_handle_t* handle) {
  delete handle->rep;
  delete handle;
}

void crocksdb_put(crocksdb_t* db, const crocksdb_writeoptions_t* options,
                  const char* key, size_t keylen, const char* val,
                  size_t vallen, char** errptr) {
  SaveError(errptr,
            db->rep->Put(options->rep, Slice(key, keylen), Slice(val, vallen)));
}

void crocksdb_put_cf(crocksdb_t* db, const crocksdb_writeoptions_t* options,
                     crocksdb_column_family_handle_t* column_family,
                     const char* key, size_t keylen, const char* val,
                     size_t vallen, char** errptr) {
  SaveError(errptr, db->rep->Put(options->rep, column_family->rep,
                                 Slice(key, keylen), Slice(val, vallen)));
}

void crocksdb_delete(crocksdb_t* db, const crocksdb_writeoptions_t* options,
                     const char* key, size_t keylen, char** errptr) {
  SaveError(errptr, db->rep->Delete(options->rep, Slice(key, keylen)));
}

void crocksdb_delete_cf(crocksdb_t* db, const crocksdb_writeoptions_t* options,
                        crocksdb_column_family_handle_t* column_family,
                        const char* key, size_t keylen, char** errptr) {
  SaveError(errptr, db->rep->Delete(options->rep, column_family->rep,
                                    Slice(key, keylen)));
}

void crocksdb_single_delete(crocksdb_t* db,
                            const crocksdb_writeoptions_t* options,
                            const char* key, size_t keylen, char** errptr) {
  SaveError(errptr, db->rep->SingleDelete(options->rep, Slice(key, keylen)));
}

void crocksdb_single_delete_cf(crocksdb_t* db,
                               const crocksdb_writeoptions_t* options,
                               crocksdb_column_family_handle_t* column_family,
                               const char* key, size_t keylen, char** errptr) {
  SaveError(errptr, db->rep->SingleDelete(options->rep, column_family->rep,
                                          Slice(key, keylen)));
}

void crocksdb_delete_range_cf(crocksdb_t* db,
                              const crocksdb_writeoptions_t* options,
                              crocksdb_column_family_handle_t* column_family,
                              const char* begin_key, size_t begin_keylen,
                              const char* end_key, size_t end_keylen,
                              char** errptr) {
  SaveError(errptr, db->rep->DeleteRange(options->rep, column_family->rep,
                                         Slice(begin_key, begin_keylen),
                                         Slice(end_key, end_keylen)));
}

void crocksdb_merge(crocksdb_t* db, const crocksdb_writeoptions_t* options,
                    const char* key, size_t keylen, const char* val,
                    size_t vallen, char** errptr) {
  SaveError(errptr, db->rep->Merge(options->rep, Slice(key, keylen),
                                   Slice(val, vallen)));
}

void crocksdb_merge_cf(crocksdb_t* db, const crocksdb_writeoptions_t* options,
                       crocksdb_column_family_handle_t* column_family,
                       const char* key, size_t keylen, const char* val,
                       size_t vallen, char** errptr) {
  SaveError(errptr, db->rep->Merge(options->rep, column_family->rep,
                                   Slice(key, keylen), Slice(val, vallen)));
}

void crocksdb_write(crocksdb_t* db, const crocksdb_writeoptions_t* options,
                    crocksdb_writebatch_t* batch, char** errptr) {
  SaveError(errptr, db->rep->Write(options->rep, &batch->rep));
}

void crocksdb_write_callback(crocksdb_t* db,
                             const crocksdb_writeoptions_t* options,
                             crocksdb_writebatch_t* batch,
                             crocksdb_post_write_callback_t* callback,
                             char** errptr) {
  SaveError(errptr, db->rep->Write(options->rep, &batch->rep, callback));
}

void crocksdb_write_multi_batch(crocksdb_t* db,
                                const crocksdb_writeoptions_t* options,
                                crocksdb_writebatch_t** batches,
                                size_t batch_size, char** errptr) {
  std::vector<WriteBatch*> ws;
  for (size_t i = 0; i < batch_size; i++) {
    ws.push_back(&batches[i]->rep);
  }
  SaveError(errptr,
            db->rep->MultiBatchWrite(options->rep, std::move(ws), nullptr));
}

void crocksdb_write_multi_batch_callback(
    crocksdb_t* db, const crocksdb_writeoptions_t* options,
    crocksdb_writebatch_t** batches, size_t batch_size,
    crocksdb_post_write_callback_t* callback, char** errptr) {
  std::vector<WriteBatch*> ws;
  for (size_t i = 0; i < batch_size; i++) {
    ws.push_back(&batches[i]->rep);
  }
  SaveError(errptr,
            db->rep->MultiBatchWrite(options->rep, std::move(ws), callback));
}

char* crocksdb_get(crocksdb_t* db, const crocksdb_readoptions_t* options,
                   const char* key, size_t keylen, size_t* vallen,
                   char** errptr) {
  char* result = nullptr;
  std::string tmp;
  Status s = db->rep->Get(options->rep, Slice(key, keylen), &tmp);
  if (s.ok()) {
    *vallen = tmp.size();
    result = CopyString(tmp);
  } else {
    *vallen = 0;
    if (!s.IsNotFound()) {
      SaveError(errptr, s);
    }
  }
  return result;
}

char* crocksdb_get_cf(crocksdb_t* db, const crocksdb_readoptions_t* options,
                      crocksdb_column_family_handle_t* column_family,
                      const char* key, size_t keylen, size_t* vallen,
                      char** errptr) {
  char* result = nullptr;
  std::string tmp;
  Status s =
      db->rep->Get(options->rep, column_family->rep, Slice(key, keylen), &tmp);
  if (s.ok()) {
    *vallen = tmp.size();
    result = CopyString(tmp);
  } else {
    *vallen = 0;
    if (!s.IsNotFound()) {
      SaveError(errptr, s);
    }
  }
  return result;
}

void crocksdb_multi_get(crocksdb_t* db, const crocksdb_readoptions_t* options,
                        size_t num_keys, const char* const* keys_list,
                        const size_t* keys_list_sizes, char** values_list,
                        size_t* values_list_sizes, char** errs) {
  std::vector<Slice> keys(num_keys);
  for (size_t i = 0; i < num_keys; i++) {
    keys[i] = Slice(keys_list[i], keys_list_sizes[i]);
  }
  std::vector<std::string> values(num_keys);
  std::vector<Status> statuses = db->rep->MultiGet(options->rep, keys, &values);
  for (size_t i = 0; i < num_keys; i++) {
    if (statuses[i].ok()) {
      values_list[i] = CopyString(values[i]);
      values_list_sizes[i] = values[i].size();
      errs[i] = nullptr;
    } else {
      values_list[i] = nullptr;
      values_list_sizes[i] = 0;
      if (!statuses[i].IsNotFound()) {
        errs[i] = strdup(statuses[i].ToString().c_str());
      } else {
        errs[i] = nullptr;
      }
    }
  }
}

void crocksdb_multi_get_cf(
    crocksdb_t* db, const crocksdb_readoptions_t* options,
    const crocksdb_column_family_handle_t* const* column_families,
    size_t num_keys, const char* const* keys_list,
    const size_t* keys_list_sizes, char** values_list,
    size_t* values_list_sizes, char** errs) {
  std::vector<Slice> keys(num_keys);
  std::vector<ColumnFamilyHandle*> cfs(num_keys);
  for (size_t i = 0; i < num_keys; i++) {
    keys[i] = Slice(keys_list[i], keys_list_sizes[i]);
    cfs[i] = column_families[i]->rep;
  }
  std::vector<std::string> values(num_keys);
  std::vector<Status> statuses =
      db->rep->MultiGet(options->rep, cfs, keys, &values);
  for (size_t i = 0; i < num_keys; i++) {
    if (statuses[i].ok()) {
      values_list[i] = CopyString(values[i]);
      values_list_sizes[i] = values[i].size();
      errs[i] = nullptr;
    } else {
      values_list[i] = nullptr;
      values_list_sizes[i] = 0;
      if (!statuses[i].IsNotFound()) {
        errs[i] = strdup(statuses[i].ToString().c_str());
      } else {
        errs[i] = nullptr;
      }
    }
  }
}

crocksdb_iterator_t* crocksdb_create_iterator(
    crocksdb_t* db, const crocksdb_readoptions_t* options) {
  crocksdb_iterator_t* result = new crocksdb_iterator_t;
  result->rep = db->rep->NewIterator(options->rep);
  return result;
}

crocksdb_iterator_t* crocksdb_create_iterator_cf(
    crocksdb_t* db, const crocksdb_readoptions_t* options,
    crocksdb_column_family_handle_t* column_family) {
  crocksdb_iterator_t* result = new crocksdb_iterator_t;
  result->rep = db->rep->NewIterator(options->rep, column_family->rep);
  return result;
}

void crocksdb_create_iterators(
    crocksdb_t* db, crocksdb_readoptions_t* opts,
    crocksdb_column_family_handle_t** column_families,
    crocksdb_iterator_t** iterators, size_t size, char** errptr) {
  std::vector<ColumnFamilyHandle*> column_families_vec(size);
  for (size_t i = 0; i < size; i++) {
    column_families_vec.push_back(column_families[i]->rep);
  }

  std::vector<Iterator*> res;
  Status status = db->rep->NewIterators(opts->rep, column_families_vec, &res);
  if (SaveError(errptr, status)) {
    for (size_t i = 0; i < res.size(); i++) {
      delete res[i];
    }
    return;
  }
  assert(res.size() == size);

  for (size_t i = 0; i < size; i++) {
    iterators[i] = new crocksdb_iterator_t;
    iterators[i]->rep = res[i];
  }
}

const crocksdb_snapshot_t* crocksdb_create_snapshot(crocksdb_t* db) {
  crocksdb_snapshot_t* result = new crocksdb_snapshot_t;
  result->rep = db->rep->GetSnapshot();
  return result;
}

void crocksdb_release_snapshot(crocksdb_t* db,
                               const crocksdb_snapshot_t* snapshot) {
  db->rep->ReleaseSnapshot(snapshot->rep);
  delete snapshot;
}

uint64_t crocksdb_get_snapshot_sequence_number(
    const crocksdb_snapshot_t* snapshot) {
  return snapshot->rep->GetSequenceNumber();
}

crocksdb_map_property_t* crocksdb_create_map_property() {
  return new crocksdb_map_property_t;
}

void crocksdb_destroy_map_property(crocksdb_map_property_t* info) {
  delete info;
}

unsigned char crocksdb_get_map_property_cf(
    crocksdb_t* db, crocksdb_column_family_handle_t* column_family,
    const char* property, crocksdb_map_property_t* info) {
  return db->rep->GetMapProperty(column_family->rep, property, &info->rep);
}

char* crocksdb_map_property_value(crocksdb_map_property_t* info,
                                  const char* propname) {
  auto iter = info->rep.find(std::string(propname));
  if (iter != info->rep.end()) {
    return strdup(iter->second.c_str());
  } else {
    return nullptr;
  }
}

uint64_t crocksdb_map_property_int_value(crocksdb_map_property_t* info,
                                         const char* propname) {
  auto iter = info->rep.find(std::string(propname));
  if (iter != info->rep.end()) {
    return (uint64_t)stoll(iter->second, nullptr);
  } else {
    return 0;
  }
}

char* crocksdb_property_value(crocksdb_t* db, const char* propname) {
  std::string tmp;
  if (db->rep->GetProperty(Slice(propname), &tmp)) {
    // We use strdup() since we expect human readable output.
    return strdup(tmp.c_str());
  } else {
    return nullptr;
  }
}

char* crocksdb_property_value_cf(crocksdb_t* db,
                                 crocksdb_column_family_handle_t* column_family,
                                 const char* propname) {
  std::string tmp;
  if (db->rep->GetProperty(column_family->rep, Slice(propname), &tmp)) {
    // We use strdup() since we expect human readable output.
    return strdup(tmp.c_str());
  } else {
    return nullptr;
  }
}

void crocksdb_approximate_sizes(crocksdb_t* db, int num_ranges,
                                const char* const* range_start_key,
                                const size_t* range_start_key_len,
                                const char* const* range_limit_key,
                                const size_t* range_limit_key_len,
                                uint64_t* sizes) {
  Range* ranges = new Range[num_ranges];
  for (int i = 0; i < num_ranges; i++) {
    ranges[i].start = Slice(range_start_key[i], range_start_key_len[i]);
    ranges[i].limit = Slice(range_limit_key[i], range_limit_key_len[i]);
  }
  db->rep->GetApproximateSizes(ranges, num_ranges, sizes);
  delete[] ranges;
}

void crocksdb_approximate_sizes_cf(
    crocksdb_t* db, crocksdb_column_family_handle_t* column_family,
    int num_ranges, const char* const* range_start_key,
    const size_t* range_start_key_len, const char* const* range_limit_key,
    const size_t* range_limit_key_len, uint64_t* sizes) {
  Range* ranges = new Range[num_ranges];
  for (int i = 0; i < num_ranges; i++) {
    ranges[i].start = Slice(range_start_key[i], range_start_key_len[i]);
    ranges[i].limit = Slice(range_limit_key[i], range_limit_key_len[i]);
  }
  db->rep->GetApproximateSizes(column_family->rep, ranges, num_ranges, sizes);
  delete[] ranges;
}

void crocksdb_approximate_memtable_stats(const crocksdb_t* db,
                                         const char* range_start_key,
                                         size_t range_start_key_len,
                                         const char* range_limit_key,
                                         size_t range_limit_key_len,
                                         uint64_t* count, uint64_t* size) {
  auto start = Slice(range_start_key, range_start_key_len);
  auto limit = Slice(range_limit_key, range_limit_key_len);
  Range range(start, limit);
  db->rep->GetApproximateMemTableStats(range, count, size);
}

void crocksdb_approximate_memtable_stats_cf(
    const crocksdb_t* db, const crocksdb_column_family_handle_t* cf,
    const char* range_start_key, size_t range_start_key_len,
    const char* range_limit_key, size_t range_limit_key_len, uint64_t* count,
    uint64_t* size) {
  auto start = Slice(range_start_key, range_start_key_len);
  auto limit = Slice(range_limit_key, range_limit_key_len);
  Range range(start, limit);
  db->rep->GetApproximateMemTableStats(cf->rep, range, count, size);
}

void crocksdb_approximate_active_memtable_stats_cf(
    const crocksdb_t* db, const crocksdb_column_family_handle_t* cf,
    uint64_t* memory_bytes, uint64_t* oldest_key_time) {
  db->rep->GetApproximateActiveMemTableStats(cf->rep, memory_bytes,
                                             oldest_key_time);
}

void crocksdb_delete_file(crocksdb_t* db, const char* name, char** errptr) {
  SaveError(errptr, db->rep->DeleteFile(name));
}

const crocksdb_livefiles_t* crocksdb_livefiles(crocksdb_t* db) {
  crocksdb_livefiles_t* result = new crocksdb_livefiles_t;
  db->rep->GetLiveFilesMetaData(&result->rep);
  return result;
}

void crocksdb_compact_range(crocksdb_t* db, const char* start_key,
                            size_t start_key_len, const char* limit_key,
                            size_t limit_key_len) {
  Slice a, b;
  db->rep->CompactRange(
      CompactRangeOptions(),
      // Pass nullptr Slice if corresponding "const char*" is nullptr
      (start_key ? (a = Slice(start_key, start_key_len), &a) : nullptr),
      (limit_key ? (b = Slice(limit_key, limit_key_len), &b) : nullptr));
}

void crocksdb_compact_range_cf(crocksdb_t* db,
                               crocksdb_column_family_handle_t* column_family,
                               const char* start_key, size_t start_key_len,
                               const char* limit_key, size_t limit_key_len) {
  Slice a, b;
  db->rep->CompactRange(
      CompactRangeOptions(), column_family->rep,
      // Pass nullptr Slice if corresponding "const char*" is nullptr
      (start_key ? (a = Slice(start_key, start_key_len), &a) : nullptr),
      (limit_key ? (b = Slice(limit_key, limit_key_len), &b) : nullptr));
}

void crocksdb_compact_range_opt(crocksdb_t* db, crocksdb_compactoptions_t* opt,
                                const char* start_key, size_t start_key_len,
                                const char* limit_key, size_t limit_key_len) {
  Slice a, b;
  db->rep->CompactRange(
      opt->rep,
      // Pass nullptr Slice if corresponding "const char*" is nullptr
      (start_key ? (a = Slice(start_key, start_key_len), &a) : nullptr),
      (limit_key ? (b = Slice(limit_key, limit_key_len), &b) : nullptr));
}

void crocksdb_compact_range_cf_opt(
    crocksdb_t* db, crocksdb_column_family_handle_t* column_family,
    crocksdb_compactoptions_t* opt, const char* start_key, size_t start_key_len,
    const char* limit_key, size_t limit_key_len) {
  Slice a, b;
  db->rep->CompactRange(
      opt->rep, column_family->rep,
      // Pass nullptr Slice if corresponding "const char*" is nullptr
      (start_key ? (a = Slice(start_key, start_key_len), &a) : nullptr),
      (limit_key ? (b = Slice(limit_key, limit_key_len), &b) : nullptr));
}

void crocksdb_check_in_range(crocksdb_t* db, const char* start_key,
                             size_t start_key_len, const char* limit_key,
                             size_t limit_key_len, char** errptr) {
  Slice a, b;
  SaveError(
      errptr,
      db->rep->CheckInRange(
          // Pass nullptr Slice if corresponding "const char*" is nullptr
          (start_key ? (a = Slice(start_key, start_key_len), &a) : nullptr),
          (limit_key ? (b = Slice(limit_key, limit_key_len), &b) : nullptr)));
}

void crocksdb_flush(crocksdb_t* db, const crocksdb_flushoptions_t* options,
                    char** errptr) {
  SaveError(errptr, db->rep->Flush(options->rep));
}

void crocksdb_flush_cf(crocksdb_t* db,
                       crocksdb_column_family_handle_t* column_family,
                       const crocksdb_flushoptions_t* options, char** errptr) {
  SaveError(errptr, db->rep->Flush(options->rep, column_family->rep));
}

void crocksdb_flush_cfs(crocksdb_t* db,
                        const crocksdb_column_family_handle_t** column_familys,
                        int num_handles, const crocksdb_flushoptions_t* options,
                        char** errptr) {
  std::vector<rocksdb::ColumnFamilyHandle*> handles(num_handles);
  for (int i = 0; i < num_handles; i++) {
    handles[i] = column_familys[i]->rep;
  }
  SaveError(errptr, db->rep->Flush(options->rep, handles));
}

void crocksdb_flush_wal(crocksdb_t* db, unsigned char sync, char** errptr) {
  SaveError(errptr, db->rep->FlushWAL(sync));
}

void crocksdb_sync_wal(crocksdb_t* db, char** errptr) {
  SaveError(errptr, db->rep->SyncWAL());
}

uint64_t crocksdb_get_latest_sequence_number(crocksdb_t* db) {
  return db->rep->GetLatestSequenceNumber();
}

void crocksdb_disable_file_deletions(crocksdb_t* db, char** errptr) {
  SaveError(errptr, db->rep->DisableFileDeletions());
}

void crocksdb_enable_file_deletions(crocksdb_t* db, unsigned char force,
                                    char** errptr) {
  SaveError(errptr, db->rep->EnableFileDeletions(force));
}

crocksdb_options_t* crocksdb_get_db_options(crocksdb_t* db) {
  auto opts = new crocksdb_options_t;
  opts->rep = Options(db->rep->GetDBOptions(), ColumnFamilyOptions());
  return opts;
}

void crocksdb_set_db_options(crocksdb_t* db, const char** names,
                             const char** values, size_t num_options,
                             char** errptr) {
  std::unordered_map<std::string, std::string> options;
  for (size_t i = 0; i < num_options; i++) {
    options.emplace(names[i], values[i]);
  }
  SaveError(errptr, db->rep->SetDBOptions(options));
}

crocksdb_options_t* crocksdb_get_options_cf(
    const crocksdb_t* db, crocksdb_column_family_handle_t* column_family) {
  crocksdb_options_t* options = new crocksdb_options_t;
  options->rep = db->rep->GetOptions(column_family->rep);
  return options;
}

void crocksdb_set_options_cf(crocksdb_t* db,
                             crocksdb_column_family_handle_t* cf,
                             const char** names, const char** values,
                             size_t num_options, char** errptr) {
  std::unordered_map<std::string, std::string> options;
  for (size_t i = 0; i < num_options; i++) {
    options.emplace(names[i], values[i]);
  }
  SaveError(errptr, db->rep->SetOptions(cf->rep, options));
}

void crocksdb_destroy_db(const crocksdb_options_t* options, const char* name,
                         char** errptr) {
  SaveError(errptr, DestroyDB(name, options->rep));
}

void crocksdb_repair_db(const crocksdb_options_t* options, const char* name,
                        char** errptr) {
  SaveError(errptr, RepairDB(name, options->rep));
}

void crocksdb_iter_destroy(crocksdb_iterator_t* iter) {
  delete iter->rep;
  delete iter;
}

unsigned char crocksdb_iter_valid(const crocksdb_iterator_t* iter) {
  return iter->rep->Valid();
}

void crocksdb_iter_seek_to_first(crocksdb_iterator_t* iter) {
  iter->rep->SeekToFirst();
}

void crocksdb_iter_seek_to_last(crocksdb_iterator_t* iter) {
  iter->rep->SeekToLast();
}

void crocksdb_iter_seek(crocksdb_iterator_t* iter, const char* k, size_t klen) {
  iter->rep->Seek(Slice(k, klen));
}

void crocksdb_iter_seek_for_prev(crocksdb_iterator_t* iter, const char* k,
                                 size_t klen) {
  iter->rep->SeekForPrev(Slice(k, klen));
}

void crocksdb_iter_next(crocksdb_iterator_t* iter) { iter->rep->Next(); }

void crocksdb_iter_prev(crocksdb_iterator_t* iter) { iter->rep->Prev(); }

const char* crocksdb_iter_key(const crocksdb_iterator_t* iter, size_t* klen) {
  Slice s = iter->rep->key();
  *klen = s.size();
  return s.data();
}

const char* crocksdb_iter_value(const crocksdb_iterator_t* iter, size_t* vlen) {
  Slice s = iter->rep->value();
  *vlen = s.size();
  return s.data();
}

void crocksdb_iter_get_error(const crocksdb_iterator_t* iter, char** errptr) {
  SaveError(errptr, iter->rep->status());
}

crocksdb_writebatch_t* crocksdb_writebatch_create() {
  return new crocksdb_writebatch_t;
}

crocksdb_writebatch_t* crocksdb_writebatch_create_with_capacity(
    size_t reserved_bytes) {
  crocksdb_writebatch_t* b = new crocksdb_writebatch_t;
  b->rep = WriteBatch(reserved_bytes);
  return b;
}

crocksdb_writebatch_t* crocksdb_writebatch_create_from(const char* rep,
                                                       size_t size) {
  crocksdb_writebatch_t* b = new crocksdb_writebatch_t;
  b->rep = WriteBatch(std::string(rep, size));
  return b;
}

void crocksdb_writebatch_destroy(crocksdb_writebatch_t* b) { delete b; }

void crocksdb_writebatch_clear(crocksdb_writebatch_t* b) { b->rep.Clear(); }

int crocksdb_writebatch_count(crocksdb_writebatch_t* b) {
  return b->rep.Count();
}

void crocksdb_writebatch_put(crocksdb_writebatch_t* b, const char* key,
                             size_t klen, const char* val, size_t vlen) {
  b->rep.Put(Slice(key, klen), Slice(val, vlen));
}

void crocksdb_writebatch_put_cf(crocksdb_writebatch_t* b,
                                crocksdb_column_family_handle_t* column_family,
                                const char* key, size_t klen, const char* val,
                                size_t vlen) {
  b->rep.Put(column_family->rep, Slice(key, klen), Slice(val, vlen));
}

void crocksdb_writebatch_putv(crocksdb_writebatch_t* b, int num_keys,
                              const char* const* keys_list,
                              const size_t* keys_list_sizes, int num_values,
                              const char* const* values_list,
                              const size_t* values_list_sizes) {
  std::vector<Slice> key_slices(num_keys);
  for (int i = 0; i < num_keys; i++) {
    key_slices[i] = Slice(keys_list[i], keys_list_sizes[i]);
  }
  std::vector<Slice> value_slices(num_values);
  for (int i = 0; i < num_values; i++) {
    value_slices[i] = Slice(values_list[i], values_list_sizes[i]);
  }
  b->rep.Put(SliceParts(key_slices.data(), num_keys),
             SliceParts(value_slices.data(), num_values));
}

void crocksdb_writebatch_putv_cf(crocksdb_writebatch_t* b,
                                 crocksdb_column_family_handle_t* column_family,
                                 int num_keys, const char* const* keys_list,
                                 const size_t* keys_list_sizes, int num_values,
                                 const char* const* values_list,
                                 const size_t* values_list_sizes) {
  std::vector<Slice> key_slices(num_keys);
  for (int i = 0; i < num_keys; i++) {
    key_slices[i] = Slice(keys_list[i], keys_list_sizes[i]);
  }
  std::vector<Slice> value_slices(num_values);
  for (int i = 0; i < num_values; i++) {
    value_slices[i] = Slice(values_list[i], values_list_sizes[i]);
  }
  b->rep.Put(column_family->rep, SliceParts(key_slices.data(), num_keys),
             SliceParts(value_slices.data(), num_values));
}

void crocksdb_writebatch_merge(crocksdb_writebatch_t* b, const char* key,
                               size_t klen, const char* val, size_t vlen) {
  b->rep.Merge(Slice(key, klen), Slice(val, vlen));
}

void crocksdb_writebatch_merge_cf(
    crocksdb_writebatch_t* b, crocksdb_column_family_handle_t* column_family,
    const char* key, size_t klen, const char* val, size_t vlen) {
  b->rep.Merge(column_family->rep, Slice(key, klen), Slice(val, vlen));
}

void crocksdb_writebatch_mergev(crocksdb_writebatch_t* b, int num_keys,
                                const char* const* keys_list,
                                const size_t* keys_list_sizes, int num_values,
                                const char* const* values_list,
                                const size_t* values_list_sizes) {
  std::vector<Slice> key_slices(num_keys);
  for (int i = 0; i < num_keys; i++) {
    key_slices[i] = Slice(keys_list[i], keys_list_sizes[i]);
  }
  std::vector<Slice> value_slices(num_values);
  for (int i = 0; i < num_values; i++) {
    value_slices[i] = Slice(values_list[i], values_list_sizes[i]);
  }
  b->rep.Merge(SliceParts(key_slices.data(), num_keys),
               SliceParts(value_slices.data(), num_values));
}

void crocksdb_writebatch_mergev_cf(
    crocksdb_writebatch_t* b, crocksdb_column_family_handle_t* column_family,
    int num_keys, const char* const* keys_list, const size_t* keys_list_sizes,
    int num_values, const char* const* values_list,
    const size_t* values_list_sizes) {
  std::vector<Slice> key_slices(num_keys);
  for (int i = 0; i < num_keys; i++) {
    key_slices[i] = Slice(keys_list[i], keys_list_sizes[i]);
  }
  std::vector<Slice> value_slices(num_values);
  for (int i = 0; i < num_values; i++) {
    value_slices[i] = Slice(values_list[i], values_list_sizes[i]);
  }
  b->rep.Merge(column_family->rep, SliceParts(key_slices.data(), num_keys),
               SliceParts(value_slices.data(), num_values));
}

void crocksdb_writebatch_delete(crocksdb_writebatch_t* b, const char* key,
                                size_t klen) {
  b->rep.Delete(Slice(key, klen));
}

void crocksdb_writebatch_delete_cf(
    crocksdb_writebatch_t* b, crocksdb_column_family_handle_t* column_family,
    const char* key, size_t klen) {
  b->rep.Delete(column_family->rep, Slice(key, klen));
}

void crocksdb_writebatch_single_delete(crocksdb_writebatch_t* b,
                                       const char* key, size_t klen) {
  b->rep.SingleDelete(Slice(key, klen));
}

void crocksdb_writebatch_single_delete_cf(
    crocksdb_writebatch_t* b, crocksdb_column_family_handle_t* column_family,
    const char* key, size_t klen) {
  b->rep.SingleDelete(column_family->rep, Slice(key, klen));
}

void crocksdb_writebatch_deletev(crocksdb_writebatch_t* b, int num_keys,
                                 const char* const* keys_list,
                                 const size_t* keys_list_sizes) {
  std::vector<Slice> key_slices(num_keys);
  for (int i = 0; i < num_keys; i++) {
    key_slices[i] = Slice(keys_list[i], keys_list_sizes[i]);
  }
  b->rep.Delete(SliceParts(key_slices.data(), num_keys));
}

void crocksdb_writebatch_deletev_cf(
    crocksdb_writebatch_t* b, crocksdb_column_family_handle_t* column_family,
    int num_keys, const char* const* keys_list, const size_t* keys_list_sizes) {
  std::vector<Slice> key_slices(num_keys);
  for (int i = 0; i < num_keys; i++) {
    key_slices[i] = Slice(keys_list[i], keys_list_sizes[i]);
  }
  b->rep.Delete(column_family->rep, SliceParts(key_slices.data(), num_keys));
}

void crocksdb_writebatch_delete_range(crocksdb_writebatch_t* b,
                                      const char* start_key,
                                      size_t start_key_len, const char* end_key,
                                      size_t end_key_len) {
  b->rep.DeleteRange(Slice(start_key, start_key_len),
                     Slice(end_key, end_key_len));
}

void crocksdb_writebatch_delete_range_cf(
    crocksdb_writebatch_t* b, crocksdb_column_family_handle_t* column_family,
    const char* start_key, size_t start_key_len, const char* end_key,
    size_t end_key_len) {
  b->rep.DeleteRange(column_family->rep, Slice(start_key, start_key_len),
                     Slice(end_key, end_key_len));
}

void crocksdb_writebatch_delete_rangev(crocksdb_writebatch_t* b, int num_keys,
                                       const char* const* start_keys_list,
                                       const size_t* start_keys_list_sizes,
                                       const char* const* end_keys_list,
                                       const size_t* end_keys_list_sizes) {
  std::vector<Slice> start_key_slices(num_keys);
  std::vector<Slice> end_key_slices(num_keys);
  for (int i = 0; i < num_keys; i++) {
    start_key_slices[i] = Slice(start_keys_list[i], start_keys_list_sizes[i]);
    end_key_slices[i] = Slice(end_keys_list[i], end_keys_list_sizes[i]);
  }
  b->rep.DeleteRange(SliceParts(start_key_slices.data(), num_keys),
                     SliceParts(end_key_slices.data(), num_keys));
}

void crocksdb_writebatch_delete_rangev_cf(
    crocksdb_writebatch_t* b, crocksdb_column_family_handle_t* column_family,
    int num_keys, const char* const* start_keys_list,
    const size_t* start_keys_list_sizes, const char* const* end_keys_list,
    const size_t* end_keys_list_sizes) {
  std::vector<Slice> start_key_slices(num_keys);
  std::vector<Slice> end_key_slices(num_keys);
  for (int i = 0; i < num_keys; i++) {
    start_key_slices[i] = Slice(start_keys_list[i], start_keys_list_sizes[i]);
    end_key_slices[i] = Slice(end_keys_list[i], end_keys_list_sizes[i]);
  }
  b->rep.DeleteRange(column_family->rep,
                     SliceParts(start_key_slices.data(), num_keys),
                     SliceParts(end_key_slices.data(), num_keys));
}

void crocksdb_writebatch_put_log_data(crocksdb_writebatch_t* b,
                                      const char* blob, size_t len) {
  b->rep.PutLogData(Slice(blob, len));
}

void crocksdb_writebatch_iterate(crocksdb_writebatch_t* b, void* state,
                                 void (*put)(void*, const char* k, size_t klen,
                                             const char* v, size_t vlen),
                                 void (*deleted)(void*, const char* k,
                                                 size_t klen)) {
  class HandlerWrapper : public WriteBatch::Handler {
   public:
    void* state_;
    void (*put_)(void*, const char* k, size_t klen, const char* v, size_t vlen);
    void (*deleted_)(void*, const char* k, size_t klen);
    void Put(const Slice& key, const Slice& value) override {
      (*put_)(state_, key.data(), key.size(), value.data(), value.size());
    }
    void Delete(const Slice& key) override {
      (*deleted_)(state_, key.data(), key.size());
    }
  };
  HandlerWrapper handler;
  handler.state_ = state;
  handler.put_ = put;
  handler.deleted_ = deleted;
  b->rep.Iterate(&handler);
}

void crocksdb_writebatch_iterate_cf(
    crocksdb_writebatch_t* b, void* state,
    void (*put)(void*, const char* k, size_t klen, const char* v, size_t vlen),
    void (*put_cf)(void*, uint32_t cf, const char* k, size_t klen,
                   const char* v, size_t vlen),
    void (*deleted)(void*, const char* k, size_t klen),
    void (*deleted_cf)(void*, uint32_t cf, const char* k, size_t klen)) {
  class HandlerWrapper : public WriteBatch::Handler {
   public:
    void* state_;
    void (*put_)(void*, const char* k, size_t klen, const char* v, size_t vlen);
    void (*put_cf_)(void*, uint32_t cf, const char* k, size_t klen,
                    const char* v, size_t vlen);
    void (*deleted_)(void*, const char* k, size_t klen);
    void (*deleted_cf_)(void*, uint32_t cf, const char* k, size_t klen);

    void Put(const Slice& key, const Slice& value) override {
      (*put_)(state_, key.data(), key.size(), value.data(), value.size());
    }

    Status PutCF(uint32_t column_family_id, const Slice& key,
                 const Slice& value) override {
      (*put_cf_)(state_, column_family_id, key.data(), key.size(), value.data(),
                 value.size());
      return Status::OK();
    }

    void Delete(const Slice& key) override {
      (*deleted_)(state_, key.data(), key.size());
    }

    Status DeleteCF(uint32_t column_family_id, const Slice& key) override {
      (*deleted_cf_)(state_, column_family_id, key.data(), key.size());
      return Status::OK();
    }
  };
  HandlerWrapper handler;
  handler.state_ = state;
  handler.put_ = put;
  handler.put_cf_ = put_cf;
  handler.deleted_ = deleted;
  handler.deleted_cf_ = deleted_cf;
  b->rep.Iterate(&handler);
}

const char* crocksdb_writebatch_data(crocksdb_writebatch_t* b, size_t* size) {
  *size = b->rep.GetDataSize();
  return b->rep.Data().c_str();
}

void crocksdb_writebatch_set_save_point(crocksdb_writebatch_t* b) {
  b->rep.SetSavePoint();
}

void crocksdb_writebatch_pop_save_point(crocksdb_writebatch_t* b,
                                        char** errptr) {
  SaveError(errptr, b->rep.PopSavePoint());
}

void crocksdb_writebatch_rollback_to_save_point(crocksdb_writebatch_t* b,
                                                char** errptr) {
  SaveError(errptr, b->rep.RollbackToSavePoint());
}

void crocksdb_writebatch_set_content(crocksdb_writebatch_t* b, const char* data,
                                     size_t dlen) {
  rocksdb::WriteBatchInternal::SetContents(&b->rep, Slice(data, dlen));
}

void crocksdb_writebatch_append_content(crocksdb_writebatch_t* dest,
                                        const char* data, size_t dlen) {
  rocksdb::WriteBatchInternal::AppendContents(&dest->rep, Slice(data, dlen));
}

int crocksdb_writebatch_ref_count(const char* data, size_t dlen) {
  Slice s(data, dlen);
  rocksdb::WriteBatch::WriteBatchRef ref(s);
  return ref.Count();
}

crocksdb_writebatch_iterator_t* crocksdb_writebatch_ref_iterator_create(
    const char* data, size_t dlen) {
  Slice input(data, dlen);
  rocksdb::WriteBatch::WriteBatchRef ref(input);
  auto it = new crocksdb_writebatch_iterator_t;
  it->rep = ref.NewIterator();
  it->rep->SeekToFirst();
  return it;
}

crocksdb_writebatch_iterator_t* crocksdb_writebatch_iterator_create(
    crocksdb_writebatch_t* dest) {
  auto it = new crocksdb_writebatch_iterator_t;
  it->rep = dest->rep.NewIterator();
  it->rep->SeekToFirst();
  return it;
}

void crocksdb_writebatch_iterator_destroy(crocksdb_writebatch_iterator_t* it) {
  delete it->rep;
  delete it;
}

unsigned char crocksdb_writebatch_iterator_valid(
    crocksdb_writebatch_iterator_t* it) {
  return it->rep->Valid();
}

void crocksdb_writebatch_iterator_next(crocksdb_writebatch_iterator_t* it) {
  it->rep->Next();
}

const char* crocksdb_writebatch_iterator_key(crocksdb_writebatch_iterator_t* it,
                                             size_t* klen) {
  *klen = it->rep->Key().size();
  return it->rep->Key().data();
}

const char* crocksdb_writebatch_iterator_value(
    crocksdb_writebatch_iterator_t* it, size_t* klen) {
  *klen = it->rep->Value().size();
  return it->rep->Value().data();
}

uint32_t crocksdb_writebatch_iterator_value_type(
    crocksdb_writebatch_iterator_t* it) {
  return static_cast<uint32_t>(it->rep->GetValueType());
}

uint32_t crocksdb_writebatch_iterator_column_family_id(
    crocksdb_writebatch_iterator_t* it) {
  return it->rep->GetColumnFamilyId();
}

crocksdb_block_based_table_options_t* crocksdb_block_based_options_create() {
  return new crocksdb_block_based_table_options_t;
}

void crocksdb_block_based_options_destroy(
    crocksdb_block_based_table_options_t* options) {
  delete options;
}

void crocksdb_block_based_options_set_metadata_block_size(
    crocksdb_block_based_table_options_t* options, size_t block_size) {
  options->rep.metadata_block_size = block_size;
}

void crocksdb_block_based_options_set_block_size(
    crocksdb_block_based_table_options_t* options, size_t block_size) {
  options->rep.block_size = block_size;
}

void crocksdb_block_based_options_set_block_size_deviation(
    crocksdb_block_based_table_options_t* options, int block_size_deviation) {
  options->rep.block_size_deviation = block_size_deviation;
}

void crocksdb_block_based_options_set_block_restart_interval(
    crocksdb_block_based_table_options_t* options, int block_restart_interval) {
  options->rep.block_restart_interval = block_restart_interval;
}

void crocksdb_block_based_options_set_filter_policy(
    crocksdb_block_based_table_options_t* options,
    crocksdb_filterpolicy_t* filter_policy) {
  options->rep.filter_policy.reset(filter_policy);
}

void crocksdb_block_based_options_set_no_block_cache(
    crocksdb_block_based_table_options_t* options,
    unsigned char no_block_cache) {
  options->rep.no_block_cache = no_block_cache;
}

void crocksdb_block_based_options_set_block_cache(
    crocksdb_block_based_table_options_t* options,
    crocksdb_cache_t* block_cache) {
  if (block_cache) {
    options->rep.block_cache = block_cache->rep;
  }
}

void crocksdb_block_based_options_set_whole_key_filtering(
    crocksdb_block_based_table_options_t* options, unsigned char v) {
  options->rep.whole_key_filtering = v;
}

void crocksdb_block_based_options_set_format_version(
    crocksdb_block_based_table_options_t* options, int v) {
  options->rep.format_version = v;
}

void crocksdb_block_based_options_set_index_type(
    crocksdb_block_based_table_options_t* options, uint32_t v) {
  options->rep.index_type = static_cast<BlockBasedTableOptions::IndexType>(v);
}

void crocksdb_block_based_options_set_optimize_filters_for_memory(
    crocksdb_block_based_table_options_t* options, unsigned char v) {
  options->rep.optimize_filters_for_memory = v;
}

void crocksdb_block_based_options_set_partition_filters(
    crocksdb_block_based_table_options_t* options, unsigned char v) {
  options->rep.partition_filters = v;
}

void crocksdb_block_based_options_set_cache_index_and_filter_blocks(
    crocksdb_block_based_table_options_t* options, unsigned char v) {
  options->rep.cache_index_and_filter_blocks = v;
}

void crocksdb_block_based_options_set_pin_top_level_index_and_filter(
    crocksdb_block_based_table_options_t* options, unsigned char v) {
  options->rep.pin_top_level_index_and_filter = v;
}

void crocksdb_block_based_options_set_cache_index_and_filter_blocks_with_high_priority(
    crocksdb_block_based_table_options_t* options, unsigned char v) {
  options->rep.cache_index_and_filter_blocks_with_high_priority = v;
}

void crocksdb_block_based_options_set_pin_l0_filter_and_index_blocks_in_cache(
    crocksdb_block_based_table_options_t* options, unsigned char v) {
  options->rep.pin_l0_filter_and_index_blocks_in_cache = v;
}

void crocksdb_block_based_options_set_read_amp_bytes_per_bit(
    crocksdb_block_based_table_options_t* options, int v) {
  options->rep.read_amp_bytes_per_bit = v;
}

void crocksdb_block_based_options_set_prepopulate_block_cache(
    crocksdb_block_based_table_options_t* options, uint32_t v) {
  options->rep.prepopulate_block_cache =
      static_cast<BlockBasedTableOptions::PrepopulateBlockCache>(v);
}

void crocksdb_block_based_options_set_checksum(
    crocksdb_block_based_table_options_t* options, uint32_t v) {
  options->rep.checksum = static_cast<rocksdb::ChecksumType>(v);
}

void crocksdb_options_set_block_based_table_factory(
    crocksdb_options_t* opt,
    crocksdb_block_based_table_options_t* table_options) {
  if (table_options) {
    opt->rep.table_factory.reset(
        rocksdb::NewBlockBasedTableFactory(table_options->rep));
  }
}

void crocksdb_options_set_max_subcompactions(crocksdb_options_t* opt,
                                             uint32_t v) {
  opt->rep.max_subcompactions = v;
}

void crocksdb_options_set_wal_bytes_per_sync(crocksdb_options_t* opt,
                                             uint64_t v) {
  opt->rep.wal_bytes_per_sync = v;
}

static BlockBasedTableOptions* get_block_based_table_options(
    crocksdb_options_t* opt) {
  if (opt && opt->rep.table_factory != nullptr) {
    BlockBasedTableOptions* table_opt =
        opt->rep.table_factory->GetOptions<BlockBasedTableOptions>();
    if (table_opt &&
        strcmp(opt->rep.table_factory->Name(), block_base_table_str) == 0) {
      return table_opt;
    }
  }
  return nullptr;
}

size_t crocksdb_options_get_block_cache_usage(crocksdb_options_t* opt) {
  auto opts = get_block_based_table_options(opt);
  if (opts && opts->block_cache) {
    return opts->block_cache->GetUsage();
  }
  return 0;
}

void crocksdb_options_set_block_cache_capacity(crocksdb_options_t* opt,
                                               size_t capacity, char** errptr) {
  Status s;
  auto opts = get_block_based_table_options(opt);
  if (opts && opts->block_cache) {
    opts->block_cache->SetCapacity(capacity);
  } else {
    s = Status::InvalidArgument("failed to get block based table options");
  }
  SaveError(errptr, s);
}

size_t crocksdb_options_get_block_cache_capacity(crocksdb_options_t* opt) {
  auto opts = get_block_based_table_options(opt);
  if (opts && opts->block_cache) {
    return opts->block_cache->GetCapacity();
  }
  return 0;
}

/* FlushJobInfo */

const char* crocksdb_flushjobinfo_cf_name(const crocksdb_flushjobinfo_t* info,
                                          size_t* size) {
  *size = info->rep.cf_name.size();
  return info->rep.cf_name.data();
}

const char* crocksdb_flushjobinfo_file_path(const crocksdb_flushjobinfo_t* info,
                                            size_t* size) {
  *size = info->rep.file_path.size();
  return info->rep.file_path.data();
}

const crocksdb_table_properties_t* crocksdb_flushjobinfo_table_properties(
    const crocksdb_flushjobinfo_t* info) {
  return reinterpret_cast<const crocksdb_table_properties_t*>(
      &info->rep.table_properties);
}

unsigned char crocksdb_flushjobinfo_triggered_writes_slowdown(
    const crocksdb_flushjobinfo_t* info) {
  return info->rep.triggered_writes_slowdown;
}

unsigned char crocksdb_flushjobinfo_triggered_writes_stop(
    const crocksdb_flushjobinfo_t* info) {
  return info->rep.triggered_writes_stop;
}

uint64_t crocksdb_flushjobinfo_largest_seqno(
    const crocksdb_flushjobinfo_t* info) {
  return info->rep.largest_seqno;
}

uint64_t crocksdb_flushjobinfo_smallest_seqno(
    const crocksdb_flushjobinfo_t* info) {
  return info->rep.smallest_seqno;
}

void crocksdb_reset_status(crocksdb_status_ptr_t* status_ptr) {
  auto ptr = status_ptr->rep;
  *ptr = Status::OK();
}

/* CompactionJobInfo */

void crocksdb_compactionjobinfo_status(const crocksdb_compactionjobinfo_t* info,
                                       char** errptr) {
  SaveError(errptr, info->rep.status);
}

const char* crocksdb_compactionjobinfo_cf_name(
    const crocksdb_compactionjobinfo_t* info, size_t* size) {
  *size = info->rep.cf_name.size();
  return info->rep.cf_name.data();
}

size_t crocksdb_compactionjobinfo_input_files_count(
    const crocksdb_compactionjobinfo_t* info) {
  return info->rep.input_files.size();
}

const char* crocksdb_compactionjobinfo_input_file_at(
    const crocksdb_compactionjobinfo_t* info, size_t pos, size_t* size) {
  const std::string& path = info->rep.input_files[pos];
  *size = path.size();
  return path.data();
}

size_t crocksdb_compactionjobinfo_output_files_count(
    const crocksdb_compactionjobinfo_t* info) {
  return info->rep.output_files.size();
}

const char* crocksdb_compactionjobinfo_output_file_at(
    const crocksdb_compactionjobinfo_t* info, size_t pos, size_t* size) {
  const std::string& path = info->rep.output_files[pos];
  *size = path.size();
  return path.data();
}

const crocksdb_table_properties_collection_t*
crocksdb_compactionjobinfo_table_properties(
    const crocksdb_compactionjobinfo_t* info) {
  return reinterpret_cast<const crocksdb_table_properties_collection_t*>(
      &info->rep.table_properties);
}

uint64_t crocksdb_compactionjobinfo_elapsed_micros(
    const crocksdb_compactionjobinfo_t* info) {
  return info->rep.stats.elapsed_micros;
}

uint64_t crocksdb_compactionjobinfo_num_corrupt_keys(
    const crocksdb_compactionjobinfo_t* info) {
  return info->rep.stats.num_corrupt_keys;
}

int crocksdb_compactionjobinfo_base_input_level(
    const crocksdb_compactionjobinfo_t* info) {
  return info->rep.base_input_level;
}

int crocksdb_compactionjobinfo_output_level(
    const crocksdb_compactionjobinfo_t* info) {
  return info->rep.output_level;
}

size_t crocksdb_compactionjobinfo_num_input_files(
    const crocksdb_compactionjobinfo_t* info) {
  return info->rep.stats.num_input_files;
}

size_t crocksdb_compactionjobinfo_num_input_files_at_output_level(
    const crocksdb_compactionjobinfo_t* info) {
  return info->rep.stats.num_input_files_at_output_level;
}

uint64_t crocksdb_compactionjobinfo_input_records(
    const crocksdb_compactionjobinfo_t* info) {
  return info->rep.stats.num_input_records;
}

uint64_t crocksdb_compactionjobinfo_output_records(
    const crocksdb_compactionjobinfo_t* info) {
  return info->rep.stats.num_output_records;
}

uint64_t crocksdb_compactionjobinfo_total_input_bytes(
    const crocksdb_compactionjobinfo_t* info) {
  return info->rep.stats.total_input_bytes;
}

uint64_t crocksdb_compactionjobinfo_total_output_bytes(
    const crocksdb_compactionjobinfo_t* info) {
  return info->rep.stats.total_output_bytes;
}

uint32_t crocksdb_compactionjobinfo_compaction_reason(
    const crocksdb_compactionjobinfo_t* info) {
  return static_cast<uint32_t>(info->rep.compaction_reason);
}

/* SubcompactionJobInfo */

void crocksdb_subcompactionjobinfo_status(
    const crocksdb_subcompactionjobinfo_t* info, char** errptr) {
  SaveError(errptr, info->rep.status);
}

const char* crocksdb_subcompactionjobinfo_cf_name(
    const crocksdb_subcompactionjobinfo_t* info, size_t* size) {
  *size = info->rep.cf_name.size();
  return info->rep.cf_name.data();
}

uint64_t crocksdb_subcompactionjobinfo_thread_id(
    const crocksdb_subcompactionjobinfo_t* info) {
  return info->rep.thread_id;
}

int crocksdb_subcompactionjobinfo_base_input_level(
    const crocksdb_subcompactionjobinfo_t* info) {
  return info->rep.base_input_level;
}

int crocksdb_subcompactionjobinfo_output_level(
    const crocksdb_subcompactionjobinfo_t* info) {
  return info->rep.output_level;
}

/* ExternalFileIngestionInfo */

const char* crocksdb_externalfileingestioninfo_cf_name(
    const crocksdb_externalfileingestioninfo_t* info, size_t* size) {
  *size = info->rep.cf_name.size();
  return info->rep.cf_name.data();
}

const char* crocksdb_externalfileingestioninfo_internal_file_path(
    const crocksdb_externalfileingestioninfo_t* info, size_t* size) {
  *size = info->rep.internal_file_path.size();
  return info->rep.internal_file_path.data();
}

const crocksdb_table_properties_t*
crocksdb_externalfileingestioninfo_table_properties(
    const crocksdb_externalfileingestioninfo_t* info) {
  return reinterpret_cast<const crocksdb_table_properties_t*>(
      &info->rep.table_properties);
}

const int crocksdb_externalfileingestioninfo_picked_level(
    const crocksdb_externalfileingestioninfo_t* info) {
  return info->rep.picked_level;
}

/* External write stall info */
extern C_ROCKSDB_LIBRARY_API const char* crocksdb_writestallinfo_cf_name(
    const crocksdb_writestallinfo_t* info, size_t* size) {
  *size = info->rep.cf_name.size();
  return info->rep.cf_name.data();
}

const crocksdb_writestallcondition_t* crocksdb_writestallinfo_cur(
    const crocksdb_writestallinfo_t* info) {
  return reinterpret_cast<const crocksdb_writestallcondition_t*>(
      &info->rep.condition.cur);
}

const crocksdb_writestallcondition_t* crocksdb_writestallinfo_prev(
    const crocksdb_writestallinfo_t* info) {
  return reinterpret_cast<const crocksdb_writestallcondition_t*>(
      &info->rep.condition.prev);
}

const char* crocksdb_memtableinfo_cf_name(const crocksdb_memtableinfo_t* info,
                                          size_t* size) {
  *size = info->rep.cf_name.size();
  return info->rep.cf_name.data();
}

uint64_t crocksdb_memtableinfo_first_seqno(
    const crocksdb_memtableinfo_t* info) {
  return info->rep.first_seqno;
}
uint64_t crocksdb_memtableinfo_earliest_seqno(
    const crocksdb_memtableinfo_t* info) {
  return info->rep.earliest_seqno;
}
uint64_t crocksdb_memtableinfo_largest_seqno(
    const crocksdb_memtableinfo_t* info) {
  return info->rep.largest_seqno;
}
uint64_t crocksdb_memtableinfo_num_entries(
    const crocksdb_memtableinfo_t* info) {
  return info->rep.num_entries;
}
uint64_t crocksdb_memtableinfo_num_deletes(
    const crocksdb_memtableinfo_t* info) {
  return info->rep.num_deletes;
}

/* event listener */

struct crocksdb_eventlistener_t : public EventListener {
  void* state_;
  void (*destructor_)(void*);
  void (*on_flush_begin)(void*, crocksdb_t*, const crocksdb_flushjobinfo_t*);
  void (*on_flush_completed)(void*, crocksdb_t*,
                             const crocksdb_flushjobinfo_t*);
  void (*on_compaction_begin)(void*, crocksdb_t*,
                              const crocksdb_compactionjobinfo_t*);
  void (*on_compaction_completed)(void*, crocksdb_t*,
                                  const crocksdb_compactionjobinfo_t*);
  void (*on_subcompaction_begin)(void*, const crocksdb_subcompactionjobinfo_t*);
  void (*on_subcompaction_completed)(void*,
                                     const crocksdb_subcompactionjobinfo_t*);
  void (*on_external_file_ingested)(
      void*, crocksdb_t*, const crocksdb_externalfileingestioninfo_t*);
  void (*on_background_error)(void*, uint32_t, crocksdb_status_ptr_t*);
  void (*on_stall_conditions_changed)(void*, const crocksdb_writestallinfo_t*);
  void (*on_memtable_sealed)(void*, const crocksdb_memtableinfo_t*);

  virtual void OnFlushBegin(DB* db, const FlushJobInfo& info) {
    crocksdb_t c_db = {db};
    on_flush_begin(state_, &c_db,
                   reinterpret_cast<const crocksdb_flushjobinfo_t*>(&info));
  }

  virtual void OnFlushCompleted(DB* db, const FlushJobInfo& info) {
    crocksdb_t c_db = {db};
    on_flush_completed(state_, &c_db,
                       reinterpret_cast<const crocksdb_flushjobinfo_t*>(&info));
  }

  virtual void OnCompactionBegin(DB* db, const CompactionJobInfo& info) {
    crocksdb_t c_db = {db};
    on_compaction_begin(
        state_, &c_db,
        reinterpret_cast<const crocksdb_compactionjobinfo_t*>(&info));
  }

  virtual void OnCompactionCompleted(DB* db, const CompactionJobInfo& info) {
    crocksdb_t c_db = {db};
    on_compaction_completed(
        state_, &c_db,
        reinterpret_cast<const crocksdb_compactionjobinfo_t*>(&info));
  }

  virtual void OnSubcompactionBegin(const SubcompactionJobInfo& info) {
    on_subcompaction_begin(
        state_,
        reinterpret_cast<const crocksdb_subcompactionjobinfo_t*>(&info));
  }

  virtual void OnSubcompactionCompleted(const SubcompactionJobInfo& info) {
    on_subcompaction_completed(
        state_,
        reinterpret_cast<const crocksdb_subcompactionjobinfo_t*>(&info));
  }

  virtual void OnExternalFileIngested(DB* db,
                                      const ExternalFileIngestionInfo& info) {
    crocksdb_t c_db = {db};
    on_external_file_ingested(
        state_, &c_db,
        reinterpret_cast<const crocksdb_externalfileingestioninfo_t*>(&info));
  }

  virtual void OnBackgroundError(BackgroundErrorReason reason, Status* status) {
    crocksdb_status_ptr_t* s = new crocksdb_status_ptr_t;
    s->rep = status;
    on_background_error(state_, static_cast<uint32_t>(reason), s);
    delete s;
  }

  virtual void OnStallConditionsChanged(const WriteStallInfo& info) {
    on_stall_conditions_changed(
        state_, reinterpret_cast<const crocksdb_writestallinfo_t*>(&info));
  }

  virtual void OnMemTableSealed(const MemTableInfo& info) {
    on_memtable_sealed(state_,
                       reinterpret_cast<const crocksdb_memtableinfo_t*>(&info));
  }

  virtual ~crocksdb_eventlistener_t() { destructor_(state_); }
};

crocksdb_eventlistener_t* crocksdb_eventlistener_create(
    void* state_, void (*destructor_)(void*), on_flush_begin_cb on_flush_begin,
    on_flush_completed_cb on_flush_completed,
    on_compaction_begin_cb on_compaction_begin,
    on_compaction_completed_cb on_compaction_completed,
    on_subcompaction_begin_cb on_subcompaction_begin,
    on_subcompaction_completed_cb on_subcompaction_completed,
    on_external_file_ingested_cb on_external_file_ingested,
    on_background_error_cb on_background_error,
    on_stall_conditions_changed_cb on_stall_conditions_changed,
    on_memtable_sealed_cb on_memtable_sealed) {
  crocksdb_eventlistener_t* et = new crocksdb_eventlistener_t;
  et->state_ = state_;
  et->destructor_ = destructor_;
  et->on_flush_begin = on_flush_begin;
  et->on_flush_completed = on_flush_completed;
  et->on_compaction_begin = on_compaction_begin;
  et->on_compaction_completed = on_compaction_completed;
  et->on_subcompaction_begin = on_subcompaction_begin;
  et->on_subcompaction_completed = on_subcompaction_completed;
  et->on_external_file_ingested = on_external_file_ingested;
  et->on_background_error = on_background_error;
  et->on_stall_conditions_changed = on_stall_conditions_changed;
  et->on_memtable_sealed = on_memtable_sealed;
  return et;
}

void crocksdb_eventlistener_destroy(crocksdb_eventlistener_t* t) { delete t; }

void crocksdb_options_add_eventlistener(crocksdb_options_t* opt,
                                        crocksdb_eventlistener_t* t) {
  opt->rep.listeners.emplace_back(std::shared_ptr<EventListener>(t));
}

crocksdb_cuckoo_table_options_t* crocksdb_cuckoo_options_create() {
  return new crocksdb_cuckoo_table_options_t;
}

void crocksdb_cuckoo_options_destroy(crocksdb_cuckoo_table_options_t* options) {
  delete options;
}

void crocksdb_cuckoo_options_set_hash_ratio(
    crocksdb_cuckoo_table_options_t* options, double v) {
  options->rep.hash_table_ratio = v;
}

void crocksdb_cuckoo_options_set_max_search_depth(
    crocksdb_cuckoo_table_options_t* options, uint32_t v) {
  options->rep.max_search_depth = v;
}

void crocksdb_cuckoo_options_set_cuckoo_block_size(
    crocksdb_cuckoo_table_options_t* options, uint32_t v) {
  options->rep.cuckoo_block_size = v;
}

void crocksdb_cuckoo_options_set_identity_as_first_hash(
    crocksdb_cuckoo_table_options_t* options, unsigned char v) {
  options->rep.identity_as_first_hash = v;
}

void crocksdb_cuckoo_options_set_use_module_hash(
    crocksdb_cuckoo_table_options_t* options, unsigned char v) {
  options->rep.use_module_hash = v;
}

void crocksdb_options_set_cuckoo_table_factory(
    crocksdb_options_t* opt, crocksdb_cuckoo_table_options_t* table_options) {
  if (table_options) {
    opt->rep.table_factory.reset(
        rocksdb::NewCuckooTableFactory(table_options->rep));
  }
}

crocksdb_options_t* crocksdb_options_create() { return new crocksdb_options_t; }

crocksdb_options_t* crocksdb_options_copy(const crocksdb_options_t* other) {
  return new crocksdb_options_t{Options(other->rep)};
}

void crocksdb_options_destroy(crocksdb_options_t* options) { delete options; }

void crocksdb_column_family_descriptor_destroy(
    crocksdb_column_family_descriptor* cf_desc) {
  delete cf_desc;
}

const char* crocksdb_name_from_column_family_descriptor(
    const crocksdb_column_family_descriptor* cf_desc) {
  return cf_desc->rep.name.c_str();
}

crocksdb_options_t* crocksdb_options_from_column_family_descriptor(
    const crocksdb_column_family_descriptor* cf_desc) {
  crocksdb_options_t* options = new crocksdb_options_t;
  *static_cast<ColumnFamilyOptions*>(&options->rep) = cf_desc->rep.options;
  return options;
}

void crocksdb_options_increase_parallelism(crocksdb_options_t* opt,
                                           int total_threads) {
  opt->rep.IncreaseParallelism(total_threads);
}

void crocksdb_options_optimize_for_point_lookup(crocksdb_options_t* opt,
                                                uint64_t block_cache_size_mb) {
  opt->rep.OptimizeForPointLookup(block_cache_size_mb);
}

void crocksdb_options_optimize_level_style_compaction(
    crocksdb_options_t* opt, uint64_t memtable_memory_budget) {
  opt->rep.OptimizeLevelStyleCompaction(memtable_memory_budget);
}

void crocksdb_options_optimize_universal_style_compaction(
    crocksdb_options_t* opt, uint64_t memtable_memory_budget) {
  opt->rep.OptimizeUniversalStyleCompaction(memtable_memory_budget);
}

void crocksdb_options_set_compaction_filter(
    crocksdb_options_t* opt, crocksdb_compactionfilter_t* filter) {
  opt->rep.compaction_filter = filter;
}

void crocksdb_options_set_compaction_filter_factory(
    crocksdb_options_t* opt, crocksdb_compactionfilterfactory_t* factory) {
  opt->rep.compaction_filter_factory =
      std::shared_ptr<CompactionFilterFactory>(factory);
}

void crocksdb_options_compaction_readahead_size(crocksdb_options_t* opt,
                                                size_t s) {
  opt->rep.compaction_readahead_size = s;
}

void crocksdb_options_set_comparator(crocksdb_options_t* opt,
                                     crocksdb_comparator_t* cmp) {
  opt->rep.comparator = cmp;
}

void crocksdb_options_set_merge_operator(
    crocksdb_options_t* opt, crocksdb_mergeoperator_t* merge_operator) {
  opt->rep.merge_operator = std::shared_ptr<MergeOperator>(merge_operator);
}

void crocksdb_options_set_create_if_missing(crocksdb_options_t* opt,
                                            unsigned char v) {
  opt->rep.create_if_missing = v;
}

void crocksdb_options_set_create_missing_column_families(
    crocksdb_options_t* opt, unsigned char v) {
  opt->rep.create_missing_column_families = v;
}

void crocksdb_options_set_error_if_exists(crocksdb_options_t* opt,
                                          unsigned char v) {
  opt->rep.error_if_exists = v;
}

void crocksdb_options_set_paranoid_checks(crocksdb_options_t* opt,
                                          unsigned char v) {
  opt->rep.paranoid_checks = v;
}

void crocksdb_options_set_env(crocksdb_options_t* opt, crocksdb_env_t* env) {
  opt->rep.env = (env ? env->rep : nullptr);
}

void crocksdb_options_set_write_buffer_manager(
    crocksdb_options_t* opt, crocksdb_write_buffer_manager_t* wbm) {
  opt->rep.write_buffer_manager = wbm->rep;
}

void crocksdb_options_set_cf_write_buffer_manager(
    crocksdb_options_t* opt, crocksdb_write_buffer_manager_t* wbm) {
  opt->rep.cf_write_buffer_manager = wbm->rep;
}

void crocksdb_options_set_compaction_thread_limiter(
    crocksdb_options_t* opt, crocksdb_concurrent_task_limiter_t* limiter) {
  opt->rep.compaction_thread_limiter = limiter->rep;
}

crocksdb_concurrent_task_limiter_t*
crocksdb_options_get_compaction_thread_limiter(crocksdb_options_t* opt) {
  if (opt->rep.compaction_thread_limiter != nullptr) {
    crocksdb_concurrent_task_limiter_t* limiter =
        new crocksdb_concurrent_task_limiter_t;
    limiter->rep = opt->rep.compaction_thread_limiter;
    return limiter;
  }
  return nullptr;
}

crocksdb_logger_t* crocksdb_logger_create(void* rep, void (*destructor_)(void*),
                                          crocksdb_logger_logv_cb logv) {
  crocksdb_logger_t* logger = new crocksdb_logger_t;
  crocksdb_logger_impl_t* li = new crocksdb_logger_impl_t;
  li->rep = rep;
  li->destructor_ = destructor_;
  li->logv_internal_ = logv;
  logger->rep = std::shared_ptr<Logger>(li);
  return logger;
}

void crocksdb_options_set_info_log(crocksdb_options_t* opt,
                                   crocksdb_logger_t* l) {
  if (l) {
    opt->rep.info_log = l->rep;
  }
}

void crocksdb_options_set_info_log_level(crocksdb_options_t* opt, uint32_t v) {
  opt->rep.info_log_level = static_cast<InfoLogLevel>(v);
}

void crocksdb_options_set_db_write_buffer_size(crocksdb_options_t* opt,
                                               size_t s) {
  opt->rep.db_write_buffer_size = s;
}

void crocksdb_options_set_write_buffer_size(crocksdb_options_t* opt, size_t s) {
  opt->rep.write_buffer_size = s;
}

size_t crocksdb_options_get_write_buffer_size(crocksdb_options_t* opt) {
  return opt->rep.write_buffer_size;
}

void crocksdb_options_set_max_open_files(crocksdb_options_t* opt, int n) {
  opt->rep.max_open_files = n;
}

void crocksdb_options_set_max_total_wal_size(crocksdb_options_t* opt,
                                             uint64_t n) {
  opt->rep.max_total_wal_size = n;
}

void crocksdb_options_set_target_file_size_base(crocksdb_options_t* opt,
                                                uint64_t n) {
  opt->rep.target_file_size_base = n;
}

uint64_t crocksdb_options_get_target_file_size_base(
    const crocksdb_options_t* opt) {
  return opt->rep.target_file_size_base;
}

void crocksdb_options_set_target_file_size_multiplier(crocksdb_options_t* opt,
                                                      int n) {
  opt->rep.target_file_size_multiplier = n;
}

void crocksdb_options_set_max_bytes_for_level_base(crocksdb_options_t* opt,
                                                   uint64_t n) {
  opt->rep.max_bytes_for_level_base = n;
}

uint64_t crocksdb_options_get_max_bytes_for_level_base(
    crocksdb_options_t* opt) {
  return opt->rep.max_bytes_for_level_base;
}

void crocksdb_options_set_level_compaction_dynamic_level_bytes(
    crocksdb_options_t* opt, unsigned char v) {
  opt->rep.level_compaction_dynamic_level_bytes = v;
}

unsigned char crocksdb_options_get_level_compaction_dynamic_level_bytes(
    const crocksdb_options_t* options) {
  return options->rep.level_compaction_dynamic_level_bytes;
}

void crocksdb_options_set_max_bytes_for_level_multiplier(
    crocksdb_options_t* opt, double n) {
  opt->rep.max_bytes_for_level_multiplier = n;
}

double crocksdb_options_get_max_bytes_for_level_multiplier(
    crocksdb_options_t* opt) {
  return opt->rep.max_bytes_for_level_multiplier;
}

void crocksdb_options_set_max_compaction_bytes(crocksdb_options_t* opt,
                                               uint64_t n) {
  opt->rep.max_compaction_bytes = n;
}

uint64_t crocksdb_options_get_max_compaction_bytes(crocksdb_options_t* opt) {
  return opt->rep.max_compaction_bytes;
}

void crocksdb_options_set_max_bytes_for_level_multiplier_additional(
    crocksdb_options_t* opt, int* level_values, size_t num_levels) {
  opt->rep.max_bytes_for_level_multiplier_additional.resize(num_levels);
  for (size_t i = 0; i < num_levels; ++i) {
    opt->rep.max_bytes_for_level_multiplier_additional[i] = level_values[i];
  }
}

crocksdb_sst_partitioner_factory_t*
crocksdb_options_get_sst_partitioner_factory(crocksdb_options_t* opt) {
  crocksdb_sst_partitioner_factory_t* factory =
      new crocksdb_sst_partitioner_factory_t;
  factory->rep = opt->rep.sst_partitioner_factory;
  return factory;
}

void crocksdb_options_set_sst_partitioner_factory(
    crocksdb_options_t* opt, crocksdb_sst_partitioner_factory_t* factory) {
  opt->rep.sst_partitioner_factory = factory->rep;
}

void crocksdb_options_set_num_levels(crocksdb_options_t* opt, int n) {
  opt->rep.num_levels = n;
}

int crocksdb_options_get_num_levels(crocksdb_options_t* opt) {
  return opt->rep.num_levels;
}

void crocksdb_options_set_level0_file_num_compaction_trigger(
    crocksdb_options_t* opt, int n) {
  opt->rep.level0_file_num_compaction_trigger = n;
}

int crocksdb_options_get_level0_file_num_compaction_trigger(
    crocksdb_options_t* opt) {
  return opt->rep.level0_file_num_compaction_trigger;
}

void crocksdb_options_set_level0_slowdown_writes_trigger(
    crocksdb_options_t* opt, int n) {
  opt->rep.level0_slowdown_writes_trigger = n;
}

int crocksdb_options_get_level0_slowdown_writes_trigger(
    crocksdb_options_t* opt) {
  return opt->rep.level0_slowdown_writes_trigger;
}

void crocksdb_options_set_level0_stop_writes_trigger(crocksdb_options_t* opt,
                                                     int n) {
  opt->rep.level0_stop_writes_trigger = n;
}

int crocksdb_options_get_level0_stop_writes_trigger(crocksdb_options_t* opt) {
  return opt->rep.level0_stop_writes_trigger;
}

void crocksdb_options_set_wal_recovery_mode(crocksdb_options_t* opt,
                                            uint32_t mode) {
  opt->rep.wal_recovery_mode = static_cast<WALRecoveryMode>(mode);
}

void crocksdb_options_set_compression(crocksdb_options_t* opt, int t) {
  opt->rep.compression = static_cast<CompressionType>(t);
}

uint32_t crocksdb_options_get_compression(crocksdb_options_t* opt) {
  return static_cast<uint32_t>(opt->rep.compression);
}

void crocksdb_options_set_compression_per_level(crocksdb_options_t* opt,
                                                uint32_t* level_values,
                                                size_t num_levels) {
  opt->rep.compression_per_level.resize(num_levels);
  for (size_t i = 0; i < num_levels; ++i) {
    opt->rep.compression_per_level[i] =
        static_cast<CompressionType>(level_values[i]);
  }
}

size_t crocksdb_options_get_compression_level_number(crocksdb_options_t* opt) {
  return opt->rep.compression_per_level.size();
}

void crocksdb_options_get_compression_per_level(crocksdb_options_t* opt,
                                                uint32_t* level_values) {
  for (size_t i = 0; i < opt->rep.compression_per_level.size(); i++) {
    level_values[i] = static_cast<uint32_t>(opt->rep.compression_per_level[i]);
  }
}

void crocksdb_options_set_compression_options(crocksdb_options_t* opt,
                                              int w_bits, int level,
                                              int strategy, int max_dict_bytes,
                                              int zstd_max_train_bytes,
                                              int parallel_threads) {
  opt->rep.compression_opts.window_bits = w_bits;
  opt->rep.compression_opts.level = level;
  opt->rep.compression_opts.strategy = strategy;
  opt->rep.compression_opts.max_dict_bytes = max_dict_bytes;
  opt->rep.compression_opts.zstd_max_train_bytes = zstd_max_train_bytes;
  opt->rep.compression_opts.parallel_threads = parallel_threads;
}

void crocksdb_options_set_bottommost_compression_options(
    crocksdb_options_t* opt, int w_bits, int level, int strategy,
    int max_dict_bytes, int zstd_max_train_bytes, int parallel_threads) {
  opt->rep.bottommost_compression_opts.window_bits = w_bits;
  opt->rep.bottommost_compression_opts.level = level;
  opt->rep.bottommost_compression_opts.strategy = strategy;
  opt->rep.bottommost_compression_opts.max_dict_bytes = max_dict_bytes;
  opt->rep.bottommost_compression_opts.zstd_max_train_bytes =
      zstd_max_train_bytes;
  opt->rep.bottommost_compression_opts.parallel_threads = parallel_threads;
  opt->rep.bottommost_compression_opts.enabled = true;
}

void crocksdb_options_set_use_direct_reads(crocksdb_options_t* opt,
                                           unsigned char v) {
  opt->rep.use_direct_reads = v;
}

void crocksdb_options_set_use_direct_io_for_flush_and_compaction(
    crocksdb_options_t* opt, unsigned char v) {
  opt->rep.use_direct_io_for_flush_and_compaction = v;
}

void crocksdb_options_set_prefix_extractor(
    crocksdb_options_t* opt, crocksdb_slicetransform_t* prefix_extractor) {
  opt->rep.prefix_extractor.reset(prefix_extractor);
}

void crocksdb_options_set_optimize_filters_for_hits(crocksdb_options_t* opt,
                                                    unsigned char v) {
  opt->rep.optimize_filters_for_hits = v;
}

void crocksdb_options_set_memtable_insert_with_hint_prefix_extractor(
    crocksdb_options_t* opt, crocksdb_slicetransform_t* prefix_extractor) {
  opt->rep.memtable_insert_with_hint_prefix_extractor.reset(prefix_extractor);
}

void crocksdb_options_set_use_fsync(crocksdb_options_t* opt, int use_fsync) {
  opt->rep.use_fsync = use_fsync;
}

void crocksdb_options_set_db_paths(crocksdb_options_t* opt,
                                   const char* const* dbpath_list,
                                   const size_t* path_lens,
                                   const uint64_t* target_size, int num_paths) {
  std::vector<DbPath> db_paths;
  for (int i = 0; i < num_paths; ++i) {
    db_paths.emplace_back(
        DbPath(std::string(dbpath_list[i], path_lens[i]), target_size[i]));
  }
  opt->rep.db_paths = db_paths;
}

size_t crocksdb_options_get_db_paths_num(crocksdb_options_t* opt) {
  return opt->rep.db_paths.size();
}

const char* crocksdb_options_get_db_path(crocksdb_options_t* opt,
                                         size_t index) {
  return opt->rep.db_paths[index].path.data();
}

uint64_t crocksdb_options_get_path_target_size(crocksdb_options_t* opt,
                                               size_t index) {
  return opt->rep.db_paths[index].target_size;
}

void crocksdb_options_set_db_log_dir(crocksdb_options_t* opt,
                                     const char* db_log_dir) {
  opt->rep.db_log_dir = db_log_dir;
}

void crocksdb_options_set_wal_dir(crocksdb_options_t* opt, const char* v) {
  opt->rep.wal_dir = v;
}

void crocksdb_options_set_wal_ttl_seconds(crocksdb_options_t* opt,
                                          uint64_t ttl) {
  opt->rep.WAL_ttl_seconds = ttl;
}

void crocksdb_options_set_wal_size_limit_mb(crocksdb_options_t* opt,
                                            uint64_t limit) {
  opt->rep.WAL_size_limit_MB = limit;
}

void crocksdb_options_set_manifest_preallocation_size(crocksdb_options_t* opt,
                                                      size_t v) {
  opt->rep.manifest_preallocation_size = v;
}

void crocksdb_options_set_allow_mmap_reads(crocksdb_options_t* opt,
                                           unsigned char v) {
  opt->rep.allow_mmap_reads = v;
}

void crocksdb_options_set_allow_mmap_writes(crocksdb_options_t* opt,
                                            unsigned char v) {
  opt->rep.allow_mmap_writes = v;
}

void crocksdb_options_set_is_fd_close_on_exec(crocksdb_options_t* opt,
                                              unsigned char v) {
  opt->rep.is_fd_close_on_exec = v;
}

void crocksdb_options_set_stats_dump_period_sec(crocksdb_options_t* opt,
                                                unsigned int v) {
  opt->rep.stats_dump_period_sec = v;
}

void crocksdb_options_set_stats_persist_period_sec(crocksdb_options_t* opt,
                                                   uint32_t v) {
  opt->rep.stats_persist_period_sec = v;
}

void crocksdb_options_set_advise_random_on_open(crocksdb_options_t* opt,
                                                unsigned char v) {
  opt->rep.advise_random_on_open = v;
}

void crocksdb_options_set_access_hint_on_compaction_start(
    crocksdb_options_t* opt, int v) {
  switch (v) {
    case 0:
      opt->rep.access_hint_on_compaction_start = rocksdb::Options::NONE;
      break;
    case 1:
      opt->rep.access_hint_on_compaction_start = rocksdb::Options::NORMAL;
      break;
    case 2:
      opt->rep.access_hint_on_compaction_start = rocksdb::Options::SEQUENTIAL;
      break;
    case 3:
      opt->rep.access_hint_on_compaction_start = rocksdb::Options::WILLNEED;
      break;
  }
}

void crocksdb_options_set_use_adaptive_mutex(crocksdb_options_t* opt,
                                             unsigned char v) {
  opt->rep.use_adaptive_mutex = v;
}

void crocksdb_options_set_bytes_per_sync(crocksdb_options_t* opt, uint64_t v) {
  opt->rep.bytes_per_sync = v;
}

void crocksdb_options_set_enable_pipelined_write(crocksdb_options_t* opt,
                                                 unsigned char v) {
  opt->rep.enable_pipelined_write = v;
}

void crocksdb_options_set_enable_multi_batch_write(crocksdb_options_t* opt,
                                                   unsigned char v) {
  opt->rep.enable_multi_batch_write = v;
}

unsigned char crocksdb_options_is_enable_multi_batch_write(
    crocksdb_options_t* opt) {
  return opt->rep.enable_multi_batch_write;
}

void crocksdb_options_set_unordered_write(crocksdb_options_t* opt,
                                          unsigned char v) {
  opt->rep.unordered_write = v;
}

void crocksdb_options_set_allow_concurrent_memtable_write(
    crocksdb_options_t* opt, unsigned char v) {
  opt->rep.allow_concurrent_memtable_write = v;
}

void crocksdb_options_set_manual_wal_flush(crocksdb_options_t* opt,
                                           unsigned char v) {
  opt->rep.manual_wal_flush = v;
}

void crocksdb_options_set_enable_write_thread_adaptive_yield(
    crocksdb_options_t* opt, unsigned char v) {
  opt->rep.enable_write_thread_adaptive_yield = v;
}

void crocksdb_options_set_max_sequential_skip_in_iterations(
    crocksdb_options_t* opt, uint64_t v) {
  opt->rep.max_sequential_skip_in_iterations = v;
}

void crocksdb_options_set_max_write_buffer_number(crocksdb_options_t* opt,
                                                  int n) {
  opt->rep.max_write_buffer_number = n;
}

int crocksdb_options_get_max_write_buffer_number(crocksdb_options_t* opt) {
  return opt->rep.max_write_buffer_number;
}

void crocksdb_options_set_min_write_buffer_number_to_merge(
    crocksdb_options_t* opt, int n) {
  opt->rep.min_write_buffer_number_to_merge = n;
}

int crocksdb_options_get_min_write_buffer_number_to_merge(
    crocksdb_options_t* opt) {
  return opt->rep.min_write_buffer_number_to_merge;
}

void crocksdb_options_set_max_write_buffer_number_to_maintain(
    crocksdb_options_t* opt, int n) {
  opt->rep.max_write_buffer_number_to_maintain = n;
}

void crocksdb_options_set_max_background_jobs(crocksdb_options_t* opt, int n) {
  opt->rep.max_background_jobs = n;
}

int crocksdb_options_get_max_background_jobs(const crocksdb_options_t* opt) {
  return opt->rep.max_background_jobs;
}

void crocksdb_options_set_max_background_compactions(crocksdb_options_t* opt,
                                                     int n) {
  opt->rep.max_background_compactions = n;
}

int crocksdb_options_get_max_background_compactions(
    const crocksdb_options_t* opt) {
  return opt->rep.max_background_compactions;
}

void crocksdb_options_set_max_background_flushes(crocksdb_options_t* opt,
                                                 int n) {
  opt->rep.max_background_flushes = n;
}

int crocksdb_options_get_max_background_flushes(const crocksdb_options_t* opt) {
  return opt->rep.max_background_flushes;
}

void crocksdb_options_set_max_log_file_size(crocksdb_options_t* opt, size_t v) {
  opt->rep.max_log_file_size = v;
}

void crocksdb_options_set_log_file_time_to_roll(crocksdb_options_t* opt,
                                                size_t v) {
  opt->rep.log_file_time_to_roll = v;
}

void crocksdb_options_set_keep_log_file_num(crocksdb_options_t* opt, size_t v) {
  opt->rep.keep_log_file_num = v;
}

void crocksdb_options_set_recycle_log_file_num(crocksdb_options_t* opt,
                                               size_t v) {
  opt->rep.recycle_log_file_num = v;
}

void crocksdb_options_set_soft_pending_compaction_bytes_limit(
    crocksdb_options_t* opt, size_t v) {
  opt->rep.soft_pending_compaction_bytes_limit = v;
}

size_t crocksdb_options_get_soft_pending_compaction_bytes_limit(
    crocksdb_options_t* opt) {
  return opt->rep.soft_pending_compaction_bytes_limit;
}

void crocksdb_options_set_hard_pending_compaction_bytes_limit(
    crocksdb_options_t* opt, size_t v) {
  opt->rep.hard_pending_compaction_bytes_limit = v;
}

size_t crocksdb_options_get_hard_pending_compaction_bytes_limit(
    crocksdb_options_t* opt) {
  return opt->rep.hard_pending_compaction_bytes_limit;
}

void crocksdb_options_set_max_manifest_file_size(crocksdb_options_t* opt,
                                                 size_t v) {
  opt->rep.max_manifest_file_size = v;
}

void crocksdb_options_set_table_cache_numshardbits(crocksdb_options_t* opt,
                                                   int v) {
  opt->rep.table_cache_numshardbits = v;
}

void crocksdb_options_set_writable_file_max_buffer_size(crocksdb_options_t* opt,
                                                        int v) {
  opt->rep.writable_file_max_buffer_size = v;
}

void crocksdb_options_set_arena_block_size(crocksdb_options_t* opt, size_t v) {
  opt->rep.arena_block_size = v;
}

void crocksdb_options_set_disable_auto_compactions(crocksdb_options_t* opt,
                                                   int disable) {
  opt->rep.disable_auto_compactions = disable;
}

int crocksdb_options_get_disable_auto_compactions(
    const crocksdb_options_t* opt) {
  return opt->rep.disable_auto_compactions;
}

void crocksdb_options_set_disable_write_stall(crocksdb_options_t* opt,
                                              unsigned char disable) {
  opt->rep.disable_write_stall = disable;
}

unsigned char crocksdb_options_get_disable_write_stall(
    const crocksdb_options_t* opt) {
  return opt->rep.disable_write_stall;
}

void crocksdb_options_set_delete_obsolete_files_period_micros(
    crocksdb_options_t* opt, uint64_t v) {
  opt->rep.delete_obsolete_files_period_micros = v;
}

void crocksdb_options_prepare_for_bulk_load(crocksdb_options_t* opt) {
  opt->rep.PrepareForBulkLoad();
}

void crocksdb_options_set_memtable_vector_rep(crocksdb_options_t* opt) {
  opt->rep.memtable_factory.reset(new rocksdb::VectorRepFactory);
}

void crocksdb_options_set_memtable_prefix_bloom_size_ratio(
    crocksdb_options_t* opt, double v) {
  opt->rep.memtable_prefix_bloom_size_ratio = v;
}

void crocksdb_options_set_memtable_huge_page_size(crocksdb_options_t* opt,
                                                  size_t v) {
  opt->rep.memtable_huge_page_size = v;
}
const char* crocksdb_options_get_memtable_factory_name(
    crocksdb_options_t* opt) {
  if (!opt->rep.memtable_factory) {
    return nullptr;
  }
  return opt->rep.memtable_factory->Name();
}

void crocksdb_options_set_hash_skip_list_rep(
    crocksdb_options_t* opt, size_t bucket_count, int32_t skiplist_height,
    int32_t skiplist_branching_factor) {
  rocksdb::MemTableRepFactory* factory = rocksdb::NewHashSkipListRepFactory(
      bucket_count, skiplist_height, skiplist_branching_factor);
  opt->rep.memtable_factory.reset(factory);
}

void crocksdb_options_set_hash_link_list_rep(crocksdb_options_t* opt,
                                             size_t bucket_count) {
  opt->rep.memtable_factory.reset(
      rocksdb::NewHashLinkListRepFactory(bucket_count));
}

void crocksdb_options_set_doubly_skip_list_rep(crocksdb_options_t* opt) {
  rocksdb::MemTableRepFactory* factory = new rocksdb::DoublySkipListFactory();
  opt->rep.memtable_factory.reset(factory);
}

void crocksdb_options_set_plain_table_factory(crocksdb_options_t* opt,
                                              uint32_t user_key_len,
                                              int bloom_bits_per_key,
                                              double hash_table_ratio,
                                              size_t index_sparseness) {
  rocksdb::PlainTableOptions options;
  options.user_key_len = user_key_len;
  options.bloom_bits_per_key = bloom_bits_per_key;
  options.hash_table_ratio = hash_table_ratio;
  options.index_sparseness = index_sparseness;

  rocksdb::TableFactory* factory = rocksdb::NewPlainTableFactory(options);
  opt->rep.table_factory.reset(factory);
}

void crocksdb_options_set_max_successive_merges(crocksdb_options_t* opt,
                                                size_t v) {
  opt->rep.max_successive_merges = v;
}

void crocksdb_options_set_bloom_locality(crocksdb_options_t* opt, uint32_t v) {
  opt->rep.bloom_locality = v;
}

void crocksdb_options_set_inplace_update_support(crocksdb_options_t* opt,
                                                 unsigned char v) {
  opt->rep.inplace_update_support = v;
}

void crocksdb_options_set_inplace_update_num_locks(crocksdb_options_t* opt,
                                                   size_t v) {
  opt->rep.inplace_update_num_locks = v;
}

void crocksdb_options_set_report_bg_io_stats(crocksdb_options_t* opt, int v) {
  opt->rep.report_bg_io_stats = v;
}

void crocksdb_options_set_compaction_readahead_size(crocksdb_options_t* opt,
                                                    size_t v) {
  opt->rep.compaction_readahead_size = v;
}

void crocksdb_options_set_compaction_style(crocksdb_options_t* opt,
                                           uint32_t style) {
  opt->rep.compaction_style = static_cast<rocksdb::CompactionStyle>(style);
}

void crocksdb_options_set_universal_compaction_options(
    crocksdb_options_t* opt, crocksdb_universal_compaction_options_t* uco) {
  opt->rep.compaction_options_universal = *(uco->rep);
}

void crocksdb_options_set_fifo_compaction_options(
    crocksdb_options_t* opt, crocksdb_fifo_compaction_options_t* fifo) {
  opt->rep.compaction_options_fifo = fifo->rep;
}

void crocksdb_options_set_compaction_priority(crocksdb_options_t* opt,
                                              uint32_t priority) {
  opt->rep.compaction_pri = static_cast<rocksdb::CompactionPri>(priority);
}

void crocksdb_options_set_delayed_write_rate(crocksdb_options_t* opt,
                                             uint64_t delayed_write_rate) {
  opt->rep.delayed_write_rate = delayed_write_rate;
}

void crocksdb_options_set_force_consistency_checks(crocksdb_options_t* opt,
                                                   unsigned char v) {
  opt->rep.force_consistency_checks = v;
}

unsigned char crocksdb_options_get_force_consistency_checks(
    crocksdb_options_t* opt) {
  return opt->rep.force_consistency_checks;
}

void crocksdb_options_set_ttl(crocksdb_options_t* opt, uint64_t ttl) {
  opt->rep.ttl = ttl;
}

uint64_t crocksdb_options_get_ttl(const crocksdb_options_t* opt) {
  return opt->rep.ttl;
}

void crocksdb_options_set_periodic_compaction_seconds(crocksdb_options_t* opt,
                                                      uint64_t seconds) {
  opt->rep.periodic_compaction_seconds = seconds;
}

uint64_t crocksdb_options_get_periodic_compaction_seconds(
    const crocksdb_options_t* opt) {
  return opt->rep.periodic_compaction_seconds;
}

void crocksdb_options_set_bottommost_file_compaction_delay(
    crocksdb_options_t* opt, uint32_t delay) {
  opt->rep.bottommost_file_compaction_delay = delay;
}

uint32_t crocksdb_options_get_bottommost_file_compaction_delay(
    const crocksdb_options_t* opt) {
  return opt->rep.bottommost_file_compaction_delay;
}

void crocksdb_options_set_statistics(crocksdb_options_t* opt,
                                     crocksdb_statistics_t* statistics) {
  opt->rep.statistics = statistics->rep;
}
crocksdb_statistics_t* crocksdb_options_get_statistics(
    crocksdb_options_t* opt) {
  crocksdb_statistics_t* statistics = new crocksdb_statistics_t;
  statistics->rep = opt->rep.statistics;
  return statistics;
}

crocksdb_statistics_t* crocksdb_statistics_create() {
  crocksdb_statistics_t* statistics = new crocksdb_statistics_t;
  statistics->rep = rocksdb::CreateDBStatistics();
  return statistics;
}

crocksdb_statistics_t* crocksdb_titan_statistics_create() {
  crocksdb_statistics_t* statistics = new crocksdb_statistics_t;
  statistics->rep = rocksdb::titandb::CreateDBStatistics();
  return statistics;
}

crocksdb_statistics_t* crocksdb_empty_statistics_create() {
  crocksdb_statistics_t* statistics = new crocksdb_statistics_t;
  statistics->rep = nullptr;
  return statistics;
}

void crocksdb_statistics_destroy(crocksdb_statistics_t* statistics) {
  if (statistics->rep) {
    statistics->rep.reset();
  }
  delete statistics;
}

unsigned char crocksdb_statistics_is_empty(crocksdb_statistics_t* statistics) {
  return statistics->rep == nullptr;
}

void crocksdb_statistics_reset(crocksdb_statistics_t* statistics) {
  if (statistics->rep) {
    statistics->rep->Reset();
  }
}

char* crocksdb_statistics_to_string(crocksdb_statistics_t* statistics) {
  if (statistics->rep) {
    return strdup(statistics->rep->ToString().c_str());
  }
  return nullptr;
}

uint64_t crocksdb_statistics_get_ticker_count(crocksdb_statistics_t* statistics,
                                              uint32_t ticker_type) {
  if (statistics->rep) {
    return statistics->rep->getTickerCount(ticker_type);
  }
  return 0;
}

uint64_t crocksdb_statistics_get_and_reset_ticker_count(
    crocksdb_statistics_t* statistics, uint32_t ticker_type) {
  if (statistics->rep) {
    return statistics->rep->getAndResetTickerCount(ticker_type);
  }
  return 0;
}

char* crocksdb_statistics_get_histogram_string(
    crocksdb_statistics_t* statistics, uint32_t type) {
  if (statistics->rep) {
    return strdup(statistics->rep->getHistogramString(type).c_str());
  }
  return nullptr;
}

unsigned char crocksdb_statistics_get_histogram(
    crocksdb_statistics_t* statistics, uint32_t type, double* median,
    double* percentile95, double* percentile99, double* average,
    double* standard_deviation, double* max) {
  if (statistics->rep) {
    crocksdb_histogramdata_t data;
    statistics->rep->histogramData(type, &data.rep);
    *median = data.rep.median;
    *percentile95 = data.rep.percentile95;
    *percentile99 = data.rep.percentile99;
    *average = data.rep.average;
    *standard_deviation = data.rep.standard_deviation;
    *max = data.rep.max;
    return 1;
  }
  return 0;
}

void crocksdb_options_set_ratelimiter(crocksdb_options_t* opt,
                                      crocksdb_ratelimiter_t* limiter) {
  opt->rep.rate_limiter = limiter->rep;
}

crocksdb_ratelimiter_t* crocksdb_options_get_ratelimiter(
    crocksdb_options_t* opt) {
  if (opt->rep.rate_limiter != nullptr) {
    crocksdb_ratelimiter_t* limiter = new crocksdb_ratelimiter_t;
    limiter->rep = opt->rep.rate_limiter;
    return limiter;
  }
  return nullptr;
}

crocksdb_write_buffer_manager_t* crocksdb_options_get_write_buffer_manager(
    crocksdb_options_t* opt) {
  if (opt->rep.write_buffer_manager != nullptr) {
    crocksdb_write_buffer_manager_t* manager =
        new crocksdb_write_buffer_manager_t;
    manager->rep = opt->rep.write_buffer_manager;
    return manager;
  }
  return nullptr;
}

crocksdb_write_buffer_manager_t* crocksdb_options_get_cf_write_buffer_manager(
    crocksdb_options_t* opt) {
  if (opt->rep.cf_write_buffer_manager != nullptr) {
    crocksdb_write_buffer_manager_t* manager =
        new crocksdb_write_buffer_manager_t;
    manager->rep = opt->rep.cf_write_buffer_manager;
    return manager;
  }
  return nullptr;
}

void crocksdb_options_set_vector_memtable_factory(crocksdb_options_t* opt,
                                                  uint64_t reserved_bytes) {
  opt->rep.memtable_factory.reset(new VectorRepFactory(reserved_bytes));
}

void crocksdb_options_set_atomic_flush(crocksdb_options_t* opt,
                                       unsigned char enable) {
  opt->rep.atomic_flush = enable;
}

void crocksdb_options_avoid_flush_during_recovery(crocksdb_options_t* opt,
                                                  unsigned char avoid) {
  opt->rep.avoid_flush_during_recovery = avoid;
}

void crocksdb_options_avoid_flush_during_shutdown(crocksdb_options_t* opt,
                                                  unsigned char avoid) {
  opt->rep.avoid_flush_during_shutdown = avoid;
}

void crocksdb_options_set_track_and_verify_wals_in_manifest(
    crocksdb_options_t* opt, unsigned char track_wals_in_manifest) {
  opt->rep.track_and_verify_wals_in_manifest = track_wals_in_manifest;
}

unsigned char crocksdb_load_latest_options(
    const char* dbpath, crocksdb_env_t* env, crocksdb_options_t* db_options,
    crocksdb_column_family_descriptor*** cf_descs, size_t* cf_descs_len,
    unsigned char ignore_unknown_options, char** errptr) {
  std::vector<ColumnFamilyDescriptor> tmp_cf_descs;
  ConfigOptions config_options;
  config_options.ignore_unknown_options = ignore_unknown_options;
  config_options.env = env->rep;
  Status s = rocksdb::LoadLatestOptions(config_options, dbpath,
                                        &db_options->rep, &tmp_cf_descs);

  *errptr = nullptr;
  if (s.IsNotFound()) return false;
  if (SaveError(errptr, s)) return false;

  *cf_descs_len = tmp_cf_descs.size();
  (*cf_descs) = (crocksdb_column_family_descriptor**)malloc(
      sizeof(crocksdb_column_family_descriptor*) * (*cf_descs_len));
  for (std::size_t i = 0; i < *cf_descs_len; ++i) {
    (*cf_descs)[i] =
        new crocksdb_column_family_descriptor{std::move(tmp_cf_descs[i])};
  }

  return true;
}

crocksdb_ratelimiter_t* crocksdb_ratelimiter_create(int64_t rate_bytes_per_sec,
                                                    int64_t refill_period_us,
                                                    int32_t fairness) {
  crocksdb_ratelimiter_t* rate_limiter = new crocksdb_ratelimiter_t;
  rate_limiter->rep = std::shared_ptr<RateLimiter>(
      NewGenericRateLimiter(rate_bytes_per_sec, refill_period_us, fairness));
  return rate_limiter;
}

crocksdb_ratelimiter_t* crocksdb_ratelimiter_create_with_auto_tuned(
    int64_t rate_bytes_per_sec, int64_t refill_period_us, int32_t fairness,
    uint32_t mode, unsigned char auto_tuned) {
  crocksdb_ratelimiter_t* rate_limiter = new crocksdb_ratelimiter_t;
  RateLimiter::Mode m = static_cast<RateLimiter::Mode>(mode);
  rate_limiter->rep = std::shared_ptr<RateLimiter>(NewGenericRateLimiter(
      rate_bytes_per_sec, refill_period_us, fairness, m, auto_tuned));
  return rate_limiter;
}

crocksdb_ratelimiter_t*
crocksdb_writeampbasedratelimiter_create_with_auto_tuned(
    int64_t rate_bytes_per_sec, int64_t refill_period_us, int32_t fairness,
    uint32_t mode, unsigned char auto_tuned, int tune_per_secs,
    size_t smooth_window_size, size_t recent_window_size) {
  crocksdb_ratelimiter_t* rate_limiter = new crocksdb_ratelimiter_t;
  RateLimiter::Mode m = static_cast<RateLimiter::Mode>(mode);
  rate_limiter->rep = std::shared_ptr<RateLimiter>(NewWriteAmpBasedRateLimiter(
      rate_bytes_per_sec, refill_period_us, fairness, m, auto_tuned,
      tune_per_secs, smooth_window_size, recent_window_size));
  return rate_limiter;
}

void crocksdb_ratelimiter_destroy(crocksdb_ratelimiter_t* limiter) {
  if (limiter->rep) {
    limiter->rep.reset();
  }
  delete limiter;
}

void crocksdb_ratelimiter_set_bytes_per_second(crocksdb_ratelimiter_t* limiter,
                                               int64_t rate_bytes_per_sec) {
  limiter->rep->SetBytesPerSecond(rate_bytes_per_sec);
}

void crocksdb_ratelimiter_set_auto_tuned(crocksdb_ratelimiter_t* limiter,
                                         unsigned char auto_tuned) {
  limiter->rep->SetAutoTuned(auto_tuned);
}

int64_t crocksdb_ratelimiter_get_singleburst_bytes(
    crocksdb_ratelimiter_t* limiter) {
  return limiter->rep->GetSingleBurstBytes();
}

void crocksdb_ratelimiter_request(crocksdb_ratelimiter_t* limiter,
                                  int64_t bytes, unsigned char pri) {
  limiter->rep->Request(bytes, static_cast<Env::IOPriority>(pri), nullptr);
}

int64_t crocksdb_ratelimiter_get_total_bytes_through(
    crocksdb_ratelimiter_t* limiter, unsigned char pri) {
  return limiter->rep->GetTotalBytesThrough(static_cast<Env::IOPriority>(pri));
}

int64_t crocksdb_ratelimiter_get_bytes_per_second(
    crocksdb_ratelimiter_t* limiter) {
  return limiter->rep->GetBytesPerSecond();
}

unsigned char crocksdb_ratelimiter_get_auto_tuned(
    crocksdb_ratelimiter_t* limiter) {
  return limiter->rep->GetAutoTuned();
}

int64_t crocksdb_ratelimiter_get_total_requests(crocksdb_ratelimiter_t* limiter,
                                                unsigned char pri) {
  return limiter->rep->GetTotalRequests(static_cast<Env::IOPriority>(pri));
}

crocksdb_write_buffer_manager_t* crocksdb_write_buffer_manager_create(
    size_t flush_size, float stall_ratio, unsigned char flush_oldest_first) {
  crocksdb_write_buffer_manager_t* wbm = new crocksdb_write_buffer_manager_t;
  wbm->rep = std::make_shared<WriteBufferManager>(
      flush_size, nullptr, stall_ratio, flush_oldest_first);
  return wbm;
}

void crocksdb_write_buffer_manager_set_flush_size(
    crocksdb_write_buffer_manager_t* wbm, size_t flush_size) {
  wbm->rep->SetFlushSize(flush_size);
}

size_t crocksdb_write_buffer_manager_flush_size(
    crocksdb_write_buffer_manager_t* wbm) {
  return wbm->rep->flush_size();
}

void crocksdb_write_buffer_manager_set_flush_oldest_first(
    crocksdb_write_buffer_manager_t* wbm, unsigned char flush_oldest_first) {
  wbm->rep->SetFlushOldestFirst(flush_oldest_first);
}

size_t crocksdb_write_buffer_manager_memory_usage(
    crocksdb_write_buffer_manager_t* wbm) {
  return wbm->rep->memory_usage();
}

void crocksdb_write_buffer_manager_destroy(
    crocksdb_write_buffer_manager_t* wbm) {
  delete wbm;
}

crocksdb_concurrent_task_limiter_t* crocksdb_concurrent_task_limiter_create(
    const char* name, uint32_t limit) {
  crocksdb_concurrent_task_limiter_t* limiter =
      new crocksdb_concurrent_task_limiter_t;
  limiter->rep.reset(NewConcurrentTaskLimiter(name, limit));
  return limiter;
}

void crocksdb_concurrent_task_limiter_set_limit(
    crocksdb_concurrent_task_limiter_t* limiter, uint32_t limit) {
  limiter->rep->SetMaxOutstandingTask(limit);
}

void crocksdb_concurrent_task_limiter_destroy(
    crocksdb_concurrent_task_limiter_t* limiter) {
  delete limiter;
}

/*
TODO:
DB::OpenForReadOnly
DB::KeyMayExist
DB::GetOptions
DB::GetSortedWalFiles
DB::GetLatestSequenceNumber
DB::GetUpdatesSince
DB::GetDbIdentity
DB::RunManualCompaction
custom cache
table_properties_collectors
*/

crocksdb_compactionfilter_t* crocksdb_compactionfilter_create(
    void* state, void (*destructor)(void*),
    uint32_t (*filter)(void*, int level, const char* key, size_t key_length,
                       uint32_t value_type, const char* existing_value,
                       size_t value_length, char** new_value,
                       size_t* new_value_length, char** skip_until,
                       size_t* skip_until_length),
    const char* (*name)(void*)) {
  crocksdb_compactionfilter_t* result = new crocksdb_compactionfilter_t;
  result->state_ = state;
  result->destructor_ = destructor;
  result->filter_ = filter;
  result->name_ = name;
  return result;
}

void crocksdb_compactionfilter_destroy(crocksdb_compactionfilter_t* filter) {
  delete filter;
}

unsigned char crocksdb_compactionfiltercontext_is_full_compaction(
    crocksdb_compactionfiltercontext_t* context) {
  return context->rep.is_full_compaction;
}

unsigned char crocksdb_compactionfiltercontext_is_manual_compaction(
    crocksdb_compactionfiltercontext_t* context) {
  return context->rep.is_manual_compaction;
}

unsigned char crocksdb_compactionfiltercontext_is_bottommost_level(
    crocksdb_compactionfiltercontext_t* context) {
  return context->rep.is_bottommost_level;
}

crocksdb_table_properties_collection_t*
crocksdb_compactionfiltercontext_input_table_properties(
    crocksdb_compactionfiltercontext_t* context) {
  return (
      crocksdb_table_properties_collection_t*)(&context->rep
                                                    .input_table_properties);
}

uint32_t crocksdb_compactionfiltercontext_reason(
    crocksdb_compactionfiltercontext_t* context) {
  return static_cast<uint32_t>(context->rep.reason);
}

crocksdb_compactionfilterfactory_t* crocksdb_compactionfilterfactory_create(
    void* state, void (*destructor)(void*),
    crocksdb_compactionfilter_t* (*create_compaction_filter)(
        void*, crocksdb_compactionfiltercontext_t* context),
    unsigned char (*should_filter_table_file_creation)(void*, uint32_t reason),
    const char* (*name)(void*)) {
  crocksdb_compactionfilterfactory_t* result =
      new crocksdb_compactionfilterfactory_t;
  result->state_ = state;
  result->destructor_ = destructor;
  result->create_compaction_filter_ = create_compaction_filter;
  result->should_filter_table_file_creation_ =
      should_filter_table_file_creation;
  result->name_ = name;
  return result;
}

void crocksdb_compactionfilterfactory_destroy(
    crocksdb_compactionfilterfactory_t* factory) {
  delete factory;
}

crocksdb_comparator_t* crocksdb_comparator_create(
    void* state, void (*destructor)(void*),
    int (*compare)(void*, const char* a, size_t alen, const char* b,
                   size_t blen),
    const char* (*name)(void*)) {
  crocksdb_comparator_t* result = new crocksdb_comparator_t;
  result->state_ = state;
  result->destructor_ = destructor;
  result->compare_ = compare;
  result->name_ = name;
  return result;
}

void crocksdb_comparator_destroy(crocksdb_comparator_t* cmp) { delete cmp; }

void crocksdb_filterpolicy_destroy(crocksdb_filterpolicy_t* filter) {
  delete filter;
}

// Make a crocksdb_filterpolicy_t, but override all of its methods so
// they delegate to a NewBloomFilterPolicy() instead of user
// supplied C functions.
struct FilterPolicyWrapper : public crocksdb_filterpolicy_t {
  const FilterPolicy* rep_;
  ~FilterPolicyWrapper() override { delete rep_; }
  const char* Name() const override { return rep_->Name(); }
  const char* CompatibilityName() const override {
    return rep_->CompatibilityName();
  }
  // No need to override GetFilterBitsBuilder if this one is overridden
  FilterBitsBuilder* GetBuilderWithContext(
      const FilterBuildingContext& context) const override {
    return rep_->GetBuilderWithContext(context);
  }
  FilterBitsReader* GetFilterBitsReader(const Slice& contents) const override {
    return rep_->GetFilterBitsReader(contents);
  }
  static void DoNothing(void*) {}
};

crocksdb_filterpolicy_t* crocksdb_filterpolicy_create_bloom_format(
    double bits_per_key, bool original_format) {
  FilterPolicyWrapper* wrapper = new FilterPolicyWrapper;
  wrapper->rep_ = NewBloomFilterPolicy(bits_per_key, original_format);
  wrapper->state_ = nullptr;
  wrapper->destructor_ = &FilterPolicyWrapper::DoNothing;
  return wrapper;
}

crocksdb_filterpolicy_t* crocksdb_filterpolicy_create_bloom_full(
    double bits_per_key) {
  return crocksdb_filterpolicy_create_bloom_format(bits_per_key, false);
}

crocksdb_filterpolicy_t* crocksdb_filterpolicy_create_bloom(
    double bits_per_key) {
  return crocksdb_filterpolicy_create_bloom_format(bits_per_key, true);
}

crocksdb_filterpolicy_t* crocksdb_filterpolicy_create_ribbon(
    double bloom_equivalent_bits_per_key, int bloom_before_level) {
  FilterPolicyWrapper* wrapper = new FilterPolicyWrapper;
  wrapper->rep_ =
      NewRibbonFilterPolicy(bloom_equivalent_bits_per_key, bloom_before_level);
  wrapper->state_ = nullptr;
  wrapper->destructor_ = &FilterPolicyWrapper::DoNothing;
  return wrapper;
}

crocksdb_mergeoperator_t* crocksdb_mergeoperator_create(
    void* state, void (*destructor)(void*),
    char* (*full_merge)(void*, const char* key, size_t key_length,
                        const char* existing_value,
                        size_t existing_value_length,
                        const char* const* operands_list,
                        const size_t* operands_list_length, int num_operands,
                        unsigned char* success, size_t* new_value_length),
    char* (*partial_merge)(void*, const char* key, size_t key_length,
                           const char* const* operands_list,
                           const size_t* operands_list_length, int num_operands,
                           unsigned char* success, size_t* new_value_length),
    void (*delete_value)(void*, const char* value, size_t value_length),
    const char* (*name)(void*)) {
  crocksdb_mergeoperator_t* result = new crocksdb_mergeoperator_t;
  result->state_ = state;
  result->destructor_ = destructor;
  result->full_merge_ = full_merge;
  result->partial_merge_ = partial_merge;
  result->delete_value_ = delete_value;
  result->name_ = name;
  return result;
}

void crocksdb_mergeoperator_destroy(crocksdb_mergeoperator_t* merge_operator) {
  delete merge_operator;
}

crocksdb_readoptions_t* crocksdb_readoptions_create() {
  return new crocksdb_readoptions_t;
}

void crocksdb_readoptions_destroy(crocksdb_readoptions_t* opt) { delete opt; }

void crocksdb_readoptions_set_verify_checksums(crocksdb_readoptions_t* opt,
                                               unsigned char v) {
  opt->rep.verify_checksums = v;
}

void crocksdb_readoptions_set_fill_cache(crocksdb_readoptions_t* opt,
                                         unsigned char v) {
  opt->rep.fill_cache = v;
}

void crocksdb_readoptions_set_auto_prefix_mode(crocksdb_readoptions_t* opt,
                                               unsigned char v) {
  opt->rep.auto_prefix_mode = v;
}

void crocksdb_readoptions_set_adaptive_readahead(crocksdb_readoptions_t* opt,
                                                 unsigned char v) {
  opt->rep.adaptive_readahead = v;
}

void crocksdb_readoptions_set_snapshot(crocksdb_readoptions_t* opt,
                                       const crocksdb_snapshot_t* snap) {
  opt->rep.snapshot = (snap ? snap->rep : nullptr);
}

void crocksdb_readoptions_set_iterate_lower_bound(crocksdb_readoptions_t* opt,
                                                  const char* key,
                                                  size_t keylen) {
  if (key == nullptr) {
    opt->lower_bound = Slice();
    opt->rep.iterate_lower_bound = nullptr;
  } else {
    opt->lower_bound = Slice(key, keylen);
    opt->rep.iterate_lower_bound = &opt->lower_bound;
  }
}

void crocksdb_readoptions_set_iterate_upper_bound(crocksdb_readoptions_t* opt,
                                                  const char* key,
                                                  size_t keylen) {
  if (key == nullptr) {
    opt->upper_bound = Slice();
    opt->rep.iterate_upper_bound = nullptr;
  } else {
    opt->upper_bound = Slice(key, keylen);
    opt->rep.iterate_upper_bound = &opt->upper_bound;
  }
}

void crocksdb_readoptions_set_read_tier(crocksdb_readoptions_t* opt, int v) {
  opt->rep.read_tier = static_cast<rocksdb::ReadTier>(v);
}

void crocksdb_readoptions_set_tailing(crocksdb_readoptions_t* opt,
                                      unsigned char v) {
  opt->rep.tailing = v;
}

void crocksdb_readoptions_set_managed(crocksdb_readoptions_t* opt,
                                      unsigned char v) {
  opt->rep.managed = v;
}

void crocksdb_readoptions_set_readahead_size(crocksdb_readoptions_t* opt,
                                             size_t v) {
  opt->rep.readahead_size = v;
}

void crocksdb_readoptions_set_max_skippable_internal_keys(
    crocksdb_readoptions_t* opt, uint64_t n) {
  opt->rep.max_skippable_internal_keys = n;
}

void crocksdb_readoptions_set_total_order_seek(crocksdb_readoptions_t* opt,
                                               unsigned char v) {
  opt->rep.total_order_seek = v;
}

void crocksdb_readoptions_set_prefix_same_as_start(crocksdb_readoptions_t* opt,
                                                   unsigned char v) {
  opt->rep.prefix_same_as_start = v;
}

void crocksdb_readoptions_set_pin_data(crocksdb_readoptions_t* opt,
                                       unsigned char v) {
  opt->rep.pin_data = v;
}

void crocksdb_readoptions_set_background_purge_on_iterator_cleanup(
    crocksdb_readoptions_t* opt, unsigned char v) {
  opt->rep.background_purge_on_iterator_cleanup = v;
}

void crocksdb_readoptions_set_ignore_range_deletions(
    crocksdb_readoptions_t* opt, unsigned char v) {
  opt->rep.ignore_range_deletions = v;
}

struct TableFilterCtx {
  TableFilterCtx(void* ctx, void (*destroy)(void*))
      : ctx_(ctx), destroy_(destroy) {}
  ~TableFilterCtx() { destroy_(ctx_); }

  void* ctx_;
  void (*destroy_)(void*);
};

struct TableFilter {
  // After passing TableFilter to ReadOptions, ReadOptions will be copyed
  // several times, so we need use shared_ptr to control the ctx_ resource
  // destroy ctx_ only when the last ReadOptions out of its life time.
  TableFilter(void* ctx,
              unsigned char (*table_filter)(void*,
                                            const crocksdb_table_properties_t*),
              void (*destroy)(void*))
      : ctx_(std::make_shared<TableFilterCtx>(ctx, destroy)),
        table_filter_(table_filter) {}

  TableFilter(const TableFilter& f)
      : ctx_(f.ctx_), table_filter_(f.table_filter_) {}

  bool operator()(const TableProperties& prop) {
    return table_filter_(
        ctx_->ctx_,
        reinterpret_cast<const crocksdb_table_properties_t*>(&prop));
  }

  shared_ptr<TableFilterCtx> ctx_;
  unsigned char (*table_filter_)(void*, const crocksdb_table_properties_t*);

 private:
  TableFilter() {}
};

void crocksdb_readoptions_set_table_filter(
    crocksdb_readoptions_t* opt, void* ctx,
    unsigned char (*table_filter)(void*, const crocksdb_table_properties_t*),
    void (*destroy)(void*)) {
  opt->rep.table_filter = TableFilter(ctx, table_filter, destroy);
}

crocksdb_writeoptions_t* crocksdb_writeoptions_create() {
  return new crocksdb_writeoptions_t;
}

void crocksdb_writeoptions_destroy(crocksdb_writeoptions_t* opt) { delete opt; }

void crocksdb_writeoptions_set_sync(crocksdb_writeoptions_t* opt,
                                    unsigned char v) {
  opt->rep.sync = v;
}

void crocksdb_writeoptions_disable_wal(crocksdb_writeoptions_t* opt,
                                       int disable) {
  opt->rep.disableWAL = disable;
}

void crocksdb_writeoptions_set_ignore_missing_column_families(
    crocksdb_writeoptions_t* opt, unsigned char v) {
  opt->rep.ignore_missing_column_families = v;
}

void crocksdb_writeoptions_set_no_slowdown(crocksdb_writeoptions_t* opt,
                                           unsigned char v) {
  opt->rep.no_slowdown = v;
}

void crocksdb_writeoptions_set_low_pri(crocksdb_writeoptions_t* opt,
                                       unsigned char v) {
  opt->rep.low_pri = v;
}

void crocksdb_writeoptions_set_memtable_insert_hint_per_batch(
    crocksdb_writeoptions_t* opt, unsigned char v) {
  opt->rep.memtable_insert_hint_per_batch = v;
}

crocksdb_compactoptions_t* crocksdb_compactoptions_create() {
  auto opts = new crocksdb_compactoptions_t;
  if (opts->rep.canceled == nullptr) {
    opts->rep.canceled = &GLOBAL_MANUAL_COMPACTION_CANCELED_FLAG;
  }
  return opts;
}

void crocksdb_compactoptions_destroy(crocksdb_compactoptions_t* opt) {
  delete opt;
}

void crocksdb_compactoptions_set_exclusive_manual_compaction(
    crocksdb_compactoptions_t* opt, unsigned char v) {
  opt->rep.exclusive_manual_compaction = v;
}

void crocksdb_compactoptions_set_change_level(crocksdb_compactoptions_t* opt,
                                              unsigned char v) {
  opt->rep.change_level = v;
}

void crocksdb_compactoptions_set_target_level(crocksdb_compactoptions_t* opt,
                                              int n) {
  opt->rep.target_level = n;
}

void crocksdb_compactoptions_set_target_path_id(crocksdb_compactoptions_t* opt,
                                                int n) {
  opt->rep.target_path_id = n;
}

void crocksdb_compactoptions_set_max_subcompactions(
    crocksdb_compactoptions_t* opt, int v) {
  opt->rep.max_subcompactions = v;
}

void crocksdb_compactoptions_set_bottommost_level_compaction(
    crocksdb_compactoptions_t* opt, uint32_t v) {
  opt->rep.bottommost_level_compaction =
      static_cast<BottommostLevelCompaction>(v);
}

crocksdb_flushoptions_t* crocksdb_flushoptions_create() {
  return new crocksdb_flushoptions_t;
}

void crocksdb_flushoptions_destroy(crocksdb_flushoptions_t* opt) { delete opt; }

void crocksdb_flushoptions_set_wait(crocksdb_flushoptions_t* opt,
                                    unsigned char v) {
  opt->rep.wait = v;
}

void crocksdb_flushoptions_set_allow_write_stall(crocksdb_flushoptions_t* opt,
                                                 unsigned char v) {
  opt->rep.allow_write_stall = v;
}

void crocksdb_flushoptions_set_expected_oldest_key_time(
    crocksdb_flushoptions_t* opt, uint64_t v) {
  opt->rep.expected_oldest_key_time = v;
}

void crocksdb_flushoptions_set_check_if_compaction_disabled(
    crocksdb_flushoptions_t* opt, unsigned char v) {
  opt->rep.check_if_compaction_disabled = v;
}

crocksdb_memory_allocator_t* crocksdb_jemalloc_nodump_allocator_create(
    char** errptr) {
  crocksdb_memory_allocator_t* allocator = new crocksdb_memory_allocator_t;
  rocksdb::JemallocAllocatorOptions options;
  SaveError(errptr,
            rocksdb::NewJemallocNodumpAllocator(options, &allocator->rep));
  return allocator;
}

void crocksdb_memory_allocator_destroy(crocksdb_memory_allocator_t* allocator) {
  delete allocator;
}

crocksdb_lru_cache_options_t* crocksdb_lru_cache_options_create() {
  return new crocksdb_lru_cache_options_t;
}

void crocksdb_lru_cache_options_destroy(crocksdb_lru_cache_options_t* opt) {
  delete opt;
}

void crocksdb_lru_cache_options_set_capacity(crocksdb_lru_cache_options_t* opt,
                                             size_t capacity) {
  opt->rep.capacity = capacity;
}

void crocksdb_lru_cache_options_set_num_shard_bits(
    crocksdb_lru_cache_options_t* opt, int num_shard_bits) {
  opt->rep.num_shard_bits = num_shard_bits;
}

void crocksdb_lru_cache_options_set_strict_capacity_limit(
    crocksdb_lru_cache_options_t* opt, unsigned char strict_capacity_limit) {
  opt->rep.strict_capacity_limit = strict_capacity_limit;
}

void crocksdb_lru_cache_options_set_high_pri_pool_ratio(
    crocksdb_lru_cache_options_t* opt, double high_pri_pool_ratio) {
  opt->rep.high_pri_pool_ratio = high_pri_pool_ratio;
}

void crocksdb_lru_cache_options_set_low_pri_pool_ratio(
    crocksdb_lru_cache_options_t* opt, double low_pri_pool_ratio) {
  opt->rep.low_pri_pool_ratio = low_pri_pool_ratio;
}

void crocksdb_lru_cache_options_set_memory_allocator(
    crocksdb_lru_cache_options_t* opt, crocksdb_memory_allocator_t* allocator) {
  opt->rep.memory_allocator = allocator->rep;
}

crocksdb_cache_t* crocksdb_cache_create_lru(crocksdb_lru_cache_options_t* opt) {
  crocksdb_cache_t* c = new crocksdb_cache_t;
  c->rep = NewLRUCache(opt->rep);
  return c;
}

crocksdb_hyper_clock_cache_options_t* crocksdb_hyper_clock_cache_options_create(
    size_t capacity, size_t estimated_entry_charge) {
  return new crocksdb_hyper_clock_cache_options_t{
      HyperClockCacheOptions(capacity, estimated_entry_charge)};
}

crocksdb_cache_t* crocksdb_hyper_clock_cache_options_make_shared_cache(
    crocksdb_hyper_clock_cache_options_t* opts) {
  crocksdb_cache_t* c = new crocksdb_cache_t;
  c->rep = opts->rep.MakeSharedCache();
  return c;
}

void crocksdb_cache_destroy(crocksdb_cache_t* cache) { delete cache; }

void crocksdb_cache_set_capacity(crocksdb_cache_t* cache, size_t capacity) {
  cache->rep->SetCapacity(capacity);
}

crocksdb_env_t* crocksdb_default_env_create() {
  crocksdb_env_t* result = new crocksdb_env_t;
  result->rep = Env::Default();
  result->block_cipher = nullptr;
  result->encryption_provider = nullptr;
  result->is_default = true;
  return result;
}

crocksdb_env_t* crocksdb_mem_env_create() {
  crocksdb_env_t* result = new crocksdb_env_t;
  result->rep = rocksdb::NewMemEnv(Env::Default());
  result->block_cipher = nullptr;
  result->encryption_provider = nullptr;
  result->is_default = false;
  return result;
}

struct CTRBlockCipher : public BlockCipher {
  CTRBlockCipher(size_t block_size, const std::string& cipertext)
      : block_size_(block_size), cipertext_(cipertext) {
    assert(block_size == cipertext.size());
  }

  const char* Name() const override { return "CTRBlockCipher"; }

  size_t BlockSize() override { return block_size_; }

  Status Encrypt(char* data) override {
    const char* ciper_ptr = cipertext_.c_str();
    for (size_t i = 0; i < block_size_; i++) {
      data[i] = data[i] ^ ciper_ptr[i];
    }

    return Status::OK();
  }

  Status Decrypt(char* data) override {
    Encrypt(data);
    return Status::OK();
  }

 protected:
  std::string cipertext_;
  size_t block_size_;
};

crocksdb_env_t* crocksdb_ctr_encrypted_env_create(crocksdb_env_t* base_env,
                                                  const char* ciphertext,
                                                  size_t ciphertext_len) {
  auto result = new crocksdb_env_t;
  result->block_cipher = std::make_shared<CTRBlockCipher>(
      ciphertext_len, std::string(ciphertext, ciphertext_len));
  result->encryption_provider =
      EncryptionProvider::NewCTRProvider(result->block_cipher);
  result->rep = NewEncryptedEnv(base_env->rep, result->encryption_provider);
  result->is_default = false;

  return result;
}

void crocksdb_env_set_background_threads(crocksdb_env_t* env, int n) {
  env->rep->SetBackgroundThreads(n);
}

void crocksdb_env_set_high_priority_background_threads(crocksdb_env_t* env,
                                                       int n) {
  env->rep->SetBackgroundThreads(n, Env::HIGH);
}

int crocksdb_env_get_high_priority_background_threads(crocksdb_env_t* env) {
  return env->rep->GetBackgroundThreads(Env::HIGH);
}

void crocksdb_env_join_all_threads(crocksdb_env_t* env) {
  env->rep->WaitForJoin();
}

void crocksdb_env_file_exists(crocksdb_env_t* env, const char* path,
                              char** errptr) {
  SaveError(errptr, env->rep->FileExists(path));
}

void crocksdb_env_delete_file(crocksdb_env_t* env, const char* path,
                              char** errptr) {
  SaveError(errptr, env->rep->DeleteFile(path));
}

unsigned char crocksdb_env_is_db_locked(crocksdb_env_t* env, const char* path,
                                        char** errptr) {
  FileLock* lock;
  std::string file = rocksdb::LockFileName(path);
  Status s = env->rep->LockFile(file, &lock);
  if (s.ok()) {
    env->rep->UnlockFile(lock);
    return false;
  } else {
    const char* state = s.getState();
    if (state == nullptr ||
        (std::strstr(state, "lock hold") == nullptr &&
         std::strstr(state, "While lock file") == nullptr)) {
      SaveError(errptr, s);
    }
    return true;
  }
}

void crocksdb_env_destroy(crocksdb_env_t* env) {
  if (!env->is_default) delete env->rep;
  delete env;
}

crocksdb_envoptions_t* crocksdb_envoptions_create() {
  crocksdb_envoptions_t* opt = new crocksdb_envoptions_t;
  return opt;
}

void crocksdb_envoptions_destroy(crocksdb_envoptions_t* opt) { delete opt; }

crocksdb_sequential_file_t* crocksdb_sequential_file_create(
    crocksdb_env_t* env, const char* path, const crocksdb_envoptions_t* opts,
    char** errptr) {
  std::unique_ptr<SequentialFile> result;
  if (SaveError(errptr,
                env->rep->NewSequentialFile(path, &result, opts->rep))) {
    return nullptr;
  }
  auto file = new crocksdb_sequential_file_t;
  file->rep = result.release();
  return file;
}

size_t crocksdb_sequential_file_read(crocksdb_sequential_file_t* file, size_t n,
                                     char* buf, char** errptr) {
  Slice result;
  if (SaveError(errptr, file->rep->Read(n, &result, buf))) {
    return 0;
  }
  return result.size();
}

void crocksdb_sequential_file_skip(crocksdb_sequential_file_t* file, size_t n,
                                   char** errptr) {
  SaveError(errptr, file->rep->Skip(n));
}

void crocksdb_sequential_file_destroy(crocksdb_sequential_file_t* file) {
  delete file->rep;
  delete file;
}

#ifdef OPENSSL
crocksdb_file_encryption_info_t* crocksdb_file_encryption_info_create() {
  crocksdb_file_encryption_info_t* file_info =
      new crocksdb_file_encryption_info_t;
  file_info->rep = new FileEncryptionInfo;
  return file_info;
}

void crocksdb_file_encryption_info_destroy(
    crocksdb_file_encryption_info_t* file_info) {
  delete file_info->rep;
  delete file_info;
}

uint32_t crocksdb_file_encryption_info_method(
    crocksdb_file_encryption_info_t* file_info) {
  assert(file_info != nullptr);
  assert(file_info->rep != nullptr);
  return static_cast<uint32_t>(file_info->rep->method);
}

const char* crocksdb_file_encryption_info_key(
    crocksdb_file_encryption_info_t* file_info, size_t* keylen) {
  assert(file_info != nullptr);
  assert(file_info->rep != nullptr);
  assert(keylen != nullptr);
  *keylen = file_info->rep->key.size();
  return file_info->rep->key.c_str();
}

const char* crocksdb_file_encryption_info_iv(
    crocksdb_file_encryption_info_t* file_info, size_t* ivlen) {
  assert(file_info != nullptr);
  assert(file_info->rep != nullptr);
  assert(ivlen != nullptr);
  *ivlen = file_info->rep->iv.size();
  return file_info->rep->iv.c_str();
}

void crocksdb_file_encryption_info_set_method(
    crocksdb_file_encryption_info_t* file_info, uint32_t method) {
  assert(file_info != nullptr);
  file_info->rep->method = static_cast<EncryptionMethod>(method);
}

void crocksdb_file_encryption_info_set_key(
    crocksdb_file_encryption_info_t* file_info, const char* key,
    size_t keylen) {
  assert(file_info != nullptr);
  file_info->rep->key = std::string(key, keylen);
}

void crocksdb_file_encryption_info_set_iv(
    crocksdb_file_encryption_info_t* file_info, const char* iv, size_t ivlen) {
  assert(file_info != nullptr);
  file_info->rep->iv = std::string(iv, ivlen);
}

struct crocksdb_encryption_key_manager_impl_t : public KeyManager {
  void* state;
  void (*destructor)(void*);
  crocksdb_encryption_key_manager_get_file_cb get_file;
  crocksdb_encryption_key_manager_new_file_cb new_file;
  crocksdb_encryption_key_manager_delete_file_cb delete_file;
  crocksdb_encryption_key_manager_link_file_cb link_file;

  virtual ~crocksdb_encryption_key_manager_impl_t() { destructor(state); }

  Status GetFile(const std::string& fname,
                 FileEncryptionInfo* file_info) override {
    crocksdb_file_encryption_info_t info;
    info.rep = file_info;
    const char* ret = get_file(state, fname.c_str(), &info);
    Status s;
    if (ret != nullptr) {
      s = Status::Corruption(std::string(ret));
      delete ret;
    }
    return s;
  }

  Status NewFile(const std::string& fname,
                 FileEncryptionInfo* file_info) override {
    crocksdb_file_encryption_info_t info;
    info.rep = file_info;
    const char* ret = new_file(state, fname.c_str(), &info);
    Status s;
    if (ret != nullptr) {
      s = Status::Corruption(std::string(ret));
      delete ret;
    }
    return s;
  }

  Status DeleteFile(const std::string& fname) override {
    const char* ret = delete_file(state, fname.c_str(), nullptr);
    Status s;
    if (ret != nullptr) {
      s = Status::Corruption(std::string(ret));
      delete ret;
    }
    return s;
  }

  Status LinkFile(const std::string& src_fname,
                  const std::string& dst_fname) override {
    const char* ret = link_file(state, src_fname.c_str(), dst_fname.c_str());
    Status s;
    if (ret != nullptr) {
      s = Status::Corruption(std::string(ret));
      delete ret;
    }
    return s;
  }

  Status DeleteFileExt(const std::string& fname,
                       const std::string& physical_fname) override {
    const char* ret = delete_file(state, fname.c_str(), physical_fname.c_str());
    Status s;
    if (ret != nullptr) {
      s = Status::Corruption(std::string(ret));
      delete ret;
    }
    return s;
  }
};

crocksdb_encryption_key_manager_t* crocksdb_encryption_key_manager_create(
    void* state, void (*destructor)(void*),
    crocksdb_encryption_key_manager_get_file_cb get_file,
    crocksdb_encryption_key_manager_new_file_cb new_file,
    crocksdb_encryption_key_manager_delete_file_cb delete_file,
    crocksdb_encryption_key_manager_link_file_cb link_file) {
  std::shared_ptr<crocksdb_encryption_key_manager_impl_t> key_manager_impl =
      std::make_shared<crocksdb_encryption_key_manager_impl_t>();
  key_manager_impl->state = state;
  key_manager_impl->destructor = destructor;
  key_manager_impl->get_file = get_file;
  key_manager_impl->new_file = new_file;
  key_manager_impl->delete_file = delete_file;
  key_manager_impl->link_file = link_file;
  crocksdb_encryption_key_manager_t* key_manager =
      new crocksdb_encryption_key_manager_t;
  key_manager->rep = key_manager_impl;
  return key_manager;
}

void crocksdb_encryption_key_manager_destroy(
    crocksdb_encryption_key_manager_t* key_manager) {
  delete key_manager;
}

const char* crocksdb_encryption_key_manager_get_file(
    crocksdb_encryption_key_manager_t* key_manager, const char* fname,
    crocksdb_file_encryption_info_t* file_info) {
  assert(key_manager != nullptr && key_manager->rep != nullptr);
  assert(fname != nullptr);
  assert(file_info != nullptr && file_info->rep != nullptr);
  Status s = key_manager->rep->GetFile(fname, file_info->rep);
  if (!s.ok()) {
    return strdup(s.ToString().c_str());
  }
  return nullptr;
}

const char* crocksdb_encryption_key_manager_new_file(
    crocksdb_encryption_key_manager_t* key_manager, const char* fname,
    crocksdb_file_encryption_info_t* file_info) {
  assert(key_manager != nullptr && key_manager->rep != nullptr);
  assert(fname != nullptr);
  assert(file_info != nullptr && file_info->rep != nullptr);
  Status s = key_manager->rep->NewFile(fname, file_info->rep);
  if (!s.ok()) {
    return strdup(s.ToString().c_str());
  }
  return nullptr;
}

const char* crocksdb_encryption_key_manager_delete_file(
    crocksdb_encryption_key_manager_t* key_manager, const char* fname) {
  assert(key_manager != nullptr && key_manager->rep != nullptr);
  assert(fname != nullptr);
  Status s = key_manager->rep->DeleteFile(fname);
  if (!s.ok()) {
    return strdup(s.ToString().c_str());
  }
  return nullptr;
}

const char* crocksdb_encryption_key_manager_link_file(
    crocksdb_encryption_key_manager_t* key_manager, const char* src_fname,
    const char* dst_fname) {
  assert(key_manager != nullptr && key_manager->rep != nullptr);
  assert(src_fname != nullptr);
  assert(dst_fname != nullptr);
  Status s = key_manager->rep->LinkFile(src_fname, dst_fname);
  if (!s.ok()) {
    return strdup(s.ToString().c_str());
  }
  return nullptr;
}

const char* crocksdb_encryption_key_manager_delete_file_ext(
    crocksdb_encryption_key_manager_t* key_manager, const char* fname,
    const char* physical_fname) {
  assert(key_manager != nullptr && key_manager->rep != nullptr);
  assert(fname != nullptr);
  assert(physical_fname != nullptr);
  Status s = key_manager->rep->DeleteFileExt(fname, physical_fname);
  if (!s.ok()) {
    return strdup(s.ToString().c_str());
  }
  return nullptr;
}

crocksdb_env_t* crocksdb_key_managed_encrypted_env_create(
    crocksdb_env_t* base_env, crocksdb_encryption_key_manager_t* key_manager) {
  assert(base_env != nullptr);
  assert(key_manager != nullptr);
  crocksdb_env_t* result = new crocksdb_env_t;
  result->rep = NewKeyManagedEncryptedEnv(base_env->rep, key_manager->rep);
  result->block_cipher = nullptr;
  result->encryption_provider = nullptr;
  result->is_default = false;
  return result;
}
#endif

struct crocksdb_file_system_inspector_impl_t : public FileSystemInspector {
  void* state;
  void (*destructor)(void*);
  crocksdb_file_system_inspector_read_cb read;
  crocksdb_file_system_inspector_write_cb write;

  virtual ~crocksdb_file_system_inspector_impl_t() { destructor(state); }

  Status Read(size_t len, size_t* allowed) {
    assert(allowed);
    char* err = nullptr;
    *allowed = read(state, len, &err);
    if (err) {
      Status s = Status::IOError(err);
      // malloc-ed by strdup
      free(err);
      return s;
    } else {
      return Status::OK();
    }
  }

  Status Write(size_t len, size_t* allowed) {
    assert(allowed);
    char* err = nullptr;
    *allowed = write(state, len, &err);
    if (err) {
      Status s = Status::IOError(err);
      // malloc-ed by strdup
      free(err);
      return s;
    } else {
      return Status::OK();
    }
  }
};

crocksdb_file_system_inspector_t* crocksdb_file_system_inspector_create(
    void* state, void (*destructor)(void*),
    crocksdb_file_system_inspector_read_cb read,
    crocksdb_file_system_inspector_write_cb write) {
  std::shared_ptr<crocksdb_file_system_inspector_impl_t> inspector_impl =
      std::make_shared<crocksdb_file_system_inspector_impl_t>();
  inspector_impl->state = state;
  inspector_impl->destructor = destructor;
  inspector_impl->read = read;
  inspector_impl->write = write;
  crocksdb_file_system_inspector_t* inspector =
      new crocksdb_file_system_inspector_t;
  inspector->rep = inspector_impl;
  return inspector;
}

void crocksdb_file_system_inspector_destroy(
    crocksdb_file_system_inspector_t* inspector) {
  delete inspector;
}

size_t crocksdb_file_system_inspector_read(
    crocksdb_file_system_inspector_t* inspector, size_t len, char** errptr) {
  assert(inspector != nullptr && inspector->rep != nullptr);
  size_t allowed = 0;
  SaveError(errptr, inspector->rep->Read(len, &allowed));
  return allowed;
}

size_t crocksdb_file_system_inspector_write(
    crocksdb_file_system_inspector_t* inspector, size_t len, char** errptr) {
  assert(inspector != nullptr && inspector->rep != nullptr);
  size_t allowed = 0;
  SaveError(errptr, inspector->rep->Write(len, &allowed));
  return allowed;
}

crocksdb_env_t* crocksdb_file_system_inspected_env_create(
    crocksdb_env_t* base_env, crocksdb_file_system_inspector_t* inspector) {
  assert(base_env != nullptr);
  assert(inspector != nullptr);
  crocksdb_env_t* result = new crocksdb_env_t;
  result->rep = NewFileSystemInspectedEnv(base_env->rep, inspector->rep);
  result->block_cipher = nullptr;
  result->encryption_provider = nullptr;
  result->is_default = false;
  return result;
}

crocksdb_sstfilereader_t* crocksdb_sstfilereader_create(
    const crocksdb_options_t* io_options) {
  auto reader = new crocksdb_sstfilereader_t;
  reader->rep = new SstFileReader(io_options->rep);
  return reader;
}

void crocksdb_sstfilereader_open(crocksdb_sstfilereader_t* reader,
                                 const char* name, char** errptr) {
  SaveError(errptr, reader->rep->Open(std::string(name)));
}

crocksdb_iterator_t* crocksdb_sstfilereader_new_iterator(
    crocksdb_sstfilereader_t* reader, const crocksdb_readoptions_t* options) {
  auto it = new crocksdb_iterator_t;
  it->rep = reader->rep->NewIterator(options->rep);
  return it;
}

void crocksdb_sstfilereader_read_table_properties(
    const crocksdb_sstfilereader_t* reader, void* ctx,
    void (*cb)(void*, const crocksdb_table_properties_t*)) {
  auto props = reader->rep->GetTableProperties();
  cb(ctx, reinterpret_cast<const crocksdb_table_properties_t*>(props.get()));
}

void crocksdb_sstfilereader_verify_checksum(crocksdb_sstfilereader_t* reader,
                                            char** errptr) {
  SaveError(errptr, reader->rep->VerifyChecksum());
}

void crocksdb_sstfilereader_destroy(crocksdb_sstfilereader_t* reader) {
  delete reader->rep;
  delete reader;
}

crocksdb_sstfilewriter_t* crocksdb_sstfilewriter_create(
    const crocksdb_envoptions_t* env, const crocksdb_options_t* io_options) {
  crocksdb_sstfilewriter_t* writer = new crocksdb_sstfilewriter_t;
  writer->rep = new SstFileWriter(env->rep, io_options->rep);
  return writer;
}

crocksdb_sstfilewriter_t* crocksdb_sstfilewriter_create_cf(
    const crocksdb_envoptions_t* env, const crocksdb_options_t* io_options,
    crocksdb_column_family_handle_t* column_family) {
  crocksdb_sstfilewriter_t* writer = new crocksdb_sstfilewriter_t;
  writer->rep =
      new SstFileWriter(env->rep, io_options->rep, column_family->rep);
  return writer;
}

void crocksdb_sstfilewriter_open(crocksdb_sstfilewriter_t* writer,
                                 const char* name, char** errptr) {
  SaveError(errptr, writer->rep->Open(std::string(name)));
}

void crocksdb_sstfilewriter_put(crocksdb_sstfilewriter_t* writer,
                                const char* key, size_t keylen, const char* val,
                                size_t vallen, char** errptr) {
  SaveError(errptr, writer->rep->Put(Slice(key, keylen), Slice(val, vallen)));
}

void crocksdb_sstfilewriter_merge(crocksdb_sstfilewriter_t* writer,
                                  const char* key, size_t keylen,
                                  const char* val, size_t vallen,
                                  char** errptr) {
  SaveError(errptr, writer->rep->Merge(Slice(key, keylen), Slice(val, vallen)));
}

void crocksdb_sstfilewriter_delete(crocksdb_sstfilewriter_t* writer,
                                   const char* key, size_t keylen,
                                   char** errptr) {
  SaveError(errptr, writer->rep->Delete(Slice(key, keylen)));
}

void crocksdb_sstfilewriter_delete_range(crocksdb_sstfilewriter_t* writer,
                                         const char* begin_key,
                                         size_t begin_keylen,
                                         const char* end_key, size_t end_keylen,
                                         char** errptr) {
  SaveError(errptr, writer->rep->DeleteRange(Slice(begin_key, begin_keylen),
                                             Slice(end_key, end_keylen)));
}

void crocksdb_sstfilewriter_finish(crocksdb_sstfilewriter_t* writer,
                                   crocksdb_externalsstfileinfo_t* info,
                                   char** errptr) {
  SaveError(errptr, writer->rep->Finish(&info->rep));
}

uint64_t crocksdb_sstfilewriter_file_size(crocksdb_sstfilewriter_t* writer) {
  return writer->rep->FileSize();
}

void crocksdb_sstfilewriter_destroy(crocksdb_sstfilewriter_t* writer) {
  delete writer->rep;
  delete writer;
}

crocksdb_externalsstfileinfo_t* crocksdb_externalsstfileinfo_create() {
  return new crocksdb_externalsstfileinfo_t;
};

void crocksdb_externalsstfileinfo_destroy(
    crocksdb_externalsstfileinfo_t* info) {
  delete info;
}

const char* crocksdb_externalsstfileinfo_file_path(
    crocksdb_externalsstfileinfo_t* info, size_t* size) {
  *size = info->rep.file_path.size();
  return info->rep.file_path.c_str();
}

const char* crocksdb_externalsstfileinfo_smallest_key(
    crocksdb_externalsstfileinfo_t* info, size_t* size) {
  *size = info->rep.smallest_key.size();
  return info->rep.smallest_key.c_str();
}

const char* crocksdb_externalsstfileinfo_largest_key(
    crocksdb_externalsstfileinfo_t* info, size_t* size) {
  *size = info->rep.largest_key.size();
  return info->rep.largest_key.c_str();
}

uint64_t crocksdb_externalsstfileinfo_sequence_number(
    crocksdb_externalsstfileinfo_t* info) {
  return info->rep.sequence_number;
}

uint64_t crocksdb_externalsstfileinfo_file_size(
    crocksdb_externalsstfileinfo_t* info) {
  return info->rep.file_size;
}

uint64_t crocksdb_externalsstfileinfo_num_entries(
    crocksdb_externalsstfileinfo_t* info) {
  return info->rep.num_entries;
}

crocksdb_ingestexternalfileoptions_t*
crocksdb_ingestexternalfileoptions_create() {
  crocksdb_ingestexternalfileoptions_t* opt =
      new crocksdb_ingestexternalfileoptions_t;
  return opt;
}

void crocksdb_ingestexternalfileoptions_set_move_files(
    crocksdb_ingestexternalfileoptions_t* opt, unsigned char move_files) {
  opt->rep.move_files = move_files;
}

void crocksdb_ingestexternalfileoptions_set_snapshot_consistency(
    crocksdb_ingestexternalfileoptions_t* opt,
    unsigned char snapshot_consistency) {
  opt->rep.snapshot_consistency = snapshot_consistency;
}

void crocksdb_ingestexternalfileoptions_set_allow_global_seqno(
    crocksdb_ingestexternalfileoptions_t* opt,
    unsigned char allow_global_seqno) {
  opt->rep.allow_global_seqno = allow_global_seqno;
}

void crocksdb_ingestexternalfileoptions_set_allow_blocking_flush(
    crocksdb_ingestexternalfileoptions_t* opt,
    unsigned char allow_blocking_flush) {
  opt->rep.allow_blocking_flush = allow_blocking_flush;
}

unsigned char crocksdb_ingestexternalfileoptions_get_write_global_seqno(
    const crocksdb_ingestexternalfileoptions_t* opt) {
  return opt->rep.write_global_seqno;
}

void crocksdb_ingestexternalfileoptions_set_write_global_seqno(
    crocksdb_ingestexternalfileoptions_t* opt,
    unsigned char write_global_seqno) {
  opt->rep.write_global_seqno = write_global_seqno;
}

void crocksdb_ingestexternalfileoptions_set_verify_checksums_before_ingest(
    crocksdb_ingestexternalfileoptions_t* opt,
    unsigned char verify_checksums_before_ingest) {
  opt->rep.verify_checksums_before_ingest = verify_checksums_before_ingest;
}

void crocksdb_ingestexternalfileoptions_set_allow_write(
    crocksdb_ingestexternalfileoptions_t* opt, unsigned char allow_write) {
  opt->rep.allow_write = allow_write;
}

void crocksdb_ingestexternalfileoptions_destroy(
    crocksdb_ingestexternalfileoptions_t* opt) {
  delete opt;
}

void crocksdb_ingest_external_file(
    crocksdb_t* db, const char* const* file_list, const size_t list_len,
    const crocksdb_ingestexternalfileoptions_t* opt, char** errptr) {
  std::vector<std::string> files(list_len);
  for (size_t i = 0; i < list_len; ++i) {
    files[i] = std::string(file_list[i]);
  }
  SaveError(errptr, db->rep->IngestExternalFile(files, opt->rep));
}

void crocksdb_ingest_external_file_cf(
    crocksdb_t* db, crocksdb_column_family_handle_t* handle,
    const char* const* file_list, const size_t list_len,
    const crocksdb_ingestexternalfileoptions_t* opt, char** errptr) {
  std::vector<std::string> files(list_len);
  for (size_t i = 0; i < list_len; ++i) {
    files[i] = std::string(file_list[i]);
  }
  SaveError(errptr, db->rep->IngestExternalFile(handle->rep, files, opt->rep));
}

unsigned char crocksdb_ingest_external_file_optimized(
    crocksdb_t* db, crocksdb_column_family_handle_t* handle,
    const char* const* file_list, const size_t list_len,
    const crocksdb_ingestexternalfileoptions_t* opt, char** errptr) {
  std::vector<std::string> files(list_len);
  for (size_t i = 0; i < list_len; ++i) {
    files[i] = std::string(file_list[i]);
  }
  bool has_flush = false;
  // If the file being ingested is overlapped with the memtable, it
  // will block writes and wait for flushing, which can cause high
  // write latency. So we set `allow_blocking_flush = false`.
  auto ingest_opts = opt->rep;
  ingest_opts.allow_blocking_flush = false;
  auto s = db->rep->IngestExternalFile(handle->rep, files, ingest_opts);
  if (s.IsInvalidArgument() &&
      s.ToString().find("External file requires flush") != std::string::npos) {
    // When `allow_blocking_flush = false` and the file being ingested
    // is overlapped with the memtable, `IngestExternalFile` returns
    // an invalid argument error. It is tricky to search for the
    // specific error message here but don't worry, the unit test
    // ensures that we get this right. Then we can try to flush the
    // memtable outside without blocking writes. We also set
    // `allow_write_stall = false` to prevent the flush from
    // triggering write stall.
    has_flush = true;
    FlushOptions flush_opts;
    flush_opts.wait = true;
    flush_opts.allow_write_stall = false;
    // We don't check the status of this flush because we will
    // fallback to a blocking ingestion anyway.
    db->rep->Flush(flush_opts, handle->rep);
    s = db->rep->IngestExternalFile(handle->rep, files, opt->rep);
  }
  SaveError(errptr, s);
  return has_flush;
}

crocksdb_slicetransform_t* crocksdb_slicetransform_create(
    void* state, void (*destructor)(void*),
    char* (*transform)(void*, const char* key, size_t length,
                       size_t* dst_length),
    unsigned char (*in_domain)(void*, const char* key, size_t length),
    unsigned char (*in_range)(void*, const char* key, size_t length),
    const char* (*name)(void*)) {
  crocksdb_slicetransform_t* result = new crocksdb_slicetransform_t;
  result->state_ = state;
  result->destructor_ = destructor;
  result->transform_ = transform;
  result->in_domain_ = in_domain;
  result->in_range_ = in_range;
  result->name_ = name;
  return result;
}

void crocksdb_slicetransform_destroy(crocksdb_slicetransform_t* st) {
  delete st;
}

crocksdb_slicetransform_t* crocksdb_slicetransform_create_fixed_prefix(
    size_t prefixLen) {
  struct Wrapper : public crocksdb_slicetransform_t {
    const SliceTransform* rep_;
    ~Wrapper() { delete rep_; }
    const char* Name() const override { return rep_->Name(); }
    Slice Transform(const Slice& src) const override {
      return rep_->Transform(src);
    }
    bool InDomain(const Slice& src) const override {
      return rep_->InDomain(src);
    }
    bool InRange(const Slice& src) const override { return rep_->InRange(src); }
    static void DoNothing(void*) {}
  };
  Wrapper* wrapper = new Wrapper;
  wrapper->rep_ = rocksdb::NewFixedPrefixTransform(prefixLen);
  wrapper->state_ = nullptr;
  wrapper->destructor_ = &Wrapper::DoNothing;
  return wrapper;
}

crocksdb_slicetransform_t* crocksdb_slicetransform_create_noop() {
  struct Wrapper : public crocksdb_slicetransform_t {
    const SliceTransform* rep_;
    ~Wrapper() { delete rep_; }
    const char* Name() const override { return rep_->Name(); }
    Slice Transform(const Slice& src) const override {
      return rep_->Transform(src);
    }
    bool InDomain(const Slice& src) const override {
      return rep_->InDomain(src);
    }
    bool InRange(const Slice& src) const override { return rep_->InRange(src); }
    static void DoNothing(void*) {}
  };
  Wrapper* wrapper = new Wrapper;
  wrapper->rep_ = rocksdb::NewNoopTransform();
  wrapper->state_ = nullptr;
  wrapper->destructor_ = &Wrapper::DoNothing;
  return wrapper;
}

crocksdb_universal_compaction_options_t*
crocksdb_universal_compaction_options_create() {
  crocksdb_universal_compaction_options_t* result =
      new crocksdb_universal_compaction_options_t;
  result->rep = new rocksdb::CompactionOptionsUniversal;
  return result;
}

void crocksdb_universal_compaction_options_set_size_ratio(
    crocksdb_universal_compaction_options_t* uco, int ratio) {
  uco->rep->size_ratio = ratio;
}

void crocksdb_universal_compaction_options_set_min_merge_width(
    crocksdb_universal_compaction_options_t* uco, int w) {
  uco->rep->min_merge_width = w;
}

void crocksdb_universal_compaction_options_set_max_merge_width(
    crocksdb_universal_compaction_options_t* uco, int w) {
  uco->rep->max_merge_width = w;
}

void crocksdb_universal_compaction_options_set_max_size_amplification_percent(
    crocksdb_universal_compaction_options_t* uco, int p) {
  uco->rep->max_size_amplification_percent = p;
}

void crocksdb_universal_compaction_options_set_compression_size_percent(
    crocksdb_universal_compaction_options_t* uco, int p) {
  uco->rep->compression_size_percent = p;
}

void crocksdb_universal_compaction_options_set_stop_style(
    crocksdb_universal_compaction_options_t* uco, int style) {
  uco->rep->stop_style = static_cast<rocksdb::CompactionStopStyle>(style);
}

void crocksdb_universal_compaction_options_destroy(
    crocksdb_universal_compaction_options_t* uco) {
  delete uco->rep;
  delete uco;
}

crocksdb_fifo_compaction_options_t* crocksdb_fifo_compaction_options_create() {
  crocksdb_fifo_compaction_options_t* result =
      new crocksdb_fifo_compaction_options_t;
  result->rep = CompactionOptionsFIFO();
  return result;
}

void crocksdb_fifo_compaction_options_set_max_table_files_size(
    crocksdb_fifo_compaction_options_t* fifo_opts, uint64_t size) {
  fifo_opts->rep.max_table_files_size = size;
}

void crocksdb_fifo_compaction_options_set_allow_compaction(
    crocksdb_fifo_compaction_options_t* fifo_opts,
    unsigned char allow_compaction) {
  fifo_opts->rep.allow_compaction = allow_compaction;
}

void crocksdb_fifo_compaction_options_destroy(
    crocksdb_fifo_compaction_options_t* fifo_opts) {
  delete fifo_opts;
}

void crocksdb_options_set_min_level_to_compress(crocksdb_options_t* opt,
                                                int level) {
  if (level >= 0) {
    assert(level <= opt->rep.num_levels);
    opt->rep.compression_per_level.resize(opt->rep.num_levels);
    for (int i = 0; i < level; i++) {
      opt->rep.compression_per_level[i] = rocksdb::kNoCompression;
    }
    for (int i = level; i < opt->rep.num_levels; i++) {
      opt->rep.compression_per_level[i] = opt->rep.compression;
    }
  }
}

size_t crocksdb_livefiles_count(const crocksdb_livefiles_t* lf) {
  return static_cast<int>(lf->rep.size());
}

const char* crocksdb_livefiles_name(const crocksdb_livefiles_t* lf, int index) {
  return lf->rep[index].name.c_str();
}

int crocksdb_livefiles_level(const crocksdb_livefiles_t* lf, int index) {
  return lf->rep[index].level;
}

size_t crocksdb_livefiles_size(const crocksdb_livefiles_t* lf, int index) {
  return lf->rep[index].size;
}

const char* crocksdb_livefiles_smallestkey(const crocksdb_livefiles_t* lf,
                                           int index, size_t* size) {
  *size = lf->rep[index].smallestkey.size();
  return lf->rep[index].smallestkey.data();
}

const char* crocksdb_livefiles_largestkey(const crocksdb_livefiles_t* lf,
                                          int index, size_t* size) {
  *size = lf->rep[index].largestkey.size();
  return lf->rep[index].largestkey.data();
}

extern void crocksdb_livefiles_destroy(const crocksdb_livefiles_t* lf) {
  delete lf;
}

void crocksdb_get_options_from_string(const crocksdb_options_t* base_options,
                                      const char* opts_str,
                                      crocksdb_options_t* new_options,
                                      char** errptr) {
  SaveError(errptr,
            GetOptionsFromString(base_options->rep, std::string(opts_str),
                                 &new_options->rep));
}

void crocksdb_delete_files_in_range(crocksdb_t* db, const char* start_key,
                                    size_t start_key_len, const char* limit_key,
                                    size_t limit_key_len,
                                    unsigned char include_end, char** errptr) {
  Slice a, b;
  SaveError(
      errptr,
      DeleteFilesInRange(
          db->rep, db->rep->DefaultColumnFamily(),
          (start_key ? (a = Slice(start_key, start_key_len), &a) : nullptr),
          (limit_key ? (b = Slice(limit_key, limit_key_len), &b) : nullptr),
          include_end));
}

void crocksdb_delete_files_in_range_cf(
    crocksdb_t* db, crocksdb_column_family_handle_t* column_family,
    const char* start_key, size_t start_key_len, const char* limit_key,
    size_t limit_key_len, unsigned char include_end, char** errptr) {
  Slice a, b;
  SaveError(
      errptr,
      DeleteFilesInRange(
          db->rep, column_family->rep,
          (start_key ? (a = Slice(start_key, start_key_len), &a) : nullptr),
          (limit_key ? (b = Slice(limit_key, limit_key_len), &b) : nullptr),
          include_end));
}

void crocksdb_delete_files_in_ranges_cf(
    crocksdb_t* db, crocksdb_column_family_handle_t* cf,
    const char* const* start_keys, const size_t* start_keys_lens,
    const char* const* limit_keys, const size_t* limit_keys_lens,
    size_t num_ranges, unsigned char include_end, char** errptr) {
  std::vector<Slice> starts(num_ranges);
  std::vector<Slice> limits(num_ranges);
  std::vector<RangePtr> ranges(num_ranges);
  for (auto i = 0; i < num_ranges; i++) {
    const Slice* start = nullptr;
    if (start_keys[i]) {
      starts[i] = Slice(start_keys[i], start_keys_lens[i]);
      start = &starts[i];
    }
    const Slice* limit = nullptr;
    if (limit_keys[i]) {
      limits[i] = Slice(limit_keys[i], limit_keys_lens[i]);
      limit = &limits[i];
    }
    ranges[i] = RangePtr(start, limit);
  }
  SaveError(errptr, DeleteFilesInRanges(db->rep, cf->rep, &ranges[0],
                                        num_ranges, include_end));
}

void crocksdb_free(void* ptr) { free(ptr); }

crocksdb_logger_t* crocksdb_create_env_logger(const char* fname,
                                              crocksdb_env_t* env) {
  crocksdb_logger_t* logger = new crocksdb_logger_t;
  Status s = NewEnvLogger(std::string(fname), env->rep, &logger->rep);
  if (!s.ok()) {
    delete logger;
    return NULL;
  }
  return logger;
}

crocksdb_logger_t* crocksdb_create_log_from_options(const char* path,
                                                    crocksdb_options_t* opts,
                                                    char** errptr) {
  crocksdb_logger_t* logger = new crocksdb_logger_t;
  if (SaveError(errptr, CreateLoggerFromOptions(std::string(path), opts->rep,
                                                &logger->rep))) {
    delete logger;
    return NULL;
  }

  return logger;
}

void crocksdb_log_destroy(crocksdb_logger_t* logger) { delete logger; }

crocksdb_pinnableslice_t* crocksdb_get_pinned(
    crocksdb_t* db, const crocksdb_readoptions_t* options, const char* key,
    size_t keylen, char** errptr) {
  crocksdb_pinnableslice_t* v = new (crocksdb_pinnableslice_t);
  Status s = db->rep->Get(options->rep, db->rep->DefaultColumnFamily(),
                          Slice(key, keylen), &v->rep);
  if (!s.ok()) {
    delete (v);
    if (!s.IsNotFound()) {
      SaveError(errptr, s);
    }
    return NULL;
  }
  return v;
}

crocksdb_pinnableslice_t* crocksdb_get_pinned_cf(
    crocksdb_t* db, const crocksdb_readoptions_t* options,
    crocksdb_column_family_handle_t* column_family, const char* key,
    size_t keylen, char** errptr) {
  crocksdb_pinnableslice_t* v = new (crocksdb_pinnableslice_t);
  Status s = db->rep->Get(options->rep, column_family->rep, Slice(key, keylen),
                          &v->rep);
  if (!s.ok()) {
    delete v;
    if (!s.IsNotFound()) {
      SaveError(errptr, s);
    }
    return NULL;
  }
  return v;
}

void crocksdb_pinnableslice_destroy(crocksdb_pinnableslice_t* v) { delete v; }

const char* crocksdb_pinnableslice_value(const crocksdb_pinnableslice_t* v,
                                         size_t* vlen) {
  // v can't be null.
  *vlen = v->rep.size();
  return v->rep.data();
}

size_t crocksdb_get_supported_compression_number() {
  return rocksdb::GetSupportedCompressions().size();
}

void crocksdb_get_supported_compression(uint32_t* v, size_t l) {
  auto compressions = rocksdb::GetSupportedCompressions();
  assert(compressions.size() == l);
  for (size_t i = 0; i < compressions.size(); i++) {
    v[i] = static_cast<uint32_t>(compressions[i]);
  }
}

/* Table Properties */

struct crocksdb_user_collected_properties_t {
  UserCollectedProperties rep;
};

void crocksdb_user_collected_properties_add(
    crocksdb_user_collected_properties_t* props, const char* k, size_t klen,
    const char* v, size_t vlen) {
  props->rep.emplace(
      std::make_pair(std::string(k, klen), std::string(v, vlen)));
}

struct crocksdb_user_collected_properties_iterator_t {
  UserCollectedProperties::const_iterator cur_;
  UserCollectedProperties::const_iterator end_;
};

crocksdb_user_collected_properties_iterator_t*
crocksdb_user_collected_properties_iter_create(
    const crocksdb_user_collected_properties_t* props) {
  auto it = new crocksdb_user_collected_properties_iterator_t;
  it->cur_ = props->rep.begin();
  it->end_ = props->rep.end();
  return it;
}

void crocksdb_user_collected_properties_iter_destroy(
    crocksdb_user_collected_properties_iterator_t* it) {
  delete it;
}

unsigned char crocksdb_user_collected_properties_iter_valid(
    const crocksdb_user_collected_properties_iterator_t* it) {
  return it->cur_ != it->end_;
}

void crocksdb_user_collected_properties_iter_next(
    crocksdb_user_collected_properties_iterator_t* it) {
  ++(it->cur_);
}

const char* crocksdb_user_collected_properties_iter_key(
    const crocksdb_user_collected_properties_iterator_t* it, size_t* klen) {
  *klen = it->cur_->first.size();
  return it->cur_->first.data();
}

const char* crocksdb_user_collected_properties_iter_value(
    const crocksdb_user_collected_properties_iterator_t* it, size_t* vlen) {
  *vlen = it->cur_->second.size();
  return it->cur_->second.data();
}

struct crocksdb_table_properties_t {
  const TableProperties rep;
};

uint64_t crocksdb_table_properties_get_u64(
    const crocksdb_table_properties_t* props,
    crocksdb_table_u64_property_t prop) {
  const TableProperties& rep = props->rep;
  switch (prop) {
    case kOriginalFileNumber:
      return rep.orig_file_number;
    case kDataSize:
      return rep.data_size;
    case kIndexSize:
      return rep.index_size;
    case kIndexPartitions:
      return rep.index_partitions;
    case kTopLevelIndexSize:
      return rep.top_level_index_size;
    case kIndexKeyIsUserKey:
      return rep.index_key_is_user_key;
    case kIndexValueIsDeltaEncoded:
      return rep.index_value_is_delta_encoded;
    case kFilterSize:
      return rep.filter_size;
    case kRawKeySize:
      return rep.raw_key_size;
    case kRawValueSize:
      return rep.raw_value_size;
    case kNumDataBlocks:
      return rep.num_data_blocks;
    case kNumEntries:
      return rep.num_entries;
    case kNumFilterEntries:
      return rep.num_filter_entries;
    case kNumDeletions:
      return rep.num_deletions;
    case kNumMergeOperands:
      return rep.num_merge_operands;
    case kNumRangeDeletions:
      return rep.num_range_deletions;
    case kFormatVersion:
      return rep.format_version;
    case kFixedKeyLen:
      return rep.fixed_key_len;
    case kColumnFamilyId:
      return rep.column_family_id;
    case kCreationTime:
      return rep.creation_time;
    case kOldestKeyTime:
      return rep.oldest_key_time;
    case kFileCreationTime:
      return rep.file_creation_time;
    case kSlowCompressionEstimatedDataSize:
      return rep.slow_compression_estimated_data_size;
    case kFastCompressionEstimatedDataSize:
      return rep.fast_compression_estimated_data_size;
    default:
      assert(false);
  }
}

const char* crocksdb_table_properties_get_str(
    const crocksdb_table_properties_t* props,
    crocksdb_table_str_property_t prop, size_t* slen) {
  const TableProperties& rep = props->rep;
  switch (prop) {
    case kDbId:
      *slen = rep.db_id.size();
      return rep.db_id.data();
    case kDbSessionId:
      *slen = rep.db_session_id.size();
      return rep.db_session_id.data();
    case kDbHostId:
      *slen = rep.db_host_id.size();
      return rep.db_host_id.data();
    case kColumnFamilyName:
      *slen = rep.column_family_name.size();
      return rep.column_family_name.data();
    case kFilterPolicyName:
      *slen = rep.filter_policy_name.size();
      return rep.filter_policy_name.data();
    case kComparatorName:
      *slen = rep.comparator_name.size();
      return rep.comparator_name.data();
    case kMergeOperatorName:
      *slen = rep.merge_operator_name.size();
      return rep.merge_operator_name.data();
    case kPrefixExtractorName:
      *slen = rep.prefix_extractor_name.size();
      return rep.prefix_extractor_name.data();
    case kPropertyCollectorsNames:
      *slen = rep.property_collectors_names.size();
      return rep.property_collectors_names.data();
    case kCompressionName:
      *slen = rep.compression_name.size();
      return rep.compression_name.data();
    case kCompressionOptions:
      *slen = rep.compression_options.size();
      return rep.compression_options.data();
    default:
      assert(false);
  }
}

const crocksdb_user_collected_properties_t*
crocksdb_table_properties_get_user_properties(
    const crocksdb_table_properties_t* props) {
  return reinterpret_cast<const crocksdb_user_collected_properties_t*>(
      &props->rep.user_collected_properties);
}

const char* crocksdb_user_collected_properties_get(
    const crocksdb_user_collected_properties_t* props, const char* key,
    size_t klen, size_t* vlen) {
  auto val = props->rep.find(std::string(key, klen));
  if (val == props->rep.end()) {
    return nullptr;
  }
  *vlen = val->second.size();
  return val->second.data();
}

size_t crocksdb_user_collected_properties_len(
    const crocksdb_user_collected_properties_t* props) {
  return props->rep.size();
}

/* Table Properties Collection */

struct crocksdb_table_properties_collection_t {
  TablePropertiesCollection rep_;
};

size_t crocksdb_table_properties_collection_len(
    const crocksdb_table_properties_collection_t* props) {
  return props->rep_.size();
}

void crocksdb_table_properties_collection_destroy(
    crocksdb_table_properties_collection_t* t) {
  delete t;
}

struct crocksdb_table_properties_collection_iterator_t {
  TablePropertiesCollection::const_iterator cur_;
  TablePropertiesCollection::const_iterator end_;
};

crocksdb_table_properties_collection_iterator_t*
crocksdb_table_properties_collection_iter_create(
    const crocksdb_table_properties_collection_t* collection) {
  auto it = new crocksdb_table_properties_collection_iterator_t;
  it->cur_ = collection->rep_.begin();
  it->end_ = collection->rep_.end();
  return it;
}

void crocksdb_table_properties_collection_iter_destroy(
    crocksdb_table_properties_collection_iterator_t* it) {
  delete it;
}

unsigned char crocksdb_table_properties_collection_iter_valid(
    const crocksdb_table_properties_collection_iterator_t* it) {
  return it->cur_ != it->end_;
}

void crocksdb_table_properties_collection_iter_next(
    crocksdb_table_properties_collection_iterator_t* it) {
  ++(it->cur_);
}

const char* crocksdb_table_properties_collection_iter_key(
    const crocksdb_table_properties_collection_iterator_t* it, size_t* klen) {
  *klen = it->cur_->first.size();
  return it->cur_->first.data();
}

const crocksdb_table_properties_t*
crocksdb_table_properties_collection_iter_value(
    const crocksdb_table_properties_collection_iterator_t* it) {
  if (it->cur_->second) {
    return reinterpret_cast<const crocksdb_table_properties_t*>(
        it->cur_->second.get());
  } else {
    return nullptr;
  }
}

/* Table Properties Collector */

struct crocksdb_table_properties_collector_t : public TablePropertiesCollector {
  void* state_;
  const char* (*name_)(void*);
  void (*destruct_)(void*);
  void (*add_)(void*, const char* key, size_t key_len, const char* value,
               size_t value_len, uint32_t entry_type, uint64_t seq,
               uint64_t file_size);
  void (*finish_)(void*, crocksdb_user_collected_properties_t* props);

  virtual ~crocksdb_table_properties_collector_t() { destruct_(state_); }

  virtual Status AddUserKey(const Slice& key, const Slice& value,
                            EntryType entry_type, SequenceNumber seq,
                            uint64_t file_size) override {
    add_(state_, key.data(), key.size(), value.data(), value.size(),
         static_cast<uint32_t>(entry_type), seq, file_size);
    return Status::OK();
  }

  virtual Status Finish(UserCollectedProperties* rep) override {
    finish_(state_,
            reinterpret_cast<crocksdb_user_collected_properties_t*>(rep));
    return Status::OK();
  }

  virtual UserCollectedProperties GetReadableProperties() const override {
    // Seems rocksdb will not return the readable properties and we don't need
    // them too.
    return UserCollectedProperties();
  }

  const char* Name() const override { return name_(state_); }
};

crocksdb_table_properties_collector_t*
crocksdb_table_properties_collector_create(
    void* state, const char* (*name)(void*), void (*destruct)(void*),
    void (*add)(void*, const char* key, size_t key_len, const char* value,
                size_t value_len, uint32_t entry_type, uint64_t seq,
                uint64_t file_size),
    void (*finish)(void*, crocksdb_user_collected_properties_t* props)) {
  auto c = new crocksdb_table_properties_collector_t;
  c->state_ = state;
  c->name_ = name;
  c->destruct_ = destruct;
  c->add_ = add;
  c->finish_ = finish;
  return c;
}

void crocksdb_table_properties_collector_destroy(
    crocksdb_table_properties_collector_t* c) {
  delete c;
}

/* Table Properties Collector Factory */

struct crocksdb_table_properties_collector_factory_t
    : public TablePropertiesCollectorFactory {
  void* state_;
  const char* (*name_)(void*);
  void (*destruct_)(void*);
  crocksdb_table_properties_collector_t* (*create_table_properties_collector_)(
      void*, uint32_t cf);

  virtual ~crocksdb_table_properties_collector_factory_t() {
    destruct_(state_);
  }

  virtual TablePropertiesCollector* CreateTablePropertiesCollector(
      TablePropertiesCollectorFactory::Context ctx) override {
    return create_table_properties_collector_(state_, ctx.column_family_id);
  }

  const char* Name() const override { return name_(state_); }
};

crocksdb_table_properties_collector_factory_t*
crocksdb_table_properties_collector_factory_create(
    void* state, const char* (*name)(void*), void (*destruct)(void*),
    crocksdb_table_properties_collector_t* (*create_table_properties_collector)(
        void*, uint32_t cf)) {
  auto f = new crocksdb_table_properties_collector_factory_t;
  f->state_ = state;
  f->name_ = name;
  f->destruct_ = destruct;
  f->create_table_properties_collector_ = create_table_properties_collector;
  return f;
}

void crocksdb_table_properties_collector_factory_destroy(
    crocksdb_table_properties_collector_factory_t* f) {
  delete f;
}

void crocksdb_options_add_table_properties_collector_factory(
    crocksdb_options_t* opt, crocksdb_table_properties_collector_factory_t* f) {
  opt->rep.table_properties_collector_factories.push_back(
      std::shared_ptr<TablePropertiesCollectorFactory>(f));
}

void crocksdb_options_set_compact_on_deletion(crocksdb_options_t* opt,
                                              size_t sliding_window_size,
                                              size_t deletion_trigger) {
  opt->rep.table_properties_collector_factories.push_back(
      rocksdb::NewCompactOnDeletionCollectorFactory(sliding_window_size,
                                                    deletion_trigger));
}

/* Get Table Properties */

crocksdb_table_properties_collection_t* crocksdb_get_properties_of_all_tables(
    crocksdb_t* db, char** errptr) {
  std::unique_ptr<crocksdb_table_properties_collection_t> props(
      new crocksdb_table_properties_collection_t);
  auto s = db->rep->GetPropertiesOfAllTables(&props->rep_);
  if (!s.ok()) {
    SaveError(errptr, s);
    return nullptr;
  }
  return props.release();
}

crocksdb_table_properties_collection_t*
crocksdb_get_properties_of_all_tables_cf(crocksdb_t* db,
                                         crocksdb_column_family_handle_t* cf,
                                         char** errptr) {
  std::unique_ptr<crocksdb_table_properties_collection_t> props(
      new crocksdb_table_properties_collection_t);
  auto s = db->rep->GetPropertiesOfAllTables(cf->rep, &props->rep_);
  if (!s.ok()) {
    SaveError(errptr, s);
    return nullptr;
  }
  return props.release();
}

crocksdb_table_properties_collection_t*
crocksdb_get_properties_of_tables_in_range(
    crocksdb_t* db, crocksdb_column_family_handle_t* cf, int num_ranges,
    const char* const* start_keys, const size_t* start_keys_lens,
    const char* const* limit_keys, const size_t* limit_keys_lens,
    char** errptr) {
  std::vector<Range> ranges;
  for (int i = 0; i < num_ranges; i++) {
    ranges.emplace_back(Range(Slice(start_keys[i], start_keys_lens[i]),
                              Slice(limit_keys[i], limit_keys_lens[i])));
  }
  std::unique_ptr<crocksdb_table_properties_collection_t> props(
      new crocksdb_table_properties_collection_t);
  auto s = db->rep->GetPropertiesOfTablesInRange(cf->rep, ranges.data(),
                                                 ranges.size(), &props->rep_);
  if (!s.ok()) {
    SaveError(errptr, s);
    return nullptr;
  }
  return props.release();
}

void crocksdb_set_bottommost_compression(crocksdb_options_t* opt, uint32_t c) {
  opt->rep.bottommost_compression = static_cast<CompressionType>(c);
}
// Get All Key Versions
void crocksdb_keyversions_destroy(crocksdb_keyversions_t* kvs) { delete kvs; }

crocksdb_keyversions_t* crocksdb_get_all_key_versions(
    crocksdb_t* db, const char* begin_key, size_t begin_keylen,
    const char* end_key, size_t end_keylen, char** errptr) {
  crocksdb_keyversions_t* result = new crocksdb_keyversions_t;
  constexpr size_t kMaxNumKeys = std::numeric_limits<size_t>::max();
  SaveError(errptr, GetAllKeyVersions(db->rep, Slice(begin_key, begin_keylen),
                                      Slice(end_key, end_keylen), kMaxNumKeys,
                                      &result->rep));
  return result;
}

size_t crocksdb_keyversions_count(const crocksdb_keyversions_t* kvs) {
  return kvs->rep.size();
}

const char* crocksdb_keyversions_key(const crocksdb_keyversions_t* kvs,
                                     int index) {
  return kvs->rep[index].user_key.c_str();
}

const char* crocksdb_keyversions_value(const crocksdb_keyversions_t* kvs,
                                       int index) {
  return kvs->rep[index].value.c_str();
}

uint64_t crocksdb_keyversions_seq(const crocksdb_keyversions_t* kvs,
                                  int index) {
  return kvs->rep[index].sequence;
}

int crocksdb_keyversions_type(const crocksdb_keyversions_t* kvs, int index) {
  return kvs->rep[index].type;
}

struct ExternalSstFileModifier {
  ExternalSstFileModifier(Env* env, const EnvOptions& env_options,
                          ColumnFamilyHandle* handle)
      : env_(env), env_options_(env_options), handle_(handle) {}

  Status Open(std::string file) {
    file_ = file;
    // Get External Sst File Size
    uint64_t file_size;
    auto status = env_->GetFileSize(file_, &file_size);
    if (!status.ok()) {
      return status;
    }

    // Open External Sst File
    std::unique_ptr<FSRandomAccessFile> sst_file;
    std::unique_ptr<RandomAccessFileReader> sst_file_reader;
    status = env_->GetFileSystem()->NewRandomAccessFile(
        file_, FileOptions(env_options_), &sst_file, nullptr /*dbg*/);
    if (!status.ok()) {
      return status;
    }
    sst_file_reader.reset(
        new RandomAccessFileReader(std::move(sst_file), file_));

    // Get Table Reader
    ColumnFamilyDescriptor desc;
    handle_->GetDescriptor(&desc);
    auto cfd = reinterpret_cast<ColumnFamilyHandleImpl*>(handle_)->cfd();
    auto ioptions = *cfd->ioptions();
    auto table_opt =
        TableReaderOptions(ioptions, desc.options.prefix_extractor,
                           env_options_, cfd->internal_comparator(), 0);
    // Get around global seqno check.
    table_opt.largest_seqno = kMaxSequenceNumber;
    status = ioptions.table_factory->NewTableReader(
        table_opt, std::move(sst_file_reader), file_size, &table_reader_);
    return status;
  }

  Status SetGlobalSeqNo(uint64_t seq_no, uint64_t* pre_seq_no) {
    if (table_reader_ == nullptr) {
      return Status::InvalidArgument(
          "File is not open or seq-no has been modified");
    }
    // Get the external file properties
    auto props = table_reader_->GetTableProperties();
    const auto& uprops = props->user_collected_properties;
    // Validate version and seqno offset
    auto version_iter = uprops.find(ExternalSstFilePropertyNames::kVersion);
    if (version_iter == uprops.end()) {
      return Status::Corruption("External file version not found");
    }
    uint32_t version = DecodeFixed32(version_iter->second.c_str());
    if (version != 2) {
      return Status::NotSupported("External file version should be 2");
    }

    auto seqno_iter = uprops.find(ExternalSstFilePropertyNames::kGlobalSeqno);
    if (seqno_iter == uprops.end()) {
      return Status::Corruption(
          "External file global sequence number not found");
    }
    *pre_seq_no = DecodeFixed64(seqno_iter->second.c_str());
    uint64_t offset = props->external_sst_file_global_seqno_offset;
    if (offset == 0) {
      return Status::Corruption("Was not able to find file global seqno field");
    }

    if (*pre_seq_no == seq_no) {
      // This file already have the correct global seqno
      return Status::OK();
    }

    std::unique_ptr<RandomRWFile> rwfile;
    auto status = env_->NewRandomRWFile(file_, &rwfile, env_options_);
    if (!status.ok()) {
      return status;
    }

    // Write the new seqno in the global sequence number field in the file
    std::string seqno_val;
    PutFixed64(&seqno_val, seq_no);
    status = rwfile->Write(offset, seqno_val);
    return status;
  }

 private:
  Env* env_;
  EnvOptions env_options_;
  ColumnFamilyHandle* handle_;
  std::string file_;
  std::unique_ptr<TableReader> table_reader_;
};

// !!! this function is dangerous because it uses rocksdb's non-public API !!!
// find the offset of external sst file's `global seq no` and modify it.
uint64_t crocksdb_set_external_sst_file_global_seq_no(
    crocksdb_t* db, crocksdb_column_family_handle_t* column_family,
    const char* file, uint64_t seq_no, char** errptr) {
  auto env = db->rep->GetEnv();
  EnvOptions env_options(db->rep->GetDBOptions());
  ExternalSstFileModifier modifier(env, env_options, column_family->rep);
  auto s = modifier.Open(std::string(file));
  uint64_t pre_seq_no = 0;
  if (!s.ok()) {
    SaveError(errptr, s);
    return pre_seq_no;
  }
  s = modifier.SetGlobalSeqNo(seq_no, &pre_seq_no);
  if (!s.ok()) {
    SaveError(errptr, s);
  }
  return pre_seq_no;
}

void crocksdb_get_column_family_meta_data(
    crocksdb_t* db, crocksdb_column_family_handle_t* cf,
    crocksdb_column_family_meta_data_t* meta) {
  db->rep->GetColumnFamilyMetaData(cf->rep, &meta->rep);
}

crocksdb_column_family_meta_data_t* crocksdb_column_family_meta_data_create() {
  return new crocksdb_column_family_meta_data_t();
}

void crocksdb_column_family_meta_data_destroy(
    crocksdb_column_family_meta_data_t* meta) {
  delete meta;
}

size_t crocksdb_column_family_meta_data_level_count(
    const crocksdb_column_family_meta_data_t* meta) {
  return meta->rep.levels.size();
}

const crocksdb_level_meta_data_t* crocksdb_column_family_meta_data_level_data(
    const crocksdb_column_family_meta_data_t* meta, size_t n) {
  return reinterpret_cast<const crocksdb_level_meta_data_t*>(
      &meta->rep.levels[n]);
}

size_t crocksdb_level_meta_data_file_count(
    const crocksdb_level_meta_data_t* meta) {
  return meta->rep.files.size();
}

const crocksdb_sst_file_meta_data_t* crocksdb_level_meta_data_file_data(
    const crocksdb_level_meta_data_t* meta, size_t n) {
  return reinterpret_cast<const crocksdb_sst_file_meta_data_t*>(
      &meta->rep.files[n]);
}

size_t crocksdb_sst_file_meta_data_size(
    const crocksdb_sst_file_meta_data_t* meta) {
  return meta->rep.size;
}

const char* crocksdb_sst_file_meta_data_name(
    const crocksdb_sst_file_meta_data_t* meta) {
  return meta->rep.name.data();
}

const char* crocksdb_sst_file_meta_data_smallestkey(
    const crocksdb_sst_file_meta_data_t* meta, size_t* len) {
  *len = meta->rep.smallestkey.size();
  return meta->rep.smallestkey.data();
}

const char* crocksdb_sst_file_meta_data_largestkey(
    const crocksdb_sst_file_meta_data_t* meta, size_t* len) {
  *len = meta->rep.largestkey.size();
  return meta->rep.largestkey.data();
}

crocksdb_compaction_options_t* crocksdb_compaction_options_create() {
  return new crocksdb_compaction_options_t();
}

void crocksdb_compaction_options_destroy(crocksdb_compaction_options_t* opts) {
  delete opts;
}

void crocksdb_compaction_options_set_compression(
    crocksdb_compaction_options_t* opts, int compression) {
  opts->rep.compression = static_cast<CompressionType>(compression);
}

void crocksdb_compaction_options_set_output_file_size_limit(
    crocksdb_compaction_options_t* opts, size_t size) {
  opts->rep.output_file_size_limit = size;
}

void crocksdb_compaction_options_set_max_subcompactions(
    crocksdb_compaction_options_t* opts, int v) {
  opts->rep.max_subcompactions = v;
}

void crocksdb_compact_files_cf(crocksdb_t* db,
                               crocksdb_column_family_handle_t* cf,
                               crocksdb_compaction_options_t* opts,
                               const char** input_file_names,
                               size_t input_file_count, int output_level,
                               char** errptr) {
  std::vector<std::string> input_files;
  for (size_t i = 0; i < input_file_count; i++) {
    input_files.push_back(input_file_names[i]);
  }
  auto s = db->rep->CompactFiles(opts->rep, cf->rep, input_files, output_level);
  SaveError(errptr, s);
}

/* PerfContext */

int crocksdb_get_perf_level(void) {
  return static_cast<int>(rocksdb::GetPerfLevel());
}

void crocksdb_set_perf_level(int level) {
  rocksdb::SetPerfLevel(static_cast<PerfLevel>(level));
}

struct crocksdb_perf_flags_t {
  PerfFlags rep;
};

crocksdb_perf_flags_t* crocksdb_create_perf_flags() {
  return new crocksdb_perf_flags_t;
}

void crocksdb_perf_flags_set(crocksdb_perf_flags_t* flags, uint32_t flag) {
  flags->rep.set(flag);
}

void crocksdb_destroy_perf_flags(crocksdb_perf_flags_t* flags) { delete flags; }

void crocksdb_set_perf_flags(const crocksdb_perf_flags_t* flags) {
  rocksdb::SetPerfFlags(flags->rep);
}

struct crocksdb_perf_context_t {
  PerfContext rep;
};

crocksdb_perf_context_t* crocksdb_get_perf_context(void) {
  return reinterpret_cast<crocksdb_perf_context_t*>(
      rocksdb::get_perf_context());
}

void crocksdb_perf_context_reset(crocksdb_perf_context_t* ctx) {
  ctx->rep.Reset();
}

uint64_t crocksdb_perf_context_user_key_comparison_count(
    crocksdb_perf_context_t* ctx) {
  return ctx->rep.user_key_comparison_count;
}

uint64_t crocksdb_perf_context_block_cache_hit_count(
    crocksdb_perf_context_t* ctx) {
  return ctx->rep.block_cache_hit_count;
}

uint64_t crocksdb_perf_context_block_read_count(crocksdb_perf_context_t* ctx) {
  return ctx->rep.block_read_count;
}

uint64_t crocksdb_perf_context_block_read_byte(crocksdb_perf_context_t* ctx) {
  return ctx->rep.block_read_byte;
}

uint64_t crocksdb_perf_context_block_read_time(crocksdb_perf_context_t* ctx) {
  return ctx->rep.block_read_time;
}

uint64_t crocksdb_perf_context_block_cache_index_hit_count(
    crocksdb_perf_context_t* ctx) {
  return ctx->rep.block_cache_index_hit_count;
}

uint64_t crocksdb_perf_context_index_block_read_count(
    crocksdb_perf_context_t* ctx) {
  return ctx->rep.index_block_read_count;
}

uint64_t crocksdb_perf_context_block_cache_filter_hit_count(
    crocksdb_perf_context_t* ctx) {
  return ctx->rep.block_cache_filter_hit_count;
}

uint64_t crocksdb_perf_context_filter_block_read_count(
    crocksdb_perf_context_t* ctx) {
  return ctx->rep.filter_block_read_count;
}

uint64_t crocksdb_perf_context_block_checksum_time(
    crocksdb_perf_context_t* ctx) {
  return ctx->rep.block_checksum_time;
}

uint64_t crocksdb_perf_context_block_decompress_time(
    crocksdb_perf_context_t* ctx) {
  return ctx->rep.block_decompress_time;
}

uint64_t crocksdb_perf_context_get_read_bytes(crocksdb_perf_context_t* ctx) {
  return ctx->rep.get_read_bytes;
}

uint64_t crocksdb_perf_context_multiget_read_bytes(
    crocksdb_perf_context_t* ctx) {
  return ctx->rep.multiget_read_bytes;
}

uint64_t crocksdb_perf_context_iter_read_bytes(crocksdb_perf_context_t* ctx) {
  return ctx->rep.iter_read_bytes;
}

uint64_t crocksdb_perf_context_internal_key_skipped_count(
    crocksdb_perf_context_t* ctx) {
  return ctx->rep.internal_key_skipped_count;
}

uint64_t crocksdb_perf_context_internal_delete_skipped_count(
    crocksdb_perf_context_t* ctx) {
  return ctx->rep.internal_delete_skipped_count;
}

uint64_t crocksdb_perf_context_internal_recent_skipped_count(
    crocksdb_perf_context_t* ctx) {
  return ctx->rep.internal_recent_skipped_count;
}

uint64_t crocksdb_perf_context_internal_merge_count(
    crocksdb_perf_context_t* ctx) {
  return ctx->rep.internal_merge_count;
}

uint64_t crocksdb_perf_context_get_snapshot_time(crocksdb_perf_context_t* ctx) {
  return ctx->rep.get_snapshot_time;
}

uint64_t crocksdb_perf_context_get_from_memtable_time(
    crocksdb_perf_context_t* ctx) {
  return ctx->rep.get_from_memtable_time;
}

uint64_t crocksdb_perf_context_get_from_memtable_count(
    crocksdb_perf_context_t* ctx) {
  return ctx->rep.get_from_memtable_count;
}

uint64_t crocksdb_perf_context_get_post_process_time(
    crocksdb_perf_context_t* ctx) {
  return ctx->rep.get_post_process_time;
}

uint64_t crocksdb_perf_context_get_from_output_files_time(
    crocksdb_perf_context_t* ctx) {
  return ctx->rep.get_from_output_files_time;
}

uint64_t crocksdb_perf_context_seek_on_memtable_time(
    crocksdb_perf_context_t* ctx) {
  return ctx->rep.seek_on_memtable_time;
}

uint64_t crocksdb_perf_context_seek_on_memtable_count(
    crocksdb_perf_context_t* ctx) {
  return ctx->rep.seek_on_memtable_count;
}

uint64_t crocksdb_perf_context_next_on_memtable_count(
    crocksdb_perf_context_t* ctx) {
  return ctx->rep.next_on_memtable_count;
}

uint64_t crocksdb_perf_context_prev_on_memtable_count(
    crocksdb_perf_context_t* ctx) {
  return ctx->rep.prev_on_memtable_count;
}

uint64_t crocksdb_perf_context_seek_child_seek_time(
    crocksdb_perf_context_t* ctx) {
  return ctx->rep.seek_child_seek_time;
}

uint64_t crocksdb_perf_context_seek_child_seek_count(
    crocksdb_perf_context_t* ctx) {
  return ctx->rep.seek_child_seek_count;
}

uint64_t crocksdb_perf_context_seek_min_heap_time(
    crocksdb_perf_context_t* ctx) {
  return ctx->rep.seek_min_heap_time;
}

uint64_t crocksdb_perf_context_seek_max_heap_time(
    crocksdb_perf_context_t* ctx) {
  return ctx->rep.seek_max_heap_time;
}

uint64_t crocksdb_perf_context_seek_internal_seek_time(
    crocksdb_perf_context_t* ctx) {
  return ctx->rep.seek_internal_seek_time;
}

uint64_t crocksdb_perf_context_find_next_user_entry_time(
    crocksdb_perf_context_t* ctx) {
  return ctx->rep.find_next_user_entry_time;
}

uint64_t crocksdb_perf_context_write_wal_time(crocksdb_perf_context_t* ctx) {
  return ctx->rep.write_wal_time;
}

uint64_t crocksdb_perf_context_write_memtable_time(
    crocksdb_perf_context_t* ctx) {
  return ctx->rep.write_memtable_time;
}

uint64_t crocksdb_perf_context_write_delay_time(crocksdb_perf_context_t* ctx) {
  return ctx->rep.write_delay_time;
}

uint64_t crocksdb_perf_context_write_pre_and_post_process_time(
    crocksdb_perf_context_t* ctx) {
  return ctx->rep.write_pre_and_post_process_time;
}

uint64_t crocksdb_perf_context_db_mutex_lock_nanos(
    crocksdb_perf_context_t* ctx) {
  return ctx->rep.db_mutex_lock_nanos;
}

uint64_t crocksdb_perf_context_write_thread_wait_nanos(
    crocksdb_perf_context_t* ctx) {
  return ctx->rep.write_thread_wait_nanos;
}

uint64_t crocksdb_perf_context_write_scheduling_flushes_compactions_time(
    crocksdb_perf_context_t* ctx) {
  return ctx->rep.write_scheduling_flushes_compactions_time;
}

uint64_t crocksdb_perf_context_db_condition_wait_nanos(
    crocksdb_perf_context_t* ctx) {
  return ctx->rep.db_condition_wait_nanos;
}

uint64_t crocksdb_perf_context_merge_operator_time_nanos(
    crocksdb_perf_context_t* ctx) {
  return ctx->rep.merge_operator_time_nanos;
}

uint64_t crocksdb_perf_context_read_index_block_nanos(
    crocksdb_perf_context_t* ctx) {
  return ctx->rep.read_index_block_nanos;
}

uint64_t crocksdb_perf_context_read_filter_block_nanos(
    crocksdb_perf_context_t* ctx) {
  return ctx->rep.read_filter_block_nanos;
}

uint64_t crocksdb_perf_context_new_table_block_iter_nanos(
    crocksdb_perf_context_t* ctx) {
  return ctx->rep.new_table_block_iter_nanos;
}

uint64_t crocksdb_perf_context_new_table_iterator_nanos(
    crocksdb_perf_context_t* ctx) {
  return ctx->rep.new_table_iterator_nanos;
}

uint64_t crocksdb_perf_context_block_seek_nanos(crocksdb_perf_context_t* ctx) {
  return ctx->rep.block_seek_nanos;
}

uint64_t crocksdb_perf_context_find_table_nanos(crocksdb_perf_context_t* ctx) {
  return ctx->rep.find_table_nanos;
}

uint64_t crocksdb_perf_context_bloom_memtable_hit_count(
    crocksdb_perf_context_t* ctx) {
  return ctx->rep.bloom_memtable_hit_count;
}

uint64_t crocksdb_perf_context_bloom_memtable_miss_count(
    crocksdb_perf_context_t* ctx) {
  return ctx->rep.bloom_memtable_miss_count;
}

uint64_t crocksdb_perf_context_bloom_sst_hit_count(
    crocksdb_perf_context_t* ctx) {
  return ctx->rep.bloom_sst_hit_count;
}

uint64_t crocksdb_perf_context_bloom_sst_miss_count(
    crocksdb_perf_context_t* ctx) {
  return ctx->rep.bloom_sst_miss_count;
}

uint64_t crocksdb_perf_context_env_new_sequential_file_nanos(
    crocksdb_perf_context_t* ctx) {
  return ctx->rep.env_new_sequential_file_nanos;
}

uint64_t crocksdb_perf_context_env_new_random_access_file_nanos(
    crocksdb_perf_context_t* ctx) {
  return ctx->rep.env_new_random_access_file_nanos;
}

uint64_t crocksdb_perf_context_env_new_writable_file_nanos(
    crocksdb_perf_context_t* ctx) {
  return ctx->rep.env_new_writable_file_nanos;
}

uint64_t crocksdb_perf_context_env_reuse_writable_file_nanos(
    crocksdb_perf_context_t* ctx) {
  return ctx->rep.env_reuse_writable_file_nanos;
}

uint64_t crocksdb_perf_context_env_new_random_rw_file_nanos(
    crocksdb_perf_context_t* ctx) {
  return ctx->rep.env_new_random_rw_file_nanos;
}

uint64_t crocksdb_perf_context_env_new_directory_nanos(
    crocksdb_perf_context_t* ctx) {
  return ctx->rep.env_new_directory_nanos;
}

uint64_t crocksdb_perf_context_env_file_exists_nanos(
    crocksdb_perf_context_t* ctx) {
  return ctx->rep.env_file_exists_nanos;
}

uint64_t crocksdb_perf_context_env_get_children_nanos(
    crocksdb_perf_context_t* ctx) {
  return ctx->rep.env_get_children_nanos;
}

uint64_t crocksdb_perf_context_env_get_children_file_attributes_nanos(
    crocksdb_perf_context_t* ctx) {
  return ctx->rep.env_get_children_file_attributes_nanos;
}

uint64_t crocksdb_perf_context_env_delete_file_nanos(
    crocksdb_perf_context_t* ctx) {
  return ctx->rep.env_delete_file_nanos;
}

uint64_t crocksdb_perf_context_env_create_dir_nanos(
    crocksdb_perf_context_t* ctx) {
  return ctx->rep.env_create_dir_nanos;
}

uint64_t crocksdb_perf_context_env_create_dir_if_missing_nanos(
    crocksdb_perf_context_t* ctx) {
  return ctx->rep.env_create_dir_if_missing_nanos;
}

uint64_t crocksdb_perf_context_env_delete_dir_nanos(
    crocksdb_perf_context_t* ctx) {
  return ctx->rep.env_delete_dir_nanos;
}

uint64_t crocksdb_perf_context_env_get_file_size_nanos(
    crocksdb_perf_context_t* ctx) {
  return ctx->rep.env_get_file_size_nanos;
}

uint64_t crocksdb_perf_context_env_get_file_modification_time_nanos(
    crocksdb_perf_context_t* ctx) {
  return ctx->rep.env_get_file_modification_time_nanos;
}

uint64_t crocksdb_perf_context_env_rename_file_nanos(
    crocksdb_perf_context_t* ctx) {
  return ctx->rep.env_rename_file_nanos;
}

uint64_t crocksdb_perf_context_env_link_file_nanos(
    crocksdb_perf_context_t* ctx) {
  return ctx->rep.env_link_file_nanos;
}

uint64_t crocksdb_perf_context_env_lock_file_nanos(
    crocksdb_perf_context_t* ctx) {
  return ctx->rep.env_lock_file_nanos;
}

uint64_t crocksdb_perf_context_env_unlock_file_nanos(
    crocksdb_perf_context_t* ctx) {
  return ctx->rep.env_unlock_file_nanos;
}

uint64_t crocksdb_perf_context_env_new_logger_nanos(
    crocksdb_perf_context_t* ctx) {
  return ctx->rep.env_new_logger_nanos;
}

uint64_t crocksdb_perf_context_get_cpu_nanos(crocksdb_perf_context_t* ctx) {
  return ctx->rep.get_cpu_nanos;
}

uint64_t crocksdb_perf_context_iter_next_cpu_nanos(
    crocksdb_perf_context_t* ctx) {
  return ctx->rep.iter_next_cpu_nanos;
}

uint64_t crocksdb_perf_context_iter_prev_cpu_nanos(
    crocksdb_perf_context_t* ctx) {
  return ctx->rep.iter_next_cpu_nanos;
}

uint64_t crocksdb_perf_context_iter_seek_cpu_nanos(
    crocksdb_perf_context_t* ctx) {
  return ctx->rep.iter_next_cpu_nanos;
}

uint64_t crocksdb_perf_context_encrypt_data_nanos(
    crocksdb_perf_context_t* ctx) {
  return ctx->rep.encrypt_data_nanos;
}

uint64_t crocksdb_perf_context_decrypt_data_nanos(
    crocksdb_perf_context_t* ctx) {
  return ctx->rep.decrypt_data_nanos;
}

// IOStatsContext

struct crocksdb_iostats_context_t {
  IOStatsContext rep;
};

crocksdb_iostats_context_t* crocksdb_get_iostats_context(void) {
  return reinterpret_cast<crocksdb_iostats_context_t*>(
      rocksdb::get_iostats_context());
}

void crocksdb_iostats_context_reset(crocksdb_iostats_context_t* ctx) {
  ctx->rep.Reset();
}

uint64_t crocksdb_iostats_context_bytes_written(
    crocksdb_iostats_context_t* ctx) {
  return ctx->rep.bytes_written;
}

uint64_t crocksdb_iostats_context_bytes_read(crocksdb_iostats_context_t* ctx) {
  return ctx->rep.bytes_read;
}

uint64_t crocksdb_iostats_context_open_nanos(crocksdb_iostats_context_t* ctx) {
  return ctx->rep.open_nanos;
}

uint64_t crocksdb_iostats_context_allocate_nanos(
    crocksdb_iostats_context_t* ctx) {
  return ctx->rep.allocate_nanos;
}

uint64_t crocksdb_iostats_context_write_nanos(crocksdb_iostats_context_t* ctx) {
  return ctx->rep.write_nanos;
}

uint64_t crocksdb_iostats_context_read_nanos(crocksdb_iostats_context_t* ctx) {
  return ctx->rep.read_nanos;
}

uint64_t crocksdb_iostats_context_range_sync_nanos(
    crocksdb_iostats_context_t* ctx) {
  return ctx->rep.range_sync_nanos;
}

uint64_t crocksdb_iostats_context_fsync_nanos(crocksdb_iostats_context_t* ctx) {
  return ctx->rep.fsync_nanos;
}

uint64_t crocksdb_iostats_context_prepare_write_nanos(
    crocksdb_iostats_context_t* ctx) {
  return ctx->rep.prepare_write_nanos;
}

uint64_t crocksdb_iostats_context_logger_nanos(
    crocksdb_iostats_context_t* ctx) {
  return ctx->rep.logger_nanos;
}

crocksdb_sst_partitioner_request_t* crocksdb_sst_partitioner_request_create() {
  auto* req = new crocksdb_sst_partitioner_request_t;
  req->rep =
      new PartitionerRequest(req->prev_user_key, req->current_user_key, 0);
  return req;
}

void crocksdb_sst_partitioner_request_destroy(
    crocksdb_sst_partitioner_request_t* req) {
  delete req->rep;
  delete req;
}

const char* crocksdb_sst_partitioner_request_prev_user_key(
    crocksdb_sst_partitioner_request_t* req, size_t* len) {
  const Slice* prev_key = req->rep->prev_user_key;
  *len = prev_key->size();
  return prev_key->data();
}

const char* crocksdb_sst_partitioner_request_current_user_key(
    crocksdb_sst_partitioner_request_t* req, size_t* len) {
  const Slice* current_key = req->rep->current_user_key;
  *len = current_key->size();
  return current_key->data();
}

uint64_t crocksdb_sst_partitioner_request_current_output_file_size(
    crocksdb_sst_partitioner_request_t* req) {
  return req->rep->current_output_file_size;
}

void crocksdb_sst_partitioner_request_set_prev_user_key(
    crocksdb_sst_partitioner_request_t* req, const char* key, size_t len) {
  req->prev_user_key = Slice(key, len);
  req->rep->prev_user_key = &req->prev_user_key;
}

void crocksdb_sst_partitioner_request_set_current_user_key(
    crocksdb_sst_partitioner_request_t* req, const char* key, size_t len) {
  req->current_user_key = Slice(key, len);
  req->rep->current_user_key = &req->current_user_key;
}

void crocksdb_sst_partitioner_request_set_current_output_file_size(
    crocksdb_sst_partitioner_request_t* req,
    uint64_t current_output_file_size) {
  req->rep->current_output_file_size = current_output_file_size;
}

struct crocksdb_sst_partitioner_impl_t : public SstPartitioner {
  void* underlying;
  void (*destructor)(void*);
  crocksdb_sst_partitioner_should_partition_cb should_partition_cb;
  crocksdb_sst_partitioner_can_do_trivial_move_cb can_do_trivial_move_cb;

  virtual ~crocksdb_sst_partitioner_impl_t() { destructor(underlying); }

  const char* Name() const override { return "crocksdb_sst_partitioner_impl"; }

  PartitionerResult ShouldPartition(
      const PartitionerRequest& request) override {
    crocksdb_sst_partitioner_request_t req;
    req.rep = const_cast<PartitionerRequest*>(&request);
    return static_cast<PartitionerResult>(
        should_partition_cb(underlying, &req));
  }

  bool CanDoTrivialMove(const Slice& smallest_user_key,
                        const Slice& largest_user_key) override {
    return can_do_trivial_move_cb(
        underlying, smallest_user_key.data(), smallest_user_key.size(),
        largest_user_key.data(), largest_user_key.size());
  }
};

crocksdb_sst_partitioner_t* crocksdb_sst_partitioner_create(
    void* underlying, void (*destructor)(void*),
    crocksdb_sst_partitioner_should_partition_cb should_partition_cb,
    crocksdb_sst_partitioner_can_do_trivial_move_cb can_do_trivial_move_cb) {
  crocksdb_sst_partitioner_impl_t* sst_partitioner_impl =
      new crocksdb_sst_partitioner_impl_t;
  sst_partitioner_impl->underlying = underlying;
  sst_partitioner_impl->destructor = destructor;
  sst_partitioner_impl->should_partition_cb = should_partition_cb;
  sst_partitioner_impl->can_do_trivial_move_cb = can_do_trivial_move_cb;
  crocksdb_sst_partitioner_t* sst_partitioner = new crocksdb_sst_partitioner_t;
  sst_partitioner->rep.reset(sst_partitioner_impl);
  return sst_partitioner;
}

void crocksdb_sst_partitioner_destroy(crocksdb_sst_partitioner_t* partitioner) {
  delete partitioner;
}

uint32_t crocksdb_sst_partitioner_should_partition(
    crocksdb_sst_partitioner_t* partitioner,
    crocksdb_sst_partitioner_request_t* req) {
  return static_cast<uint32_t>(partitioner->rep->ShouldPartition(*req->rep));
}

unsigned char crocksdb_sst_partitioner_can_do_trivial_move(
    crocksdb_sst_partitioner_t* partitioner, const char* smallest_user_key,
    size_t smallest_user_key_len, const char* largest_user_key,
    size_t largest_user_key_len) {
  Slice smallest_key(smallest_user_key, smallest_user_key_len);
  Slice largest_key(largest_user_key, largest_user_key_len);
  return partitioner->rep->CanDoTrivialMove(smallest_key, largest_key);
}

crocksdb_sst_partitioner_context_t* crocksdb_sst_partitioner_context_create() {
  auto* rep = new SstPartitioner::Context;
  auto* context = new crocksdb_sst_partitioner_context_t;
  context->rep = rep;
  return context;
}

void crocksdb_sst_partitioner_context_destroy(
    crocksdb_sst_partitioner_context_t* context) {
  delete context->rep;
  delete context;
}

unsigned char crocksdb_sst_partitioner_context_is_full_compaction(
    crocksdb_sst_partitioner_context_t* context) {
  return context->rep->is_full_compaction;
}

unsigned char crocksdb_sst_partitioner_context_is_manual_compaction(
    crocksdb_sst_partitioner_context_t* context) {
  return context->rep->is_manual_compaction;
}

int crocksdb_sst_partitioner_context_output_level(
    crocksdb_sst_partitioner_context_t* context) {
  return context->rep->output_level;
}

int crocksdb_sst_partitioner_context_next_level_segment_count(
    crocksdb_sst_partitioner_context_t* context) {
  return context->rep->OutputNextLevelSegmentCount();
}

size_t crocksdb_sst_partitioner_context_get_next_level_size(
    crocksdb_sst_partitioner_context_t* context, int index) {
  return context->rep->output_next_level_size[index];
}

void crocksdb_sst_partitioner_context_get_next_level_boundary(
    crocksdb_sst_partitioner_context_t* context, int index, const char** key,
    size_t* key_len) {
  const auto s = context->rep->output_next_level_boundaries[index];
  *key = s.data();
  *key_len = s.size();
}

void crocksdb_sst_partitioner_context_push_bounary_and_size(
    crocksdb_sst_partitioner_context_t* context, const char* boundary_key,
    size_t boundary_key_len, size_t size) {
  if (!context->rep->output_next_level_boundaries.empty()) {
    // The first boundary means the left-bondary, which isn't a segment.
    // Its size should be ignored.
    context->rep->output_next_level_size.push_back(size);
  }
  context->rep->output_next_level_boundaries.emplace_back(boundary_key,
                                                          boundary_key_len);
}

const char* crocksdb_sst_partitioner_context_smallest_key(
    crocksdb_sst_partitioner_context_t* context, size_t* key_len) {
  auto& smallest_key = context->rep->smallest_user_key;
  *key_len = smallest_key.size();
  return smallest_key.data();
}

const char* crocksdb_sst_partitioner_context_largest_key(
    crocksdb_sst_partitioner_context_t* context, size_t* key_len) {
  auto& largest_key = context->rep->largest_user_key;
  *key_len = largest_key.size();
  return largest_key.data();
}

void crocksdb_sst_partitioner_context_set_is_full_compaction(
    crocksdb_sst_partitioner_context_t* context,
    unsigned char is_full_compaction) {
  context->rep->is_full_compaction = is_full_compaction;
}

void crocksdb_sst_partitioner_context_set_is_manual_compaction(
    crocksdb_sst_partitioner_context_t* context,
    unsigned char is_manual_compaction) {
  context->rep->is_manual_compaction = is_manual_compaction;
}

void crocksdb_sst_partitioner_context_set_output_level(
    crocksdb_sst_partitioner_context_t* context, int output_level) {
  context->rep->output_level = output_level;
}

void crocksdb_sst_partitioner_context_set_smallest_key(
    crocksdb_sst_partitioner_context_t* context, const char* smallest_key,
    size_t key_len) {
  context->rep->smallest_user_key = Slice(smallest_key, key_len);
}

void crocksdb_sst_partitioner_context_set_largest_key(
    crocksdb_sst_partitioner_context_t* context, const char* largest_key,
    size_t key_len) {
  context->rep->largest_user_key = Slice(largest_key, key_len);
}

struct crocksdb_sst_partitioner_factory_impl_t : public SstPartitionerFactory {
  void* underlying;
  void (*destructor)(void*);
  crocksdb_sst_partitioner_factory_name_cb name_cb;
  crocksdb_sst_partitioner_factory_create_partitioner_cb create_partitioner_cb;

  virtual ~crocksdb_sst_partitioner_factory_impl_t() { destructor(underlying); }

  const char* Name() const override { return name_cb(underlying); }

  std::unique_ptr<SstPartitioner> CreatePartitioner(
      const SstPartitioner::Context& partitioner_context) const override {
    crocksdb_sst_partitioner_context_t context;
    context.rep = const_cast<SstPartitioner::Context*>(&partitioner_context);
    crocksdb_sst_partitioner_t* partitioner =
        create_partitioner_cb(underlying, &context);
    if (partitioner == nullptr) {
      return nullptr;
    }
    std::unique_ptr<SstPartitioner> rep = std::move(partitioner->rep);
    crocksdb_sst_partitioner_destroy(partitioner);
    return rep;
  }
};

crocksdb_sst_partitioner_factory_t* crocksdb_sst_partitioner_factory_create(
    void* underlying, void (*destructor)(void*),
    crocksdb_sst_partitioner_factory_name_cb name_cb,
    crocksdb_sst_partitioner_factory_create_partitioner_cb
        create_partitioner_cb) {
  crocksdb_sst_partitioner_factory_impl_t* factory_impl =
      new crocksdb_sst_partitioner_factory_impl_t;
  factory_impl->underlying = underlying;
  factory_impl->destructor = destructor;
  factory_impl->name_cb = name_cb;
  factory_impl->create_partitioner_cb = create_partitioner_cb;
  crocksdb_sst_partitioner_factory_t* factory =
      new crocksdb_sst_partitioner_factory_t;
  factory->rep.reset(factory_impl);
  return factory;
}

void crocksdb_sst_partitioner_factory_destroy(
    crocksdb_sst_partitioner_factory_t* factory) {
  delete factory;
}

const char* crocksdb_sst_partitioner_factory_name(
    crocksdb_sst_partitioner_factory_t* factory) {
  return factory->rep->Name();
}

crocksdb_sst_partitioner_t* crocksdb_sst_partitioner_factory_create_partitioner(
    crocksdb_sst_partitioner_factory_t* factory,
    crocksdb_sst_partitioner_context_t* context) {
  std::unique_ptr<SstPartitioner> rep =
      factory->rep->CreatePartitioner(*context->rep);
  if (rep == nullptr) {
    return nullptr;
  }
  crocksdb_sst_partitioner_t* partitioner = new crocksdb_sst_partitioner_t;
  partitioner->rep = std::move(rep);
  return partitioner;
}

/* Tools */

void crocksdb_run_ldb_tool(int argc, char** argv,
                           const crocksdb_options_t* opts) {
  LDBTool().Run(argc, argv, opts->rep);
}

void crocksdb_run_sst_dump_tool(int argc, char** argv,
                                const crocksdb_options_t* opts) {
  SSTDumpTool().Run(argc, argv, opts->rep);
}

/* Titan */
struct ctitandb_checkpoint_t {
  TitanCheckpoint* rep;
};

struct ctitandb_options_t {
  TitanOptions rep;
};

ctitandb_checkpoint_t* ctitandb_checkpoint_object_create(crocksdb_t* db,
                                                         char** errptr) {
  TitanCheckpoint* checkpoint;
  if (SaveError(errptr, TitanCheckpoint::Create(static_cast<TitanDB*>(db->rep),
                                                &checkpoint))) {
    return nullptr;
  }
  ctitandb_checkpoint_t* result = new ctitandb_checkpoint_t;
  result->rep = checkpoint;
  return result;
}

void ctitandb_checkpoint_create(ctitandb_checkpoint_t* checkpoint,
                                const char* basedb_checkpoint_dir,
                                const char* titan_checkpoint_dir,
                                uint64_t log_size_for_flush, char** errptr) {
  SaveError(errptr, checkpoint->rep->CreateCheckpoint(
                        std::string(basedb_checkpoint_dir),
                        std::string(titan_checkpoint_dir), log_size_for_flush));
}

void ctitandb_checkpoint_object_destroy(ctitandb_checkpoint_t* checkpoint) {
  delete checkpoint->rep;
  delete checkpoint;
}

crocksdb_t* ctitandb_open_column_families(
    const char* name, const ctitandb_options_t* tdb_options,
    int num_column_families, const char** column_family_names,
    const ctitandb_options_t** titan_column_family_options,
    crocksdb_column_family_handle_t** column_family_handles, char** errptr) {
  std::vector<TitanCFDescriptor> column_families;
  for (int i = 0; i < num_column_families; i++) {
    column_families.push_back(
        TitanCFDescriptor(std::string(column_family_names[i]),
                          TitanCFOptions(titan_column_family_options[i]->rep)));
  }

  TitanDB* db;
  std::vector<ColumnFamilyHandle*> handles;
  if (SaveError(errptr, TitanDB::Open(tdb_options->rep, std::string(name),
                                      column_families, &handles, &db))) {
    return nullptr;
  }
  for (size_t i = 0; i < handles.size(); i++) {
    crocksdb_column_family_handle_t* c_handle =
        new crocksdb_column_family_handle_t;
    c_handle->rep = handles[i];
    column_family_handles[i] = c_handle;
  }
  crocksdb_t* result = new crocksdb_t;
  result->rep = db;
  return result;
}

// Caller should make sure `db` is created from ctitandb_open_column_families.
//
// TODO: ctitandb_open_column_family should return a ctitandb_t. Caller can
// use ctitandb_t for titan specific functions.
crocksdb_column_family_handle_t* ctitandb_create_column_family(
    crocksdb_t* db, const ctitandb_options_t* titan_column_family_options,
    const char* column_family_name, char** errptr) {
  // Blindly cast db into TitanDB.
  TitanDB* titan_db = reinterpret_cast<TitanDB*>(db->rep);
  crocksdb_column_family_handle_t* handle = new crocksdb_column_family_handle_t;
  SaveError(errptr, titan_db->CreateColumnFamily(
                        TitanCFDescriptor(std::string(column_family_name),
                                          titan_column_family_options->rep),
                        &(handle->rep)));
  return handle;
}

/* TitanDBOptions */

ctitandb_options_t* ctitandb_options_create() { return new ctitandb_options_t; }

void ctitandb_options_destroy(ctitandb_options_t* opts) { delete opts; }

ctitandb_options_t* ctitandb_options_copy(ctitandb_options_t* src) {
  if (src == nullptr) {
    return nullptr;
  }
  return new ctitandb_options_t{src->rep};
}

void ctitandb_options_set_rocksdb_options(
    ctitandb_options_t* opts, const crocksdb_options_t* rocksdb_opts) {
  *(DBOptions*)&opts->rep = rocksdb_opts->rep;
  *(ColumnFamilyOptions*)&opts->rep = rocksdb_opts->rep;
}

ctitandb_options_t* ctitandb_get_titan_options_cf(
    const crocksdb_t* db, crocksdb_column_family_handle_t* column_family) {
  ctitandb_options_t* options = new ctitandb_options_t;
  TitanDB* titan_db = reinterpret_cast<TitanDB*>(db->rep);
  options->rep = titan_db->GetTitanOptions(column_family->rep);
  return options;
}

ctitandb_options_t* ctitandb_get_titan_db_options(crocksdb_t* db) {
  ctitandb_options_t* options = new ctitandb_options_t;
  TitanDB* titan_db = reinterpret_cast<TitanDB*>(db->rep);
  *static_cast<TitanDBOptions*>(&options->rep) = titan_db->GetTitanDBOptions();
  return options;
}

const char* ctitandb_options_dirname(ctitandb_options_t* opts) {
  return opts->rep.dirname.c_str();
}

void ctitandb_options_set_dirname(ctitandb_options_t* opts, const char* name) {
  opts->rep.dirname = name;
}

uint64_t ctitandb_options_min_blob_size(ctitandb_options_t* opts) {
  return opts->rep.min_blob_size;
}

void ctitandb_options_set_min_blob_size(ctitandb_options_t* opts,
                                        uint64_t size) {
  opts->rep.min_blob_size = size;
}

int ctitandb_options_blob_file_compression(ctitandb_options_t* opts) {
  return opts->rep.blob_file_compression;
}

void ctitandb_options_set_blob_file_compression(ctitandb_options_t* opts,
                                                int type) {
  opts->rep.blob_file_compression = static_cast<CompressionType>(type);
}

void ctitandb_options_set_compression_options(ctitandb_options_t* opt,
                                              int w_bits, int level,
                                              int strategy, int max_dict_bytes,
                                              int zstd_max_train_bytes) {
  opt->rep.blob_file_compression_options.window_bits = w_bits;
  opt->rep.blob_file_compression_options.level = level;
  opt->rep.blob_file_compression_options.strategy = strategy;
  opt->rep.blob_file_compression_options.max_dict_bytes = max_dict_bytes;
  opt->rep.blob_file_compression_options.zstd_max_train_bytes =
      zstd_max_train_bytes;
}

void ctitandb_decode_blob_index(const char* value, size_t value_size,
                                ctitandb_blob_index_t* index, char** errptr) {
  Slice v(value, value_size);
  BlobIndex bi;
  if (SaveError(errptr, bi.DecodeFrom(&v))) {
    return;
  }
  index->file_number = bi.file_number;
  index->blob_offset = bi.blob_handle.offset;
  index->blob_size = bi.blob_handle.size;
  index->blob_raw_size = bi.blob_handle.raw_size;
}

void ctitandb_encode_blob_index(const ctitandb_blob_index_t* index,
                                char** value, size_t* value_size) {
  BlobIndex bi;
  bi.file_number = index->file_number;
  bi.blob_handle.offset = index->blob_offset;
  bi.blob_handle.size = index->blob_size;
  bi.blob_handle.raw_size = index->blob_raw_size;
  std::string result;
  bi.EncodeTo(&result);
  *value = CopyString(result);
  *value_size = result.size();
}

void ctitandb_options_set_disable_background_gc(ctitandb_options_t* options,
                                                unsigned char disable) {
  options->rep.disable_background_gc = disable;
}

void ctitandb_options_set_level_merge(ctitandb_options_t* options,
                                      unsigned char enable) {
  options->rep.level_merge = enable;
}

void ctitandb_options_set_range_merge(ctitandb_options_t* options,
                                      unsigned char enable) {
  options->rep.range_merge = enable;
}

void ctitandb_options_set_max_sorted_runs(ctitandb_options_t* options,
                                          int size) {
  options->rep.max_sorted_runs = size;
}

void ctitandb_options_set_max_gc_batch_size(ctitandb_options_t* options,
                                            uint64_t size) {
  options->rep.max_gc_batch_size = size;
}

void ctitandb_options_set_min_gc_batch_size(ctitandb_options_t* options,
                                            uint64_t size) {
  options->rep.min_gc_batch_size = size;
}

void ctitandb_options_set_blob_file_discardable_ratio(
    ctitandb_options_t* options, double ratio) {
  options->rep.blob_file_discardable_ratio = ratio;
}

void ctitandb_options_set_merge_small_file_threshold(
    ctitandb_options_t* options, uint64_t size) {
  options->rep.merge_small_file_threshold = size;
}

void ctitandb_options_set_max_background_gc(ctitandb_options_t* options,
                                            int32_t size) {
  options->rep.max_background_gc = size;
}

void ctitandb_options_set_purge_obsolete_files_period_sec(
    ctitandb_options_t* options, unsigned int period) {
  options->rep.purge_obsolete_files_period_sec = period;
}

void ctitandb_options_set_blob_cache(ctitandb_options_t* options,
                                     crocksdb_cache_t* cache) {
  if (cache) {
    options->rep.blob_cache = cache->rep;
  }
}

size_t ctitandb_options_get_blob_cache_usage(ctitandb_options_t* opt) {
  if (opt && opt->rep.blob_cache != nullptr) {
    return opt->rep.blob_cache->GetUsage();
  }
  return 0;
}

void ctitandb_options_set_blob_cache_capacity(ctitandb_options_t* opt,
                                              size_t capacity, char** errptr) {
  Status s;
  if (opt && opt->rep.blob_cache != nullptr) {
    return opt->rep.blob_cache->SetCapacity(capacity);
  } else {
    s = Status::InvalidArgument("Blob cache was disabled.");
  }
  SaveError(errptr, s);
}

size_t ctitandb_options_get_blob_cache_capacity(ctitandb_options_t* opt) {
  if (opt && opt->rep.blob_cache != nullptr) {
    return opt->rep.blob_cache->GetCapacity();
  }
  return 0;
}

void ctitandb_options_set_discardable_ratio(ctitandb_options_t* options,
                                            double ratio) {
  options->rep.blob_file_discardable_ratio = ratio;
}

void ctitandb_options_set_blob_run_mode(ctitandb_options_t* options,
                                        uint32_t mode) {
  options->rep.blob_run_mode = static_cast<TitanBlobRunMode>(mode);
}

/* TitanReadOptions */
struct ctitandb_readoptions_t {
  TitanReadOptions rep;
};

ctitandb_readoptions_t* ctitandb_readoptions_create() {
  return new ctitandb_readoptions_t;
}

void ctitandb_readoptions_destroy(ctitandb_readoptions_t* opts) { delete opts; }

unsigned char ctitandb_readoptions_key_only(ctitandb_readoptions_t* opts) {
  return opts->rep.key_only;
}

void ctitandb_readoptions_set_key_only(ctitandb_readoptions_t* opts,
                                       unsigned char v) {
  opts->rep.key_only = v;
}

crocksdb_iterator_t* ctitandb_create_iterator(
    crocksdb_t* db, const crocksdb_readoptions_t* options,
    const ctitandb_readoptions_t* titan_options) {
  crocksdb_iterator_t* result = new crocksdb_iterator_t;
  if (titan_options == nullptr) {
    result->rep = db->rep->NewIterator(options->rep);
  } else {
    *(ReadOptions*)&titan_options->rep = options->rep;
    result->rep =
        static_cast<TitanDB*>(db->rep)->NewIterator(titan_options->rep);
  }
  return result;
}

crocksdb_iterator_t* ctitandb_create_iterator_cf(
    crocksdb_t* db, const crocksdb_readoptions_t* options,
    const ctitandb_readoptions_t* titan_options,
    crocksdb_column_family_handle_t* column_family) {
  crocksdb_iterator_t* result = new crocksdb_iterator_t;
  if (titan_options == nullptr) {
    result->rep = db->rep->NewIterator(options->rep, column_family->rep);
  } else {
    *(ReadOptions*)&titan_options->rep = options->rep;
    result->rep = static_cast<TitanDB*>(db->rep)->NewIterator(
        titan_options->rep, column_family->rep);
  }
  return result;
}

void ctitandb_create_iterators(
    crocksdb_t* db, crocksdb_readoptions_t* options,
    ctitandb_readoptions_t* titan_options,
    crocksdb_column_family_handle_t** column_families,
    crocksdb_iterator_t** iterators, size_t size, char** errptr) {
  std::vector<ColumnFamilyHandle*> column_families_vec(size);
  for (size_t i = 0; i < size; i++) {
    column_families_vec.push_back(column_families[i]->rep);
  }

  std::vector<Iterator*> res;
  Status status;
  if (titan_options == nullptr) {
    status = db->rep->NewIterators(options->rep, column_families_vec, &res);
  } else {
    *(ReadOptions*)&titan_options->rep = options->rep;
    status = static_cast<TitanDB*>(db->rep)->NewIterators(
        titan_options->rep, column_families_vec, &res);
  }
  if (SaveError(errptr, status)) {
    for (size_t i = 0; i < res.size(); i++) {
      delete res[i];
    }
    return;
  }
  assert(res.size() == size);

  for (size_t i = 0; i < size; i++) {
    iterators[i] = new crocksdb_iterator_t;
    iterators[i]->rep = res[i];
  }
}

void ctitandb_delete_files_in_range(crocksdb_t* db, const char* start_key,
                                    size_t start_key_len, const char* limit_key,
                                    size_t limit_key_len,
                                    unsigned char include_end, char** errptr) {
  Slice a, b;
  RangePtr range(
      start_key ? (a = Slice(start_key, start_key_len), &a) : nullptr,
      limit_key ? (b = Slice(limit_key, limit_key_len), &b) : nullptr);

  SaveError(errptr,
            static_cast<TitanDB*>(db->rep)->DeleteFilesInRanges(
                db->rep->DefaultColumnFamily(), &range, 1, include_end));
}

void ctitandb_delete_files_in_range_cf(
    crocksdb_t* db, crocksdb_column_family_handle_t* column_family,
    const char* start_key, size_t start_key_len, const char* limit_key,
    size_t limit_key_len, unsigned char include_end, char** errptr) {
  Slice a, b;
  RangePtr range(
      start_key ? (a = Slice(start_key, start_key_len), &a) : nullptr,
      limit_key ? (b = Slice(limit_key, limit_key_len), &b) : nullptr);

  SaveError(errptr, static_cast<TitanDB*>(db->rep)->DeleteFilesInRanges(
                        column_family->rep, &range, 1, include_end));
}

void ctitandb_delete_files_in_ranges_cf(
    crocksdb_t* db, crocksdb_column_family_handle_t* cf,
    const char* const* start_keys, const size_t* start_keys_lens,
    const char* const* limit_keys, const size_t* limit_keys_lens,
    size_t num_ranges, unsigned char include_end, char** errptr) {
  std::vector<Slice> starts(num_ranges);
  std::vector<Slice> limits(num_ranges);
  std::vector<RangePtr> ranges(num_ranges);
  for (auto i = 0; i < num_ranges; i++) {
    const Slice* start = nullptr;
    if (start_keys[i]) {
      starts[i] = Slice(start_keys[i], start_keys_lens[i]);
      start = &starts[i];
    }
    const Slice* limit = nullptr;
    if (limit_keys[i]) {
      limits[i] = Slice(limit_keys[i], limit_keys_lens[i]);
      limit = &limits[i];
    }
    ranges[i] = RangePtr(start, limit);
  }
  SaveError(errptr, static_cast<TitanDB*>(db->rep)->DeleteFilesInRanges(
                        cf->rep, &ranges[0], num_ranges, include_end));
}

void ctitandb_delete_blob_files_in_range(crocksdb_t* db, const char* start_key,
                                         size_t start_key_len,
                                         const char* limit_key,
                                         size_t limit_key_len,
                                         unsigned char include_end,
                                         char** errptr) {
  Slice a, b;
  RangePtr range(
      start_key ? (a = Slice(start_key, start_key_len), &a) : nullptr,
      limit_key ? (b = Slice(limit_key, limit_key_len), &b) : nullptr);

  SaveError(errptr,
            static_cast<TitanDB*>(db->rep)->DeleteBlobFilesInRanges(
                db->rep->DefaultColumnFamily(), &range, 1, include_end));
}

void ctitandb_delete_blob_files_in_range_cf(
    crocksdb_t* db, crocksdb_column_family_handle_t* column_family,
    const char* start_key, size_t start_key_len, const char* limit_key,
    size_t limit_key_len, unsigned char include_end, char** errptr) {
  Slice a, b;
  RangePtr range(
      start_key ? (a = Slice(start_key, start_key_len), &a) : nullptr,
      limit_key ? (b = Slice(limit_key, limit_key_len), &b) : nullptr);

  SaveError(errptr, static_cast<TitanDB*>(db->rep)->DeleteBlobFilesInRanges(
                        column_family->rep, &range, 1, include_end));
}

void ctitandb_delete_blob_files_in_ranges_cf(
    crocksdb_t* db, crocksdb_column_family_handle_t* cf,
    const char* const* start_keys, const size_t* start_keys_lens,
    const char* const* limit_keys, const size_t* limit_keys_lens,
    size_t num_ranges, unsigned char include_end, char** errptr) {
  std::vector<Slice> starts(num_ranges);
  std::vector<Slice> limits(num_ranges);
  std::vector<RangePtr> ranges(num_ranges);
  for (auto i = 0; i < num_ranges; i++) {
    const Slice* start = nullptr;
    if (start_keys[i]) {
      starts[i] = Slice(start_keys[i], start_keys_lens[i]);
      start = &starts[i];
    }
    const Slice* limit = nullptr;
    if (limit_keys[i]) {
      limits[i] = Slice(limit_keys[i], limit_keys_lens[i]);
      limit = &limits[i];
    }
    ranges[i] = RangePtr(start, limit);
  }
  SaveError(errptr, static_cast<TitanDB*>(db->rep)->DeleteBlobFilesInRanges(
                        cf->rep, &ranges[0], num_ranges, include_end));
}

}  // end extern "C"
