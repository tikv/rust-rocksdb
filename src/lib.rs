// Copyright 2014 Tyler Neely
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

extern crate core;
extern crate libc;
extern crate num;
#[macro_use]
pub extern crate librocksdb_sys;
#[cfg(test)]
extern crate tempfile;

pub use compaction_filter::CompactionFilter;
pub use event_listener::{
    CompactionJobInfo, EventListener, FlushJobInfo, IngestionInfo, WriteStallInfo,
};
pub use librocksdb_sys::{
    self as crocksdb_ffi, crocksdb_status_ptr_t as DBStatusPtr, new_bloom_filter,
    DBStatisticsHistogramType, DBStatisticsTickerType,
};
pub use merge_operator::MergeOperands;
pub use metadata::{ColumnFamilyMetaData, LevelMetaData, SstFileMetaData};
pub use perf_context::{get_perf_level, set_perf_level, IOStatsContext, PerfContext, PerfLevel};
pub use rocksdb::{
    load_latest_options, run_ldb_tool, set_external_sst_file_global_seq_no, BackupEngine, CFHandle,
    Cache, DBIterator, DBVector, Env, ExternalSstFileInfo, MapProperty, MemoryAllocator, Range,
    SeekKey, SequentialFile, SstFileReader, SstFileWriter, Writable, WriteBatch, DB,
};
pub use rocksdb_options::{
    BlockBasedOptions, CColumnFamilyDescriptor, ColumnFamilyOptions, CompactOptions,
    CompactionOptions, DBOptions, EnvOptions, FifoCompactionOptions, HistogramData,
    IngestExternalFileOptions, LRUCacheOptions, RateLimiter, ReadOptions, RestoreOptions,
    WriteOptions,
};
pub use slice_transform::SliceTransform;
pub use table_filter::TableFilter;
pub use table_properties::{
    TableProperties, TablePropertiesCollection, TablePropertiesCollectionView,
    UserCollectedProperties,
};
pub use table_properties_collector::TablePropertiesCollector;
pub use table_properties_collector_factory::TablePropertiesCollectorFactory;
pub use titan::{TitanBlobIndex, TitanDBOptions};

#[allow(deprecated)]
pub use rocksdb::Kv;

mod compaction_filter;
pub mod comparator;
mod event_listener;
pub mod merge_operator;
mod metadata;
mod perf_context;
pub mod rocksdb;
pub mod rocksdb_options;
mod slice_transform;
mod table_filter;
mod table_properties;
mod table_properties_collector;
mod table_properties_collector_factory;
mod titan;
mod util;

use std::fmt;
#[macro_use]
extern crate num_derive;

#[derive(Copy, Clone, Debug, Eq, PartialEq, FromPrimitive)]
#[repr(C)]
pub enum WriteStallCondition {
    Normal = 0,
    Delayed = 1,
    Stopped = 2,
}

#[derive(Clone, Debug, Default)]
#[repr(C)]
pub struct DBTitanBlobIndex {
    pub file_number: u64,
    pub blob_offset: u64,
    pub blob_size: u64,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, FromPrimitive)]
#[repr(C)]
pub enum DBEntryType {
    Put = 0,
    Delete = 1,
    SingleDelete = 2,
    Merge = 3,
    RangeDeletion = 4,
    BlobIndex = 5,
    Other = 6,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, FromPrimitive)]
#[repr(C)]
pub enum DBCompressionType {
    No = 0,
    Snappy = 1,
    Zlib = 2,
    Bz2 = 3,
    Lz4 = 4,
    Lz4hc = 5,
    // DBXpress = 6, not support currently.
    Zstd = 7,
    ZstdNotFinal = 0x40,
    Disable = 0xff,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, FromPrimitive)]
#[repr(C)]
pub enum DBCompactionStyle {
    Level = 0,
    Universal = 1,
    Fifo = 2,
    None = 3,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, FromPrimitive)]
#[repr(C)]
pub enum DBUniversalCompactionStyle {
    SimilarSize = 0,
    TotalSize = 1,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, FromPrimitive)]
#[repr(C)]
pub enum DBRecoveryMode {
    TolerateCorruptedTailRecords = 0,
    AbsoluteConsistency = 1,
    PointInTime = 2,
    SkipAnyCorruptedRecords = 3,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, FromPrimitive)]
#[repr(C)]
pub enum CompactionPriority {
    // In Level-based compaction, it Determines which file from a level to be
    // picked to merge to the next level. We suggest people try
    // kMinOverlappingRatio first when you tune your database.
    ByCompensatedSize = 0,
    // First compact files whose data's latest update time is oldest.
    // Try this if you only update some hot keys in small ranges.
    OldestLargestSeqFirst = 1,
    // First compact files whose range hasn't been compacted to the next level
    // for the longest. If your updates are random across the key space,
    // write amplification is slightly better with this option.
    OldestSmallestSeqFirst = 2,
    // First compact files whose ratio between overlapping size in next level
    // and its size is the smallest. It in many cases can optimize write
    // amplification.
    MinOverlappingRatio = 3,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, FromPrimitive)]
#[repr(C)]
pub enum CompactionReason {
    Unknown = 0,
    // [Level] number of L0 files > level0_file_num_compaction_trigger
    LevelL0FilesNum = 1,
    // [Level] total size of level > MaxBytesForLevel()
    LevelMaxLevelSize = 2,
    // [Universal] Compacting for size amplification
    UniversalSizeAmplification = 3,
    // [Universal] Compacting for size ratio
    UniversalSizeRatio = 4,
    // [Universal] number of sorted runs > level0_file_num_compaction_trigger
    UniversalSortedRunNum = 5,
    // [FIFO] total size > max_table_files_size
    FIFOMaxSize = 6,
    // [FIFO] reduce number of files.
    FIFOReduceNumFiles = 7,
    // [FIFO] files with creation time < (current_time - interval)
    FIFOTtl = 8,
    // Manual compaction
    ManualCompaction = 9,
    // DB::SuggestCompactRange() marked files for compaction
    FilesMarkedForCompaction = 10,
    // [Level] Automatic compaction within bottommost level to cleanup duplicate
    // versions of same user key, usually due to a released snapshot.
    BottommostFiles = 11,
    // Compaction based on TTL
    Ttl = 12,
    // According to the comments in flush_job.cc, RocksDB treats flush as
    // a level 0 compaction in internal stats.
    Flush = 13,
    // Compaction caused by external sst file ingestion
    ExternalSstIngestion = 14,
    // total number of compaction reasons, new reasons must be added above this.
    NumOfReasons = 15,
}

impl fmt::Display for CompactionReason {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, FromPrimitive)]
#[repr(C)]
pub enum DBInfoLogLevel {
    Debug = 0,
    Info = 1,
    Warn = 2,
    Error = 3,
    Fatal = 4,
    Header = 5,
    NumInfoLog = 6,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, FromPrimitive)]
#[repr(C)]
pub enum DBTableProperty {
    DataSize = 1,
    IndexSize = 2,
    FilterSize = 3,
    RawKeySize = 4,
    RawValueSize = 5,
    NumDataBlocks = 6,
    NumEntries = 7,
    FormatVersion = 8,
    FixedKeyLen = 9,
    ColumnFamilyId = 10,
    ColumnFamilyName = 11,
    FilterPolicyName = 12,
    ComparatorName = 13,
    MergeOperatorName = 14,
    PrefixExtractorName = 15,
    PropertyCollectorsNames = 16,
    CompressionName = 17,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, FromPrimitive)]
#[repr(C)]
pub enum DBBottommostLevelCompaction {
    // Skip bottommost level compaction
    Skip = 0,
    // Compact bottommost level if there is a compaction filter
    // This is the default option
    IfHaveCompactionFilter = 1,
    // Force bottommost level compaction
    Force = 2,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, FromPrimitive)]
#[repr(C)]
pub enum DBRateLimiterMode {
    ReadOnly = 1,
    WriteOnly = 2,
    AllIo = 3,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, FromPrimitive)]
#[repr(C)]
pub enum DBTitanDBBlobRunMode {
    Normal = 0,
    ReadOnly = 1,
    Fallback = 2,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, FromPrimitive)]
#[repr(C)]
pub enum IndexType {
    BinarySearch = 0,
    HashSearch = 1,
    TwoLevelIndexSearch = 2,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, FromPrimitive)]
#[repr(C)]
pub enum DBBackgroundErrorReason {
    Flush = 1,
    Compaction = 2,
    WriteCallback = 3,
    MemTable = 4,
}

#[cfg(test)]
fn tempdir_with_prefix(prefix: &str) -> tempfile::TempDir {
    tempfile::Builder::new().prefix(prefix).tempdir().expect("")
}
