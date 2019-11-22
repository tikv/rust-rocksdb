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

extern crate bzip2_sys;
extern crate libc;
#[cfg(test)]
extern crate tempfile;

use libc::{c_char, c_void};
use std::ffi::CStr;
use std::fmt;

include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
#[repr(C)]
pub enum WriteStallCondition {
    Normal = 0,
    Delayed = 1,
    Stopped = 2,
}

mod generated;
pub use generated::*;

#[repr(C)]
pub struct DBTitanDBOptions(c_void);
#[repr(C)]
pub struct DBTitanReadOptions(c_void);

#[derive(Clone, Debug, Default)]
#[repr(C)]
pub struct DBTitanBlobIndex {
    pub file_number: u64,
    pub blob_offset: u64,
    pub blob_size: u64,
}

pub fn new_lru_cache(opt: *mut crocksdb_lru_cache_options_t) -> *mut crocksdb_cache_t {
    unsafe { crocksdb_cache_create_lru(opt) }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
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

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
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

impl DBCompressionType {
    pub fn from_i32(value: i32) -> DBCompressionType {
        match value {
            0 => DBCompressionType::No,
            1 => DBCompressionType::Snappy,
            2 => DBCompressionType::Zlib,
            3 => DBCompressionType::Bz2,
            4 => DBCompressionType::Lz4,
            5 => DBCompressionType::Lz4hc,
            7 => DBCompressionType::Zstd,
            0x40 => DBCompressionType::ZstdNotFinal,
            0xff => DBCompressionType::Disable,
            _ => panic!("Unknown value: {}", value),
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
#[repr(C)]
pub enum DBCompactionStyle {
    Level = 0,
    Universal = 1,
    Fifo = 2,
    None = 3,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
#[repr(C)]
pub enum DBUniversalCompactionStyle {
    SimilarSize = 0,
    TotalSize = 1,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
#[repr(C)]
pub enum DBRecoveryMode {
    TolerateCorruptedTailRecords = 0,
    AbsoluteConsistency = 1,
    PointInTime = 2,
    SkipAnyCorruptedRecords = 3,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
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

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
#[repr(C)]
pub enum CompactionReason {
    Unknown,
    // [Level] number of L0 files > level0_file_num_compaction_trigger
    LevelL0FilesNum,
    // [Level] total size of level > MaxBytesForLevel()
    LevelMaxLevelSize,
    // [Universal] Compacting for size amplification
    UniversalSizeAmplification,
    // [Universal] Compacting for size ratio
    UniversalSizeRatio,
    // [Universal] number of sorted runs > level0_file_num_compaction_trigger
    UniversalSortedRunNum,
    // [FIFO] total size > max_table_files_size
    FIFOMaxSize,
    // [FIFO] reduce number of files.
    FIFOReduceNumFiles,
    // [FIFO] files with creation time < (current_time - interval)
    FIFOTtl,
    // Manual compaction
    ManualCompaction,
    // DB::SuggestCompactRange() marked files for compaction
    FilesMarkedForCompaction,
    // [Level] Automatic compaction within bottommost level to cleanup duplicate
    // versions of same user key, usually due to a released snapshot.
    BottommostFiles,
    // Compaction based on TTL
    Ttl,
    // According to the comments in flush_job.cc, RocksDB treats flush as
    // a level 0 compaction in internal stats.
    Flush,
    // Compaction caused by external sst file ingestion
    ExternalSstIngestion,
    // total number of compaction reasons, new reasons must be added above this.
    NumOfReasons,
}

impl fmt::Display for CompactionReason {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
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

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
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

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
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

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
#[repr(C)]
pub enum DBRateLimiterMode {
    ReadOnly = 1,
    WriteOnly = 2,
    AllIo = 3,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
#[repr(C)]
pub enum DBTitanDBBlobRunMode {
    Normal = 0,
    ReadOnly = 1,
    Fallback = 2,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
#[repr(C)]
pub enum IndexType {
    BinarySearch = 0,
    HashSearch = 1,
    TwoLevelIndexSearch = 2,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
#[repr(C)]
pub enum DBBackgroundErrorReason {
    Flush = 1,
    Compaction = 2,
    WriteCallback = 3,
    MemTable = 4,
}

pub unsafe fn error_message(ptr: *mut c_char) -> String {
    let c_str = CStr::from_ptr(ptr);
    let s = format!("{}", c_str.to_string_lossy());
    libc::free(ptr as *mut c_void);
    s
}

#[macro_export]
macro_rules! ffi_try {
    ($func:ident($($arg:expr),+)) => ({
        use std::ptr;
        let mut err = ptr::null_mut();
        let res = $crate::$func($($arg),+, &mut err);
        if !err.is_null() {
            return Err($crate::error_message(err));
        }
        res
    });
    ($func:ident()) => ({
        use std::ptr;
        let mut err = ptr::null_mut();
        let res = $crate::$func(&mut err);
        if !err.is_null() {
            return Err($crate::error_message(err));
        }
        res
    })
}
