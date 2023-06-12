// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use crocksdb_ffi::{self, DBIOStatsContext, DBPerfContext, DBPerfFlags};

use std::{
    ops::{BitOr, BitOrAssign},
    ptr::NonNull,
};

#[derive(Copy, Clone, Debug, PartialEq)]
pub enum PerfLevel {
    Uninitialized,
    Disable,
    EnableCount,
    EnableTimeExceptForMutex,
    EnableTimeAndCPUTimeExceptForMutex,
    EnableTime,
    OutOfBounds,
}

pub fn get_perf_level() -> PerfLevel {
    let v = unsafe { crocksdb_ffi::crocksdb_get_perf_level() };
    match v {
        0 => PerfLevel::Uninitialized,
        1 => PerfLevel::Disable,
        2 => PerfLevel::EnableCount,
        3 => PerfLevel::EnableTimeExceptForMutex,
        4 => PerfLevel::EnableTimeAndCPUTimeExceptForMutex,
        5 => PerfLevel::EnableTime,
        6 => PerfLevel::OutOfBounds,
        _ => unreachable!(),
    }
}

pub fn set_perf_level(level: PerfLevel) {
    let v = match level {
        PerfLevel::Uninitialized => 0,
        PerfLevel::Disable => 1,
        PerfLevel::EnableCount => 2,
        PerfLevel::EnableTimeExceptForMutex => 3,
        PerfLevel::EnableTimeAndCPUTimeExceptForMutex => 4,
        PerfLevel::EnableTime => 5,
        PerfLevel::OutOfBounds => 6,
    };
    unsafe {
        crocksdb_ffi::crocksdb_set_perf_level(v);
    }
}

pub struct PerfFlags {
    inner: NonNull<DBPerfFlags>,
}

unsafe impl Send for PerfFlags {}

unsafe impl Sync for PerfFlags {}

impl PerfFlags {
    pub fn new() -> PerfFlags {
        unsafe {
            let inner = crocksdb_ffi::crocksdb_create_perf_flags();
            PerfFlags {
                inner: NonNull::new_unchecked(inner),
            }
        }
    }
}

impl Drop for PerfFlags {
    fn drop(&mut self) {
        unsafe {
            crocksdb_ffi::crocksdb_destroy_perf_flags(self.inner.as_ptr());
        }
    }
}

impl Default for PerfFlags {
    fn default() -> PerfFlags {
        PerfFlags::new()
    }
}

// Generated from librocksdb_sys/src/generate.py and copied here to implement
// operator traits.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
#[repr(u32)]
pub enum PerfFlag {
    UserKeyComparisonCount = 0,
    BlockCacheHitCount,
    BlockReadCount,
    BlockReadByte,
    BlockReadTime,
    BlockCacheIndexHitCount,
    IndexBlockReadCount,
    BlockCacheFilterHitCount,
    FilterBlockReadCount,
    CompressionDictBlockReadCount,
    SecondaryCacheHitCount,
    BlockChecksumTime,
    BlockDecompressTime,
    GetReadBytes,
    MultigetReadBytes,
    IterReadBytes,
    InternalKeySkippedCount,
    InternalDeleteSkippedCount,
    InternalRecentSkippedCount,
    InternalMergeCount,
    GetSnapshotTime,
    GetFromMemtableTime,
    GetFromMemtableCount,
    GetPostProcessTime,
    GetFromOutputFilesTime,
    SeekOnMemtableTime,
    SeekOnMemtableCount,
    NextOnMemtableCount,
    PrevOnMemtableCount,
    SeekChildSeekTime,
    SeekChildSeekCount,
    SeekMinHeapTime,
    SeekMaxHeapTime,
    SeekInternalSeekTime,
    FindNextUserEntryTime,
    WriteWalTime,
    WriteMemtableTime,
    WriteDelayTime,
    WriteSchedulingFlushesCompactionsTime,
    WritePreAndPostProcessTime,
    WriteThreadWaitNanos,
    DbMutexLockNanos,
    DbConditionWaitNanos,
    MergeOperatorTimeNanos,
    ReadIndexBlockNanos,
    ReadFilterBlockNanos,
    NewTableBlockIterNanos,
    NewTableIteratorNanos,
    BlockSeekNanos,
    FindTableNanos,
    BloomMemtableHitCount,
    BloomMemtableMissCount,
    BloomSstHitCount,
    BloomSstMissCount,
    KeyLockWaitTime,
    KeyLockWaitCount,
    EnvNewSequentialFileNanos,
    EnvNewRandomAccessFileNanos,
    EnvNewWritableFileNanos,
    EnvReuseWritableFileNanos,
    EnvNewRandomRwFileNanos,
    EnvNewDirectoryNanos,
    EnvFileExistsNanos,
    EnvGetChildrenNanos,
    EnvGetChildrenFileAttributesNanos,
    EnvDeleteFileNanos,
    EnvCreateDirNanos,
    EnvCreateDirIfMissingNanos,
    EnvDeleteDirNanos,
    EnvGetFileSizeNanos,
    EnvGetFileModificationTimeNanos,
    EnvRenameFileNanos,
    EnvLinkFileNanos,
    EnvLockFileNanos,
    EnvUnlockFileNanos,
    EnvNewLoggerNanos,
    GetCpuNanos,
    IterNextCpuNanos,
    IterPrevCpuNanos,
    IterSeekCpuNanos,
    EncryptDataNanos,
    DecryptDataNanos,
    GetFromTableNanos,
    UserKeyReturnCount,
    BlockCacheMissCount,
    BloomFilterFullPositive,
    BloomFilterUseful,
    BloomFilterFullTruePositive,
    BytesRead,
    BytesWritten,
    OpenNanos,
    AllocateNanos,
    WriteNanos,
    ReadNanos,
    RangeSyncNanos,
    PrepareWriteNanos,
    FsyncNanos,
    LoggerNanos,
    CpuReadNanos,
    CpuWriteNanos,
}

impl BitOrAssign<PerfFlag> for PerfFlags {
    fn bitor_assign(&mut self, rhs: PerfFlag) {
        unsafe {
            crocksdb_ffi::crocksdb_perf_flags_set(self.inner.as_ptr(), rhs as u32);
        }
    }
}

impl BitOr<PerfFlag> for PerfFlags {
    type Output = PerfFlags;

    fn bitor(mut self, rhs: PerfFlag) -> PerfFlags {
        self |= rhs;
        self
    }
}

impl BitOr<PerfFlag> for PerfFlag {
    type Output = PerfFlags;

    fn bitor(self, rhs: PerfFlag) -> PerfFlags {
        let mut flags = PerfFlags::default();
        flags |= self;
        flags |= rhs;
        flags
    }
}

pub fn set_perf_flags(flags: &PerfFlags) {
    unsafe {
        crocksdb_ffi::crocksdb_set_perf_flags(flags.inner.as_ptr());
    }
}

pub struct PerfContext {
    inner: *mut DBPerfContext,
}

impl PerfContext {
    pub fn get() -> PerfContext {
        unsafe {
            PerfContext {
                inner: crocksdb_ffi::crocksdb_get_perf_context(),
            }
        }
    }

    pub fn reset(&mut self) {
        unsafe { crocksdb_ffi::crocksdb_perf_context_reset(self.inner) }
    }

    pub fn user_key_comparison_count(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_perf_context_user_key_comparison_count(self.inner) }
    }

    pub fn block_cache_hit_count(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_perf_context_block_cache_hit_count(self.inner) }
    }

    pub fn block_read_count(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_perf_context_block_read_count(self.inner) }
    }

    pub fn block_read_byte(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_perf_context_block_read_byte(self.inner) }
    }

    pub fn block_read_time(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_perf_context_block_read_time(self.inner) }
    }

    pub fn block_cache_index_hit_count(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_perf_context_block_cache_index_hit_count(self.inner) }
    }

    pub fn index_block_read_count(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_perf_context_index_block_read_count(self.inner) }
    }

    pub fn block_cache_filter_hit_count(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_perf_context_block_cache_filter_hit_count(self.inner) }
    }

    pub fn filter_block_read_count(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_perf_context_filter_block_read_count(self.inner) }
    }

    pub fn block_checksum_time(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_perf_context_block_checksum_time(self.inner) }
    }

    pub fn block_decompress_time(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_perf_context_block_decompress_time(self.inner) }
    }

    pub fn get_read_bytes(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_perf_context_get_read_bytes(self.inner) }
    }

    pub fn multiget_read_bytes(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_perf_context_multiget_read_bytes(self.inner) }
    }

    pub fn iter_read_bytes(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_perf_context_iter_read_bytes(self.inner) }
    }

    pub fn internal_key_skipped_count(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_perf_context_internal_key_skipped_count(self.inner) }
    }

    pub fn internal_delete_skipped_count(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_perf_context_internal_delete_skipped_count(self.inner) }
    }

    pub fn internal_recent_skipped_count(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_perf_context_internal_recent_skipped_count(self.inner) }
    }

    pub fn internal_merge_count(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_perf_context_internal_merge_count(self.inner) }
    }

    pub fn get_snapshot_time(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_perf_context_get_snapshot_time(self.inner) }
    }

    pub fn get_from_memtable_time(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_perf_context_get_from_memtable_time(self.inner) }
    }

    pub fn get_from_memtable_count(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_perf_context_get_from_memtable_count(self.inner) }
    }

    pub fn get_post_process_time(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_perf_context_get_post_process_time(self.inner) }
    }

    pub fn get_from_output_files_time(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_perf_context_get_from_output_files_time(self.inner) }
    }

    pub fn seek_on_memtable_time(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_perf_context_seek_on_memtable_time(self.inner) }
    }

    pub fn seek_on_memtable_count(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_perf_context_seek_on_memtable_count(self.inner) }
    }

    pub fn next_on_memtable_count(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_perf_context_next_on_memtable_count(self.inner) }
    }

    pub fn prev_on_memtable_count(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_perf_context_prev_on_memtable_count(self.inner) }
    }

    pub fn seek_child_seek_time(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_perf_context_seek_child_seek_time(self.inner) }
    }

    pub fn seek_child_seek_count(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_perf_context_seek_child_seek_count(self.inner) }
    }

    pub fn seek_min_heap_time(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_perf_context_seek_min_heap_time(self.inner) }
    }

    pub fn seek_max_heap_time(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_perf_context_seek_max_heap_time(self.inner) }
    }

    pub fn seek_internal_seek_time(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_perf_context_seek_internal_seek_time(self.inner) }
    }

    pub fn find_next_user_entry_time(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_perf_context_find_next_user_entry_time(self.inner) }
    }

    pub fn write_wal_time(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_perf_context_write_wal_time(self.inner) }
    }

    pub fn write_memtable_time(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_perf_context_write_memtable_time(self.inner) }
    }

    pub fn write_delay_time(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_perf_context_write_delay_time(self.inner) }
    }

    pub fn write_pre_and_post_process_time(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_perf_context_write_pre_and_post_process_time(self.inner) }
    }

    pub fn db_mutex_lock_nanos(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_perf_context_db_mutex_lock_nanos(self.inner) }
    }

    pub fn write_thread_wait_nanos(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_perf_context_write_thread_wait_nanos(self.inner) }
    }

    pub fn write_scheduling_flushes_compactions_time(&self) -> u64 {
        unsafe {
            crocksdb_ffi::crocksdb_perf_context_write_scheduling_flushes_compactions_time(
                self.inner,
            )
        }
    }

    pub fn db_condition_wait_nanos(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_perf_context_db_condition_wait_nanos(self.inner) }
    }

    pub fn merge_operator_time_nanos(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_perf_context_merge_operator_time_nanos(self.inner) }
    }

    pub fn read_index_block_nanos(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_perf_context_read_index_block_nanos(self.inner) }
    }

    pub fn read_filter_block_nanos(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_perf_context_read_filter_block_nanos(self.inner) }
    }

    pub fn new_table_block_iter_nanos(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_perf_context_new_table_block_iter_nanos(self.inner) }
    }

    pub fn new_table_iterator_nanos(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_perf_context_new_table_iterator_nanos(self.inner) }
    }

    pub fn block_seek_nanos(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_perf_context_block_seek_nanos(self.inner) }
    }

    pub fn find_table_nanos(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_perf_context_find_table_nanos(self.inner) }
    }

    pub fn bloom_memtable_hit_count(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_perf_context_bloom_memtable_hit_count(self.inner) }
    }

    pub fn bloom_memtable_miss_count(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_perf_context_bloom_memtable_miss_count(self.inner) }
    }

    pub fn bloom_sst_hit_count(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_perf_context_bloom_sst_hit_count(self.inner) }
    }

    pub fn bloom_sst_miss_count(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_perf_context_bloom_sst_miss_count(self.inner) }
    }

    pub fn env_new_sequential_file_nanos(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_perf_context_env_new_sequential_file_nanos(self.inner) }
    }

    pub fn env_new_random_access_file_nanos(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_perf_context_env_new_random_access_file_nanos(self.inner) }
    }

    pub fn env_new_writable_file_nanos(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_perf_context_env_new_writable_file_nanos(self.inner) }
    }

    pub fn env_reuse_writable_file_nanos(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_perf_context_env_reuse_writable_file_nanos(self.inner) }
    }

    pub fn env_new_random_rw_file_nanos(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_perf_context_env_new_random_rw_file_nanos(self.inner) }
    }

    pub fn env_new_directory_nanos(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_perf_context_env_new_directory_nanos(self.inner) }
    }

    pub fn env_file_exists_nanos(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_perf_context_env_file_exists_nanos(self.inner) }
    }

    pub fn env_get_children_nanos(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_perf_context_env_get_children_nanos(self.inner) }
    }

    pub fn env_get_children_file_attributes_nanos(&self) -> u64 {
        unsafe {
            crocksdb_ffi::crocksdb_perf_context_env_get_children_file_attributes_nanos(self.inner)
        }
    }

    pub fn env_delete_file_nanos(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_perf_context_env_delete_file_nanos(self.inner) }
    }

    pub fn env_create_dir_nanos(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_perf_context_env_create_dir_nanos(self.inner) }
    }

    pub fn env_create_dir_if_missing_nanos(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_perf_context_env_create_dir_if_missing_nanos(self.inner) }
    }

    pub fn env_delete_dir_nanos(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_perf_context_env_delete_dir_nanos(self.inner) }
    }

    pub fn env_get_file_size_nanos(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_perf_context_env_get_file_size_nanos(self.inner) }
    }

    pub fn env_get_file_modification_time_nanos(&self) -> u64 {
        unsafe {
            crocksdb_ffi::crocksdb_perf_context_env_get_file_modification_time_nanos(self.inner)
        }
    }

    pub fn env_rename_file_nanos(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_perf_context_env_rename_file_nanos(self.inner) }
    }

    pub fn env_link_file_nanos(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_perf_context_env_link_file_nanos(self.inner) }
    }

    pub fn env_lock_file_nanos(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_perf_context_env_lock_file_nanos(self.inner) }
    }

    pub fn env_unlock_file_nanos(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_perf_context_env_unlock_file_nanos(self.inner) }
    }

    pub fn env_new_logger_nanos(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_perf_context_env_new_logger_nanos(self.inner) }
    }

    pub fn get_cpu_nanos(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_perf_context_get_cpu_nanos(self.inner) }
    }

    pub fn iter_next_cpu_nanos(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_perf_context_iter_next_cpu_nanos(self.inner) }
    }

    pub fn iter_prev_cpu_nanos(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_perf_context_iter_prev_cpu_nanos(self.inner) }
    }

    pub fn iter_seek_cpu_nanos(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_perf_context_iter_seek_cpu_nanos(self.inner) }
    }

    pub fn encrypt_data_nanos(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_perf_context_encrypt_data_nanos(self.inner) }
    }

    pub fn decrypt_data_nanos(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_perf_context_decrypt_data_nanos(self.inner) }
    }
}

pub struct IOStatsContext {
    inner: *mut DBIOStatsContext,
}

impl IOStatsContext {
    pub fn get() -> IOStatsContext {
        unsafe {
            IOStatsContext {
                inner: crocksdb_ffi::crocksdb_get_iostats_context(),
            }
        }
    }

    pub fn reset(&mut self) {
        unsafe { crocksdb_ffi::crocksdb_iostats_context_reset(self.inner) }
    }

    pub fn bytes_written(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_iostats_context_bytes_written(self.inner) }
    }

    pub fn bytes_read(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_iostats_context_bytes_read(self.inner) }
    }

    pub fn open_nanos(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_iostats_context_open_nanos(self.inner) }
    }

    pub fn allocate_nanos(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_iostats_context_allocate_nanos(self.inner) }
    }

    pub fn write_nanos(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_iostats_context_write_nanos(self.inner) }
    }

    pub fn read_nanos(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_iostats_context_read_nanos(self.inner) }
    }

    pub fn range_sync_nanos(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_iostats_context_range_sync_nanos(self.inner) }
    }

    pub fn fsync_nanos(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_iostats_context_fsync_nanos(self.inner) }
    }

    pub fn prepare_write_nanos(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_iostats_context_prepare_write_nanos(self.inner) }
    }

    pub fn logger_nanos(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_iostats_context_logger_nanos(self.inner) }
    }
}

#[cfg(test)]
mod test {
    use rocksdb::{SeekKey, Writable, DB};
    use rocksdb_options::{DBOptions, FlushOptions, WriteOptions};

    use super::*;
    use crate::tempdir_with_prefix;

    #[test]
    fn test_perf_context() {
        let temp_dir = tempdir_with_prefix("test_perf_context");
        let mut opts = DBOptions::new();
        opts.create_if_missing(true);
        let db = DB::open(opts, temp_dir.path().to_str().unwrap()).unwrap();

        let n = 10;
        for i in 0..n {
            let k = &[i as u8];
            db.put(k, k).unwrap();
            if i % 2 == 0 {
                db.delete(k).unwrap();
            }
        }

        set_perf_level(PerfLevel::EnableCount);
        let mut ctx = PerfContext::get();

        let mut iter = db.iter();
        assert!(iter.seek(SeekKey::Start).unwrap());
        while iter.next().unwrap() {}
        assert_eq!(ctx.internal_key_skipped_count(), n);
        assert_eq!(ctx.internal_delete_skipped_count(), n / 2);
        assert_eq!(ctx.seek_internal_seek_time(), 0);

        ctx.reset();
        assert_eq!(ctx.internal_key_skipped_count(), 0);
        assert_eq!(ctx.internal_delete_skipped_count(), 0);

        assert_eq!(get_perf_level(), PerfLevel::EnableCount);
        set_perf_level(PerfLevel::EnableTime);
        assert_eq!(get_perf_level(), PerfLevel::EnableTime);

        let mut iter = db.iter();
        assert!(iter.seek(SeekKey::End).unwrap());
        while iter.valid().unwrap() {
            iter.prev().unwrap();
        }
        assert_eq!(ctx.internal_key_skipped_count(), n + n / 2);
        assert_eq!(ctx.internal_delete_skipped_count(), n / 2);
        assert_ne!(ctx.seek_internal_seek_time(), 0);
    }

    #[test]
    fn test_iostats_context() {
        let temp_dir = tempdir_with_prefix("test_iostats_context");
        let mut opts = DBOptions::new();
        opts.create_if_missing(true);
        let db = DB::open(opts, temp_dir.path().to_str().unwrap()).unwrap();

        set_perf_level(PerfLevel::EnableTime);
        let mut ctx = IOStatsContext::get();

        ctx.reset();
        assert_eq!(ctx.bytes_written(), 0);
        assert_eq!(ctx.bytes_read(), 0);
        assert_eq!(ctx.open_nanos(), 0);
        assert_eq!(ctx.allocate_nanos(), 0);
        assert_eq!(ctx.write_nanos(), 0);
        assert_eq!(ctx.read_nanos(), 0);
        assert_eq!(ctx.range_sync_nanos(), 0);
        assert_eq!(ctx.fsync_nanos(), 0);
        assert_eq!(ctx.prepare_write_nanos(), 0);
        assert_eq!(ctx.logger_nanos(), 0);

        let mut wopts = WriteOptions::new();
        wopts.set_sync(true);
        let mut fopts = FlushOptions::default();
        fopts.set_wait(true);
        let n = 10;
        for i in 0..n {
            let k = &[i as u8];
            db.put_opt(k, k, &wopts).unwrap();
            db.flush(&fopts).unwrap();
            assert_eq!(db.get(k).unwrap().unwrap(), k);
        }

        assert!(ctx.bytes_written() > 0);
        assert!(ctx.bytes_read() > 0);
        assert!(ctx.open_nanos() > 0);
        assert!(ctx.write_nanos() > 0);
        assert!(ctx.read_nanos() > 0);
        assert!(ctx.fsync_nanos() > 0);
        assert!(ctx.prepare_write_nanos() > 0);
        assert!(ctx.logger_nanos() > 0);
    }

    #[test]
    fn test_perf_flags() {
        let temp_dir = tempdir_with_prefix("test_perf_flags");
        let mut opts = DBOptions::new();
        opts.create_if_missing(true);
        let db = DB::open(opts, temp_dir.path().to_str().unwrap()).unwrap();

        let n = 10;
        for i in 0..n {
            let k = &[i as u8];
            db.put(k, k).unwrap();
            if i % 2 == 0 {
                db.delete(k).unwrap();
            }
        }

        set_perf_level(PerfLevel::Disable);
        set_perf_flags(&(PerfFlag::InternalKeySkippedCount | PerfFlag::InternalDeleteSkippedCount));
        let mut ctx = PerfContext::get();

        let mut iter = db.iter();
        assert!(iter.seek(SeekKey::Start).unwrap());
        while iter.next().unwrap() {}
        assert_eq!(ctx.internal_key_skipped_count(), n);
        assert_eq!(ctx.internal_delete_skipped_count(), n / 2);
        assert_eq!(ctx.seek_internal_seek_time(), 0);

        ctx.reset();
        assert_eq!(ctx.internal_key_skipped_count(), 0);
        assert_eq!(ctx.internal_delete_skipped_count(), 0);

        set_perf_flags(&(PerfFlag::InternalKeySkippedCount | PerfFlag::SeekInternalSeekTime));
        let mut iter = db.iter();
        assert!(iter.seek(SeekKey::End).unwrap());
        while iter.valid().unwrap() {
            iter.prev().unwrap();
        }
        assert_eq!(ctx.internal_key_skipped_count(), n + n / 2);
        assert_eq!(ctx.internal_delete_skipped_count(), 0);
        assert_ne!(ctx.seek_internal_seek_time(), 0);
    }
}
