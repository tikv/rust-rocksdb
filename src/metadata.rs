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

use crocksdb_ffi::{self, DBColumnFamilyMetaData, DBLevelMetaData, DBLivefiles, DBSstFileMetaData};
use std::ffi::CStr;
use std::slice;

use libc::size_t;

pub struct ColumnFamilyMetaData {
    inner: *mut DBColumnFamilyMetaData,
}

impl ColumnFamilyMetaData {
    pub fn from_ptr(inner: *mut DBColumnFamilyMetaData) -> ColumnFamilyMetaData {
        ColumnFamilyMetaData { inner }
    }

    pub fn get_levels(&self) -> Vec<LevelMetaData> {
        let mut levels = Vec::new();
        unsafe {
            let n = crocksdb_ffi::crocksdb_column_family_meta_data_level_count(self.inner);
            for i in 0..n {
                levels.push(self.get_level(i));
            }
        }
        levels
    }

    /// The caller must ensure that the level is less than the bottommost one.
    pub fn get_level(&self, level: usize) -> LevelMetaData {
        unsafe {
            let n = crocksdb_ffi::crocksdb_column_family_meta_data_level_count(self.inner);
            assert!(level < n);
            let data = crocksdb_ffi::crocksdb_column_family_meta_data_level_data(self.inner, level);
            LevelMetaData::from_ptr(data, self)
        }
    }
}

impl Drop for ColumnFamilyMetaData {
    fn drop(&mut self) {
        unsafe {
            crocksdb_ffi::crocksdb_column_family_meta_data_destroy(self.inner);
        }
    }
}

pub struct LevelMetaData<'a> {
    inner: *const DBLevelMetaData,
    _mark: &'a ColumnFamilyMetaData,
}

impl<'a> LevelMetaData<'a> {
    pub fn from_ptr(
        inner: *const DBLevelMetaData,
        _mark: &'a ColumnFamilyMetaData,
    ) -> LevelMetaData<'a> {
        LevelMetaData { inner, _mark }
    }

    pub fn get_files(&self) -> Vec<SstFileMetaData<'a>> {
        let mut files = Vec::new();
        unsafe {
            let n = crocksdb_ffi::crocksdb_level_meta_data_file_count(self.inner);
            for i in 0..n {
                let data = crocksdb_ffi::crocksdb_level_meta_data_file_data(self.inner, i);
                files.push(SstFileMetaData::from_ptr(data, self._mark));
            }
        }
        files
    }
}

pub struct SstFileMetaData<'a> {
    inner: *const DBSstFileMetaData,
    _mark: &'a ColumnFamilyMetaData,
}

impl<'a> SstFileMetaData<'a> {
    pub fn from_ptr(
        inner: *const DBSstFileMetaData,
        _mark: &'a ColumnFamilyMetaData,
    ) -> SstFileMetaData<'a> {
        SstFileMetaData { inner, _mark }
    }

    pub fn get_size(&self) -> usize {
        unsafe { crocksdb_ffi::crocksdb_sst_file_meta_data_size(self.inner) }
    }

    pub fn get_name(&self) -> String {
        unsafe {
            let ptr = crocksdb_ffi::crocksdb_sst_file_meta_data_name(self.inner);
            CStr::from_ptr(ptr).to_string_lossy().into_owned()
        }
    }

    pub fn get_smallestkey(&self) -> &[u8] {
        let mut len: size_t = 0;
        unsafe {
            let ptr = crocksdb_ffi::crocksdb_sst_file_meta_data_smallestkey(self.inner, &mut len);
            slice::from_raw_parts(ptr as *const u8, len)
        }
    }

    pub fn get_largestkey(&self) -> &[u8] {
        let mut len: size_t = 0;
        unsafe {
            let ptr = crocksdb_ffi::crocksdb_sst_file_meta_data_largestkey(self.inner, &mut len);
            slice::from_raw_parts(ptr as *const u8, len)
        }
    }

    pub fn get_num_entries(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_sst_file_meta_data_num_entries(self.inner) }
    }

    pub fn get_num_deletions(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_sst_file_meta_data_num_deletions(self.inner) }
    }
}

pub struct LiveFiles {
    inner: *mut DBLivefiles,
}

impl LiveFiles {
    pub fn from_ptr(inner: *mut DBLivefiles) -> LiveFiles {
        LiveFiles { inner }
    }

    pub fn get_files_count(&self) -> usize {
        unsafe { crocksdb_ffi::crocksdb_livefiles_count(self.inner) }
    }

    #[allow(dead_code)]
    pub fn get_size(&self, index: i32) -> usize {
        unsafe { crocksdb_ffi::crocksdb_livefiles_size(self.inner, index) }
    }

    pub fn get_name(&self, index: i32) -> String {
        unsafe {
            let ptr = crocksdb_ffi::crocksdb_livefiles_name(self.inner, index);
            CStr::from_ptr(ptr).to_string_lossy().into_owned()
        }
    }

    pub fn get_smallestkey(&self, index: i32) -> &[u8] {
        let mut len: size_t = 0;
        unsafe {
            let ptr = crocksdb_ffi::crocksdb_livefiles_smallestkey(self.inner, index, &mut len);
            slice::from_raw_parts(ptr as *const u8, len)
        }
    }

    pub fn get_largestkey(&self, index: i32) -> &[u8] {
        let mut len: size_t = 0;
        unsafe {
            let ptr = crocksdb_ffi::crocksdb_livefiles_largestkey(self.inner, index, &mut len);
            slice::from_raw_parts(ptr as *const u8, len)
        }
    }
}

impl Drop for LiveFiles {
    fn drop(&mut self) {
        unsafe {
            crocksdb_ffi::crocksdb_livefiles_destroy(self.inner);
        }
    }
}

/// Represents metadata for an SST file with its key range information.
#[derive(Debug, Clone)]
pub struct SstFileInfo {
    /// The name/path of the SST file
    pub name: String,
    /// The size of the file in bytes
    pub size: usize,
    /// The level where this file resides
    pub level: usize,
    /// The smallest key in this file
    pub smallest_key: Vec<u8>,
    /// The largest key in this file
    pub largest_key: Vec<u8>,
    /// The number of entries in this file
    pub num_entries: u64,
    /// The number of deletions in this file
    pub num_deletions: u64,
}

impl SstFileInfo {
    /// Check if this SST file overlaps with the given key range.
    /// 
    /// # Arguments
    /// * `start_key` - The start of the key range (inclusive), None means no lower bound
    /// * `end_key` - The end of the key range (exclusive), None means no upper bound
    /// 
    /// # Returns
    /// `true` if the file overlaps with the range, `false` otherwise
    pub fn overlaps_with_range(&self, start_key: Option<&[u8]>, end_key: Option<&[u8]>) -> bool {
        // Check if file's largest key is before the start of the range
        if let Some(start) = start_key {
            if self.largest_key < start {
                return false;
            }
        }
        
        // Check if file's smallest key is at or after the end of the range
        if let Some(end) = end_key {
            if self.smallest_key >= end {
                return false;
            }
        }
        
        true
    }
    
    /// Check if this SST file is completely contained within the given key range.
    /// 
    /// # Arguments
    /// * `start_key` - The start of the key range (inclusive), None means no lower bound
    /// * `end_key` - The end of the key range (exclusive), None means no upper bound
    /// 
    /// # Returns
    /// `true` if the file is completely contained within the range, `false` otherwise
    pub fn is_contained_in_range(&self, start_key: Option<&[u8]>, end_key: Option<&[u8]>) -> bool {
        // Check if file's smallest key is at or after the start of the range
        if let Some(start) = start_key {
            if self.smallest_key < start {
                return false;
            }
        }
        
        // Check if file's largest key is before the end of the range
        if let Some(end) = end_key {
            if self.largest_key >= end {
                return false;
            }
        }
        
        true
    }
}
