use std::ffi::{CStr, CString};
use std::ops::Deref;

use crocksdb_ffi::{self, ctitandb_blob_index_t, ctitandb_options_t};
use librocksdb_sys::ctitandb_encode_blob_index;
use rocksdb::Cache;
use rocksdb_options::LRUCacheOptions;
use std::ops::DerefMut;
use std::os::raw::c_double;
use std::os::raw::c_int;
use std::ptr;
use std::slice;
use {DBCompressionType, DBTitanDBBlobRunMode};

pub type DBTitanDBOptions = ctitandb_options_t;

pub struct TitanDBOptions {
    pub inner: *mut DBTitanDBOptions,
}

impl TitanDBOptions {
    pub fn new() -> Self {
        unsafe {
            Self {
                inner: crocksdb_ffi::ctitandb_options_create(),
            }
        }
    }

    pub fn dirname(&self) -> &str {
        unsafe {
            let name = crocksdb_ffi::ctitandb_options_dirname(self.inner);
            CStr::from_ptr(name).to_str().unwrap()
        }
    }

    pub fn set_dirname(&mut self, name: &str) {
        let s = CString::new(name).unwrap();
        // Safety: set_dirname copies the C string into std::string. We
        // still own s and must drop it.
        unsafe {
            crocksdb_ffi::ctitandb_options_set_dirname(self.inner, s.as_ptr());
        }
    }

    pub fn min_blob_size(&self) -> u64 {
        unsafe { crocksdb_ffi::ctitandb_options_min_blob_size(self.inner) }
    }

    pub fn set_min_blob_size(&mut self, size: u64) {
        unsafe {
            crocksdb_ffi::ctitandb_options_set_min_blob_size(self.inner, size);
        }
    }

    pub fn blob_file_compression(&self) -> DBCompressionType {
        unsafe {
            num::FromPrimitive::from_i32(crocksdb_ffi::ctitandb_options_blob_file_compression(
                self.inner,
            ))
            .unwrap()
        }
    }

    pub fn set_blob_file_compression(&mut self, t: DBCompressionType) {
        unsafe {
            crocksdb_ffi::ctitandb_options_set_blob_file_compression(self.inner, t as i32);
        }
    }

    pub fn set_disable_background_gc(&mut self, disable: bool) {
        unsafe {
            crocksdb_ffi::ctitandb_options_set_disable_background_gc(self.inner, disable as u8);
        }
    }

    pub fn set_level_merge(&mut self, enable: bool) {
        unsafe {
            crocksdb_ffi::ctitandb_options_set_level_merge(self.inner, enable as u8);
        }
    }

    pub fn set_range_merge(&mut self, enable: bool) {
        unsafe {
            crocksdb_ffi::ctitandb_options_set_range_merge(self.inner, enable as u8);
        }
    }

    pub fn set_max_sorted_runs(&mut self, size: i32) {
        unsafe {
            crocksdb_ffi::ctitandb_options_set_max_sorted_runs(self.inner, size);
        }
    }

    pub fn set_max_background_gc(&mut self, size: i32) {
        unsafe {
            crocksdb_ffi::ctitandb_options_set_max_background_gc(self.inner, size);
        }
    }

    pub fn set_purge_obsolete_files_period(&mut self, period: usize) {
        unsafe {
            crocksdb_ffi::ctitandb_options_set_purge_obsolete_files_period_sec(
                self.inner,
                period as u32,
            );
        }
    }

    pub fn set_min_gc_batch_size(&mut self, size: u64) {
        unsafe {
            crocksdb_ffi::ctitandb_options_set_min_gc_batch_size(self.inner, size);
        }
    }

    pub fn set_max_gc_batch_size(&mut self, size: u64) {
        unsafe {
            crocksdb_ffi::ctitandb_options_set_max_gc_batch_size(self.inner, size);
        }
    }

    pub fn set_blob_cache(
        &mut self,
        size: usize,
        shard_bits: c_int,
        capacity_limit: bool,
        pri_ratio: c_double,
    ) {
        let mut cache_opt = LRUCacheOptions::new();
        cache_opt.set_capacity(size);
        cache_opt.set_num_shard_bits(shard_bits);
        cache_opt.set_strict_capacity_limit(capacity_limit);
        cache_opt.set_high_pri_pool_ratio(pri_ratio);
        let cache = Cache::new_lru_cache(cache_opt);
        unsafe {
            crocksdb_ffi::ctitandb_options_set_blob_cache(self.inner, cache.inner);
        }
    }

    pub fn set_discardable_ratio(&mut self, ratio: f64) {
        unsafe {
            crocksdb_ffi::ctitandb_options_set_discardable_ratio(self.inner, ratio);
        }
    }

    pub fn set_sample_ratio(&mut self, ratio: f64) {
        unsafe {
            crocksdb_ffi::ctitandb_options_set_sample_ratio(self.inner, ratio);
        }
    }

    pub fn set_merge_small_file_threshold(&mut self, size: u64) {
        unsafe {
            crocksdb_ffi::ctitandb_options_set_merge_small_file_threshold(self.inner, size);
        }
    }

    pub fn set_blob_run_mode(&mut self, t: DBTitanDBBlobRunMode) {
        unsafe {
            crocksdb_ffi::ctitandb_options_set_blob_run_mode(self.inner, t as i32);
        }
    }
}

impl Drop for TitanDBOptions {
    fn drop(&mut self) {
        unsafe {
            crocksdb_ffi::ctitandb_options_destroy(self.inner);
        }
    }
}

#[derive(Debug)]
pub struct TitanBlobIndex {
    inner: ctitandb_blob_index_t,
}

impl Default for TitanBlobIndex {
    fn default() -> TitanBlobIndex {
        let blob_index = ctitandb_blob_index_t {
            file_number: 0,
            blob_offset: 0,
            blob_size: 0,
        };
        TitanBlobIndex { inner: blob_index }
    }
}

impl TitanBlobIndex {
    pub fn decode(value: &[u8]) -> Result<Self, String> {
        let mut index = Self::default();
        unsafe {
            ffi_try!(ctitandb_decode_blob_index(
                value.as_ptr() as *const i8,
                value.len() as usize,
                &mut index.inner as *mut ctitandb_blob_index_t
            ));
        }
        Ok(index)
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut value = ptr::null_mut();
        let mut value_size: usize = 0;
        unsafe {
            ctitandb_encode_blob_index(&self.inner, &mut value, &mut value_size);
            let slice = slice::from_raw_parts(value as *mut u8, value_size);
            let vec = slice.to_vec();
            libc::free(value as *mut libc::c_void);
            vec
        }
    }
}

impl Deref for TitanBlobIndex {
    type Target = ctitandb_blob_index_t;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for TitanBlobIndex {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}
