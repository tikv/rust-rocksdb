// Copyright 2017 PingCAP, Inc.
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

use crocksdb_ffi::{
    self, crocksdb_backgrounderrorreason_t, crocksdb_compaction_reason_t,
    crocksdb_compactionjobinfo_t, crocksdb_eventlistener_t, crocksdb_externalfileingestioninfo_t,
    crocksdb_flushjobinfo_t, crocksdb_status_ptr_t, crocksdb_t, crocksdb_writestallcondition_t,
    crocksdb_writestallinfo_t,
};
use libc::c_void;
use std::path::Path;
use std::{slice, str};
use {TableProperties, TablePropertiesCollectionView};

macro_rules! fetch_str {
    ($func:ident($($arg:expr),*)) => ({
        let mut len = 0;
        let ptr = crocksdb_ffi::$func($($arg),*, &mut len);
        let s = slice::from_raw_parts(ptr as *const u8, len);
        str::from_utf8(s).unwrap()
    })
}

pub struct FlushJobInfo(crocksdb_flushjobinfo_t);

impl FlushJobInfo {
    pub fn cf_name(&self) -> &str {
        unsafe { fetch_str!(crocksdb_flushjobinfo_cf_name(&self.0)) }
    }

    pub fn file_path(&self) -> &Path {
        let p = unsafe { fetch_str!(crocksdb_flushjobinfo_file_path(&self.0)) };
        Path::new(p)
    }

    pub fn table_properties(&self) -> &TableProperties {
        unsafe {
            let prop = crocksdb_ffi::crocksdb_flushjobinfo_table_properties(&self.0);
            TableProperties::from_ptr(prop)
        }
    }

    pub fn triggered_writes_slowdown(&self) -> bool {
        unsafe { crocksdb_ffi::crocksdb_flushjobinfo_triggered_writes_slowdown(&self.0) != 0 }
    }

    pub fn triggered_writes_stop(&self) -> bool {
        unsafe { crocksdb_ffi::crocksdb_flushjobinfo_triggered_writes_stop(&self.0) != 0 }
    }
}

pub struct CompactionJobInfo(crocksdb_compactionjobinfo_t);

impl CompactionJobInfo {
    pub fn status(&self) -> Result<(), String> {
        unsafe { ffi_try!(crocksdb_compactionjobinfo_status(&self.0)) }
        Ok(())
    }

    pub fn cf_name(&self) -> &str {
        unsafe { fetch_str!(crocksdb_compactionjobinfo_cf_name(&self.0)) }
    }

    pub fn input_file_count(&self) -> usize {
        unsafe { crocksdb_ffi::crocksdb_compactionjobinfo_input_files_count(&self.0) }
    }

    pub fn input_file_at(&self, pos: usize) -> &Path {
        let p = unsafe { fetch_str!(crocksdb_compactionjobinfo_input_file_at(&self.0, pos)) };
        Path::new(p)
    }

    pub fn output_file_count(&self) -> usize {
        unsafe { crocksdb_ffi::crocksdb_compactionjobinfo_output_files_count(&self.0) }
    }

    pub fn output_file_at(&self, pos: usize) -> &Path {
        let p = unsafe { fetch_str!(crocksdb_compactionjobinfo_output_file_at(&self.0, pos)) };
        Path::new(p)
    }

    pub fn table_properties(&self) -> &TablePropertiesCollectionView {
        unsafe {
            let prop = crocksdb_ffi::crocksdb_compactionjobinfo_table_properties(&self.0);
            TablePropertiesCollectionView::from_ptr(prop)
        }
    }

    pub fn elapsed_micros(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_compactionjobinfo_elapsed_micros(&self.0) }
    }

    pub fn num_corrupt_keys(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_compactionjobinfo_num_corrupt_keys(&self.0) }
    }

    pub fn output_level(&self) -> i32 {
        unsafe { crocksdb_ffi::crocksdb_compactionjobinfo_output_level(&self.0) }
    }

    pub fn input_records(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_compactionjobinfo_input_records(&self.0) }
    }

    pub fn output_records(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_compactionjobinfo_output_records(&self.0) }
    }

    pub fn total_input_bytes(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_compactionjobinfo_total_input_bytes(&self.0) }
    }

    pub fn total_output_bytes(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_compactionjobinfo_total_output_bytes(&self.0) }
    }

    pub fn compaction_reason(&self) -> crocksdb_compaction_reason_t {
        unsafe { *crocksdb_ffi::crocksdb_compactionjobinfo_compaction_reason(&self.0) }
    }
}

pub struct IngestionInfo(crocksdb_externalfileingestioninfo_t);

impl IngestionInfo {
    pub fn cf_name(&self) -> &str {
        unsafe { fetch_str!(crocksdb_externalfileingestioninfo_cf_name(&self.0)) }
    }

    pub fn internal_file_path(&self) -> &Path {
        let p = unsafe {
            fetch_str!(crocksdb_externalfileingestioninfo_internal_file_path(
                &self.0
            ))
        };
        Path::new(p)
    }

    pub fn table_properties(&self) -> &TableProperties {
        unsafe {
            let prop = crocksdb_ffi::crocksdb_externalfileingestioninfo_table_properties(&self.0);
            TableProperties::from_ptr(prop)
        }
    }
}

pub struct WriteStallInfo(crocksdb_writestallinfo_t);

impl WriteStallInfo {
    pub fn cf_name(&self) -> &str {
        unsafe { fetch_str!(crocksdb_writestallinfo_cf_name(&self.0)) }
    }
    pub fn cur(&self) -> crocksdb_writestallcondition_t {
        unsafe { *crocksdb_ffi::crocksdb_writestallinfo_cur(&self.0) }
    }
    pub fn prev(&self) -> crocksdb_writestallcondition_t {
        unsafe { *crocksdb_ffi::crocksdb_writestallinfo_prev(&self.0) }
    }
}

/// EventListener trait contains a set of call-back functions that will
/// be called when specific RocksDB event happens such as flush.  It can
/// be used as a building block for developing custom features such as
/// stats-collector or external compaction algorithm.
///
/// Note that call-back functions should not run for an extended period of
/// time before the function returns, otherwise RocksDB may be blocked.
/// For more information, please see
/// [doc of rocksdb](https://github.com/facebook/rocksdb/blob/master/include/rocksdb/listener.h).
pub trait EventListener: Send + Sync {
    fn on_flush_completed(&self, _: &FlushJobInfo) {}
    fn on_compaction_completed(&self, _: &CompactionJobInfo) {}
    fn on_external_file_ingested(&self, _: &IngestionInfo) {}
    fn on_background_error(&self, _: crocksdb_backgrounderrorreason_t, _: Result<(), String>) {}
    fn on_stall_conditions_changed(&self, _: &WriteStallInfo) {}
}

extern "C" fn destructor(ctx: *mut c_void) {
    unsafe {
        Box::from_raw(ctx as *mut Box<dyn EventListener>);
    }
}

// Maybe we should reuse db instance?
// TODO: refactor DB implement so that we can convert DBInstance to DB.
extern "C" fn on_flush_completed(
    ctx: *mut c_void,
    _: *mut crocksdb_t,
    info: *const crocksdb_flushjobinfo_t,
) {
    let (ctx, info) = unsafe {
        (
            &*(ctx as *mut Box<dyn EventListener>),
            &*(info as *const FlushJobInfo),
        )
    };
    ctx.on_flush_completed(info);
}

extern "C" fn on_compaction_completed(
    ctx: *mut c_void,
    _: *mut crocksdb_t,
    info: *const crocksdb_compactionjobinfo_t,
) {
    let (ctx, info) = unsafe {
        (
            &*(ctx as *mut Box<dyn EventListener>),
            &*(info as *const CompactionJobInfo),
        )
    };
    ctx.on_compaction_completed(info);
}

extern "C" fn on_external_file_ingested(
    ctx: *mut c_void,
    _: *mut crocksdb_t,
    info: *const crocksdb_externalfileingestioninfo_t,
) {
    let (ctx, info) = unsafe {
        (
            &*(ctx as *mut Box<dyn EventListener>),
            &*(info as *const IngestionInfo),
        )
    };
    ctx.on_external_file_ingested(info);
}

extern "C" fn on_background_error(
    ctx: *mut c_void,
    reason: crocksdb_backgrounderrorreason_t,
    status: *mut crocksdb_status_ptr_t,
) {
    let (ctx, result) = unsafe {
        (
            &*(ctx as *mut Box<dyn EventListener>),
            || -> Result<(), String> {
                ffi_try!(crocksdb_status_ptr_get_error(status));
                Ok(())
            }(),
        )
    };
    ctx.on_background_error(reason, result);
}

extern "C" fn on_stall_conditions_changed(
    ctx: *mut c_void,
    info: *const crocksdb_writestallinfo_t,
) {
    let (ctx, info) = unsafe {
        (
            &*(ctx as *mut Box<dyn EventListener>),
            &*(info as *const WriteStallInfo),
        )
    };
    ctx.on_stall_conditions_changed(info);
}

pub fn new_event_listener<L: EventListener>(l: L) -> *mut crocksdb_eventlistener_t {
    let p: Box<dyn EventListener> = Box::new(l);
    unsafe {
        crocksdb_ffi::crocksdb_eventlistener_create(
            Box::into_raw(Box::new(p)) as *mut c_void,
            Some(destructor),
            Some(on_flush_completed),
            Some(on_compaction_completed),
            Some(on_external_file_ingested),
            Some(on_background_error),
            Some(on_stall_conditions_changed),
        )
    }
}
