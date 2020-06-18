// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crocksdb_ffi::{
    self, DBSstPartitioner, DBSstPartitionerContext, DBSstPartitionerFactory, DBSstPartitionerState,
};
use libc::{c_char, c_uchar, c_void, size_t};
use std::{ffi::CString, mem, slice};

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct SstPartitionerState<'a> {
    pub next_key: &'a [u8],
    pub current_output_file_size: u64,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct SstPartitionerContext<'a> {
    pub is_full_compaction: bool,
    pub is_manual_compaction: bool,
    pub output_level: i32,
    pub smallest_key: &'a [u8],
    pub largest_key: &'a [u8],
}

pub trait SstPartitioner {
    fn should_partition(&self, state: &SstPartitionerState) -> bool;
    fn reset(&self, key: &[u8]);
}

extern "C" fn sst_partitioner_destructor(ctx: *mut c_void) {
    unsafe {
        // Recover from raw pointer and implicitly drop.
        Box::from_raw(ctx as *mut Box<dyn SstPartitioner>);
    }
}

extern "C" fn sst_partitioner_should_partition(
    ctx: *mut c_void,
    state: *mut DBSstPartitionerState,
) -> c_uchar {
    let partitioner = unsafe { &*(ctx as *mut Box<dyn SstPartitioner>) };
    let state = unsafe {
        let mut key_len: usize = 0;
        let next_key: *const u8 = mem::transmute(
            crocksdb_ffi::crocksdb_sst_partitioner_state_next_key(state, &mut key_len),
        );
        SstPartitionerState {
            next_key: slice::from_raw_parts(next_key, key_len),
            current_output_file_size:
                crocksdb_ffi::crocksdb_sst_partitioner_state_current_output_file_size(state),
        }
    };
    partitioner.should_partition(&state) as _
}

extern "C" fn sst_partitioner_reset(ctx: *mut c_void, key: *const c_char, key_len: size_t) {
    let partitioner = unsafe { &*(ctx as *mut Box<dyn SstPartitioner>) };
    let key_buf = unsafe {
        let key_ptr: *const u8 = mem::transmute(key);
        slice::from_raw_parts(key_ptr, key_len)
    };
    partitioner.reset(key_buf);
}

pub trait SstPartitionerFactory: Sync + Send {
    fn name(&self) -> &CString;
    fn create_partitioner(&self, context: &SstPartitionerContext) -> Box<dyn SstPartitioner>;
}

extern "C" fn sst_partitioner_factory_destroy(ctx: *mut c_void) {
    unsafe {
        // Recover from raw pointer and implicitly drop.
        Box::from_raw(ctx as *mut Box<dyn SstPartitionerFactory>);
    }
}

extern "C" fn sst_partitioner_factory_name(ctx: *mut c_void) -> *const c_char {
    let factory = unsafe { &*(ctx as *mut Box<dyn SstPartitionerFactory>) };
    factory.name().as_ptr()
}

extern "C" fn sst_partitioner_factory_create_partitioner(
    ctx: *mut c_void,
    context: *mut DBSstPartitionerContext,
) -> *mut DBSstPartitioner {
    let factory = unsafe { &*(ctx as *mut Box<dyn SstPartitionerFactory>) };
    let context = unsafe {
        let mut smallest_key_len: usize = 0;
        let smallest_key: *const u8 =
            mem::transmute(crocksdb_ffi::crocksdb_sst_partitioner_context_smallest_key(
                context,
                &mut smallest_key_len,
            ));
        let mut largest_key_len: usize = 0;
        let largest_key: *const u8 =
            mem::transmute(crocksdb_ffi::crocksdb_sst_partitioner_context_largest_key(
                context,
                &mut largest_key_len,
            ));
        SstPartitionerContext {
            is_full_compaction: crocksdb_ffi::crocksdb_sst_partitioner_context_is_full_compaction(
                context,
            ) != 0,
            is_manual_compaction:
                crocksdb_ffi::crocksdb_sst_partitioner_context_is_manual_compaction(context) != 0,
            output_level: crocksdb_ffi::crocksdb_sst_partitioner_context_output_level(context),
            smallest_key: slice::from_raw_parts(smallest_key, smallest_key_len),
            largest_key: slice::from_raw_parts(largest_key, largest_key_len),
        }
    };
    let partitioner = factory.create_partitioner(&context);
    let ctx = Box::into_raw(Box::new(partitioner)) as *mut c_void;
    unsafe {
        crocksdb_ffi::crocksdb_sst_partitioner_create(
            ctx,
            sst_partitioner_destructor,
            sst_partitioner_should_partition,
            sst_partitioner_reset,
        )
    }
}

pub fn new_sst_partitioner_factory<F: SstPartitionerFactory>(
    factory: F,
) -> *mut DBSstPartitionerFactory {
    let factory: Box<dyn SstPartitionerFactory> = Box::new(factory);
    unsafe {
        crocksdb_ffi::crocksdb_sst_partitioner_factory_create(
            Box::into_raw(Box::new(factory)) as *mut c_void,
            sst_partitioner_factory_destroy,
            sst_partitioner_factory_name,
            sst_partitioner_factory_create_partitioner,
        )
    }
}
