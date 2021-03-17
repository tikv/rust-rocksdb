// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crocksdb_ffi::{
    self, DBLevelRegionAccessor, DBLevelRegionAccessorRequest, DBLevelRegionAccessorResult,
};
use libc::{c_char, c_void, size_t};
use std::{ffi::CString, slice};

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct LevelRegionAccessorRequest<'a> {
    pub smallest_user_key: &'a [u8],
    pub largest_user_key: &'a [u8],
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LevelRegionBoundaries {
    pub start_key: Vec<u8>,
    pub end_key: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LevelRegionAccessorResult {
    pub regions:  Vec<LevelRegionBoundaries>,
}

pub struct AccessorResult {
    pub(crate) inner: *mut DBLevelRegionAccessorResult,
}

unsafe impl Send for AccessorResult {}
impl Default for AccessorResult {
    fn default() -> AccessorResult {
        AccessorResult {
            inner: unsafe { crocksdb_ffi::crocksdb_level_region_accessor_result_create() },
        }
    }
}

impl AccessorResult {
    pub fn new() -> AccessorResult { AccessorResult::default() }

    pub fn append(&mut self, start_key: &[u8], end_key: &[u8]) {
        unsafe {
            crocksdb_ffi::crocksdb_level_region_accessor_result_append(
                self.inner,
                start_key.as_ptr(),
                start_key.len() as size_t,
                end_key.as_ptr(),
                end_key.len() as size_t,
            );
        }
    }
}


pub trait LevelRegionAccessor {
    fn name(&self) -> &CString;
    fn level_regions(&self, req: &LevelRegionAccessorRequest) -> LevelRegionAccessorResult;
}

extern "C" fn level_region_destructor<P: LevelRegionAccessor>(ctx: *mut c_void) {
    unsafe {
        // Recover from raw pointer and implicitly drop.
        Box::from_raw(ctx as *mut P);
    }
}

extern "C" fn level_region_accessor_name<A: LevelRegionAccessor>(
    ctx: *mut c_void,
) -> *const c_char {
    let accessor = unsafe { &*(ctx as *mut A) };
    accessor.name().as_ptr()
}

extern "C" fn level_region_accessor_level_regions<A: LevelRegionAccessor>(
    ctx: *mut c_void,
    request: *mut DBLevelRegionAccessorRequest,
) -> *const DBLevelRegionAccessorResult {
    let accessor = unsafe { &*(ctx as *mut A) };
    let req = unsafe {
        let mut smallest_key_len: usize = 0;
        let smallest_key = crocksdb_ffi::crocksdb_level_region_accessor_request_smallest_user_key(
            request,
            &mut smallest_key_len,
        ) as *const u8;
        let mut largest_key_len: usize = 0;
        let largest_key = crocksdb_ffi::crocksdb_level_region_accessor_request_largest_user_key(
            request,
            &mut largest_key_len,
        ) as *const u8;
        LevelRegionAccessorRequest {
            smallest_user_key: slice::from_raw_parts(smallest_key, smallest_key_len),
            largest_user_key: slice::from_raw_parts(largest_key, largest_key_len),
        }
    };
    let result = accessor.level_regions(&req);
    let mut r = AccessorResult::new();
    for region in result.regions {
        r.append(region.start_key.as_slice(), region.end_key.as_slice());
    }
    r.inner
}

pub fn new_level_region_accessor<A: 'static + LevelRegionAccessor>(
    accessor: A,
) -> *mut DBLevelRegionAccessor {
    unsafe {
        crocksdb_ffi::crocksdb_level_region_accessor_create(
            Box::into_raw(Box::new(accessor)) as *mut c_void,
            level_region_destructor::<A>,
            level_region_accessor_name::<A>,
            level_region_accessor_level_regions::<A>,
        )
    }
}
