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

#[cfg(test)]
mod test {
    use std::{
        ffi::{CStr, CString},
        sync::{Arc, Mutex},
    };

    use super::*;

    struct TestState {
        pub call_level_regions: usize,
        pub drop_accessor: usize,
        pub level_regions_result: LevelRegionAccessorResult,

        // LevelRegionAccessorRequest fields
        pub smallest_user_key: Option<Vec<u8>>,
        pub largest_user_key: Option<Vec<u8>>,
    }

    impl Default for TestState {
        fn default() -> Self {
            TestState {
                call_level_regions: 0,
                drop_accessor: 0,
                level_regions_result: LevelRegionAccessorResult{regions: Vec::new()},
                smallest_user_key: None,
                largest_user_key: None,
            }
        }
    }

    struct TestLevelRegionAccessor {
        state: Arc<Mutex<TestState>>,
    }

    lazy_static! {
        static ref ACCESSOR_NAME: CString =
            CString::new(b"TestLevelRegionAccessor".to_vec()).unwrap();
    }

    impl LevelRegionAccessor for TestLevelRegionAccessor {
        fn name(&self) -> &CString { &ACCESSOR_NAME }

        fn level_regions(&self, req: &LevelRegionAccessorRequest) -> LevelRegionAccessorResult {
            let mut s = self.state.lock().unwrap();
            s.call_level_regions += 1;
            s.smallest_user_key = Some(req.smallest_user_key.to_vec());
            s.largest_user_key = Some(req.largest_user_key.to_vec());

            s.level_regions_result.clone()
        }
    }

    impl Drop for TestLevelRegionAccessor {
        fn drop(&mut self) {
            self.state.lock().unwrap().drop_accessor += 1;
        }
    }

    #[test]
    fn accessor_name() {
        let s = Arc::new(Mutex::new(TestState::default()));
        let accessor = new_level_region_accessor(TestLevelRegionAccessor {state: s});
        let accessor_name =
            unsafe { CStr::from_ptr(crocksdb_ffi::crocksdb_level_region_accessor_name(accessor)) };
        assert_eq!(*ACCESSOR_NAME.as_c_str(), *accessor_name);
        unsafe {
            crocksdb_ffi::crocksdb_level_region_accessor_destroy(accessor);
        }
    }

    #[test]
    fn accessor_level_regions() {
        const ACCESSOR_RESULT: LevelRegionAccessorResult = LevelRegionAccessorResult { regions: Vec::new() };
        const BOUNDARIES_1: &[u8] = b"test_key_region_abc";
        const BOUNDARIES_2: &[u8] = b"test_key_region_def";
        const BOUNDARIES_3: &[u8] = b"test_key_region_ghi";
        const BOUNDARIES_4: &[u8] = b"test_key_region_jkl";
        let region_1: LevelRegionBoundaries = LevelRegionBoundaries {
            start_key: BOUNDARIES_1.to_vec(),
            end_key: BOUNDARIES_2.to_vec(),
        };
        let region_2: LevelRegionBoundaries = LevelRegionBoundaries {
            start_key: BOUNDARIES_2.to_vec(),
            end_key: BOUNDARIES_3.to_vec(),
        };
        let region_3: LevelRegionBoundaries = LevelRegionBoundaries {
            start_key: BOUNDARIES_3.to_vec(),
            end_key: BOUNDARIES_4.to_vec(),
        };
        ACCESSOR_RESULT.regions.push(region_1);
        ACCESSOR_RESULT.regions.push(region_2);
        ACCESSOR_RESULT.regions.push(region_3);

        const SMALLEST_USER_KEY: &[u8] = b"test_key_region_bcd";
        const LARGEST_USER_KEY: &[u8] = b"test_key_region_hij";

        const EQUAL: bool = true;

        let s = Arc::new(Mutex::new(TestState::default()));
        s.lock().unwrap().level_regions_result = ACCESSOR_RESULT;
        let accessor = new_level_region_accessor(TestLevelRegionAccessor {state: s.clone()});
        let req = unsafe { crocksdb_ffi::crocksdb_level_region_accessor_request_create() };
        unsafe {
            crocksdb_ffi::crocksdb_level_region_accessor_request_set_smallest_user_key(
              req,
              SMALLEST_USER_KEY.as_ptr() as *const c_char,
              SMALLEST_USER_KEY.len(),
            );
            crocksdb_ffi::crocksdb_level_region_accessor_request_set_largest_user_key(
                req,
                LARGEST_USER_KEY.as_ptr() as *const c_char,
                LARGEST_USER_KEY.len(),
            );
        }
        let accessor_result =
            unsafe { crocksdb_ffi::crocksdb_level_region_accessor_level_regions(accessor, req) };
        let mut r = AccessorResult::new();
        for region in ACCESSOR_RESULT.regions {
            r.append(region.start_key.as_slice(), region.end_key.as_slice());
        }
        let res = unsafe { crocksdb_ffi::crocksdb_level_region_accessor_result_equal(r.inner, accessor_result as *mut DBLevelRegionAccessorResult) };
        assert_eq!(EQUAL, res);
        let sl = s.lock().unwrap();
        assert_eq!(1, sl.call_level_regions);
        assert_eq!(SMALLEST_USER_KEY, sl.smallest_user_key.as_ref().unwrap().as_slice());
        assert_eq!(LARGEST_USER_KEY, sl.largest_user_key.as_ref().unwrap().as_slice());
        unsafe {
            crocksdb_ffi::crocksdb_level_region_accessor_destroy(accessor);
        }
    }

    #[test]
    fn drop() {
        let s = Arc::new(Mutex::new(TestState::default()));
        let accessor = new_level_region_accessor(TestLevelRegionAccessor {state: s.clone()});
        {
            let sl = s.lock().unwrap();
            assert_eq!(0, sl.drop_accessor);
        }
        unsafe {
            crocksdb_ffi::crocksdb_level_region_accessor_destroy(accessor);
        }
        {
            let sl = s.lock().unwrap();
            assert_eq!(1, sl.drop_accessor);
        }
    }
}
