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

#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#[allow(clippy::all)]
mod bindings {
    include!(env!("BINDING_PATH"));
}
pub use bindings::*;

extern crate bzip2_sys;
extern crate libc;
#[cfg(test)]
extern crate tempfile;

use libc::{c_char, c_int, c_void};
use std::ffi::CStr;

pub fn new_bloom_filter(bits: c_int) -> *mut crocksdb_filterpolicy_t {
    unsafe { crocksdb_filterpolicy_create_bloom(bits) }
}

pub unsafe fn new_lru_cache(opt: *mut crocksdb_lru_cache_options_t) -> *mut crocksdb_cache_t {
    crocksdb_cache_create_lru(opt)
}

mod generated;
pub use generated::*;

#[repr(C)]
pub struct DBTitanDBOptions(c_void);
#[repr(C)]
pub struct DBTitanReadOptions(c_void);

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
