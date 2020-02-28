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

use crocksdb_ffi::{
    self, DBEncryptionKeyManagerInstance, DBEncryptionMethod, DBFileEncryptionInfo,
};

use libc::{self, c_char, c_void, size_t, strdup};
use std::ffi::{CStr, CString};
use std::ptr;

pub struct FileEncryptionInfo {
    method: DBEncryptionMethod,
    key: &[u8],
    iv: &[u8],
}

impl FileEncryptionInfo {
    pub fn copy_to(&self, file_info: *mut DBFileEncryptionInfo) {
        unsafe {
            crocksdb_ffi::crocksdb_file_encryption_info_set_method(file_info, self.method);
            crocksdb_ffi::crocksdb_file_encryption_info_set_key(
                file_info,
                CString::new(self.key).unwrap().as_ptr(),
                self.key.len() as size_t,
            );
            crocksdb_ffi::crocksdb_file_encryption_info_set_iv(
                file_info,
                CString::new(self.iv).unwrap().as_ptr(),
                self.iv.len() as size_t,
            );
        }
    }
}

pub trait EncryptionKeyManager: Send + Sync {
    fn get_file(&mut self, fname: &str) -> Result<FileEncryptionInfo>;
    fn new_file(&mut self, fname: &str) -> Result<FileEncryptionInfo>;
    fn delete_file(&mut self, fname: &str) -> Result<()>;
}

pub struct DBEncryptionKeyManager {
    inner: *mut DBEncryptionKeyManagerInstance,
}

impl Drop for DBEncryptionKeyManager {
    fn drop(&mut self) {
        unsafe {
            crocksdb_ffi::crocksdb_encryption_key_manager_destroy(self.inner);
        }
    }
}

extern "C" fn encryption_key_manager_destructor(ctx: *mut c_void) {
    unsafe {
        Box::from_raw(ctx as *mut Box<dyn EncryptionKeyManager>);
    }
}

extern "C" fn encryption_key_manager_get_file(
    ctx: *mut c_void,
    fname: *const c_char,
    file_info: *mut DBFileEncryptionInfo,
) {
    let ctx = unsafe { &*(ctx as *mut Box<dyn EncryptionKeyManager>) };
    let fname = match CStr::from_ptr(fname).to_str() {
        Ok(ret) => ret,
        Err(err) => {
            return strdup(
                CString::new(format!(
                    "Encryption key manager encounter non-utf8 file name: {}",
                    err
                ))
                .into_raw(),
            );
        }
    };
    match ctx.get_file(fname) {
        Ok(ret) => {
            ret.copy_to(file_info);
            ptr::null()
        }
        Err(err) => stddup(
            CString::new(format!("Encryption key manager get file failure: {}", err)).into_raw(),
        ),
    }
}

extern "C" fn encryption_key_manager_new_file(
    ctx: *mut c_void,
    fname: *const c_char,
    file_info: *mut DBFileEncryptionInfo,
) {
    let ctx = unsafe { &*(ctx as *mut Box<dyn EncryptionKeyManager>) };
    let fname = match CStr::from_ptr(fname).to_str() {
        Ok(ret) => ret,
        Err(err) => {
            return strdup(
                CString::new(format!(
                    "Encryption key manager encounter non-utf8 file name: {}",
                    err
                ))
                .into_raw(),
            );
        }
    };
    match ctx.new_file(fname) {
        Ok(ret) => {
            ret.copy_to(file_info);
            ptr::null()
        }
        Err(err) => stddup(
            CString::new(format!("Encryption key manager new file failure: {}", err)).into_raw(),
        ),
    }
}

extern "C" fn encryption_key_manager_delete_file(ctx: *mut c_void, fname: *const c_char) {
    let ctx = unsafe { &*(ctx as *mut Box<dyn EncryptionKeyManager>) };
    let fname = match CStr::from_ptr(fname).to_str() {
        Ok(ret) => ret,
        Err(err) => {
            return strdup(
                CString::new(format!(
                    "Encryption key manager encounter non-utf8 file name: {}",
                    err
                ))
                .into_raw(),
            );
        }
    };
    match ctx.delete_file(fname) {
        Ok() => ptr::null(),
        Err(err) => stddup(
            CString::new(format!(
                "Encryption key manager delete file failure: {}",
                err
            ))
            .into_raw(),
        ),
    }
}

impl DBEncryptionKeyManager {
    pub fn new(key_manager: Box<dyn EncryptionKeyManager>) -> DBEncryptionKeyManager {
        unsafe {
            crocksdb_ffi::crocksdb_encryption_key_manager_create(
                Box::into_raw(key_manager) as *mut c_void,
                encryption_key_manager_destructor,
                encryption_key_manager_get_file,
                encryption_key_manager_new_file,
                encryption_key_manager_delete_file,
            )
        }
    }
}
