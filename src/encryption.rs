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

use libc::{c_char, c_void, size_t};
use std::ffi::{CStr, CString};
use std::fmt::{self, Debug, Formatter};
use std::io::{Error, ErrorKind, Result};
use std::mem::transmute;
use std::ptr;
use std::slice;
use std::sync::Arc;

#[derive(Clone)]
pub struct FileEncryptionInfo {
    pub method: DBEncryptionMethod,
    pub key: Vec<u8>,
    pub iv: Vec<u8>,
}

impl Default for FileEncryptionInfo {
    fn default() -> Self {
        FileEncryptionInfo {
            method: DBEncryptionMethod::Unknown,
            key: vec![],
            iv: vec![],
        }
    }
}

impl Debug for FileEncryptionInfo {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "FileEncryptionInfo [method={}, key=...<{} bytes>, iv=...<{} bytes>]",
            self.method,
            self.key.len(),
            self.iv.len()
        )
    }
}

impl FileEncryptionInfo {
    pub fn copy_to(&self, file_info: *mut DBFileEncryptionInfo) {
        unsafe {
            crocksdb_ffi::crocksdb_file_encryption_info_set_method(file_info, self.method);
            crocksdb_ffi::crocksdb_file_encryption_info_set_key(
                file_info,
                CString::new(self.key.clone()).unwrap().as_ptr(),
                self.key.len() as size_t,
            );
            crocksdb_ffi::crocksdb_file_encryption_info_set_iv(
                file_info,
                CString::new(self.iv.clone()).unwrap().as_ptr(),
                self.iv.len() as size_t,
            );
        }
    }
}

pub trait EncryptionKeyManager: Sync + Send {
    fn get_file(&self, fname: &str) -> Result<FileEncryptionInfo>;
    fn new_file(&self, fname: &str) -> Result<FileEncryptionInfo>;
    fn delete_file(&self, fname: &str) -> Result<()>;
}

pub struct DBEncryptionKeyManager {
    pub inner: *mut DBEncryptionKeyManagerInstance,
}

impl Drop for DBEncryptionKeyManager {
    fn drop(&mut self) {
        unsafe {
            println!("drop");
            crocksdb_ffi::crocksdb_encryption_key_manager_destroy(self.inner);
        }
    }
}

unsafe impl Send for DBEncryptionKeyManager {}
unsafe impl Sync for DBEncryptionKeyManager {}

// The implementation of EncryptionKeyManager is used to test calling the methods through FFI.
/*
impl EncryptionKeyManager for DBEncryptionKeyManager {
    fn get_file(&self, fname: &str) -> Result<FileEncryptionInfo> {
        let ret: Result<FileEncryptionInfo>;
        unsafe {
            let file_info = crocksdb_ffi::crocksdb_file_encryption_info_create();
            let err = crocksdb_ffi::crocksdb_encryption_key_manager_get_file(
                self.inner,
                CString::new(fname).unwrap().as_ptr(),
                file_info,
            );
            if err == ptr::null() {
                let mut key_len: size_t = 0;
                let mut iv_len: size_t = 0;
                let key = transmute::<*const c_char, *const u8>(
                    crocksdb_ffi::crocksdb_file_encryption_info_key(file_info, &mut key_len),
                );
                let iv = transmute::<*const c_char, *const u8>(
                    crocksdb_ffi::crocksdb_file_encryption_info_iv(file_info, &mut iv_len),
                );
                ret = Ok(FileEncryptionInfo {
                    method: crocksdb_ffi::crocksdb_file_encryption_info_method(file_info),
                    key: slice::from_raw_parts(key, key_len).to_vec(),
                    iv: slice::from_raw_parts(iv, iv_len).to_vec(),
                });
            } else {
                ret = Err(Error::new(
                    ErrorKind::Other,
                    format!("{}", CStr::from_ptr(err).to_str().unwrap()),
                ));
                libc::free(err as _);
            }
            crocksdb_ffi::crocksdb_file_encryption_info_destroy(file_info);
        }
        ret
    }

    fn new_file(&self, _fname: &str) -> Result<FileEncryptionInfo> {
        Ok(FileEncryptionInfo {
            method: DBEncryptionMethod::Unknown,
            key: vec![],
            iv: vec![],
        })
    }

    fn delete_file(&self, _fname: &str) -> Result<()> {
        Ok(())
    }
}
*/

extern "C" fn encryption_key_manager_destructor(ctx: *mut c_void) {
    unsafe {
        // implicitly drop.
        Arc::from_raw(transmute::<*mut c_void, *const Box<dyn EncryptionKeyManager>>(ctx));
    }
}

extern "C" fn encryption_key_manager_get_file(
    ctx: *mut c_void,
    fname: *const c_char,
    file_info: *mut DBFileEncryptionInfo,
) -> *const c_char {
    let key_manager =
        unsafe { &*(transmute::<*mut c_void, *const Box<dyn EncryptionKeyManager>>(ctx)) };
    let fname = match unsafe { CStr::from_ptr(fname).to_str() } {
        Ok(ret) => ret,
        Err(err) => {
            return unsafe {
                libc::strdup(
                    CString::new(format!(
                        "Encryption key manager encounter non-utf8 file name: {}",
                        err
                    ))
                    .unwrap()
                    .into_raw(),
                )
            };
        }
    };
    match key_manager.get_file(fname) {
        Ok(ret) => {
            ret.copy_to(file_info);
            ptr::null()
        }
        Err(err) => unsafe {
            libc::strdup(
                CString::new(format!("Encryption key manager get file failure: {}", err))
                    .unwrap()
                    .into_raw(),
            )
        },
    }
}

extern "C" fn encryption_key_manager_new_file(
    ctx: *mut c_void,
    fname: *const c_char,
    file_info: *mut DBFileEncryptionInfo,
) -> *const c_char {
    let key_manager =
        unsafe { &*(transmute::<*mut c_void, *const Box<dyn EncryptionKeyManager>>(ctx)) };
    let fname = match unsafe { CStr::from_ptr(fname).to_str() } {
        Ok(ret) => ret,
        Err(err) => {
            return unsafe {
                libc::strdup(
                    CString::new(format!(
                        "Encryption key manager encounter non-utf8 file name: {}",
                        err
                    ))
                    .unwrap()
                    .into_raw(),
                )
            };
        }
    };
    match key_manager.new_file(fname) {
        Ok(ret) => {
            ret.copy_to(file_info);
            ptr::null()
        }
        Err(err) => unsafe {
            libc::strdup(
                CString::new(format!("Encryption key manager new file failure: {}", err))
                    .unwrap()
                    .into_raw(),
            )
        },
    }
}

extern "C" fn encryption_key_manager_delete_file(
    ctx: *mut c_void,
    fname: *const c_char,
) -> *const c_char {
    let key_manager =
        unsafe { &*(transmute::<*mut c_void, *const Box<dyn EncryptionKeyManager>>(ctx)) };
    let fname = match unsafe { CStr::from_ptr(fname).to_str() } {
        Ok(ret) => ret,
        Err(err) => {
            return unsafe {
                libc::strdup(
                    CString::new(format!(
                        "Encryption key manager encounter non-utf8 file name: {}",
                        err
                    ))
                    .unwrap()
                    .into_raw(),
                )
            };
        }
    };
    match key_manager.delete_file(fname) {
        Ok(()) => ptr::null(),
        Err(err) => unsafe {
            libc::strdup(
                CString::new(format!(
                    "Encryption key manager delete file failure: {}",
                    err
                ))
                .unwrap()
                .into_raw(),
            )
        },
    }
}

impl DBEncryptionKeyManager {
    pub fn new(key_manager: Arc<Box<dyn EncryptionKeyManager>>) -> DBEncryptionKeyManager {
        let instance = unsafe {
            println!("rust ctor");
            crocksdb_ffi::crocksdb_encryption_key_manager_create(
                Arc::into_raw(key_manager) as *mut c_void,
                encryption_key_manager_destructor,
                encryption_key_manager_get_file,
                encryption_key_manager_new_file,
                encryption_key_manager_delete_file,
            )
        };
        DBEncryptionKeyManager { inner: instance }
    }
}

#[cfg(test)]
mod test {
    use std::io::{Error, ErrorKind};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use super::*;

    #[derive(Clone)]
    struct TestEncryptionKeyManager {
        pub get_file_called: usize,
        pub new_file_called: usize,
        pub delete_file_called: usize,
        pub drop_called: Option<Arc<AtomicUsize>>,
        pub fname_got: String,
        pub return_value: Option<FileEncryptionInfo>,
    }

    impl Drop for TestEncryptionKeyManager {
        fn drop(&mut self) {
            if let Some(drop_called) = &self.drop_called {
                drop_called.fetch_add(1, Ordering::SeqCst);
            }
        }
    }

    impl EncryptionKeyManager for Mutex<TestEncryptionKeyManager> {
        fn get_file(&self, fname: &str) -> Result<FileEncryptionInfo> {
            let mut key_manager = self.lock().unwrap();
            key_manager.get_file_called += 1;
            key_manager.fname_got = fname.to_string();
            match &key_manager.return_value {
                Some(file_info) => Ok(file_info.clone()),
                None => Err(Error::new(ErrorKind::Other, "")),
            }
        }
        fn new_file(&self, fname: &str) -> Result<FileEncryptionInfo> {
            let mut key_manager = self.lock().unwrap();
            key_manager.new_file_called += 1;
            key_manager.fname_got = fname.to_string();
            match &key_manager.return_value {
                Some(file_info) => Ok(file_info.clone()),
                None => Err(Error::new(ErrorKind::Other, "")),
            }
        }
        fn delete_file(&self, fname: &str) -> Result<()> {
            let mut key_manager = self.lock().unwrap();
            key_manager.delete_file_called += 1;
            key_manager.fname_got = fname.to_string();
            match &key_manager.return_value {
                Some(_) => Ok(()),
                None => Err(Error::new(ErrorKind::Other, "")),
            }
        }
    }

    #[test]
    fn create_and_destroy() {
        let drop_called = Arc::new(AtomicUsize::new(0));
        let key_manager: Arc<Box<dyn EncryptionKeyManager>> =
            Arc::new(Box::new(Mutex::new(TestEncryptionKeyManager {
                get_file_called: 0,
                new_file_called: 0,
                delete_file_called: 0,
                drop_called: Some(drop_called.clone()),
                fname_got: "".to_string(),
                return_value: None,
            })));
        let db_key_manager = DBEncryptionKeyManager::new(key_manager.clone());
        drop(key_manager);
        assert_eq!(0, drop_called.load(Ordering::SeqCst));
        drop(db_key_manager);
        assert_eq!(1, drop_called.load(Ordering::SeqCst));
    }

    #[test]
    fn get_file() {
        let key_manager: Arc<Box<dyn EncryptionKeyManager>> =
            Arc::new(Box::new(Mutex::new(TestEncryptionKeyManager {
                get_file_called: 0,
                new_file_called: 0,
                delete_file_called: 0,
                drop_called: None,
                fname_got: "".to_string(),
                return_value: Some(FileEncryptionInfo {
                    method: DBEncryptionMethod::Aes128Ctr,
                    key: b"test_key_get_file".to_vec(),
                    iv: b"test_iv_get_file".to_vec(),
                }),
            })));
        let _db_key_manager = DBEncryptionKeyManager::new(key_manager.clone());
        /*
        let file_info = db_key_manager.get_file("get_file_path").unwrap();
        assert_eq!(DBEncryptionMethod::Aes128Ctr, file_info.method);
        assert_eq!(b"test_key_get_file", file_info.key.as_slice());
        assert_eq!(b"test_iv_get_file", file_info.iv.as_slice());
        */
    }
}
