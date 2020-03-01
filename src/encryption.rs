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

use libc::{c_char, c_void, size_t, strdup};
use std::ffi::{CStr, CString};
use std::fmt::{self, Debug, Formatter};
use std::io::Result;
use std::ptr;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct FileEncryptionInfo {
    pub method: DBEncryptionMethod,
    pub key: Vec<u8>,
    pub iv: Vec<u8>,
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

pub trait EncryptionKeyManager {
    fn get_file(&self, fname: &str) -> Result<FileEncryptionInfo>;
    fn new_file(&self, fname: &str) -> Result<FileEncryptionInfo>;
    fn delete_file(&self, fname: &str) -> Result<()>;
}

impl EncryptionKeyManager for Mutex<T: EncryptionKeyManager> {
    fn get_file(&self, fname: &str) -> Result<FileEncryptionInfo> {
        let mut key_manager = self.lock().map_err(|e|
    }
}

pub struct DBEncryptionKeyManager {
    pub inner: *mut DBEncryptionKeyManagerInstance,

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
        Arc::from_raw(ctx as *const Arc<dyn EncryptionKeyManager>);
    }
}

extern "C" fn encryption_key_manager_get_file(
    ctx: *mut c_void,
    fname: *const c_char,
    file_info: *mut DBFileEncryptionInfo,
) -> *const c_char {
    let ctx = unsafe { &mut *(ctx as *mut Arc<dyn EncryptionKeyManager>) };
    let fname = match unsafe { CStr::from_ptr(fname).to_str() } {
        Ok(ret) => ret,
        Err(err) => {
            return unsafe {
                strdup(
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
    match ctx.get_file(fname) {
        Ok(ret) => {
            ret.copy_to(file_info);
            ptr::null()
        }
        Err(err) => unsafe {
            strdup(
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
    let ctx = unsafe { &mut *(ctx as *mut Arc<dyn EncryptionKeyManager>) };
    let fname = match unsafe { CStr::from_ptr(fname).to_str() } {
        Ok(ret) => ret,
        Err(err) => {
            return unsafe {
                strdup(
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
    match ctx.new_file(fname) {
        Ok(ret) => {
            ret.copy_to(file_info);
            ptr::null()
        }
        Err(err) => unsafe {
            strdup(
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
    let ctx = unsafe { &mut *(ctx as *mut Arc<dyn EncryptionKeyManager>) };
    let fname = match unsafe { CStr::from_ptr(fname).to_str() } {
        Ok(ret) => ret,
        Err(err) => {
            return unsafe {
                strdup(
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
    match ctx.delete_file(fname) {
        Ok(()) => ptr::null(),
        Err(err) => unsafe {
            strdup(
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
    pub fn new(key_manager: Arc<dyn EncryptionKeyManager>) -> DBEncryptionKeyManager {
        let instance = unsafe {
            crocksdb_ffi::crocksdb_encryption_key_manager_create(
                Arc::into_raw(key_manager.clone()) as *mut c_void,
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
    use std::io::Error;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use super::*;

    struct TestEncryptionKeyManager {
        pub get_file_called: usize,
        pub new_file_called: usize,
        pub delete_file_called: usize,
        pub drop_called: Option<Arc<AtomicUsize>>,
        pub fname_got: String,
        pub return_value: Result<FileEncryptionInfo>,
    }

    impl Default for TestEncryptionKeyManager {
        fn default() -> Self {
            TestEncryptionKeyManager {
                get_file_called: 0,
                new_file_called: 0,
                delete_file_called: 0,
                drop_called: None,
                fname_got: String::new(),
                return_value: Ok(FileEncryptionInfo {
                    method: DBEncryptionMethod::Unknown,
                    key: vec![],
                    iv: vec![],
                }),
            }
        }
    }

    impl Drop for TestEncryptionKeyManager {
        fn drop(&mut self) {
            if let Some(drop_called) = &self.drop_called {
                println!("drop");
                drop_called.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    impl EncryptionKeyManager for Mutex<TestEncryptionKeyManager> {
        fn get_file(&mut self, fname: &str) -> Result<FileEncryptionInfo> {
            let key_manager = self.get_mut().unwrap();
            key_manager.get_file_called += 1;
            key_manager.fname_got = fname.to_string();
            match &key_manager.return_value {
                Ok(file_info) => Ok(file_info.clone()),
                Err(err) => Err(Error::new(err.kind(), "")),
            }
        }
        fn new_file(&mut self, fname: &str) -> Result<FileEncryptionInfo> {
            let key_manager = self.get_mut().unwrap();
            key_manager.new_file_called += 1;
            key_manager.fname_got = fname.to_string();
            match &key_manager.return_value {
                Ok(file_info) => Ok(file_info.clone()),
                Err(err) => Err(Error::new(err.kind(), "")),
            }
        }
        fn delete_file(&mut self, fname: &str) -> Result<()> {
            let key_manager = self.get_mut().unwrap();
            key_manager.delete_file_called += 1;
            key_manager.fname_got = fname.to_string();
            match &key_manager.return_value {
                Ok(_) => Ok(()),
                Err(err) => Err(Error::new(err.kind(), "")),
            }
        }
    }

    #[test]
    fn create_and_destroy() {
        let key_manager = Arc::new(Mutex::new(TestEncryptionKeyManager::default()));
        let drop_called = Arc::new(AtomicUsize::new(0));
        key_manager.lock().unwrap().drop_called = Some(drop_called.clone());
        {
            println!("new");
            DBEncryptionKeyManager::new(key_manager);
            println!("end");
            // Dropped implicitly.
        }
        assert!(drop_called.load(Ordering::Relaxed) == 1);
    }
}
