// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

pub use crocksdb_ffi::{self, DBFileSystemInspectorInstance};

use libc::{c_char, c_void, size_t, strdup};
use std::sync::Arc;

// Inspect global IO flow. No per-file inspection for now.
pub trait FileSystemInspector: Sync + Send {
    fn read(&self, len: usize) -> Result<usize, String>;
    fn write(&self, len: usize) -> Result<usize, String>;
}

extern "C" fn file_system_inspector_destructor(ctx: *mut c_void) {
    unsafe {
        // Recover from raw pointer and implicitly drop.
        Box::from_raw(ctx as *mut Arc<dyn FileSystemInspector>);
    }
}

extern "C" fn file_system_inspector_read(
    ctx: *mut c_void,
    len: size_t,
    errptr: *mut *mut c_char,
) -> size_t {
    let file_system_inspector = unsafe { &*(ctx as *mut Arc<dyn FileSystemInspector>) };
    match file_system_inspector.read(len) {
        Ok(ret) => ret,
        Err(e) => {
            unsafe {
                *errptr = strdup(e.as_ptr() as *const c_char);
            }
            0
        }
    }
}

extern "C" fn file_system_inspector_write(
    ctx: *mut c_void,
    len: size_t,
    errptr: *mut *mut c_char,
) -> size_t {
    let file_system_inspector = unsafe { &*(ctx as *mut Arc<dyn FileSystemInspector>) };
    match file_system_inspector.write(len) {
        Ok(ret) => ret,
        Err(e) => {
            unsafe {
                *errptr = strdup(e.as_ptr() as *const c_char);
            }
            0
        }
    }
}

pub struct DBFileSystemInspector {
    pub inner: *mut DBFileSystemInspectorInstance,
}

unsafe impl Send for DBFileSystemInspector {}
unsafe impl Sync for DBFileSystemInspector {}

impl DBFileSystemInspector {
    pub fn new(file_system_inspector: Arc<dyn FileSystemInspector>) -> DBFileSystemInspector {
        // Size of Arc<dyn T>::into_raw is of 128-bits, which couldn't be used as C-style pointer.
        // Boxing it to make a 64-bits pointer.
        let ctx = Box::into_raw(Box::new(file_system_inspector)) as *mut c_void;
        let instance = unsafe {
            crocksdb_ffi::crocksdb_file_system_inspector_create(
                ctx,
                file_system_inspector_destructor,
                file_system_inspector_read,
                file_system_inspector_write,
            )
        };
        DBFileSystemInspector { inner: instance }
    }
}

impl Drop for DBFileSystemInspector {
    fn drop(&mut self) {
        unsafe {
            crocksdb_ffi::crocksdb_file_system_inspector_destroy(self.inner);
        }
    }
}

#[cfg(test)]
impl FileSystemInspector for DBFileSystemInspector {
    fn read(&self, len: usize) -> Result<usize, String> {
        let ret = unsafe {
            ffi_try!(crocksdb_ffi::crocksdb_file_system_inspector_read(
                self.inner, len
            ))
        };
        Ok(ret)
    }
    fn write(&self, len: usize) -> Result<usize, String> {
        let ret = unsafe {
            ffi_try!(crocksdb_ffi::crocksdb_file_system_inspector_write(
                self.inner, len
            ))
        };
        Ok(ret)
    }
}
