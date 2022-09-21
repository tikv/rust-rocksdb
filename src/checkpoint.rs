// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::ffi::CString;
use std::path::Path;

pub struct Checkpoint {
    ptr: *mut librocksdb_sys::DBCheckpoint,
    is_titan: bool,
}

impl Checkpoint {
    /// Creates new checkpoint object for specific DB.
    pub(crate) fn new(
        db: *mut librocksdb_sys::DBInstance,
        is_titan: bool,
    ) -> Result<Checkpoint, String> {
        if is_titan {
            let ptr = unsafe { ffi_try!(ctitandb_checkpoint_object_create(db)) };
            return Ok(Checkpoint { ptr, is_titan });
        }
        let ptr = unsafe { ffi_try!(crocksdb_checkpoint_object_create(db)) };
        Ok(Checkpoint { ptr, is_titan })
    }
    /// Creates new physical DB checkpoint in directory specified by `path`.
    ///
    /// Builds an openable snapshot of RocksDB on the same disk, which
    /// accepts an output directory on the same disk, and under the directory
    /// (1) hard-linked SST files pointing to existing live SST files
    /// SST files will be copied if output directory is on a different filesystem
    /// (2) a copied manifest files and other files
    /// The directory should not already exist and will be created by this API.
    /// The directory will be an absolute path
    /// log_size_for_flush: if the total log file size is equal or larger than
    /// this value, then a flush is triggered for all the column families. The
    /// default value is 0, which means flush is always triggered. If you move
    /// away from the default, the checkpoint may not contain up-to-date data
    /// if WAL writing is not always enabled.
    /// Flush will always trigger if it is 2PC.
    pub fn create_at(&mut self, out_dir: &Path, log_size_for_flush: u64) -> Result<(), String> {
        let out_dir = match out_dir.to_str().and_then(|s| CString::new(s).ok()) {
            Some(s) => s,
            None => return Err(format!("{} is not a valid directory", out_dir.display())),
        };
        if self.is_titan {
            unsafe {
                ffi_try!(ctitandb_checkpoint_create(
                    self.ptr,
                    out_dir.as_ptr(),
                    log_size_for_flush
                ));
            }
            return Ok(());
        }
        unsafe {
            ffi_try!(crocksdb_checkpoint_create(
                self.ptr,
                out_dir.as_ptr(),
                log_size_for_flush
            ));
        }

        Ok(())
    }
}

impl Drop for Checkpoint {
    fn drop(&mut self) {
        if self.is_titan {
            unsafe {
                librocksdb_sys::ctitandb_checkpoint_object_destroy(self.ptr);
            }
            return;
        }
        unsafe {
            librocksdb_sys::crocksdb_checkpoint_object_destroy(self.ptr);
        }
    }
}
