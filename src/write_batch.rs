use crocksdb_ffi::{self, DBCFHandle, DBWriteBatch};
use libc::size_t;

pub struct CFHandle {
    pub(crate) inner: *mut DBCFHandle,
}

impl CFHandle {
    pub fn id(&self) -> u32 {
        unsafe { crocksdb_ffi::crocksdb_column_family_handle_id(self.inner) }
    }
}

impl Drop for CFHandle {
    fn drop(&mut self) {
        unsafe {
            crocksdb_ffi::crocksdb_column_family_handle_destroy(self.inner);
        }
    }
}

pub struct WriteBatch {
    pub(crate) inner: *mut DBWriteBatch,
}

unsafe impl Send for WriteBatch {}

impl Default for WriteBatch {
    fn default() -> WriteBatch {
        WriteBatch {
            inner: unsafe { crocksdb_ffi::crocksdb_writebatch_create() },
        }
    }
}

impl Drop for WriteBatch {
    fn drop(&mut self) {
        unsafe { crocksdb_ffi::crocksdb_writebatch_destroy(self.inner) }
    }
}

impl WriteBatch {
    pub fn new() -> WriteBatch {
        WriteBatch::default()
    }

    pub fn with_capacity(cap: usize) -> WriteBatch {
        WriteBatch {
            inner: unsafe { crocksdb_ffi::crocksdb_writebatch_create_with_capacity(cap) },
        }
    }

    pub fn count(&self) -> usize {
        unsafe { crocksdb_ffi::crocksdb_writebatch_count(self.inner) as usize }
    }

    pub fn is_empty(&self) -> bool {
        self.count() == 0
    }

    pub fn data_size(&self) -> usize {
        unsafe {
            let mut data_size: usize = 0;
            let _ = crocksdb_ffi::crocksdb_writebatch_data(self.inner, &mut data_size);
            return data_size;
        }
    }

    pub fn clear(&mut self) {
        unsafe {
            crocksdb_ffi::crocksdb_writebatch_clear(self.inner);
        }
    }

    pub fn set_save_point(&mut self) {
        unsafe {
            crocksdb_ffi::crocksdb_writebatch_set_save_point(self.inner);
        }
    }

    pub fn rollback_to_save_point(&mut self) -> Result<(), String> {
        unsafe {
            ffi_try!(crocksdb_writebatch_rollback_to_save_point(self.inner));
        }
        Ok(())
    }

    pub fn pop_save_point(&mut self) -> Result<(), String> {
        unsafe {
            ffi_try!(crocksdb_writebatch_pop_save_point(self.inner));
        }
        Ok(())
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), String> {
        unsafe {
            crocksdb_ffi::crocksdb_writebatch_put(
                self.inner,
                key.as_ptr(),
                key.len() as size_t,
                value.as_ptr(),
                value.len() as size_t,
            );
            Ok(())
        }
    }

    pub fn put_cf(&mut self, cf: &CFHandle, key: &[u8], value: &[u8]) -> Result<(), String> {
        unsafe {
            crocksdb_ffi::crocksdb_writebatch_put_cf(
                self.inner,
                cf.inner,
                key.as_ptr(),
                key.len() as size_t,
                value.as_ptr(),
                value.len() as size_t,
            );
            Ok(())
        }
    }

    pub fn merge(&mut self, key: &[u8], value: &[u8]) -> Result<(), String> {
        unsafe {
            crocksdb_ffi::crocksdb_writebatch_merge(
                self.inner,
                key.as_ptr(),
                key.len() as size_t,
                value.as_ptr(),
                value.len() as size_t,
            );
            Ok(())
        }
    }

    pub fn merge_cf(&mut self, cf: &CFHandle, key: &[u8], value: &[u8]) -> Result<(), String> {
        unsafe {
            crocksdb_ffi::crocksdb_writebatch_merge_cf(
                self.inner,
                cf.inner,
                key.as_ptr(),
                key.len() as size_t,
                value.as_ptr(),
                value.len() as size_t,
            );
            Ok(())
        }
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<(), String> {
        unsafe {
            crocksdb_ffi::crocksdb_writebatch_delete(self.inner, key.as_ptr(), key.len() as size_t);
            Ok(())
        }
    }

    pub fn delete_cf(&mut self, cf: &CFHandle, key: &[u8]) -> Result<(), String> {
        unsafe {
            crocksdb_ffi::crocksdb_writebatch_delete_cf(
                self.inner,
                cf.inner,
                key.as_ptr(),
                key.len() as size_t,
            );
            Ok(())
        }
    }

    pub fn single_delete(&mut self, key: &[u8]) -> Result<(), String> {
        unsafe {
            crocksdb_ffi::crocksdb_writebatch_single_delete(
                self.inner,
                key.as_ptr(),
                key.len() as size_t,
            );
            Ok(())
        }
    }

    pub fn single_delete_cf(&mut self, cf: &CFHandle, key: &[u8]) -> Result<(), String> {
        unsafe {
            crocksdb_ffi::crocksdb_writebatch_single_delete_cf(
                self.inner,
                cf.inner,
                key.as_ptr(),
                key.len() as size_t,
            );
            Ok(())
        }
    }

    pub fn delete_range(&mut self, begin_key: &[u8], end_key: &[u8]) -> Result<(), String> {
        unsafe {
            crocksdb_ffi::crocksdb_writebatch_delete_range(
                self.inner,
                begin_key.as_ptr(),
                begin_key.len(),
                end_key.as_ptr(),
                end_key.len(),
            );
            Ok(())
        }
    }

    pub fn delete_range_cf(
        &mut self,
        cf: &CFHandle,
        begin_key: &[u8],
        end_key: &[u8],
    ) -> Result<(), String> {
        unsafe {
            crocksdb_ffi::crocksdb_writebatch_delete_range_cf(
                self.inner,
                cf.inner,
                begin_key.as_ptr(),
                begin_key.len(),
                end_key.as_ptr(),
                end_key.len(),
            );
            Ok(())
        }
    }
}
