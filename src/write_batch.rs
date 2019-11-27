use crocksdb_ffi::{self, DBWriteBatch};

use libc::size_t;
use rocksdb::{CFHandle, WriteBatchBase};
use librocksdb_sys::{DBInstance, DBWriteOptions};


pub struct WriteBatch {
    pub(super) inner: *mut DBWriteBatch,
}

unsafe impl Send for WriteBatch {}

impl Default for WriteBatch {
    fn default() -> WriteBatch {
        WriteBatch {
            inner: unsafe { crocksdb_ffi::crocksdb_writebatch_create() },
        }
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
}

impl WriteBatchBase for WriteBatch {

    fn write_into_rocksdb(&self, db: *mut DBInstance, writeopts: *mut DBWriteOptions) -> Result<(), String> {
        unsafe {
            ffi_try!(crocksdb_write(db, writeopts, self.inner));
        }
        Ok(())
    }

    fn count(&self) -> usize {
        unsafe { crocksdb_ffi::crocksdb_writebatch_count(self.inner) as usize }
    }

    fn is_empty(&self) -> bool {
        self.count() == 0
    }

    fn data_size(&self) -> usize {
        unsafe {
            let mut data_size: usize = 0;
            let _ = crocksdb_ffi::crocksdb_writebatch_data(self.inner, &mut data_size);
            return data_size;
        }
    }

    fn clear(&mut self) {
        unsafe {
            crocksdb_ffi::crocksdb_writebatch_clear(self.inner);
        }
    }

    fn set_save_point(&mut self) {
        unsafe {
            crocksdb_ffi::crocksdb_writebatch_set_save_point(self.inner);
        }
    }

    fn rollback_to_save_point(&mut self) -> Result<(), String> {
        unsafe {
            ffi_try!(crocksdb_writebatch_rollback_to_save_point(self.inner));
        }
        Ok(())
    }

    fn pop_save_point(&mut self) -> Result<(), String> {
        unsafe {
            ffi_try!(crocksdb_writebatch_pop_save_point(self.inner));
        }
        Ok(())
    }

    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), String> {
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

    fn put_cf(&mut self, cf: &CFHandle, key: &[u8], value: &[u8]) -> Result<(), String> {
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

    fn merge(&mut self, key: &[u8], value: &[u8]) -> Result<(), String> {
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

    fn merge_cf(&mut self, cf: &CFHandle, key: &[u8], value: &[u8]) -> Result<(), String> {
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

    fn delete(&mut self, key: &[u8]) -> Result<(), String> {
        unsafe {
            crocksdb_ffi::crocksdb_writebatch_delete(self.inner, key.as_ptr(), key.len() as size_t);
            Ok(())
        }
    }

    fn delete_cf(&mut self, cf: &CFHandle, key: &[u8]) -> Result<(), String> {
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

    fn single_delete(&mut self, key: &[u8]) -> Result<(), String> {
        unsafe {
            crocksdb_ffi::crocksdb_writebatch_single_delete(
                self.inner,
                key.as_ptr(),
                key.len() as size_t,
            );
            Ok(())
        }
    }

    fn single_delete_cf(&mut self, cf: &CFHandle, key: &[u8]) -> Result<(), String> {
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

    fn delete_range(&mut self, begin_key: &[u8], end_key: &[u8]) -> Result<(), String> {
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

    fn delete_range_cf(
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

impl Drop for WriteBatch {
    fn drop(&mut self) {
        unsafe { crocksdb_ffi::crocksdb_writebatch_destroy(self.inner) }
    }
}

pub struct WriteBatchVec {
    batches: Vec<*mut DBWriteBatch>,
    batch_capacity: usize,
    batch_limit: usize,
    cur: usize,
    batch_count: usize,
    key_count: usize,
    save_point: Vec<usize>,
}

unsafe impl Send for WriteBatchVec {}

impl Default for WriteBatchVec {
    fn default() -> WriteBatchVec {
        let mut batches = Vec::default();
        let batch = unsafe { crocksdb_ffi::crocksdb_writebatch_create() };
        batches.push(batch);
        WriteBatchVec {
            batches,
            batch_capacity: 0,
            batch_limit: 16,
            cur: 0,
            batch_count: 1,
            key_count: 0,
            save_point: vec![]
        }
    }
}

impl WriteBatchVec {
    pub fn new(batch_capacity: usize, batch_limit: usize) -> WriteBatchVec {
        let mut batches = Vec::default();
        let batch = unsafe { crocksdb_ffi::crocksdb_writebatch_create_with_capacity(batch_capacity) };
        batches.push(batch);
        WriteBatchVec {
            batches,
            batch_capacity,
            batch_limit,
            cur: 0,
            batch_count: 0,
            key_count: 0,
            save_point: vec![]
        }
    }

    pub fn maybe_switch_current_batch(&mut self) {
        self.key_count += 1;
        self.batch_count += 1;
        if self.batch_count > self.batch_limit {
            if self.cur + 1 == self.batches.len() {
                unsafe {
                    let batch = if self.batch_capacity > 0 {
                        crocksdb_ffi::crocksdb_writebatch_create_with_capacity(self.batch_capacity)
                    } else {
                        crocksdb_ffi::crocksdb_writebatch_create()
                    };
                    self.batches.push(batch);
                }
            }
            self.batch_count = 0;
            self.cur += 1;
        }
    }
}

impl Drop for WriteBatchVec {
    fn drop(&mut self) {
        for inner in self.batches.iter() {
            unsafe { crocksdb_ffi::crocksdb_writebatch_destroy(*inner) }
        }
    }
}

impl WriteBatchBase for WriteBatchVec {
    fn write_into_rocksdb(&self, db: *mut DBInstance, writeopts: *mut DBWriteOptions) -> Result<(), String> {
        unsafe {
            ffi_try!(crocksdb_write_multi_batch(
                    db,
                    writeopts,
                    self.batches.as_ptr(),
                    self.cur + 1
                ));
        }
        Ok(())
    }

    fn count(&self) -> usize {
        self.key_count
    }

    fn is_empty(&self) -> bool {
        self.key_count == 0
    }

    fn data_size(&self) -> usize {
        unsafe {
            let mut sum_size: usize = 0;
            for i in 0..(self.cur + 1) {
                let mut data_size: usize = 0;
                let _ = crocksdb_ffi::crocksdb_writebatch_data(self.batches[i], &mut data_size);
                sum_size += data_size;
            }
            return sum_size;
        }
    }

    fn clear(&mut self) {
        unsafe {
            for i in 0..(self.cur + 1) {
                crocksdb_ffi::crocksdb_writebatch_clear(self.batches[i]);
            }
        }
        self.cur = 0;
        self.key_count = 0;
        self.batch_count = 0;
        self.save_point.clear();
    }

    fn set_save_point(&mut self) {
        unsafe {
            self.save_point.push(self.cur);
            crocksdb_ffi::crocksdb_writebatch_set_save_point(self.batches[self.cur]);
        }
    }

    fn rollback_to_save_point(&mut self) -> Result<(), String> {
        let v = self.save_point.pop().unwrap();
        unsafe {
            for i in v..self.cur {
                self.key_count -= crocksdb_ffi::crocksdb_writebatch_count(self.batches[i + 1]) as usize;
                crocksdb_ffi::crocksdb_writebatch_clear(self.batches[i + 1]);
            }
            self.key_count -= crocksdb_ffi::crocksdb_writebatch_count(self.batches[v]) as usize;
            ffi_try!(crocksdb_writebatch_rollback_to_save_point(self.batches[v]));
            self.batch_count = crocksdb_ffi::crocksdb_writebatch_count(self.batches[v]) as usize;
            self.key_count += self.batch_count;
            self.cur = v;
        }
        Ok(())
    }

    fn pop_save_point(&mut self) -> Result<(), String> {
        let v = self.save_point.pop().unwrap();
        unsafe {
            ffi_try!(crocksdb_writebatch_pop_save_point(self.batches[v]));
        }
        Ok(())
    }

    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), String> {
        self.maybe_switch_current_batch();
        unsafe {
            crocksdb_ffi::crocksdb_writebatch_put(
                self.batches[self.cur],
                key.as_ptr(),
                key.len() as size_t,
                value.as_ptr(),
                value.len() as size_t,
            );
            Ok(())
        }
    }

    fn put_cf(&mut self, cf: &CFHandle, key: &[u8], value: &[u8]) -> Result<(), String> {
        self.maybe_switch_current_batch();
        unsafe {
            crocksdb_ffi::crocksdb_writebatch_put_cf(
                self.batches[self.cur],
                cf.inner,
                key.as_ptr(),
                key.len() as size_t,
                value.as_ptr(),
                value.len() as size_t,
            );
            Ok(())
        }
    }

    fn merge(&mut self, key: &[u8], value: &[u8]) -> Result<(), String> {
        self.maybe_switch_current_batch();
        unsafe {
            crocksdb_ffi::crocksdb_writebatch_merge(
                self.batches[self.cur],
                key.as_ptr(),
                key.len() as size_t,
                value.as_ptr(),
                value.len() as size_t,
            );
            Ok(())
        }
    }

    fn merge_cf(&mut self, cf: &CFHandle, key: &[u8], value: &[u8]) -> Result<(), String> {
        self.maybe_switch_current_batch();
        unsafe {
            crocksdb_ffi::crocksdb_writebatch_merge_cf(
                self.batches[self.cur],
                cf.inner,
                key.as_ptr(),
                key.len() as size_t,
                value.as_ptr(),
                value.len() as size_t,
            );
            Ok(())
        }
    }

    fn delete(&mut self, key: &[u8]) -> Result<(), String> {
        self.maybe_switch_current_batch();
        unsafe {
            crocksdb_ffi::crocksdb_writebatch_delete(self.batches[self.cur], key.as_ptr(), key.len() as size_t);
            Ok(())
        }
    }

    fn delete_cf(&mut self, cf: &CFHandle, key: &[u8]) -> Result<(), String> {
        self.maybe_switch_current_batch();
        unsafe {
            crocksdb_ffi::crocksdb_writebatch_delete_cf(
                self.batches[self.cur],
                cf.inner,
                key.as_ptr(),
                key.len() as size_t,
            );
            Ok(())
        }
    }

    fn single_delete(&mut self, key: &[u8]) -> Result<(), String> {
        self.maybe_switch_current_batch();
        unsafe {
            crocksdb_ffi::crocksdb_writebatch_single_delete(
                self.batches[self.cur],
                key.as_ptr(),
                key.len() as size_t,
            );
            Ok(())
        }
    }

    fn single_delete_cf(&mut self, cf: &CFHandle, key: &[u8]) -> Result<(), String> {
        self.maybe_switch_current_batch();
        unsafe {
            crocksdb_ffi::crocksdb_writebatch_single_delete_cf(
                self.batches[self.cur],
                cf.inner,
                key.as_ptr(),
                key.len() as size_t,
            );
            Ok(())
        }
    }

    fn delete_range(&mut self, begin_key: &[u8], end_key: &[u8]) -> Result<(), String> {
        self.maybe_switch_current_batch();
        unsafe {
            crocksdb_ffi::crocksdb_writebatch_delete_range(
                self.batches[self.cur],
                begin_key.as_ptr(),
                begin_key.len(),
                end_key.as_ptr(),
                end_key.len(),
            );
            Ok(())
        }
    }

    fn delete_range_cf(
        &mut self,
        cf: &CFHandle,
        begin_key: &[u8],
        end_key: &[u8],
    ) -> Result<(), String> {
        self.maybe_switch_current_batch();
        unsafe {
            crocksdb_ffi::crocksdb_writebatch_delete_range_cf(
                self.batches[self.cur],
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

