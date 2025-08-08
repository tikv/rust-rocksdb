use crocksdb_ffi;
use librocksdb_sys::DBEnv;

pub struct SstFileManager {
    pub(crate) inner: *mut crocksdb_ffi::SstFileManager,
}

impl SstFileManager {
    pub fn new(env: *const DBEnv) -> SstFileManager {
        unsafe {
            SstFileManager {
                inner: crocksdb_ffi::crocksdb_sstfilemanager_create(env),
            }
        }
    }

    pub fn get_total_size(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_sstfilemanager_get_total_size(self.inner) }
    }
}
