use cxx::SharedPtr;
use futures::executor::block_on;
use rocksdb::crocksdb_ffi::DBReadOptions;
use rocksdb::rocksdb::*;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

#[cxx::bridge]
mod ffi {
    /// Enums must match the enums in include/rocksdb/status.h and
    /// include/rocksdb/io_status.h
    #[repr(u8)]
    #[derive(Copy, Clone, Debug)]
    enum StatusCode {
        kOk = 0,
        kNotFound = 1,
        kCorruption = 2,
        kNotSupported = 3,
        kInvalidArgument = 4,
        kIOError = 5,
        kMergeInProgress = 6,
        kIncomplete = 7,
        kShutdownInProgress = 8,
        kTimedOut = 9,
        kAborted = 10,
        kBusy = 11,
        kExpired = 12,
        kTryAgain = 13,
        kCompactionTooLarge = 14,
        kColumnFamilyDropped = 15,
        kMaxCode,
    }

    #[repr(u8)]
    #[derive(Copy, Clone, Debug)]
    enum StatusSubCode {
        kNone = 0,
        kMutexTimeout = 1,
        kLockTimeout = 2,
        kLockLimit = 3,
        kNoSpace = 4,
        kDeadlock = 5,
        kStaleFile = 6,
        kMemoryLimit = 7,
        kSpaceLimit = 8,
        kPathNotFound = 9,
        KMergeOperandsInsufficientCapacity = 10,
        kManualCompactionPaused = 11,
        kOverwritten = 12,
        kTxnNotPrepared = 13,
        kIOFenced = 14,
        kIOUringSqeFull = 15,
        kIOUringSubmitError = 16,
        kMaxSubCode,
    }

    #[repr(u8)]
    #[derive(Copy, Clone, Debug)]
    enum StatusSeverity {
        kNoError = 0,
        kSoftError = 1,
        kHardError = 2,
        kFatalError = 3,
        kUnrecoverableError = 4,
        kMaxSeverity,
    }

    #[derive(Copy, Clone, Debug)]
    struct RustStatus {
        code: StatusCode,
        sub_code: StatusSubCode,
        severity: StatusSeverity,
    }

    #[repr(u8)]
    #[derive(Copy, Clone, Debug)]
    enum IOErrorScope {
        kIOErrorScopeFileSystem,
        kIOErrorScopeFile,
        kIOErrorScopeRange,
        kIOErrorScopeMax,
    }

    #[derive(Copy, Clone, Debug)]
    struct RustIOStatus {
        status: RustStatus,
        retryable: bool,
        data_loss: bool,
        scope: IOErrorScope,
    }

    #[derive(Copy, Clone, Debug)]
    struct Async_result {
        /// These are opaque values that are dereferened by the
        /// C++ code.
        m_pinnable: *mut PinnableSlice,
        m_async_future: *mut Async_future,
        m_async_reader: *const Async_reader,
    }

    unsafe extern "C++" {
        include!("rocksdb/db.h");
        include!("crocksdb/c.h");
        include!("cxxrocksdb.h");

        type CRocksDB;

        #[namespace = "rocksdb"]
        type ReadOptions;

        #[namespace = "rocksdb"]
        type PinnableSlice;

        #[namespace = "rocksdb"]
        type Async_future;

        type Async_reader;

        unsafe fn new_async_reader(
            db: *mut CRocksDB,
            io_uring_size: u32,
        ) -> SharedPtr<Async_reader>;

        unsafe fn setup_io_uring_sq_handler(self: &Async_reader, ropts: *mut ReadOptions);

        unsafe fn get(
            self: &Async_reader,
            ropts: *const ReadOptions,
            arg: String,) -> Async_result;

        unsafe fn get_async_result(ar: Async_result, v: &mut String) -> RustStatus;

        fn io_uring_peek_cq(self: &Async_reader) -> bool;
        fn io_uring_reap_cq(self: &Async_reader) -> u32;
        fn pending_io_uring_sqe_count(self: &Async_reader) -> u32;
    }
}

impl ffi::RustStatus {
    pub fn new() -> Self {
        Self {
            code: ffi::StatusCode::kOk,
            sub_code: ffi::StatusSubCode::kNone,
            severity: ffi::StatusSeverity::kNoError,
        }
    }

    pub fn new_error(code: ffi::StatusCode) -> Self {
        Self {
            code: code,
            sub_code: ffi::StatusSubCode::kNone,
            severity: ffi::StatusSeverity::kNoError,
        }
    }

    pub fn ok(&self) -> bool {
        self.code == ffi::StatusCode::kOk
    }

    pub fn err(&self) -> (ffi::StatusCode, ffi::StatusSubCode, ffi::StatusSeverity) {
        (self.code, self.sub_code, self.severity)
    }
}

impl ffi::RustIOStatus {
    pub fn new() -> Self {
        let status = ffi::RustStatus::new();
        Self {
            status,
            retryable: false,
            data_loss: false,
            scope: ffi::IOErrorScope::kIOErrorScopeFile,
        }
    }

    pub fn new_error() -> Self {
        Self {
            status: ffi::RustStatus::new_error(ffi::StatusCode::kIOError),
            retryable: false,
            data_loss: false,
            scope: ffi::IOErrorScope::kIOErrorScopeFile,
        }
    }

    pub fn ok(&self) -> bool {
        self.status.ok()
    }

    pub fn err(&self) -> Self {
        Self {
            status: self.status,
            retryable: self.retryable,
            data_loss: self.data_loss,
            scope: self.scope,
        }
    }
}

impl std::fmt::Debug for ffi::Async_reader {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:p}", self)
    }
}

#[derive(Debug)]
struct RocksDB {
    async_reader: SharedPtr<ffi::Async_reader>,
}

impl RocksDB {
    pub fn new(db: &mut rocksdb::DB) -> Self {
        let db: *mut ffi::CRocksDB = db.get_inner() as *mut _ as *mut ffi::CRocksDB;
        let async_reader = unsafe { ffi::new_async_reader(db, 2) };

        Self {
            async_reader: async_reader,
        }
    }

    fn setup_io_uring_sq_handler(&self, ropts: *mut DBReadOptions) {
        let ropts: *mut ffi::ReadOptions = ropts as *mut _ as *mut ffi::ReadOptions;
        unsafe {
            self.async_reader.setup_io_uring_sq_handler(ropts);
        }
    }

    fn get(
        &self,
        ropts: *const DBReadOptions,
        k: String,
    ) -> impl Future<Output = (String, ffi::RustStatus)> {
        let ropts: *const ffi::ReadOptions = ropts as *const _ as *const ffi::ReadOptions;

        unsafe { self.async_reader.get(ropts, k) }
    }

    fn get_result(ar: ffi::Async_result) -> (String, ffi::RustStatus) {
        let mut v: String = Default::default();
        let s = unsafe { ffi::get_async_result(ar, &mut v) };

        (v, s)
    }
}

impl Future for ffi::Async_result {
    type Output = (String, ffi::RustStatus);

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let async_reader = unsafe { (*self).m_async_reader.as_ref().unwrap() };

        if async_reader.pending_io_uring_sqe_count() == 0 {
            return Poll::Ready( unsafe { RocksDB::get_result(*self) });
        } else if async_reader.io_uring_peek_cq() {
            let n_reaped = async_reader.io_uring_reap_cq();
            assert!(n_reaped > 0);
            println!("n_reaped: {}", n_reaped);
            return Poll::Ready( unsafe { RocksDB::get_result(*self) });
        } else {
            println!("pending");
            return Poll::Pending;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn populate() {
        let mut db = rocksdb::rocksdb::DB::open_default("/tmp/rocksdb/storage").unwrap();
        for i in 1..1001 {
            if i == 3 {
                continue;
            }
            let k = format!("k{}", i);
            let v = format!("v{}", i);

            db.put(k.as_bytes(), v.as_bytes());
        }
    }

    #[test]
    fn async_get_key_test() {
        populate();

        let mut db = rocksdb::rocksdb::DB::open_default("/tmp/rocksdb/storage").unwrap();
        let ropts = unsafe { rocksdb::crocksdb_ffi::crocksdb_readoptions_create() };

        let db = RocksDB::new(&mut db);

        db.setup_io_uring_sq_handler(ropts);

        let mut futures = vec![];

        for i in 1..1001 {
            let k = format!("k{}", i);
            futures.push(db.get(ropts, k));
        }

        let f = async { futures::future::join_all(futures).await };
        let results = block_on(f);

        for r in results {
            // println!("v: {:?}", r);
        }
    }
}
