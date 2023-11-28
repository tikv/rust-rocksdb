// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

use rocksdb::CompactionFilterDecision;
use rocksdb::CompactionFilterValueType;
use rocksdb::TitanDBOptions;
use rocksdb::{ColumnFamilyOptions, CompactionFilter, DBOptions, Writable, DB};

use super::tempdir_with_prefix;

struct Filter {
    drop_called: Arc<AtomicBool>,
    filtered_kvs: Arc<RwLock<Vec<(Vec<u8>, Vec<u8>)>>>,
}

impl CompactionFilter for Filter {
    fn unsafe_filter(
        &mut self,
        _: usize,
        key: &[u8],
        _: u64,
        value: &[u8],
        _: CompactionFilterValueType,
    ) -> CompactionFilterDecision {
        self.filtered_kvs
            .write()
            .unwrap()
            .push((key.to_vec(), value.to_vec()));
        CompactionFilterDecision::Remove
    }
}

impl Drop for Filter {
    fn drop(&mut self) {
        self.drop_called.store(true, Ordering::Relaxed);
    }
}

#[test]
fn test_compaction_filter() {
    test_compaction_filter_impl(false);
}

#[test]
fn test_compaction_filter_with_titan() {
    test_compaction_filter_impl(true);
}

fn test_compaction_filter_impl(titan: bool) {
    let path = tempdir_with_prefix("_rust_rocksdb_writebacktest");
    let drop_called = Arc::new(AtomicBool::new(false));
    let filtered_kvs = Arc::new(RwLock::new(vec![]));

    // reregister with ignore_snapshots set to true
    let mut cf_opts = ColumnFamilyOptions::new();
    cf_opts
        .set_compaction_filter::<&str, Filter>(
            "test",
            Filter {
                drop_called: drop_called.clone(),
                filtered_kvs: filtered_kvs.clone(),
            },
        )
        .unwrap();

    let mut opts = DBOptions::new();
    opts.create_if_missing(true);
    if titan {
        let mut tdb_opts = TitanDBOptions::new();
        tdb_opts.set_min_blob_size(0);
        opts.set_titandb_options(&tdb_opts);
    }
    {
        let db = DB::open_cf(
            opts,
            path.path().to_str().unwrap(),
            vec![("default", cf_opts)],
        )
        .unwrap();

        let samples = vec![
            (b"key1".to_vec(), b"value1".to_vec()),
            (b"key2".to_vec(), b"value2".to_vec()),
        ];

        for &(ref k, ref v) in &samples {
            db.put(k, v).unwrap();
            assert_eq!(v.as_slice(), &*db.get(k).unwrap().unwrap());
        }

        let _snap = db.snapshot();
        // Because ignore_snapshots is true, so all the keys will be compacted.
        db.compact_range(Some(b"key1"), Some(b"key3"));
        for &(ref k, _) in &samples {
            assert!(db.get(k).unwrap().is_none());
        }
        assert_eq!(*filtered_kvs.read().unwrap(), samples);
    }
    assert!(drop_called.load(Ordering::Relaxed));
}
