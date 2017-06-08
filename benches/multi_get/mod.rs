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

use rocksdb::{DB, Options, Writable, SeekKey};
use tempdir::TempDir;
use test::Bencher;

const START: usize = 200;
const END: usize = 208;
const KEY_CNT: usize = 100000;

#[bench]
fn bench_multi_get(b: &mut Bencher) {
    let path = TempDir::new("_rust_rocksdb_multi_get").expect("");
    let mut opts = Options::new();
    opts.create_if_missing(true);
    let db = DB::open(opts, path.path().to_str().unwrap()).unwrap();

    let all_keys: Vec<_> = (0..(KEY_CNT + 1000))
        .map(|i| format!("k_{:06}", i).into_bytes())
        .collect();
    let all_keys: Vec<_> = all_keys.iter().map(|k| k.as_slice()).collect();

    for sec in 0..(KEY_CNT / 10000) {
        let sst_keys = &all_keys[(sec * 10000)..((sec + 1) * 10000)];
        for i in 0..sst_keys.len() {
            db.put(sst_keys[i], b"v").unwrap();
        }
        db.flush(true).unwrap();
    }

    let mem_keys = &all_keys[KEY_CNT..KEY_CNT + 1000];
    for i in 0..mem_keys.len() {
        db.put(mem_keys[i], b"v").unwrap();
    }

    let keys = &all_keys[(KEY_CNT + START)..(KEY_CNT + END)];
    b.iter(|| {
        db.multi_get(keys).unwrap();
    });
}

#[bench]
fn bench_get_serially(b: &mut Bencher) {
    let path = TempDir::new("_rust_rocksdb_get_serially").expect("");
    let mut opts = Options::new();
    opts.create_if_missing(true);
    let db = DB::open(opts, path.path().to_str().unwrap()).unwrap();

    let all_keys: Vec<_> = (0..(KEY_CNT + 1000))
        .map(|i| format!("k_{:06}", i).into_bytes())
        .collect();
    let all_keys: Vec<_> = all_keys.iter().map(|k| k.as_slice()).collect();

    for sec in 0..(KEY_CNT / 10000) {
        let sst_keys = &all_keys[(sec * 10000)..((sec + 1) * 10000)];
        for i in 0..sst_keys.len() {
            db.put(sst_keys[i], b"v").unwrap();
        }
        db.flush(true).unwrap();
    }

    let mem_keys = &all_keys[KEY_CNT..KEY_CNT + 1000];
    for i in 0..mem_keys.len() {
        db.put(mem_keys[i], b"v").unwrap();
    }

    let keys = &all_keys[(KEY_CNT + START)..(KEY_CNT + END)];
    b.iter(|| {
        for k in keys {
            db.get(k).unwrap();
        }
    });
}

#[bench]
fn bench_iterator(b: &mut Bencher) {
    let path = TempDir::new("_rust_rocksdb_iterator").expect("");
    let mut opts = Options::new();
    opts.create_if_missing(true);
    let db = DB::open(opts, path.path().to_str().unwrap()).unwrap();

    let all_keys: Vec<_> = (0..(KEY_CNT + 1000))
        .map(|i| format!("k_{:06}", i).into_bytes())
        .collect();
    let all_keys: Vec<_> = all_keys.iter().map(|k| k.as_slice()).collect();

    for sec in 0..(KEY_CNT / 10000) {
        let sst_keys = &all_keys[(sec * 10000)..((sec + 1) * 10000)];
        for i in 0..sst_keys.len() {
            db.put(sst_keys[i], b"v").unwrap();
        }
        db.flush(true).unwrap();
    }

    let mem_keys = &all_keys[KEY_CNT..KEY_CNT + 1000];
    for i in 0..mem_keys.len() {
        db.put(mem_keys[i], b"v").unwrap();
    }

    b.iter(|| {
        let mut iter = db.iter();
        assert!(iter.seek(SeekKey::Key(all_keys[KEY_CNT + START])));
        iter.value();
        for _ in (START + 1)..END {
            assert!(iter.next());
            iter.value();
        }
    });
}
