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

use rocksdb::{Writable, DB, TablePropertiesCollector, TablePropertiesCollectorFactory, Options};
use rocksdb::crocksdb_ffi::EntryType;
use std::collections::HashMap;
use tempdir::TempDir;

struct CollectorFactory {
    _cnt: u64,
}

struct Collector {
    cnt: u32,
}

impl TablePropertiesCollector for Collector {
    fn add_userkey(&mut self, key: &[u8], _: &[u8], _: EntryType) {
        if key[0] == b'A' {
            self.cnt = self.cnt + 1;
        }
    }
    fn finish(&mut self) -> HashMap<String, String> {
        let mut user_properties: HashMap<String, String> = HashMap::new();
        user_properties.insert(String::from("The number of keys started with A"),
                               format!("{}", self.cnt));
        user_properties
    }
}

impl TablePropertiesCollectorFactory for CollectorFactory {
    fn create_table_properties_collector(&mut self, _: u32) -> Box<TablePropertiesCollector> {
        Box::new(Collector { cnt: 0 })
    }
}

#[test]
fn test_table_properties_collector_factory() {
    use std::mem;

    let path = TempDir::new("_rust_rocksdb_collectortest").expect("");
    let mut opts = Options::new();
    opts.add_table_properties_collector_factory("test", Box::new(CollectorFactory { _cnt: 0 }))
        .unwrap();
    opts.create_if_missing(true);
    let db = DB::open(opts, path.path().to_str().unwrap()).unwrap();
    let samples = vec![(b"key1".to_vec(), b"value1".to_vec()),
                       (b"key2".to_vec(), b"value2".to_vec())];
    for &(ref k, ref v) in &samples {
        db.put(k, v).unwrap();
        assert_eq!(v.as_slice(), &*db.get(k).unwrap().unwrap());
    }
    db.flush(true).unwrap();
    drop(db);
    println!("path is {:?}", path);
    mem::forget(path);
}
