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

use rocksdb::{Writable, DB, Options};
use tempdir::TempDir;

#[test]
fn test_get_all_key_versions() {
    let mut opts = Options::new();
    opts.create_if_missing(true);
    let path = TempDir::new("_rust_rocksdb_get_all_key_version_test").expect("");
    let db = DB::open(opts, path.path().to_str().unwrap()).unwrap();

    let samples = vec![(b"key1".to_vec(), b"value1".to_vec()),
                       (b"key2".to_vec(), b"value2".to_vec()),
                       (b"key3".to_vec(), b"value3".to_vec()),
                       (b"key4".to_vec(), b"value4".to_vec())];

    // Put 4 keys.
    for &(ref k, ref v) in &samples {
        db.put(k, v).unwrap();
        assert_eq!(v.as_slice(), &*db.get(k).unwrap().unwrap());
    }
    db.flush(true).unwrap();
    let key_versions = db.get_all_key_versions(b"key2", b"key4").unwrap();
    assert_eq!(key_versions[1].user_key(), "key3");
    assert_eq!(key_versions[1].value(), "value3");
    assert_eq!(key_versions[1].sequence(), 3);
}
