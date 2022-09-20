// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use super::tempdir_with_prefix;
use rocksdb::{DBOptions, Writable, DB};
use std::path::PathBuf;

#[test]
fn test_checkpoint_basic() {
    let path = tempdir_with_prefix("_rust_rocksdb_test_checkpoint_case_1");
    let path_str = path.path().to_str().unwrap();
    let mut opts = DBOptions::new();
    opts.create_if_missing(true);
    let db = DB::open(opts.clone(), path_str).unwrap();
    db.put(b"k1", b"v1").unwrap();
    db.put(b"k2", b"v2").unwrap();
    db.put(b"k3", b"v3").unwrap();
    let mut checkpoint = db.checkpoint().unwrap();
    let checkpoint_path = PathBuf::from(path_str).join("snap_1");
    checkpoint.create_at(checkpoint_path.as_path(), 0).unwrap();
    let snap = DB::open(opts, checkpoint_path.to_str().unwrap()).unwrap();
    assert_eq!(snap.get(b"k1").unwrap().unwrap(), b"v1");
    assert_eq!(snap.get(b"k2").unwrap().unwrap(), b"v2");
    assert_eq!(snap.get(b"k3").unwrap().unwrap(), b"v3");
    db.put(b"k4", b"v4").unwrap();
    assert_eq!(snap.get(b"k4").unwrap().is_none(), true);
}
