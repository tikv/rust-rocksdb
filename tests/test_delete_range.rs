// Copyright 2014 Tyler Neely
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

use rocksdb::*;
use std::fs;
use tempdir::TempDir;

fn gen_sst(opt: Options, cf: Option<&CFHandle>, path: &str) {
    let _ = fs::remove_file(path);
    let env_opt = EnvOptions::new();
    let mut writer = if cf.is_some() {
        SstFileWriter::new_cf(env_opt, opt, cf.unwrap())
    } else {
        SstFileWriter::new(env_opt, opt)
    };
    writer.open(path).unwrap();
    writer.add(b"key1", b"value1").unwrap();
    writer.add(b"key2", b"value2").unwrap();
    writer.add(b"key3", b"value3").unwrap();
    writer.add(b"key4", b"value4").unwrap();
    writer.finish().unwrap();
}

#[test]
fn test_delete_range() {
    // Test `DB::delete_range()`
    let path = TempDir::new("_rust_rocksdb_test_delete_range").expect("");
    let db = DB::open_default(path.path().to_str().unwrap()).unwrap();

    // Prepare some data.
    let prepare_data = || {
        db.put(b"a", b"v1").unwrap();
        let a = db.get(b"a");
        assert_eq!(a.unwrap().unwrap(), b"v1");
        db.put(b"b", b"v2").unwrap();
        let b = db.get(b"b");
        assert_eq!(b.unwrap().unwrap(), b"v2");
        db.put(b"c", b"v3").unwrap();
        let c = db.get(b"c");
        assert_eq!(c.unwrap().unwrap(), b"v3");
    };
    prepare_data();

    // Ensure delete range interface works to delete the specified range `[b"a", b"c")`.
    db.delete_range(b"a", b"c").unwrap();

    let check_data = || {
        assert!(db.get(b"a").unwrap().is_none());
        assert!(db.get(b"b").unwrap().is_none());
        let c = db.get(b"c");
        assert_eq!(c.unwrap().unwrap(), b"v3");
    };
    check_data();

    // Test `DB::delete_range_cf()`
    prepare_data();
    let cf_handle = db.cf_handle("default").unwrap();
    db.delete_range_cf(cf_handle, b"a", b"c").unwrap();
    check_data();

    // Test `WriteBatch::delete_range()`
    prepare_data();
    let batch = WriteBatch::new();
    batch.delete_range(b"a", b"c").unwrap();
    assert!(db.write(batch).is_ok());
    check_data();

    // Test `WriteBatch::delete_range_cf()`
    prepare_data();
    let batch = WriteBatch::new();
    batch.delete_range_cf(cf_handle, b"a", b"c").unwrap();
    assert!(db.write(batch).is_ok());
    check_data();

    let samples_a = vec![(b"key1".to_vec(), b"value1".to_vec()),
                         (b"key2".to_vec(), b"value2".to_vec()),
                         (b"key3".to_vec(), b"value3".to_vec()),
                         (b"key4".to_vec(), b"value4".to_vec())];
    for &(ref k, ref v) in &samples_a {
        db.put(k, v).unwrap();
        assert_eq!(v.as_slice(), &*db.get(k).unwrap().unwrap());
    }
    db.flush(true).unwrap();

    let samples_b = vec![(b"key3".to_vec(), b"value5".to_vec()),
                         (b"key6".to_vec(), b"value6".to_vec()),
                         (b"key7".to_vec(), b"value7".to_vec()),
                         (b"key8".to_vec(), b"value8".to_vec())];
    for &(ref k, ref v) in &samples_b {
        db.put(k, v).unwrap();
        assert_eq!(v.as_slice(), &*db.get(k).unwrap().unwrap());
    }
    db.flush(true).unwrap();

    db.compact_range(None, None);
    assert_eq!(db.get(b"key3").unwrap().unwrap(), b"value5");
    db.delete_range(b"key1", b"key1").unwrap();
    assert_eq!(db.get(b"key1").unwrap().unwrap(), b"value1");
    db.delete_range(b"key2", b"key7").unwrap();
    assert!(db.get(b"key2").unwrap().is_none());
    assert!(db.get(b"key3").unwrap().is_none());
    assert!(db.get(b"key4").unwrap().is_none());
    assert!(db.get(b"key5").unwrap().is_none());
    assert_eq!(db.get(b"key7").unwrap().unwrap(), b"value7");
    assert_eq!(db.get(b"key8").unwrap().unwrap(), b"value8");

    db.delete_range(b"key1", b"key8").unwrap();
    assert!(db.get(b"key1").unwrap().is_none());
    assert!(db.get(b"key7").unwrap().is_none());
    assert_eq!(db.get(b"key8").unwrap().unwrap(), b"value8");
}

#[test]
fn test_delete_range_ingest_file() {
    let path = TempDir::new("_rust_rocksdb_delete_range_ingest_file").expect("");
    let path_str = path.path().to_str().unwrap();
    let mut opts = Options::new();
    opts.create_if_missing(true);
    let mut db = DB::open(opts, path_str).unwrap();
    let gen_path = TempDir::new("_rust_rocksdb_ingest_sst_gen_new_cf").expect("");
    let test_sstfile = gen_path.path().join("test_sst_file_new_cf");
    let test_sstfile_str = test_sstfile.to_str().unwrap();
    let cf_opts = Options::new();
    db.create_cf("cf1", &cf_opts).unwrap();
    let handle = db.cf_handle("cf1").unwrap();
    let ingest_opt = IngestExternalFileOptions::new();

    gen_sst(Options::new(), None, test_sstfile_str);
    db.ingest_external_file_cf(handle, &ingest_opt, &[test_sstfile_str])
        .unwrap();
    assert!(test_sstfile.exists());
    assert_eq!(db.get_cf(handle, b"key1").unwrap().unwrap(), b"value1");
    assert_eq!(db.get_cf(handle, b"key2").unwrap().unwrap(), b"value2");
    assert_eq!(db.get_cf(handle, b"key3").unwrap().unwrap(), b"value3");
    assert_eq!(db.get_cf(handle, b"key4").unwrap().unwrap(), b"value4");

    let snap = db.snapshot();

    db.delete_range_cf(handle, b"key1", b"key3").unwrap();

    assert!(db.get_cf(handle, b"key1").unwrap().is_none());
    assert_eq!(db.get_cf(handle, b"key3").unwrap().unwrap(), b"value3");
    assert_eq!(db.get_cf(handle, b"key4").unwrap().unwrap(), b"value4");

    assert_eq!(snap.get_cf(handle, b"key1").unwrap().unwrap(), b"value1");
    assert_eq!(snap.get_cf(handle, b"key4").unwrap().unwrap(), b"value4");
}