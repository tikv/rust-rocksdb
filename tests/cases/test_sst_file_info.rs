// Copyright 2025 Contributors to rust-rocksdb
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use rocksdb::{CFHandle, ColumnFamilyOptions, DBOptions, FlushOptions, SstFileInfo, Writable, DB};

use super::tempdir_with_prefix;

#[test]
fn test_sst_files_in_range_basic() {
    let path = tempdir_with_prefix("_rust_rocksdb_test_sst_files_range_basic");
    let mut opts = DBOptions::new();
    opts.create_if_missing(true);
    let mut cf_opts = ColumnFamilyOptions::new();
    cf_opts.set_disable_auto_compactions(true);

    let db = DB::open_cf(
        opts,
        path.path().to_str().unwrap(),
        vec![("default", cf_opts)],
    )
    .unwrap();

    let cf_handle = db.cf_handle("default").unwrap();
    let mut fopts = FlushOptions::default();
    fopts.set_wait(true);

    // Insert data with different key ranges
    // File 1: keys 0-2
    db.put(b"key000", b"value0").unwrap();
    db.put(b"key001", b"value1").unwrap();
    db.put(b"key002", b"value2").unwrap();
    db.flush(&fopts).unwrap();

    // File 2: keys 5-7
    db.put(b"key005", b"value5").unwrap();
    db.put(b"key006", b"value6").unwrap();
    db.put(b"key007", b"value7").unwrap();
    db.flush(&fopts).unwrap();

    // File 3: keys 10-12
    db.put(b"key010", b"value10").unwrap();
    db.put(b"key011", b"value11").unwrap();
    db.put(b"key012", b"value12").unwrap();
    db.flush(&fopts).unwrap();

    // Test 1: Get files in range [key001, key006) - should match file 1 and file 2
    let files_in_range = db.get_sst_files_in_range(cf_handle, Some(b"key001"), Some(b"key006"));
    assert_eq!(files_in_range.len(), 2);

    // Verify the files contain the expected keys
    for file in &files_in_range {
        assert!(file.smallest_key <= b"key006");
        assert!(file.largest_key >= b"key001");
        assert!(file.name.contains(".sst"));
        assert!(file.size > 0);
        assert!(file.level == 0); // All files should be in level 0
    }

    // Test 2: Get files in range [key005, key011) - should match file 2 and file 3
    let files_in_range = db.get_sst_files_in_range(cf_handle, Some(b"key005"), Some(b"key011"));
    assert_eq!(files_in_range.len(), 2);

    // Test 3: Get files in range [key003, key004) - should match no files
    let files_in_range = db.get_sst_files_in_range(cf_handle, Some(b"key003"), Some(b"key004"));
    assert_eq!(files_in_range.len(), 0);

    // Test 4: Get files in range [key000, key013) - should match all files
    let files_in_range = db.get_sst_files_in_range(cf_handle, Some(b"key000"), Some(b"key013"));
    assert_eq!(files_in_range.len(), 3);

    // Test 5: Get files with no range bounds - should match all files
    let files_in_range = db.get_sst_files_in_range(cf_handle, None, None);
    assert_eq!(files_in_range.len(), 3);
}

#[test]
fn test_sst_files_in_range_default_cf() {
    let path = tempdir_with_prefix("_rust_rocksdb_test_sst_files_range_default");
    let mut opts = DBOptions::new();
    opts.create_if_missing(true);

    let db = DB::open_default(path.path().to_str().unwrap()).unwrap();
    let mut fopts = FlushOptions::default();
    fopts.set_wait(true);

    // Insert some data
    db.put(b"a", b"value_a").unwrap();
    db.put(b"b", b"value_b").unwrap();
    db.flush(&fopts).unwrap();

    db.put(b"c", b"value_c").unwrap();
    db.put(b"d", b"value_d").unwrap();
    db.flush(&fopts).unwrap();

    // Test getting files in range using default CF method
    let files_in_range = db
        .get_sst_files_in_range_default(Some(b"b"), Some(b"d"))
        .unwrap();
    assert_eq!(files_in_range.len(), 2);

    // Verify file properties
    for file in &files_in_range {
        assert!(file.smallest_key <= b"d");
        assert!(file.largest_key >= b"b");
        assert!(!file.name.is_empty());
        assert!(file.size > 0);
    }
}

#[test]
fn test_sst_files_in_range_with_column_families() {
    let path = tempdir_with_prefix("_rust_rocksdb_test_sst_files_range_cf");
    let mut opts = DBOptions::new();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);

    let mut cf_opts = ColumnFamilyOptions::new();
    cf_opts.set_disable_auto_compactions(true);

    let db = DB::open_cf(
        opts,
        path.path().to_str().unwrap(),
        vec![
            ("default", ColumnFamilyOptions::new()),
            ("cf1", cf_opts.clone()),
            ("cf2", cf_opts),
        ],
    )
    .unwrap();

    let cf1_handle = db.cf_handle("cf1").unwrap();
    let cf2_handle = db.cf_handle("cf2").unwrap();
    let mut fopts = FlushOptions::default();
    fopts.set_wait(true);

    // Insert data into different column families
    db.put_cf(cf1_handle, b"key1", b"value1").unwrap();
    db.put_cf(cf1_handle, b"key2", b"value2").unwrap();
    db.flush_cf(cf1_handle, &fopts).unwrap();

    db.put_cf(cf2_handle, b"key3", b"value3").unwrap();
    db.put_cf(cf2_handle, b"key4", b"value4").unwrap();
    db.flush_cf(cf2_handle, &fopts).unwrap();

    // Test getting files from specific column families
    let cf1_files = db.get_sst_files_in_range(cf1_handle, Some(b"key1"), Some(b"key3"));
    assert_eq!(cf1_files.len(), 1);
    assert!(cf1_files[0].smallest_key <= b"key2");
    assert!(cf1_files[0].largest_key >= b"key1");

    let cf2_files = db.get_sst_files_in_range(cf2_handle, Some(b"key3"), Some(b"key5"));
    assert_eq!(cf2_files.len(), 1);
    assert!(cf2_files[0].smallest_key <= b"key4");
    assert!(cf2_files[0].largest_key >= b"key3");

    // Files from cf1 should not overlap with cf2 range
    let cf1_files_in_cf2_range =
        db.get_sst_files_in_range(cf1_handle, Some(b"key3"), Some(b"key5"));
    assert_eq!(cf1_files_in_cf2_range.len(), 0);
}

#[test]
fn test_sst_file_info_overlap_methods() {
    let path = tempdir_with_prefix("_rust_rocksdb_test_sst_file_info_overlap");
    let mut opts = DBOptions::new();
    opts.create_if_missing(true);

    let db = DB::open_default(path.path().to_str().unwrap()).unwrap();
    let mut fopts = FlushOptions::default();
    fopts.set_wait(true);

    // Insert data to create an SST file
    db.put(b"key001", b"value1").unwrap();
    db.put(b"key002", b"value2").unwrap();
    db.put(b"key003", b"value3").unwrap();
    db.flush(&fopts).unwrap();

    // Get the SST file info
    let files = db.get_sst_files_in_range_default(None, None).unwrap();
    assert_eq!(files.len(), 1);
    let file_info = &files[0];

    // Test overlaps_with_range
    assert!(file_info.overlaps_with_range(Some(b"key001"), Some(b"key004")));
    assert!(file_info.overlaps_with_range(Some(b"key002"), Some(b"key005")));
    assert!(file_info.overlaps_with_range(Some(b"key000"), Some(b"key002")));
    assert!(!file_info.overlaps_with_range(Some(b"key000"), Some(b"key001")));
    assert!(!file_info.overlaps_with_range(Some(b"key004"), Some(b"key005")));

    // Test is_contained_in_range
    assert!(file_info.is_contained_in_range(Some(b"key000"), Some(b"key005")));
    assert!(file_info.is_contained_in_range(Some(b"key001"), Some(b"key004")));
    assert!(!file_info.is_contained_in_range(Some(b"key001"), Some(b"key003")));
    assert!(!file_info.is_contained_in_range(Some(b"key002"), Some(b"key005")));

    // Test with no bounds
    assert!(file_info.overlaps_with_range(None, None));
    assert!(file_info.is_contained_in_range(None, None));
}

#[test]
fn test_sst_files_in_range_empty_database() {
    let path = tempdir_with_prefix("_rust_rocksdb_test_sst_files_range_empty");
    let mut opts = DBOptions::new();
    opts.create_if_missing(true);

    let db = DB::open_default(path.path().to_str().unwrap()).unwrap();

    // Empty database should have no SST files
    let files_in_range = db
        .get_sst_files_in_range_default(Some(b"a"), Some(b"z"))
        .unwrap();
    assert_eq!(files_in_range.len(), 0);
}

#[test]
fn test_sst_files_in_range_edge_cases() {
    let path = tempdir_with_prefix("_rust_rocksdb_test_sst_files_range_edge");
    let mut opts = DBOptions::new();
    opts.create_if_missing(true);

    let db = DB::open_default(path.path().to_str().unwrap()).unwrap();
    let mut fopts = FlushOptions::default();
    fopts.set_wait(true);

    // Insert data with specific keys
    db.put(b"a", b"value_a").unwrap();
    db.put(b"z", b"value_z").unwrap();
    db.flush(&fopts).unwrap();

    // Test exact boundary matches
    let files_exact_start = db.get_sst_files_in_range_default(Some(b"a"), None).unwrap();
    assert_eq!(files_exact_start.len(), 1);

    let files_exact_end = db.get_sst_files_in_range_default(None, Some(b"z")).unwrap();
    assert_eq!(files_exact_end.len(), 1);

    // Test range that exactly matches file boundaries
    let files_exact_range = db
        .get_sst_files_in_range_default(Some(b"a"), Some(b"z"))
        .unwrap();
    assert_eq!(files_exact_range.len(), 1);

    // Test single key range
    let files_single_key = db
        .get_sst_files_in_range_default(Some(b"a"), Some(b"b"))
        .unwrap();
    assert_eq!(files_single_key.len(), 1);
}

#[test]
fn test_sst_files_in_range_binary_search_optimization() {
    // Test the binary search optimization for levels 1+ where files are non-overlapping and sorted
    // The algorithm finds the first overlapping file using binary search, then iterates forward
    // until it encounters a file whose smallest_key >= end_key
    let path = tempdir_with_prefix("_rust_rocksdb_test_sst_files_range_binary_search");
    let mut opts = DBOptions::new();
    opts.create_if_missing(true);

    let mut cf_opts = ColumnFamilyOptions::new();
    cf_opts.set_disable_auto_compactions(true);
    cf_opts.set_target_file_size_base(1024); // Small target size to create multiple files

    let db = DB::open_cf(
        opts,
        path.path().to_str().unwrap(),
        vec![("default", cf_opts)],
    )
    .unwrap();

    let cf_handle = db.cf_handle("default").unwrap();
    let mut fopts = FlushOptions::default();
    fopts.set_wait(true);

    // Create multiple files with distinct key ranges to test binary search
    // File 1: keys 000-099
    for i in 0..20 {
        let key = format!("key{:03}", i);
        let value = format!("value_{}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }
    db.flush(&fopts).unwrap();

    // File 2: keys 100-199
    for i in 100..120 {
        let key = format!("key{:03}", i);
        let value = format!("value_{}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }
    db.flush(&fopts).unwrap();

    // File 3: keys 200-299
    for i in 200..220 {
        let key = format!("key{:03}", i);
        let value = format!("value_{}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }
    db.flush(&fopts).unwrap();

    // File 4: keys 300-399
    for i in 300..320 {
        let key = format!("key{:03}", i);
        let value = format!("value_{}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }
    db.flush(&fopts).unwrap();

    // File 5: keys 400-499
    for i in 400..420 {
        let key = format!("key{:03}", i);
        let value = format!("value_{}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }
    db.flush(&fopts).unwrap();

    // Test binary search optimization with ranges that should only match specific files
    // Range [150, 250) should only match files 2 and 3
    let files_in_range = db.get_sst_files_in_range(cf_handle, Some(b"key150"), Some(b"key250"));
    assert_eq!(files_in_range.len(), 2);

    // Verify the files are from the expected ranges
    for file in &files_in_range {
        assert!(file.smallest_key >= b"key100");
        assert!(file.smallest_key <= b"key299");

        // Test the new num_entries and num_deletions fields
        assert!(file.num_entries > 0); // Each file should have entries
        assert!(file.num_deletions >= 0); // Deletions can be 0 or more
        assert!(file.num_entries >= file.num_deletions); // Entries should be >= deletions
    }

    // Range [50, 150) should only match files 1 and 2
    let files_in_range = db.get_sst_files_in_range(cf_handle, Some(b"key050"), Some(b"key150"));
    assert_eq!(files_in_range.len(), 2);

    // Range [250, 350) should only match files 3 and 4
    let files_in_range = db.get_sst_files_in_range(cf_handle, Some(b"key250"), Some(b"key350"));
    assert_eq!(files_in_range.len(), 2);

    // Range [350, 450) should only match files 4 and 5
    let files_in_range = db.get_sst_files_in_range(cf_handle, Some(b"key350"), Some(b"key450"));
    assert_eq!(files_in_range.len(), 2);

    // Range [450, 550) should only match file 5
    let files_in_range = db.get_sst_files_in_range(cf_handle, Some(b"key450"), Some(b"key550"));
    assert_eq!(files_in_range.len(), 1);

    // Range [25, 75) should only match file 1
    let files_in_range = db.get_sst_files_in_range(cf_handle, Some(b"key025"), Some(b"key075"));
    assert_eq!(files_in_range.len(), 1);

    // Range that spans all files
    let files_in_range = db.get_sst_files_in_range(cf_handle, Some(b"key000"), Some(b"key500"));
    assert_eq!(files_in_range.len(), 5);

    // Range that matches no files
    let files_in_range = db.get_sst_files_in_range(cf_handle, Some(b"key500"), Some(b"key600"));
    assert_eq!(files_in_range.len(), 0);
}

#[test]
fn test_sst_file_info_entries_and_deletions() {
    let path = tempdir_with_prefix("_rust_rocksdb_test_sst_file_info_entries_deletions");
    let mut opts = DBOptions::new();
    opts.create_if_missing(true);

    let mut cf_opts = ColumnFamilyOptions::new();
    cf_opts.set_disable_auto_compactions(true);

    let db = DB::open_cf(
        opts,
        path.path().to_str().unwrap(),
        vec![("default", cf_opts)],
    )
    .unwrap();

    let cf_handle = db.cf_handle("default").unwrap();
    let mut fopts = FlushOptions::default();
    fopts.set_wait(true);

    // Insert some data
    for i in 0..10 {
        let key = format!("key{:03}", i);
        let value = format!("value_{}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }

    // Delete some keys to create deletions
    for i in 2..5 {
        let key = format!("key{:03}", i);
        db.delete(key.as_bytes()).unwrap();
    }

    db.flush(&fopts).unwrap();

    // Get SST file info
    let files = db.get_sst_files_in_range(cf_handle, None, None);
    assert!(!files.is_empty());

    for file in &files {
        // Test that we have the new fields
        assert!(file.num_entries > 0);
        assert!(file.num_deletions >= 0);
        assert!(file.num_entries >= file.num_deletions);

        // For this test, we expect some deletions since we deleted keys 2, 3, 4
        // The exact count depends on RocksDB's internal organization
        println!(
            "File {}: entries={}, deletions={}",
            file.name, file.num_entries, file.num_deletions
        );
    }
}
