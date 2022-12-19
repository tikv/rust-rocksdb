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

use rocksdb::*;
use rocksdb::{DBStatisticsHistogramType as HistogramType, DBStatisticsTickerType as TickerType};

use super::tempdir_with_prefix;

#[test]
fn test_db_statistics() {
    let path = tempdir_with_prefix("_rust_rocksdb_statistics");
    let mut opts = DBOptions::new();
    opts.create_if_missing(true);
    let statistics = Statistics::new();
    opts.set_statistics(&statistics);
    let db = DB::open(opts, path.path().to_str().unwrap()).unwrap();
    let wopts = WriteOptions::new();

    db.put_opt(b"k0", b"a", &wopts).unwrap();
    db.put_opt(b"k1", b"b", &wopts).unwrap();
    db.put_opt(b"k2", b"c", &wopts).unwrap();
    db.flush(true /* sync */).unwrap(); // flush memtable to sst file.
    assert_eq!(db.get(b"k0").unwrap().unwrap(), b"a");
    assert_eq!(db.get(b"k1").unwrap().unwrap(), b"b");
    assert_eq!(db.get(b"k2").unwrap().unwrap(), b"c");

    assert!(statistics.get_ticker_count(TickerType::BlockCacheHit) > 0);
    assert!(statistics.get_and_reset_ticker_count(TickerType::BlockCacheHit) > 0);
    assert_eq!(statistics.get_ticker_count(TickerType::BlockCacheHit), 0);
    assert!(statistics
        .get_histogram_string(HistogramType::DbGet)
        .is_some());
    assert!(statistics.get_histogram(HistogramType::DbGet).is_some());

    let get_micros = statistics.get_histogram(HistogramType::DbGet).unwrap();
    assert!(get_micros.max > 0.0);
    statistics.reset();
    let get_micros = statistics.get_histogram(HistogramType::DbGet).unwrap();
    assert_eq!(get_micros.max, 0.0);
}

#[test]
fn test_disable_db_statistics() {
    let path = tempdir_with_prefix("_rust_rocksdb_statistics");
    let mut opts = DBOptions::new();
    opts.create_if_missing(true);
    let statistics = Statistics::new_empty();
    opts.set_statistics(&statistics);
    let db = DB::open(opts, path.path().to_str().unwrap()).unwrap();
    let wopts = WriteOptions::new();

    db.put_opt(b"k0", b"a", &wopts).unwrap();
    db.put_opt(b"k1", b"b", &wopts).unwrap();
    db.put_opt(b"k2", b"c", &wopts).unwrap();
    db.flush(true /* sync */).unwrap(); // flush memtable to sst file.
    assert_eq!(db.get(b"k0").unwrap().unwrap(), b"a");
    assert_eq!(db.get(b"k1").unwrap().unwrap(), b"b");
    assert_eq!(db.get(b"k2").unwrap().unwrap(), b"c");

    assert_eq!(statistics.get_ticker_count(TickerType::BlockCacheHit), 0);
    assert_eq!(
        statistics.get_and_reset_ticker_count(TickerType::BlockCacheHit),
        0
    );
    assert!(statistics
        .get_histogram_string(HistogramType::DbGet)
        .is_none());
    assert!(statistics.get_histogram(HistogramType::DbGet).is_none());
}

#[test]
fn test_shared_db_statistics() {
    let path1 = tempdir_with_prefix("_rust_rocksdb_statistics");
    let path2 = tempdir_with_prefix("_rust_rocksdb_statistics");
    let mut opts = DBOptions::new();
    opts.create_if_missing(true);
    let statistics = Statistics::new();
    opts.set_statistics(&statistics);

    let _db_inactive = DB::open(opts.clone(), path1.path().to_str().unwrap()).unwrap();
    let db = DB::open(opts, path2.path().to_str().unwrap()).unwrap();
    let wopts = WriteOptions::new();

    db.put_opt(b"k0", b"a", &wopts).unwrap();
    db.put_opt(b"k1", b"b", &wopts).unwrap();
    db.put_opt(b"k2", b"c", &wopts).unwrap();
    db.flush(true /* sync */).unwrap(); // flush memtable to sst file.

    assert_eq!(db.get(b"k0").unwrap().unwrap(), b"a");
    assert_eq!(db.get(b"k1").unwrap().unwrap(), b"b");
    assert_eq!(db.get(b"k2").unwrap().unwrap(), b"c");

    assert!(statistics.get_ticker_count(TickerType::BlockCacheHit) > 0);
    assert!(statistics.get_and_reset_ticker_count(TickerType::BlockCacheHit) > 0);
    assert_eq!(statistics.get_ticker_count(TickerType::BlockCacheHit), 0);
    assert!(statistics
        .get_histogram_string(HistogramType::DbGet)
        .is_some());
    assert!(statistics.get_histogram(HistogramType::DbGet).is_some());

    let get_micros = statistics.get_histogram(HistogramType::DbGet).unwrap();
    assert!(get_micros.max > 0.0);
    statistics.reset();
    let get_micros = statistics.get_histogram(HistogramType::DbGet).unwrap();
    assert_eq!(get_micros.max, 0.0);
}
