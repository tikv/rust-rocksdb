use rocksdb::{ColumnFamilyOptions, DBOptions, Writable, DB};
use tempdir::TempDir;

#[test]
pub fn test_ttl() {
    let path = TempDir::new("_rust_rocksdb_ttl_test").expect("");
    let path_str = path.path().to_str().unwrap();

    // should be able to open db with ttl
    {
        let mut opts = DBOptions::new();
        let cf_opts = ColumnFamilyOptions::new();
        let ttl = 10;
        opts.create_if_missing(true);

        let mut db = match DB::open_cf_with_ttl(
            opts,
            path.path().to_str().unwrap(),
            vec![("default", cf_opts)],
            &[ttl],
        ) {
            Ok(db) => {
                println!("successfully opened db with ttl");
                db
            }
            Err(e) => panic!("failed to open db with ttl: {}", e),
        };

        match db.create_cf("cf1") {
            Ok(_) => println!("cf1 created successfully"),
            Err(e) => {
                panic!("could not create column family: {}", e);
            }
        }
        assert_eq!(db.cf_names(), vec!["cf1", "default"]);
        drop(db);
    }

    // should be able to write, read over a cf with ttl
    {
        let cf_opts = ColumnFamilyOptions::new();
        let ttl = 0;
        let db = match DB::open_cf_with_ttl(
            DBOptions::new(),
            path_str,
            vec![("cf1", cf_opts)],
            &[ttl],
        ) {
            Ok(db) => {
                println!("successfully opened cf with ttl");
                db
            }
            Err(e) => panic!("failed to open cf with ttl: {}", e),
        };
        let cf1 = db.cf_handle("cf1").unwrap();
        assert!(db.put_cf(cf1, b"k1", b"v1").is_ok());
        assert!(db.get_cf(cf1, b"k1").unwrap().unwrap().to_utf8().unwrap() == "v1");
        let p = db.put_cf(cf1, b"k1", b"a");
        assert!(p.is_ok());
    }
}
