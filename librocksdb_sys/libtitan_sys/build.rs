extern crate cc;
extern crate cmake;

use std::env;

fn main() {
    for e in env::vars() {
        println!("{:?}", e);
    }
    let cur_dir = std::env::current_dir().unwrap();
    let zlib_dir = env::var("DEP_Z_ROOT").unwrap();
    let dst = cmake::Config::new("titan")
        .define("ROCKSDB_DIR", cur_dir.join("..").join("rocksdb"))
        .define("WITH_TITAN_TESTS", "OFF")
        .define("WITH_TITAN_TOOLS", "OFF")
        .cxxflag("-DZLIB")
        .cxxflag(format!("-I{}/include", zlib_dir))
        .register_dep("BZIP2")
        .define("WITH_BZ2", "ON")
        .register_dep("LZ4")
        .define("WITH_LZ4", "ON")
        .register_dep("ZSTD")
        .define("WITH_ZSTD", "ON")
        .register_dep("SNAPPY")
        .define("WITH_SNAPPY", "ON")
        .build_target("titan")
        .very_verbose(true)
        .build();
    println!("cargo:rustc-link-search=native={}/build", dst.display());
    println!("cargo:rustc-link-lib=static=titan");
}
