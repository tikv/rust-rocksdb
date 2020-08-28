// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.
extern crate cc;
extern crate cmake;

use std::env;

fn load_aws_sdk() {
    let dst = cmake::Config::new("aws-sdk-cpp")
        .define("BUILD_ONLY", "kinesis;core;s3;transfer")
        .define("CMAKE_BUILD_TYPE", "RelWithDebInfo")
        .define("ENABLE_TESTING", "OFF")
        .define("STATIC_LINKING", "1")
        .very_verbose(true)
        .build();

    println!("cargo:rustc-link-search=native={}/lib", dst.display());
    println!("cargo:rustc-link-lib=static=aws-c-common");
    println!("cargo:rustc-link-lib=static=aws-checksums");
    println!("cargo:rustc-link-lib=static=aws-c-event-stream");
    println!("cargo:rustc-link-lib=static=aws-cpp-sdk-core");
    println!("cargo:rustc-link-lib=static=aws-cpp-sdk-kinesis");
    println!("cargo:rustc-link-lib=static=aws-cpp-sdk-s3");
    println!("cargo:rustc-link-lib=static=aws-cpp-sdk-transfer");
}

fn load_rocksdb_cloud() {
    let cur_dir = env::current_dir().unwrap();
    let mut cfg = cmake::Config::new(".");
    let dst = cfg
        .define("ROCKSDB_DIR", cur_dir.join("..").join("rocksdb"))
        .build_target("rocksdb_cloud")
        .very_verbose(true)
        .build();

    println!("cargo:rustc-link-search=native={}/build", dst.display());
    println!("cargo:rustc-link-lib=static=rocksdb_cloud");
}

fn main() {
    load_aws_sdk();
    load_rocksdb_cloud();
}
