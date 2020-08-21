// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.
extern crate cc;
extern crate cmake;

fn main() {
    let dst = cmake::Config::new("aws-sdk-cpp")
        .define("BUILD_ONLY", "kinesis;core;s3;transfer")
        .define("CMAKE_BUILD_TYPE", "RelWithDebInfo")
        .define("ENABLE_TESTING", "OFF")
        .define("STATIC_LINKING", "1")
        .very_verbose(true)
        .build();
    println!("cargo:rustc-link-search=native={}/build/aws-cpp-sdk-core", dst.display());
    println!("cargo:rustc-link-lib=static=aws-cpp-sdk-core");
    println!("cargo:rustc-link-search=native={}/build/aws-cpp-sdk-kinesis", dst.display());
    println!("cargo:rustc-link-lib=static=aws-cpp-sdk-kinesis");
    println!("cargo:rustc-link-search=native={}/build/aws-cpp-sdk-s3", dst.display());
    println!("cargo:rustc-link-lib=static=aws-cpp-sdk-s3");
    println!("cargo:rustc-link-search=native={}/build/aws-cpp-sdk-transfer", dst.display());
    println!("cargo:rustc-link-lib=static=aws-cpp-sdk-transfer");
}