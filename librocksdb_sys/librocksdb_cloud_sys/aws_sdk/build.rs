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
    println!("cargo:rustc-link-search=native={}/lib", dst.display());
    println!("cargo:rustc-link-lib=static=aws-c-common");
    println!("cargo:rustc-link-lib=static=aws-c-event-stream");
    println!("cargo:rustc-link-lib=static=aws-checksums");
}
