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

use crocksdb_ffi::{self, DBKeyVersions, DBKeyVersion, DBKeyVersionsIterator};
use libc::size_t;
use std::marker::PhantomData;
use std::slice;
use std::str;

pub fn new_key_versions() -> KeyVersions {
    KeyVersions::new()
}

pub struct KeyVersions {
    pub inner: *mut DBKeyVersions,
}

impl Drop for KeyVersions {
    fn drop(&mut self) {
        unsafe {
            crocksdb_ffi::crocksdb_keyversions_destroy(self.inner);
        }
    }
}

impl KeyVersions {
    fn new() -> KeyVersions {
        unsafe { KeyVersions { inner: crocksdb_ffi::crocksdb_keyversions_create() } }
    }

    pub fn get_vec(&self) -> Vec<KeyVersion> {
        let mut res = Vec::new();
        let mut iter = KeyVersionsIter::new(self);
        while iter.valid() {
            res.push(iter.value());
            iter.next();
        }
        res
    }
}

pub struct KeyVersionsIter<'a> {
    kvs: PhantomData<&'a ()>,
    inner: *mut DBKeyVersionsIterator,
}

impl<'a> Drop for KeyVersionsIter<'a> {
    fn drop(&mut self) {
        unsafe {
            crocksdb_ffi::crocksdb_keyversions_iterator_destroy(self.inner);
        }
    }
}

impl<'a> KeyVersionsIter<'a> {
    fn new(kvs: &'a KeyVersions) -> KeyVersionsIter<'a> {
        unsafe {
            KeyVersionsIter {
                kvs: PhantomData,
                inner: crocksdb_ffi::crocksdb_keyversions_iterator_create(kvs.inner),
            }
        }
    }

    pub fn valid(&self) -> bool {
        unsafe { crocksdb_ffi::crocksdb_keyversions_iterator_valid(self.inner) }
    }

    pub fn next(&mut self) {
        unsafe {
            crocksdb_ffi::crocksdb_keyversions_iterator_next(self.inner);
        }
    }

    pub fn value(&self) -> KeyVersion {
        unsafe {
            let kv = KeyVersion::new();
            crocksdb_ffi::crocksdb_keyversions_iterator_value(self.inner, kv.inner);
            kv
        }
    }
}

pub struct KeyVersion {
    inner: *mut DBKeyVersion,
}

impl Drop for KeyVersion {
    fn drop(&mut self) {
        unsafe {
            crocksdb_ffi::crocksdb_keyversion_destroy(self.inner);
        }
    }
}

impl KeyVersion {
    fn new() -> KeyVersion {
        unsafe { KeyVersion { inner: crocksdb_ffi::crocksdb_keyversion_create() } }
    }

    fn get_seq(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_keyversion_get_seq(self.inner) }
    }

    fn get_type(&self) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_keyversion_get_type(self.inner) }
    }

    fn get_key(&self) -> &str {
        unsafe {
            let mut slen: size_t = 0;
            let s = crocksdb_ffi::crocksdb_keyversion_get_key(self.inner, &mut slen);
            let bytes = slice::from_raw_parts(s, slen);
            str::from_utf8(bytes).unwrap()
        }
    }

    fn get_value(&self) -> &str {
        unsafe {
            let mut slen: size_t = 0;
            let s = crocksdb_ffi::crocksdb_keyversion_get_value(self.inner, &mut slen);
            let bytes = slice::from_raw_parts(s, slen);
            str::from_utf8(bytes).unwrap()
        }
    }

    pub fn sequence(&self) -> u64 {
        self.get_seq()
    }

    pub fn key_type(&self) -> u64 {
        self.get_type()
    }

    pub fn user_key(&self) -> &str {
        self.get_key()
    }

    pub fn value(&self) -> &str {
        self.get_value()
    }
}
