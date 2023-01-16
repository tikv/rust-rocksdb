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

use crocksdb_ffi::{self, DBTablePropertiesCollector, DBTablePropertiesCollectorFactory};
use libc::{c_char, c_void};
use std::ffi::CString;
use std::marker::PhantomData;
use table_properties_collector::{new_table_properties_collector, TablePropertiesCollector};

/// Constructs `TablePropertiesCollector`.
/// Internals create a new `TablePropertiesCollector` for each new table.
pub trait TablePropertiesCollectorFactory<T: TablePropertiesCollector> {
    /// Has to be thread-safe.
    fn create_table_properties_collector(&mut self, cf: u32) -> T;
}

struct TablePropertiesCollectorFactoryHandle<C, T>
where
    C: TablePropertiesCollector,
    T: TablePropertiesCollectorFactory<C>,
{
    name: CString,
    rep: T,
    _phantom: PhantomData<C>,
}

impl<C: TablePropertiesCollector, T: TablePropertiesCollectorFactory<C>>
    TablePropertiesCollectorFactoryHandle<C, T>
{
    fn new(name: &str, rep: T) -> TablePropertiesCollectorFactoryHandle<C, T> {
        TablePropertiesCollectorFactoryHandle::<C, T> {
            name: CString::new(name).unwrap(),
            rep,
            _phantom: PhantomData,
        }
    }
}

extern "C" fn name<C: TablePropertiesCollector, T: TablePropertiesCollectorFactory<C>>(
    handle: *mut c_void,
) -> *const c_char {
    unsafe {
        let handle = &mut *(handle as *mut TablePropertiesCollectorFactoryHandle<C, T>);
        handle.name.as_ptr()
    }
}

extern "C" fn destruct<C: TablePropertiesCollector, T: TablePropertiesCollectorFactory<C>>(
    handle: *mut c_void,
) {
    unsafe {
        let _ = Box::from_raw(handle as *mut TablePropertiesCollectorFactoryHandle<C, T>);
    }
}

extern "C" fn create_table_properties_collector<
    C: TablePropertiesCollector,
    T: TablePropertiesCollectorFactory<C>,
>(
    handle: *mut c_void,
    cf: u32,
) -> *mut DBTablePropertiesCollector {
    unsafe {
        let handle = &mut *(handle as *mut TablePropertiesCollectorFactoryHandle<C, T>);
        let collector = handle.rep.create_table_properties_collector(cf);
        new_table_properties_collector(handle.name.to_str().unwrap(), collector)
    }
}

pub unsafe fn new_table_properties_collector_factory<
    C: TablePropertiesCollector,
    T: TablePropertiesCollectorFactory<C>,
>(
    fname: &str,
    factory: T,
) -> *mut DBTablePropertiesCollectorFactory {
    let handle = TablePropertiesCollectorFactoryHandle::new(fname, factory);
    crocksdb_ffi::crocksdb_table_properties_collector_factory_create(
        Box::into_raw(Box::new(handle)) as *mut c_void,
        name::<C, T>,
        destruct::<C, T>,
        create_table_properties_collector::<C, T>,
    )
}
