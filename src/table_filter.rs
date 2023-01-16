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

use crocksdb_ffi::DBTableProperties;
use libc::{c_uchar, c_void};
use table_properties::TableProperties;

pub trait TableFilter {
    // A callback to determine whether relevant keys for this scan exist in a
    // given table based on the table's properties. The callback is passed the
    // properties of each table during iteration. If the callback returns false,
    // the table will not be scanned. This option only affects Iterators and has
    // no impact on point lookups.
    fn table_filter(&self, props: &TableProperties) -> bool;
}

pub extern "C" fn table_filter<T: TableFilter>(
    ctx: *mut c_void,
    props: *const DBTableProperties,
) -> c_uchar {
    unsafe {
        let filter = &*(ctx as *mut T);
        let props = &*(props as *const TableProperties);
        filter.table_filter(props) as c_uchar
    }
}

pub extern "C" fn destroy_table_filter<T: TableFilter>(filter: *mut c_void) {
    unsafe {
        let _ = Box::from_raw(filter as *mut T);
    }
}
