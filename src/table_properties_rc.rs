//! This provides reference-counted abstractions around table properties
//! collections. It is used by tikv in its own engine abstractions, to avoid the
//! complexities of lifetimes in associated types.

use std::slice;
use std::str;
use libc::size_t;
use librocksdb_sys as crocksdb_ffi;
use crocksdb_ffi::{
    DBTablePropertiesCollection,
    DBTableProperty,
};
use std::ops::Deref;

use crate::table_properties_rc_handles::{
    TablePropertiesCollectionHandle,
    TablePropertiesCollectionIteratorHandle,
    TablePropertiesHandle,
    UserCollectedPropertiesHandle,
    UserCollectedPropertiesIteratorHandle,
};


pub struct TablePropertiesCollection {
    handle: TablePropertiesCollectionHandle,
}

impl TablePropertiesCollection {
    pub unsafe fn new(ptr: *mut DBTablePropertiesCollection) -> TablePropertiesCollection {
        assert!(!ptr.is_null());
        TablePropertiesCollection {
            handle: TablePropertiesCollectionHandle::new(ptr)
        }
    }

    pub fn iter(&self) -> TablePropertiesCollectionIter {
        TablePropertiesCollectionIter::new(self.handle.clone())
    }

    pub fn len(&self) -> usize {
        unsafe { crocksdb_ffi::crocksdb_table_properties_collection_len(self.handle.ptr()) }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}


pub struct TablePropertiesCollectionIter {
    handle: TablePropertiesCollectionIteratorHandle,
}

impl TablePropertiesCollectionIter {
    fn new(collection: TablePropertiesCollectionHandle) -> TablePropertiesCollectionIter {
        TablePropertiesCollectionIter {
            handle: TablePropertiesCollectionIteratorHandle::new(collection),
        }
    }
}

impl Iterator for TablePropertiesCollectionIter {
    type Item = (TablePropertiesKey, TableProperties);

    fn next(&mut self) -> Option<Self::Item> {
        unsafe {
            loop {
                if !crocksdb_ffi::crocksdb_table_properties_collection_iter_valid(self.handle.ptr()) {
                    return None;
                }

                let mut keylen: size_t = 0;
                let key = crocksdb_ffi::crocksdb_table_properties_collection_iter_key(
                    self.handle.ptr(), &mut keylen,
                );
                let props = crocksdb_ffi::crocksdb_table_properties_collection_iter_value(self.handle.ptr());
                crocksdb_ffi::crocksdb_table_properties_collection_iter_next(self.handle.ptr());
                if !props.is_null() {
                    assert!(!key.is_null() && keylen != 0);
                    let key = TablePropertiesKey::new(key, keylen, self.handle.clone());
                    let props_handle = TablePropertiesHandle::new(props, self.handle.clone());
                    let val = TableProperties::new(props_handle);
                    return Some((key, val));
                }
            }
        }
    }
}

/// # Safety
///
/// The underlying iterator is over an unordered map of heap-allocated strings,
/// so as long as the iterator and collection are alive, the key pointers are
/// valid.
pub struct TablePropertiesKey {
    key: *const u8,
    keylen: size_t,
    _iter_handle: TablePropertiesCollectionIteratorHandle,
}

impl TablePropertiesKey {
    fn new(key: *const u8, keylen: size_t,
           _iter_handle: TablePropertiesCollectionIteratorHandle) -> TablePropertiesKey {
        TablePropertiesKey {
            key, keylen, _iter_handle,
        }
    }
}

impl Deref for TablePropertiesKey {
    type Target = str;

    fn deref(&self) -> &str {
        // Safety: creating slice from values reported by rocksdb, that should
        // be valid as long is this object is valid.
        unsafe {
            let bytes = slice::from_raw_parts(self.key, self.keylen);
            let key = str::from_utf8(bytes).unwrap();
            key
        }
    }
}

pub struct TableProperties {
    handle: TablePropertiesHandle,
}

impl TableProperties {
    fn new(handle: TablePropertiesHandle) -> TableProperties {
        TableProperties { handle }
    }

    fn get_u64(&self, prop: DBTableProperty) -> u64 {
        unsafe { crocksdb_ffi::crocksdb_table_properties_get_u64(self.handle.ptr(), prop) }
    }

    pub fn num_entries(&self) -> u64 {
        self.get_u64(DBTableProperty::NumEntries)
    }

    pub fn user_collected_properties(&self) -> UserCollectedProperties {
        UserCollectedProperties::new(self.handle.clone())
    }
}

pub struct UserCollectedProperties {
    handle: UserCollectedPropertiesHandle,
}

impl UserCollectedProperties {
    fn new(table_props_handle: TablePropertiesHandle) -> UserCollectedProperties {
        UserCollectedProperties {
            handle: UserCollectedPropertiesHandle::new(table_props_handle),
        }
    }

    pub fn iter(&self) -> UserCollectedPropertiesIter {
        UserCollectedPropertiesIter::new(self.handle.clone())
    }

    pub fn get<Q: AsRef<[u8]>>(&self, index: Q) -> Option<&[u8]> {
        let bytes = index.as_ref();
        let mut size = 0;
        unsafe {
            let ptr = crocksdb_ffi::crocksdb_user_collected_properties_get(
                self.handle.ptr(),
                bytes.as_ptr(),
                bytes.len(),
                &mut size,
            );
            if ptr.is_null() {
                return None;
            }
            Some(slice::from_raw_parts(ptr, size))
        }
    }

    pub fn len(&self) -> usize {
        unsafe { crocksdb_ffi::crocksdb_user_collected_properties_len(self.handle.ptr()) }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}


pub struct UserCollectedPropertiesIter {
    _handle: UserCollectedPropertiesIteratorHandle,
}

impl UserCollectedPropertiesIter {
    fn new(user_props: UserCollectedPropertiesHandle) -> UserCollectedPropertiesIter {
        UserCollectedPropertiesIter {
            _handle: UserCollectedPropertiesIteratorHandle::new(user_props),
        }
    }
}
