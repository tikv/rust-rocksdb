use crocksdb_ffi::{self, DBTablePropertiesCollector,EntryType};
use libc::{c_void, c_char, c_int, size_t};
use std::ffi::CString;
use std::slice;
use std::mem;
use std::collections::HashMap;

pub trait TablePropertiesCollector {
    fn add_userkey(&mut self, key: &[u8], value: &[u8], entry_type: EntryType);
    fn finish(&mut self) -> HashMap<String, String>;
}

#[repr(C)]
pub struct TablePropertiesCollectorProxy {
    pub name: CString,
    pub collector: Box<TablePropertiesCollector>,
}

pub extern "C" fn name(collector: *mut c_void) -> *const c_char {
    unsafe { 
        (*(collector as *mut TablePropertiesCollectorProxy)).name.as_ptr() 
    }
}

pub extern "C" fn destructor(collector: *mut c_void) {
    unsafe {
        Box::from_raw(collector as *mut TablePropertiesCollectorProxy);
    }
}

pub extern "C" fn add_userkey(collector: *mut c_void,
                     key: *const u8,
                     key_length: size_t,
                     value: *const u8,
                     value_length: size_t,
                     entry_type: c_int,
                     _: u64,
                     _: u64) {
    unsafe {
        let proxy = &mut *(collector as *mut TablePropertiesCollectorProxy);
        let key = slice::from_raw_parts(key, key_length);
        let value = slice::from_raw_parts(value, value_length);
        proxy.collector.add_userkey(key, value, mem::transmute(entry_type))
    }
}

pub extern "C" fn finish(collector: *mut c_void,
                     usercollectedproperties: *mut c_void) {
    unsafe {
        let collector = &mut *(collector as *mut TablePropertiesCollectorProxy);
        let props = collector.collector.finish();

        for (key, value) in props {
            crocksdb_ffi::crocksdb_add_property(usercollectedproperties, key.as_ptr(), key.len(),
             value.as_ptr(), value.len());
        }
    }
}

pub extern "C" fn readable_properties(collector: *mut c_void) {
    unsafe {
        Box::from_raw(collector as *mut TablePropertiesCollectorProxy);
    }
}
