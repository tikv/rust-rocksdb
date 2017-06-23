use crocksdb_ffi::{self, DBTablePropertiesCollectorFactory};
use libc::{c_void, c_char};
use std::ffi::CString;
use table_properties_collector::{self,TablePropertiesCollector,TablePropertiesCollectorProxy};

pub trait TablePropertiesCollectorFactory {
    fn create_table_properties_collector(&mut self, column_family_id: u32) -> Box<TablePropertiesCollector>;
}

#[repr(C)]
pub struct TablePropertiesCollectorFactoryProxy {
    name: CString,
    collector_factory: Box<TablePropertiesCollectorFactory>,
}

extern "C" fn name(collector_factory: *mut c_void) -> *const c_char {
    unsafe { (*(collector_factory as *mut TablePropertiesCollectorFactoryProxy)).name.as_ptr() }
}

extern "C" fn destructor(collector_factory: *mut c_void) {
    unsafe {
        Box::from_raw(collector_factory as *mut TablePropertiesCollectorFactoryProxy);
    }
}

extern "C" fn create_table_properties_collector(
                collector_factory: *mut c_void, context: u32)
                     -> *mut c_void
                {
    unsafe { 
        let collector_factory = &mut *(collector_factory as *mut TablePropertiesCollectorFactoryProxy);
        let proxy = Box::new(TablePropertiesCollectorProxy {
            name: collector_factory.name.clone(),
            collector: collector_factory.collector_factory.create_table_properties_collector(context),
        });
        Box::into_raw(proxy) as *mut c_void
    }
}

pub struct TablePropertiesCollectorFactoryHandle {
    pub inner: *mut DBTablePropertiesCollectorFactory,
}

impl Drop for TablePropertiesCollectorFactoryHandle {
    fn drop(&mut self) {
        unsafe {
            crocksdb_ffi::crocksdb_tablepropertiescollectorfactory_destroy(self.inner);
        }
    }
}

pub unsafe fn new_table_properties_collector_factory(c_name: CString,
                                    f: Box<TablePropertiesCollectorFactory>)
                                    -> Result<TablePropertiesCollectorFactoryHandle, String> {
    let proxy = Box::into_raw(Box::new(TablePropertiesCollectorFactoryProxy {
        name: c_name,
        collector_factory: f,
    }));
    let collector_factory = crocksdb_ffi::crocksdb_tablepropertiescollectorfactory_create(proxy as *mut c_void,
                                                                destructor,
                                                                create_table_properties_collector,
                                                                name,
                                                                table_properties_collector::destructor,
                                                                table_properties_collector::add_userkey,
                                                                table_properties_collector::finish,
                                                                table_properties_collector::readable_properties,
                                                                table_properties_collector::name
                                                                );
    Ok(TablePropertiesCollectorFactoryHandle { inner: collector_factory })
}
