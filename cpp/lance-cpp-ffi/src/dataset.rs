use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::sync::Arc;
use std::ptr;

use lance::Dataset;
use lance_core::datatypes::Schema;
use lance_io::object_store::{ObjectStore, ObjectStoreParams, ObjectStoreRegistry};
use arrow::ffi::{FFI_ArrowSchema, FFI_ArrowArray};
use arrow::ffi_stream::FFI_ArrowArrayStream;

use crate::types::*;
use crate::error::CLanceError;
use crate::RT;

#[repr(C)]
pub struct CLanceDataset {
    inner: Arc<Dataset>,
}

#[no_mangle]
pub extern "C" fn lance_dataset_open(
    uri: *const c_char,
    storage_options: *const CStringMap,
    error: *mut *mut CLanceError,
) -> *mut CLanceDataset {
    let result = std::panic::catch_unwind(|| {
        unsafe {
            let uri_str = CStr::from_ptr(uri).to_string_lossy().into_owned();
            let storage_opts = c_string_map_to_hashmap(storage_options);
            
            RT.block_on(async move {
                let object_params = ObjectStoreParams {
                    storage_options: if storage_opts.is_empty() { None } else { Some(storage_opts) },
                    ..Default::default()
                };
                
                let dataset = Dataset::open_with_params(&uri_str, &object_params).await?;
                
                Ok(CLanceDataset {
                    inner: Arc::new(dataset),
                })
            })
        }
    });
    
    match result {
        Ok(Ok(dataset)) => {
            unsafe { *error = Box::into_raw(Box::new(CLanceError::success())); }
            Box::into_raw(Box::new(dataset))
        }
        Ok(Err(e)) => {
            unsafe { *error = Box::into_raw(Box::new(CLanceError::from_lance_error(e))); }
            ptr::null_mut()
        }
        Err(_) => {
            unsafe { 
                *error = Box::into_raw(Box::new(CLanceError {
                    message: CString::new("Panic occurred").unwrap().into_raw(),
                    code: -1,
                }));
            }
            ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "C" fn lance_dataset_schema(
    dataset: *const CLanceDataset,
    schema_out: *mut CArrowSchema,
    error: *mut *mut CLanceError,
) {
    if dataset.is_null() || schema_out.is_null() {
        unsafe {
            *error = Box::into_raw(Box::new(CLanceError {
                message: CString::new("Null pointer").unwrap().into_raw(),
                code: -1,
            }));
        }
        return;
    }
    
    unsafe {
        let dataset = &*dataset;
        let schema = dataset.inner.schema();
        match FFI_ArrowSchema::try_from(schema.as_ref()) {
            Ok(ffi_schema) => {
                (*schema_out).ptr = Box::into_raw(Box::new(ffi_schema));
                *error = Box::into_raw(Box::new(CLanceError::success()));
            }
            Err(e) => {
                *error = Box::into_raw(Box::new(CLanceError {
                    message: CString::new(format!("Schema conversion error: {}", e))
                        .unwrap().into_raw(),
                    code: -1,
                }));
            }
        }
    }
}

#[no_mangle]
pub extern "C" fn lance_dataset_count_rows(
    dataset: *const CLanceDataset,
    error: *mut *mut CLanceError,
) -> u64 {
    if dataset.is_null() {
        unsafe {
            *error = Box::into_raw(Box::new(CLanceError {
                message: CString::new("Dataset is null").unwrap().into_raw(),
                code: -1,
            }));
        }
        return 0;
    }
    
    let result = std::panic::catch_unwind(|| {
        unsafe {
            let dataset = &*dataset;
            RT.block_on(async move {
                dataset.inner.count_rows(None).await
            })
        }
    });
    
    match result {
        Ok(Ok(count)) => {
            unsafe { *error = Box::into_raw(Box::new(CLanceError::success())); }
            count as u64
        }
        Ok(Err(e)) => {
            unsafe { *error = Box::into_raw(Box::new(CLanceError::from_lance_error(e))); }
            0
        }
        Err(_) => {
            unsafe { 
                *error = Box::into_raw(Box::new(CLanceError {
                    message: CString::new("Panic occurred").unwrap().into_raw(),
                    code: -1,
                }));
            }
            0
        }
    }
}

#[no_mangle]
pub extern "C" fn lance_dataset_free(dataset: *mut CLanceDataset) {
    if !dataset.is_null() {
        unsafe {
            let _ = Box::from_raw(dataset);
        }
    }
}
