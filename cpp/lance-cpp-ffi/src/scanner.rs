use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::sync::Arc;
use std::ptr;

use lance::dataset::Scanner;
use arrow::ffi_stream::FFI_ArrowArrayStream;

use crate::types::*;
use crate::error::CLanceError;
use crate::dataset::CLanceDataset;
use crate::RT;

#[repr(C)]
pub struct CLanceScanner {
    inner: Scanner,
}

#[no_mangle]
pub extern "C" fn lance_dataset_scanner(
    dataset: *const CLanceDataset,
    error: *mut *mut CLanceError,
) -> *mut CLanceScanner {
    if dataset.is_null() {
        unsafe {
            *error = Box::into_raw(Box::new(CLanceError {
                message: CString::new("Dataset is null").unwrap().into_raw(),
                code: -1,
            }));
        }
        return ptr::null_mut();
    }
    
    unsafe {
        let dataset = &*dataset;
        let scanner = dataset.inner.scan();
        *error = Box::into_raw(Box::new(CLanceError::success()));
        Box::into_raw(Box::new(CLanceScanner { inner: scanner }))
    }
}

#[no_mangle]
pub extern "C" fn lance_scanner_project(
    scanner: *mut CLanceScanner,
    columns: *const CStringArray,
    error: *mut *mut CLanceError,
) -> *mut CLanceScanner {
    if scanner.is_null() {
        unsafe {
            *error = Box::into_raw(Box::new(CLanceError {
                message: CString::new("Scanner is null").unwrap().into_raw(),
                code: -1,
            }));
        }
        return ptr::null_mut();
    }
    
    let result = std::panic::catch_unwind(|| {
        unsafe {
            let scanner = Box::from_raw(scanner);
            let column_names = c_string_array_to_vec(columns);
            
            let new_scanner = scanner.inner.project(&column_names)?;
            Ok(CLanceScanner { inner: new_scanner })
        }
    });
    
    match result {
        Ok(Ok(new_scanner)) => {
            unsafe { *error = Box::into_raw(Box::new(CLanceError::success())); }
            Box::into_raw(Box::new(new_scanner))
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
pub extern "C" fn lance_scanner_filter(
    scanner: *mut CLanceScanner,
    filter: *const c_char,
    error: *mut *mut CLanceError,
) -> *mut CLanceScanner {
    if scanner.is_null() || filter.is_null() {
        unsafe {
            *error = Box::into_raw(Box::new(CLanceError {
                message: CString::new("Null pointer").unwrap().into_raw(),
                code: -1,
            }));
        }
        return ptr::null_mut();
    }
    
    let result = std::panic::catch_unwind(|| {
        unsafe {
            let scanner = Box::from_raw(scanner);
            let filter_str = CStr::from_ptr(filter).to_string_lossy();
            
            let new_scanner = scanner.inner.filter(&filter_str)?;
            Ok(CLanceScanner { inner: new_scanner })
        }
    });
    
    match result {
        Ok(Ok(new_scanner)) => {
            unsafe { *error = Box::into_raw(Box::new(CLanceError::success())); }
            Box::into_raw(Box::new(new_scanner))
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
pub extern "C" fn lance_scanner_to_stream(
    scanner: *const CLanceScanner,
    stream_out: *mut CArrowArrayStream,
    error: *mut *mut CLanceError,
) {
    if scanner.is_null() || stream_out.is_null() {
        unsafe {
            *error = Box::into_raw(Box::new(CLanceError {
                message: CString::new("Null pointer").unwrap().into_raw(),
                code: -1,
            }));
        }
        return;
    }
    
    let result = std::panic::catch_unwind(|| {
        unsafe {
            let scanner = &*scanner;
            RT.block_on(async move {
                let stream = scanner.inner.try_into_stream().await?;
                let ffi_stream = FFI_ArrowArrayStream::new(stream);
                (*stream_out).ptr = Box::into_raw(Box::new(ffi_stream));
                Ok(())
            })
        }
    });
    
    match result {
        Ok(Ok(_)) => {
            unsafe { *error = Box::into_raw(Box::new(CLanceError::success())); }
        }
        Ok(Err(e)) => {
            unsafe { *error = Box::into_raw(Box::new(CLanceError::from_lance_error(e))); }
        }
        Err(_) => {
            unsafe { 
                *error = Box::into_raw(Box::new(CLanceError {
                    message: CString::new("Panic occurred").unwrap().into_raw(),
                    code: -1,
                }));
            }
        }
    }
}

#[no_mangle]
pub extern "C" fn lance_scanner_free(scanner: *mut CLanceScanner) {
    if !scanner.is_null() {
        unsafe {
            let _ = Box::from_raw(scanner);
        }
    }
}
