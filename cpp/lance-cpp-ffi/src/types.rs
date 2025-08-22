use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::collections::HashMap;
use libc::size_t;

#[repr(C)]
pub struct CStringArray {
    pub data: *const *const c_char,
    pub len: size_t,
}

#[repr(C)]
pub struct CStringMap {
    pub keys: *const *const c_char,
    pub values: *const *const c_char,
    pub len: size_t,
}

#[repr(C)]
pub struct CArrowSchema {
    pub ptr: *mut arrow::ffi::FFI_ArrowSchema,
}

#[repr(C)]
pub struct CArrowArray {
    pub ptr: *mut arrow::ffi::FFI_ArrowArray,
}

#[repr(C)]
pub struct CArrowArrayStream {
    pub ptr: *mut arrow::ffi_stream::FFI_ArrowArrayStream,
}

pub unsafe fn c_string_array_to_vec(arr: *const CStringArray) -> Vec<String> {
    if arr.is_null() {
        return Vec::new();
    }
    
    let arr = &*arr;
    let mut result = Vec::with_capacity(arr.len);
    
    for i in 0..arr.len {
        let c_str = CStr::from_ptr(*arr.data.add(i));
        result.push(c_str.to_string_lossy().into_owned());
    }
    
    result
}

pub unsafe fn c_string_map_to_hashmap(map: *const CStringMap) -> HashMap<String, String> {
    if map.is_null() {
        return HashMap::new();
    }
    
    let map = &*map;
    let mut result = HashMap::with_capacity(map.len);
    
    for i in 0..map.len {
        let key = CStr::from_ptr(*map.keys.add(i));
        let value = CStr::from_ptr(*map.values.add(i));
        result.insert(
            key.to_string_lossy().into_owned(),
            value.to_string_lossy().into_owned(),
        );
    }
    
    result
}
