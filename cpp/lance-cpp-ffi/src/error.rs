use std::ffi::CString;
use std::os::raw::c_char;
use lance_core::Error as LanceError;

#[repr(C)]
pub struct CLanceError {
    pub message: *mut c_char,
    pub code: i32,
}

impl CLanceError {
    pub fn from_lance_error(error: LanceError) -> Self {
        let message = CString::new(error.to_string())
            .unwrap_or_else(|_| CString::new("Unknown error").unwrap());
        
        Self {
            message: message.into_raw(),
            code: match error {
                LanceError::InvalidInput { .. } => 1,
                LanceError::DatasetAlreadyExists { .. } => 2,
                LanceError::SchemaMismatch { .. } => 3,
                _ => 999,
            }
        }
    }
    
    pub fn success() -> Self {
        Self {
            message: std::ptr::null_mut(),
            code: 0,
        }
    }
}

#[no_mangle]
pub extern "C" fn lance_error_free(error: *mut CLanceError) {
    if !error.is_null() {
        unsafe {
            let error = Box::from_raw(error);
            if !error.message.is_null() {
                let _ = CString::from_raw(error.message);
            }
        }
    }
}
