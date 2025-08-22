use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::sync::{Arc, Mutex};
use std::ptr;

use lance_file::v2::{
    reader::{FileReader, FileReaderOptions, ReaderProjection},
    writer::{FileWriter, FileWriterOptions}
};
use lance_io::object_store::{ObjectStore, ObjectStoreParams, ObjectStoreRegistry};
use lance_io::scheduler::{ScanScheduler, SchedulerConfig};
use lance_io::utils::CachedFileSize;
use lance_core::cache::LanceCache;
use lance_encoding::decoder::{DecoderPlugins, FilterExpression};
use lance_file::version::LanceFileVersion;
use arrow::ffi::{FFI_ArrowSchema, FFI_ArrowArray};
use arrow::ffi_stream::FFI_ArrowArrayStream;
use arrow_array::RecordBatch;
use object_store::path::Path;

use crate::types::*;
use crate::error::CLanceError;
use crate::RT;

#[repr(C)]
pub struct CLanceFileReader {
    inner: Arc<FileReader>,
}

#[repr(C)]
pub struct CLanceFileWriter {
    inner: Arc<Mutex<FileWriter>>,
}

#[no_mangle]
pub extern "C" fn lance_file_reader_open(
    uri: *const c_char,
    storage_options: *const CStringMap,
    columns: *const CStringArray,
    error: *mut *mut CLanceError,
) -> *mut CLanceFileReader {
    let result = std::panic::catch_unwind(|| {
        unsafe {
            let uri_str = CStr::from_ptr(uri).to_string_lossy().into_owned();
            let storage_opts = c_string_map_to_hashmap(storage_options);
            let column_names = c_string_array_to_vec(columns);
            
            RT.block_on(async move {
                let object_params = ObjectStoreParams {
                    storage_options: if storage_opts.is_empty() { None } else { Some(storage_opts) },
                    ..Default::default()
                };
                
                let (obj_store, path) = ObjectStore::from_uri_and_params(
                    Arc::new(ObjectStoreRegistry::default()),
                    &uri_str,
                    &object_params,
                ).await?;
                
                let config = SchedulerConfig::max_bandwidth(&obj_store);
                let scheduler = ScanScheduler::new(obj_store, config);
                
                let file = scheduler
                    .open_file(&Path::parse(&path)?, &CachedFileSize::unknown())
                    .await?;
                
                let file_metadata = FileReader::read_all_metadata(&file).await?;
                
                let base_projection = if !column_names.is_empty() {
                    Some(ReaderProjection::from_column_names(
                        file_metadata.version(),
                        &file_metadata.file_schema,
                        &column_names.iter().map(|s| s.as_str()).collect::<Vec<&str>>(),
                    )?)
                } else {
                    None
                };
                
                let reader = FileReader::try_open_with_file_metadata(
                    file,
                    path,
                    base_projection,
                    Arc::<DecoderPlugins>::default(),
                    Arc::new(file_metadata),
                    &LanceCache::no_cache(),
                    FileReaderOptions::default(),
                ).await?;
                
                Ok(CLanceFileReader {
                    inner: Arc::new(reader),
                })
            })
        }
    });
    
    match result {
        Ok(Ok(reader)) => {
            unsafe { *error = Box::into_raw(Box::new(CLanceError::success())); }
            Box::into_raw(Box::new(reader))
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
pub extern "C" fn lance_file_reader_num_rows(
    reader: *const CLanceFileReader,
    error: *mut *mut CLanceError,
) -> u64 {
    if reader.is_null() {
        unsafe {
            *error = Box::into_raw(Box::new(CLanceError {
                message: CString::new("Reader is null").unwrap().into_raw(),
                code: -1,
            }));
        }
        return 0;
    }
    
    unsafe {
        let reader = &*reader;
        *error = Box::into_raw(Box::new(CLanceError::success()));
        reader.inner.num_rows()
    }
}

#[no_mangle]
pub extern "C" fn lance_file_reader_schema(
    reader: *const CLanceFileReader,
    schema_out: *mut CArrowSchema,
    error: *mut *mut CLanceError,
) {
    if reader.is_null() || schema_out.is_null() {
        unsafe {
            *error = Box::into_raw(Box::new(CLanceError {
                message: CString::new("Null pointer").unwrap().into_raw(),
                code: -1,
            }));
        }
        return;
    }
    
    unsafe {
        let reader = &*reader;
        match reader.inner.schema() {
            Ok(schema) => {
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
            Err(e) => {
                *error = Box::into_raw(Box::new(CLanceError::from_lance_error(e)));
            }
        }
    }
}

#[no_mangle]
pub extern "C" fn lance_file_reader_read_all(
    reader: *const CLanceFileReader,
    batch_size: u32,
    stream_out: *mut CArrowArrayStream,
    error: *mut *mut CLanceError,
) {
    if reader.is_null() || stream_out.is_null() {
        unsafe {
            *error = Box::into_raw(Box::new(CLanceError {
                message: CString::new("Null pointer").unwrap().into_raw(),
                code: -1,
            }));
        }
        return;
    }
    
    unsafe {
        let reader = &*reader;
        match reader.inner.read_stream(
            lance_io::ReadBatchParams::RangeFull,
            batch_size,
            16, // batch_readahead
            FilterExpression::no_filter(),
        ) {
            Ok(stream) => {
                let ffi_stream = FFI_ArrowArrayStream::new(stream);
                (*stream_out).ptr = Box::into_raw(Box::new(ffi_stream));
                *error = Box::into_raw(Box::new(CLanceError::success()));
            }
            Err(e) => {
                *error = Box::into_raw(Box::new(CLanceError::from_lance_error(e)));
            }
        }
    }
}

#[no_mangle]
pub extern "C" fn lance_file_reader_free(reader: *mut CLanceFileReader) {
    if !reader.is_null() {
        unsafe {
            let _ = Box::from_raw(reader);
        }
    }
}

// File Writer Implementation
#[no_mangle]
pub extern "C" fn lance_file_writer_create(
    uri: *const c_char,
    schema: *const CArrowSchema,
    storage_options: *const CStringMap,
    error: *mut *mut CLanceError,
) -> *mut CLanceFileWriter {
    let result = std::panic::catch_unwind(|| {
        unsafe {
            let uri_str = CStr::from_ptr(uri).to_string_lossy().into_owned();
            let storage_opts = c_string_map_to_hashmap(storage_options);
            
            RT.block_on(async move {
                let object_params = ObjectStoreParams {  
                    storage_options: if storage_opts.is_empty() { None } else { Some(storage_opts) },  
                    ..Default::default()  
                };  
                  
                let (obj_store, path) = ObjectStore::from_uri_and_params(  
                    Arc::new(ObjectStoreRegistry::default()),  
                    &uri_str,  
                    &object_params,  
                ).await?;  
                  
                let obj_writer = obj_store.create(&Path::parse(&path)?).await?;  
                  
                let lance_schema = if !schema.is_null() {  
                    let arrow_schema = arrow::ffi::from_ffi((*schema).ptr, &[])?;  
                    Some(lance_core::datatypes::Schema::try_from(&arrow_schema)?)  
                } else {  
                    None  
                };  
                  
                let writer = if let Some(schema) = lance_schema {  
                    FileWriter::try_new(obj_writer, schema, FileWriterOptions::default())?  
                } else {  
                    FileWriter::new_lazy(obj_writer, FileWriterOptions::default())  
                };  
                  
                Ok(CLanceFileWriter {  
                    inner: Arc::new(Mutex::new(writer)),  
                })  
            })  
        }  
    });  
      
    match result {  
        Ok(Ok(writer)) => {  
            unsafe { *error = Box::into_raw(Box::new(CLanceError::success())); }  
            Box::into_raw(Box::new(writer))  
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
pub extern "C" fn lance_file_writer_write_batch(  
    writer: *const CLanceFileWriter,  
    batch: *const CArrowArray,  
    schema: *const CArrowSchema,  
    error: *mut *mut CLanceError,  
) {  
    if writer.is_null() || batch.is_null() || schema.is_null() {  
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
            let writer = &*writer;  
            let arrow_schema = arrow::ffi::from_ffi((*schema).ptr, &[])?;  
            let arrow_array = arrow::ffi::from_ffi((*batch).ptr, &arrow_schema)?;  
            let record_batch = RecordBatch::from(&arrow_array);  
              
            RT.block_on(async move {  
                writer.inner.lock().unwrap().write_batch(&record_batch).await  
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
pub extern "C" fn lance_file_writer_finish(  
    writer: *const CLanceFileWriter,  
    error: *mut *mut CLanceError,  
) -> u64 {  
    if writer.is_null() {  
        unsafe {  
            *error = Box::into_raw(Box::new(CLanceError {  
                message: CString::new("Writer is null").unwrap().into_raw(),  
                code: -1,  
            }));  
        }  
        return 0;  
    }  
      
    let result = std::panic::catch_unwind(|| {  
        unsafe {  
            let writer = &*writer;  
            RT.block_on(async move {  
                writer.inner.lock().unwrap().finish().await  
            })  
        }  
    });  
      
    match result {  
        Ok(Ok(rows)) => {  
            unsafe { *error = Box::into_raw(Box::new(CLanceError::success())); }  
            rows  
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
pub extern "C" fn lance_file_writer_free(writer: *mut CLanceFileWriter) {  
    if !writer.is_null() {  
        unsafe {  
            let _ = Box::from_raw(writer);  
        }  
    }  
}
