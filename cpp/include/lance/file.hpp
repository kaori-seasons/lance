#pragma once

#include <memory>
#include <string>
#include <vector>
#include <map>
#include <arrow/api.h>
#include <arrow/c/abi.h>
#include "error.hpp"

extern "C" {
    struct CLanceFileReader;
    struct CLanceFileWriter;
    struct CLanceError;
    struct CArrowSchema;
    struct CArrowArray;
    struct CArrowArrayStream;
    struct CStringMap;
    struct CStringArray;
    
    // File Reader functions
    CLanceFileReader* lance_file_reader_open(
        const char* uri,
        const CStringMap* storage_options,
        const CStringArray* columns,
        CLanceError** error
    );
    
    uint64_t lance_file_reader_num_rows(
        const CLanceFileReader* reader,
        CLanceError** error
    );
    
    void lance_file_reader_schema(
        const CLanceFileReader* reader,
        CArrowSchema* schema_out,
        CLanceError** error
    );
    
    void lance_file_reader_read_all(
        const CLanceFileReader* reader,
        uint32_t batch_size,
        CArrowArrayStream* stream_out,
        CLanceError** error
    );
    
    void lance_file_reader_free(CLanceFileReader* reader);
    
    // File Writer functions
    CLanceFileWriter* lance_file_writer_create(
        const char* uri,
        const CArrowSchema* schema,
        const CStringMap* storage_options,
        CLanceError** error
    );
    
    void lance_file_writer_write_batch(
        const CLanceFileWriter* writer,
        const CArrowArray* batch,
        const CArrowSchema* schema,
        CLanceError** error
    );
    
    uint64_t lance_file_writer_finish(
        const CLanceFileWriter* writer,
        CLanceError** error
    );
    
    void lance_file_writer_free(CLanceFileWriter* writer);
    
    // Error handling
    void lance_error_free(CLanceError* error);
}

namespace lance {

class LanceFileReader {
public:
    static LanceFileReader open(
        const std::string& path,
        const std::map<std::string, std::string>& storage_options = {},
        const std::vector<std::string>& columns = {}
    );
    
    ~LanceFileReader();
    
    // Move constructor and assignment
    LanceFileReader(LanceFileReader&& other) noexcept;
    LanceFileReader& operator=(LanceFileReader&& other) noexcept;
    
    // Delete copy constructor and assignment
    LanceFileReader(const LanceFileReader&) = delete;
    LanceFileReader& operator=(const LanceFileReader&) = delete;
    
    uint64_t num_rows() const;
    std::shared_ptr<arrow::Schema> schema() const;
    std::shared_ptr<arrow::Table> read_all(uint32_t batch_size = 1024) const;

private:  
    explicit LanceFileReader(CLanceFileReader* reader);  
    CLanceFileReader* reader_;  
      
    void check_error(CLanceError* error) const;  
    std::vector<const char*> string_vector_to_c_array(const std::vector<std::string>& strings) const;  
    std::vector<const char*> string_map_keys_to_c_array(const std::map<std::string, std::string>& map) const;  
    std::vector<const char*> string_map_values_to_c_array(const std::map<std::string, std::string>& map) const;  
};  
  
class LanceFileWriter {  
public:  
    LanceFileWriter(const std::string& path,  
                   std::shared_ptr<arrow::Schema> schema = nullptr,  
                   const std::map<std::string, std::string>& storage_options = {});  
      
    ~LanceFileWriter();  
      
    // Move constructor and assignment  
    LanceFileWriter(LanceFileWriter&& other) noexcept;  
    LanceFileWriter& operator=(LanceFileWriter&& other) noexcept;  
      
    // Delete copy constructor and assignment  
    LanceFileWriter(const LanceFileWriter&) = delete;  
    LanceFileWriter& operator=(const LanceFileWriter&) = delete;  
      
    void write_batch(std::shared_ptr<arrow::RecordBatch> batch);  
    uint64_t finish();  
  
private:  
    CLanceFileWriter* writer_;  
      
    void check_error(CLanceError* error) const;  
    std::vector<const char*> string_map_keys_to_c_array(const std::map<std::string, std::string>& map) const;  
    std::vector<const char*> string_map_values_to_c_array(const std::map<std::string, std::string>& map) const;  
};  
  
} // namespace lance
