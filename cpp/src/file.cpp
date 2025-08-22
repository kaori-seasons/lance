#include "lance/file.hpp"
#include <arrow/c/bridge.h>
#include <stdexcept>

namespace lance {

void LanceFileReader::check_error(CLanceError* error) const {
    if (error && error->code != 0) {
        std::string message = error->message ? std::string(error->message) : "Unknown error";
        int code = error->code;
        lance_error_free(error);
        
        switch (code) {
            case 1:
                throw InvalidInputError(message);
            case 2:
                throw DatasetAlreadyExistsError(message);
            case 3:
                throw SchemaMismatchError(message);
            default:
                throw LanceError(message, code);
        }
    }
    if (error) {
        lance_error_free(error);
    }
}

std::vector<const char*> LanceFileReader::string_vector_to_c_array(const std::vector<std::string>& strings) const {
    std::vector<const char*> c_strings;
    c_strings.reserve(strings.size());
    for (const auto& str : strings) {
        c_strings.push_back(str.c_str());
    }
    return c_strings;
}

std::vector<const char*> LanceFileReader::string_map_keys_to_c_array(const std::map<std::string, std::string>& map) const {
    std::vector<const char*> keys;
    keys.reserve(map.size());
    for (const auto& pair : map) {
        keys.push_back(pair.first.c_str());
    }
    return keys;
}

std::vector<const char*> LanceFileReader::string_map_values_to_c_array(const std::map<std::string, std::string>& map) const {
    std::vector<const char*> values;
    values.reserve(map.size());
    for (const auto& pair : map) {
        values.push_back(pair.second.c_str());
    }
    return values;
}

LanceFileReader::LanceFileReader(CLanceFileReader* reader) : reader_(reader) {}

LanceFileReader LanceFileReader::open(
    const std::string& path,
    const std::map<std::string, std::string>& storage_options,
    const std::vector<std::string>& columns
) {
    CLanceError* error = nullptr;
    
    // Prepare storage options
    CStringMap c_storage_options = {nullptr, nullptr, 0};
    std::vector<const char*> keys, values;
    if (!storage_options.empty()) {
        keys = LanceFileReader().string_map_keys_to_c_array(storage_options);
        values = LanceFileReader().string_map_values_to_c_array(storage_options);
        c_storage_options.keys = keys.data();
        c_storage_options.values = values.data();
        c_storage_options.len = storage_options.size();
    }
    
    // Prepare columns
    CStringArray c_columns = {nullptr, 0};
    std::vector<const char*> column_ptrs;
    if (!columns.empty()) {
        column_ptrs = LanceFileReader().string_vector_to_c_array(columns);
        c_columns.data = column_ptrs.data();
        c_columns.len = columns.size();
    }
    
    CLanceFileReader* reader = lance_file_reader_open(
        path.c_str(),
        storage_options.empty() ? nullptr : &c_storage_options,
        columns.empty() ? nullptr : &c_columns,
        &error
    );
    
    if (!reader) {
        LanceFileReader dummy(nullptr);
        dummy.check_error(error);
        throw LanceError("Failed to open file reader");
    }
    
    LanceFileReader result(reader);
    result.check_error(error);
    return result;
}

LanceFileReader::~LanceFileReader() {
    if (reader_) {
        lance_file_reader_free(reader_);
    }
}

LanceFileReader::LanceFileReader(LanceFileReader&& other) noexcept : reader_(other.reader_) {
    other.reader_ = nullptr;
}

LanceFileReader& LanceFileReader::operator=(LanceFileReader&& other) noexcept {
    if (this != &other) {
        if (reader_) {
            lance_file_reader_free(reader_);
        }
        reader_ = other.reader_;
        other.reader_ = nullptr;
    }
    return *this;
}

uint64_t LanceFileReader::num_rows() const {
    CLanceError* error = nullptr;
    uint64_t rows = lance_file_reader_num_rows(reader_, &error);
    check_error(error);
    return rows;
}

std::shared_ptr<arrow::Schema> LanceFileReader::schema() const {
    CLanceError* error = nullptr;
    CArrowSchema c_schema = {nullptr};
    
    lance_file_reader_schema(reader_, &c_schema, &error);
    check_error(error);
    
    if (!c_schema.ptr) {
        throw LanceError("Failed to get schema");
    }
    
    auto result = arrow::ImportSchema(c_schema.ptr);
    if (!result.ok()) {
        throw LanceError("Failed to import Arrow schema: " + result.status().ToString());
    }
    
    return result.ValueOrDie();
}

std::shared_ptr<arrow::Table> LanceFileReader::read_all(uint32_t batch_size) const {
    CLanceError* error = nullptr;
    CArrowArrayStream c_stream = {nullptr};
    
    lance_file_reader_read_all(reader_, batch_size, &c_stream, &error);
    check_error(error);
    
    if (!c_stream.ptr) {
        throw LanceError("Failed to create stream");
    }
    
    auto stream_result = arrow::ImportRecordBatchReader(c_stream.ptr);
    if (!stream_result.ok()) {
        throw LanceError("Failed to import Arrow stream: " + stream_result.status().ToString());
    }
    
    auto table_result = stream_result.ValueOrDie()->ToTable();
    if (!table_result.ok()) {
        throw LanceError("Failed to convert stream to table: " + table_result.status().ToString());
    }
    
    return table_result.ValueOrDie();
}

// LanceFileWriter implementation
LanceFileWriter::LanceFileWriter(
    const std::string& path,
    std::shared_ptr<arrow::Schema> schema,
    const std::map<std::string, std::string>& storage_options
) {
    CLanceError* error = nullptr;
    
    // Prepare storage options
    CStringMap c_storage_options = {nullptr, nullptr, 0};
    std::vector<const char*> keys, values;
    if (!storage_options.empty()) {
        keys = string_map_keys_to_c_array(storage_options);
        values = string_map_values_to_c_array(storage_options);
        c_storage_options.keys = keys.data();
        c_storage_options.values = values.data();
        c_storage_options.len = storage_options.size();
    }
    
    // Prepare schema
    CArrowSchema c_schema = {nullptr};
    if (schema) {
        auto export_result = arrow::ExportSchema(*schema);
        if (!export_result.ok()) {
            throw LanceError("Failed to export Arrow schema: " + export_result.status().ToString());
        }
        c_schema.ptr = export_result.ValueOrDie().release();
    }
    
    writer_ = lance_file_writer_create(
        path.c_str(),
        schema ? &c_schema : nullptr,
        storage_options.empty() ? nullptr : &c_storage_options,
        &error
    );
    
    if (!writer_) {
        check_error(error);
        throw LanceError("Failed to create file writer");
    }
    
    check_error(error);
}

LanceFileWriter::~LanceFileWriter() {
    if (writer_) {
        lance_file_writer_free(writer_);
    }
}

LanceFileWriter::LanceFileWriter(LanceFileWriter&& other) noexcept : writer_(other.writer_) {
    other.writer_ = nullptr;
}

LanceFileWriter& LanceFileWriter::operator=(LanceFileWriter&& other) noexcept {
    if (this != &other) {
        if (writer_) {
            lance_file_writer_free(writer_);
        }
        writer_ = other.writer_;
        other.writer_ = nullptr;
    }
    return *this;
}

void LanceFileWriter::check_error(CLanceError* error) const {
    if (error && error->code != 0) {
        std::string message = error->message ? std::string(error->message) : "Unknown error";
        int code = error->code;
        lance_error_free(error);
        
        switch (code) {
            case 1:
                throw InvalidInputError(message);
            case 2:
                throw DatasetAlreadyExistsError(message);
            case 3:
                throw SchemaMismatchError(message);
            default:
                throw LanceError(message, code);
        }
    }
    if (error) {
        lance_error_free(error);
    }
}

std::vector<const char*> LanceFileWriter::string_map_keys_to_c_array(const std::map<std::string, std::string>& map) const {
    std::vector<const char*> keys;
    keys.reserve(map.size());
    for (const auto& pair : map) {
        keys.push_back(pair.first.c_str());
    }
    return keys;
}

std::vector<const char*> LanceFileWriter::string_map_values_to_c_array(const std::map<std::string, std::string>& map) const {
    std::vector<const char*> values;
    values.reserve(map.size());
    for (const auto& pair : map) {
        values.push_back(pair.second.c_str());
    }
    return values;
}

void LanceFileWriter::write_batch(std::shared_ptr<arrow::RecordBatch> batch) {
    CLanceError* error = nullptr;
    
    // Export batch to C ABI
    auto batch_export = arrow::ExportRecordBatch(*batch);
    if (!batch_export.ok()) {
        throw LanceError("Failed to export RecordBatch: " + batch_export.status().ToString());
    }
    
    auto schema_export = arrow::ExportSchema(*batch->schema());
    if (!schema_export.ok()) {
        throw LanceError("Failed to export Schema: " + schema_export.status().ToString());
    }
    
    CArrowArray c_array = {batch_export.ValueOrDie().release()};
    CArrowSchema c_schema = {schema_export.ValueOrDie().release()};
    
    lance_file_writer_write_batch(writer_, &c_array, &c_schema, &error);
    check_error(error);
}

uint64_t LanceFileWriter::finish() {
    CLanceError* error = nullptr;
    uint64_t rows = lance_file_writer_finish(writer_, &error);
    check_error(error);
    return rows;
}

} // namespace lance
