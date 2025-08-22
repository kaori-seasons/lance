#include "lance/scanner.hpp"
#include <arrow/c/bridge.h>
#include <stdexcept>

namespace lance {

void Scanner::check_error(CLanceError* error) const {
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

std::vector<const char*> Scanner::string_vector_to_c_array(const std::vector<std::string>& strings) const {
    std::vector<const char*> c_strings;
    c_strings.reserve(strings.size());
    for (const auto& str : strings) {
        c_strings.push_back(str.c_str());
    }
    return c_strings;
}

Scanner::Scanner(CLanceScanner* scanner) : scanner_(scanner) {}

Scanner::~Scanner() {
    if (scanner_) {
        lance_scanner_free(scanner_);
    }
}

Scanner::Scanner(Scanner&& other) noexcept : scanner_(other.scanner_) {
    other.scanner_ = nullptr;
}

Scanner& Scanner::operator=(Scanner&& other) noexcept {
    if (this != &other) {
        if (scanner_) {
            lance_scanner_free(scanner_);
        }
        scanner_ = other.scanner_;
        other.scanner_ = nullptr;
    }
    return *this;
}

Scanner Scanner::project(const std::vector<std::string>& columns) {
    CLanceError* error = nullptr;
    
    // Prepare columns array
    CStringArray c_columns = {nullptr, 0};
    std::vector<const char*> column_ptrs;
    if (!columns.empty()) {
        column_ptrs = string_vector_to_c_array(columns);
        c_columns.data = column_ptrs.data();
        c_columns.len = columns.size();
    }
    
    CLanceScanner* new_scanner = lance_scanner_project(
        scanner_,
        columns.empty() ? nullptr : &c_columns,
        &error
    );
    
    if (!new_scanner) {
        check_error(error);
        throw LanceError("Failed to create projected scanner");
    }
    
    // Transfer ownership
    scanner_ = nullptr;
    Scanner result(new_scanner);
    result.check_error(error);
    return result;
}

Scanner Scanner::filter(const std::string& filter_expr) {
    CLanceError* error = nullptr;
    
    CLanceScanner* new_scanner = lance_scanner_filter(
        scanner_,
        filter_expr.c_str(),
        &error
    );
    
    if (!new_scanner) {
        check_error(error);
        throw LanceError("Failed to create filtered scanner");
    }
    
    // Transfer ownership
    scanner_ = nullptr;
    Scanner result(new_scanner);
    result.check_error(error);
    return result;
}

std::shared_ptr<arrow::Table> Scanner::to_table() {
    CLanceError* error = nullptr;
    CArrowArrayStream c_stream = {nullptr};
    
    lance_scanner_to_stream(scanner_, &c_stream, &error);
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

} // namespace lance
