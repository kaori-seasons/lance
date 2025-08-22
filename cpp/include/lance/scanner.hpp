#pragma once

#include <memory>
#include <string>
#include <vector>
#include <arrow/api.h>
#include "error.hpp"

extern "C" {
    struct CLanceScanner;
    struct CLanceDataset;
    struct CLanceError;
    struct CArrowArrayStream;
    struct CStringArray;
    
    CLanceScanner* lance_dataset_scanner(
        const CLanceDataset* dataset,
        CLanceError** error
    );
    
    CLanceScanner* lance_scanner_project(
        CLanceScanner* scanner,
        const CStringArray* columns,
        CLanceError** error
    );
    
    CLanceScanner* lance_scanner_filter(
        CLanceScanner* scanner,
        const char* filter,
        CLanceError** error
    );
    
    void lance_scanner_to_stream(
        const CLanceScanner* scanner,
        CArrowArrayStream* stream_out,
        CLanceError** error
    );
    
    void lance_scanner_free(CLanceScanner* scanner);
}

namespace lance {

class Dataset; // Forward declaration

class Scanner {
public:
    Scanner(const Scanner&) = delete;
    Scanner& operator=(const Scanner&) = delete;
    
    Scanner(Scanner&& other) noexcept;
    Scanner& operator=(Scanner&& other) noexcept;
    
    ~Scanner();
    
    Scanner project(const std::vector<std::string>& columns);
    Scanner filter(const std::string& filter_expr);
    std::shared_ptr<arrow::Table> to_table();

private:
    friend class Dataset;
    explicit Scanner(CLanceScanner* scanner);
    CLanceScanner* scanner_;
    
    void check_error(CLanceError* error) const;
    std::vector<const char*> string_vector_to_c_array(const std::vector<std::string>& strings) const;
};

} // namespace lance
