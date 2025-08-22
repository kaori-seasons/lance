#pragma once

#include <memory>
#include <string>
#include <map>
#include <arrow/api.h>
#include "error.hpp"
#include "scanner.hpp"

extern "C" {
    struct CLanceDataset;
    struct CLanceError;
    struct CArrowSchema;
    struct CStringMap;
    
    CLanceDataset* lance_dataset_open(
        const char* uri,
        const CStringMap* storage_options,
        CLanceError** error
    );
    
    void lance_dataset_schema(
        const CLanceDataset* dataset,
        CArrowSchema* schema_out,
        CLanceError** error
    );
    
    uint64_t lance_dataset_count_rows(
        const CLanceDataset* dataset,
        CLanceError** error
    );
    
    void lance_dataset_free(CLanceDataset* dataset);
}

namespace lance {

class Dataset {
public:
    static Dataset open(
        const std::string& uri,
        const std::map<std::string, std::string>& storage_options = {}
    );
    
    ~Dataset();
    
    // Move constructor and assignment
    Dataset(Dataset&& other) noexcept;
    Dataset& operator=(Dataset&& other) noexcept;
    
    // Delete copy constructor and assignment
    Dataset(const Dataset&) = delete;
    Dataset& operator=(const Dataset&) = delete;
    
    std::shared_ptr<arrow::Schema> schema() const;
    uint64_t count_rows() const;
    Scanner scanner() const;

private:
    explicit Dataset(CLanceDataset* dataset);
    CLanceDataset* dataset_;
    
    void check_error(CLanceError* error) const;
    std::vector<const char*> string_map_keys_to_c_array(const std::map<std::string, std::string>& map) const;
    std::vector<const char*> string_map_values_to_c_array(const std::map<std::string, std::string>& map) const;
};

} // namespace lance
