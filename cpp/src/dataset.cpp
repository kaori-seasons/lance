#include "lance/dataset.hpp"
#include <arrow/c/bridge.h>
#include <stdexcept>

namespace lance {

void Dataset::check_error(CLanceError* error) const {
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

std::vector<const char*> Dataset::string_map_keys_to_c_array(const std::map<std::string, std::string>& map) const {
    std::vector<const char*> keys;
    keys.reserve(map.size());
    for (const auto& pair : map) {
        keys.push_back(pair.first.c_str());
    }
    return keys;
}

std::vector<const char*> Dataset::string_map_values_to_c_array(const std::map<std::string, std::string>& map) const {
    std::vector<const char*> values;
    values.reserve(map.size());
    for (const auto& pair : map) {
        values.push_back(pair.second.c_str());
    }
    return values;
}

Dataset::Dataset(CLanceDataset* dataset) : dataset_(dataset) {}

Dataset Dataset::open(
    const std::string& uri,
    const std::map<std::string, std::string>& storage_options
) {
    CLanceError* error = nullptr;
    
    // Prepare storage options
    CStringMap c_storage_options = {nullptr, nullptr, 0};
    std::vector<const char*> keys, values;
    if (!storage_options.empty()) {
        Dataset dummy(nullptr);
        keys = dummy.string_map_keys_to_c_array(storage_options);
        values = dummy.string_map_values_to_c_array(storage_options);
        c_storage_options.keys = keys.data();
        c_storage_options.values = values.data();
        c_storage_options.len = storage_options.size();
    }
    
    CLanceDataset* dataset = lance_dataset_open(
        uri.c_str(),
        storage_options.empty() ? nullptr : &c_storage_options,
        &error
    );
    
    if (!dataset) {
        Dataset dummy(nullptr);
        dummy.check_error(error);
        throw LanceError("Failed to open dataset");
    }
    
    Dataset result(dataset);
    result.check_error(error);
    return result;
}

Dataset::~Dataset() {
    if (dataset_) {
        lance_dataset_free(dataset_);
    }
}

Dataset::Dataset(Dataset&& other) noexcept : dataset_(other.dataset_) {
    other.dataset_ = nullptr;
}

Dataset& Dataset::operator=(Dataset&& other) noexcept {
    if (this != &other) {
        if (dataset_) {
            lance_dataset_free(dataset_);
        }
        dataset_ = other.dataset_;
        other.dataset_ = nullptr;
    }
    return *this;
}

std::shared_ptr<arrow::Schema> Dataset::schema() const {
    CLanceError* error = nullptr;
    CArrowSchema c_schema = {nullptr};
    
    lance_dataset_schema(dataset_, &c_schema, &error);
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

uint64_t Dataset::count_rows() const {
    CLanceError* error = nullptr;
    uint64_t count = lance_dataset_count_rows(dataset_, &error);
    check_error(error);
    return count;
}

Scanner Dataset::scanner() const {
    CLanceError* error = nullptr;
    
    CLanceScanner* scanner = lance_dataset_scanner(dataset_, &error);
    
    if (!scanner) {
        check_error(error);
        throw LanceError("Failed to create scanner");
    }
    
    Scanner result(scanner);
    result.check_error(error);
    return result;
}

} // namespace lance
