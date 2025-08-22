#pragma once

#include <stdexcept>
#include <string>

namespace lance {

class LanceError : public std::runtime_error {
public:
    explicit LanceError(const std::string& message, int code = 0)
        : std::runtime_error(message), code_(code) {}
    
    int code() const { return code_; }

private:
    int code_;
};

class InvalidInputError : public LanceError {
public:
    explicit InvalidInputError(const std::string& message)
        : LanceError(message, 1) {}
};

class DatasetAlreadyExistsError : public LanceError {
public:
    explicit DatasetAlreadyExistsError(const std::string& message)
        : LanceError(message, 2) {}
};

class SchemaMismatchError : public LanceError {
public:
    explicit SchemaMismatchError(const std::string& message)
        : LanceError(message, 3) {}
};

} // namespace lance
