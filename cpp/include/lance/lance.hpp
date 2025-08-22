#pragma once

#include "error.hpp"
#include "file.hpp"
#include "dataset.hpp"
#include "scanner.hpp"

extern "C" {
    bool lance_init();
    void lance_cleanup();
}

namespace lance {

// Initialize Lance library
inline bool init() {
    return lance_init();
}

// Cleanup Lance library
inline void cleanup() {
    lance_cleanup();
}

} // namespace lance
