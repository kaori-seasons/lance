cmake_minimum_required(VERSION 3.16)
project(lance-cpp)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Find Arrow
find_package(Arrow REQUIRED)
find_package(ArrowDataset REQUIRED)

# Build Rust FFI library
add_custom_target(lance_ffi ALL
    COMMAND cargo build --release
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/lance-cpp-ffi
    COMMENT "Building Rust FFI library"
)

# Include directories
include_directories(include)
include_directories(${ARROW_INCLUDE_DIR})

# Source files
set(SOURCES
    src/file.cpp
    src/dataset.cpp
    src/scanner.cpp
)

# Create library
add_library(lance-cpp SHARED ${SOURCES})

# Link libraries
target_link_libraries(lance-cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/lance-cpp-ffi/target/release/liblance_cpp_ffi.a
    ${ARROW_SHARED_LIB}
    ${ARROW_DATASET_SHARED_LIB}
    pthread
    dl
)

# Add dependency on Rust build
add_dependencies(lance-cpp lance_ffi)

# Install
install(TARGETS lance-cpp DESTINATION lib)
install(DIRECTORY include/ DESTINATION include)
