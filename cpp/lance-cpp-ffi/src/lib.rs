// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::sync::{Arc, LazyLock};
use tokio::runtime::Runtime;

pub mod dataset;
pub mod file;
pub mod scanner;
pub mod error;
pub mod types;

// Global runtime for async operations
static RT: LazyLock<Runtime> = LazyLock::new(|| {
    Runtime::new().expect("Failed to create tokio runtime")
});

#[no_mangle]
pub extern "C" fn lance_init() -> bool {
    // Initialize the runtime
    LazyLock::force(&RT);
    true
}

#[no_mangle]
pub extern "C" fn lance_cleanup() {
    // Cleanup if needed
}
