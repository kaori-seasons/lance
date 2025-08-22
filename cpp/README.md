# Lance C++ Client

这是Lance数据格式的C++客户端实现，提供了完整的文件操作、数据集管理和查询功能。

## 功能特性

- **文件操作**: 支持Lance文件的读写操作
- **数据集管理**: 打开和管理Lance数据集
- **查询功能**: 支持列投影和过滤查询
- **Arrow集成**: 与Apache Arrow生态系统无缝集成
- **错误处理**: 完善的异常处理机制
- **内存安全**: 使用RAII模式管理资源

## 架构设计

### 分层架构

1. **Rust FFI层** (`lance-cpp-ffi/`): 提供C语言接口
2. **C++封装层** (`include/lance/`, `src/`): 提供类型安全的C++接口
3. **构建系统** (`CMakeLists.txt`): 自动化构建和依赖管理

### 核心组件

- `LanceFileReader`: 文件读取器
- `LanceFileWriter`: 文件写入器
- `Dataset`: 数据集管理
- `Scanner`: 查询扫描器
- `LanceError`: 错误处理

## 安装要求

### 系统要求
- C++17兼容的编译器
- CMake 3.16+
- Apache Arrow 53.0.0+
- Rust 1.70+

### 依赖安装

#### Ubuntu/Debian
```bash
# 安装Arrow
sudo apt-get install libarrow-dev libarrow-dataset-dev

# 安装Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

#### macOS
```bash
# 安装Arrow
brew install apache-arrow

# 安装Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

#### Windows
```bash
# 安装Arrow (使用vcpkg)
vcpkg install arrow

# 安装Rust
# 下载并运行 rustup-init.exe
```

## 构建说明

### 1. 克隆项目
```bash
git clone https://github.com/lancedb/lance.git
cd lance/cpp
```

### 2. 构建Rust FFI库
```bash
cd lance-cpp-ffi
cargo build --release
cd ..
```

### 3. 构建C++库
```bash
mkdir build && cd build
cmake ..
make -j$(nproc)
```

### 4. 安装
```bash
sudo make install
```

## 使用方法

### 基本示例

```cpp
#include <lance/lance.hpp>
#include <arrow/api.h>

int main() {
    // 初始化Lance
    if (!lance::init()) {
        std::cerr << "Failed to initialize Lance" << std::endl;
        return 1;
    }

    try {
        // 创建测试数据
        auto schema = arrow::schema({
            arrow::field("id", arrow::int64()),
            arrow::field("name", arrow::utf8()),
            arrow::field("value", arrow::float64())
        });

        // 写入数据
        {
            lance::LanceFileWriter writer("test.lance", schema);
            
            auto id_array = arrow::Int64Array::Make({1, 2, 3}).ValueOrDie();
            auto name_array = arrow::StringArray::Make({"Alice", "Bob", "Charlie"}).ValueOrDie();
            auto value_array = arrow::DoubleArray::Make({1.1, 2.2, 3.3}).ValueOrDie();
            
            auto batch = arrow::RecordBatch::Make(schema, 3, {id_array, name_array, value_array});
            writer.write_batch(batch);
            
            uint64_t rows_written = writer.finish();
            std::cout << "Wrote " << rows_written << " rows" << std::endl;
        }

        // 读取数据
        {
            auto reader = lance::LanceFileReader::open("test.lance");
            std::cout << "File has " << reader.num_rows() << " rows" << std::endl;
            
            auto table = reader.read_all();
            std::cout << "Read table with " << table->num_rows() << " rows" << std::endl;
        }

        // 数据集操作
        {
            auto dataset = lance::Dataset::open("test.lance");
            std::cout << "Dataset has " << dataset.count_rows() << " rows" << std::endl;
            
            auto scanner = dataset.scanner()
                .project({"id", "name"})
                .filter("id > 1");
            
            auto result = scanner.to_table();
            std::cout << "Filtered result has " << result->num_rows() << " rows" << std::endl;
        }

    } catch (const lance::LanceError& e) {
        std::cerr << "Lance error: " << e.what() << " (code: " << e.code() << ")" << std::endl;
        return 1;
    }

    lance::cleanup();
    return 0;
}
```

### 编译示例
```bash
g++ -std=c++17 -I/usr/local/include -L/usr/local/lib \
    -o example examples/basic_usage.cpp -llance-cpp -larrow
```

## API参考

### LanceFileReader

#### 构造函数
```cpp
static LanceFileReader open(
    const std::string& path,
    const std::map<std::string, std::string>& storage_options = {},
    const std::vector<std::string>& columns = {}
);
```

#### 方法
- `uint64_t num_rows() const`: 获取行数
- `std::shared_ptr<arrow::Schema> schema() const`: 获取模式
- `std::shared_ptr<arrow::Table> read_all(uint32_t batch_size = 1024) const`: 读取所有数据

### LanceFileWriter

#### 构造函数
```cpp
LanceFileWriter(
    const std::string& path,
    std::shared_ptr<arrow::Schema> schema = nullptr,
    const std::map<std::string, std::string>& storage_options = {}
);
```

#### 方法
- `void write_batch(std::shared_ptr<arrow::RecordBatch> batch)`: 写入批次数据
- `uint64_t finish()`: 完成写入并返回行数

### Dataset

#### 构造函数
```cpp
static Dataset open(
    const std::string& uri,
    const std::map<std::string, std::string>& storage_options = {}
);
```

#### 方法
- `std::shared_ptr<arrow::Schema> schema() const`: 获取模式
- `uint64_t count_rows() const`: 获取行数
- `Scanner scanner() const`: 创建扫描器

### Scanner

#### 方法
- `Scanner project(const std::vector<std::string>& columns)`: 列投影
- `Scanner filter(const std::string& filter_expr)`: 过滤查询
- `std::shared_ptr<arrow::Table> to_table()`: 转换为Arrow表

## 错误处理

所有Lance操作都使用异常处理机制：

```cpp
try {
    auto dataset = lance::Dataset::open("data.lance");
    // ... 操作
} catch (const lance::LanceError& e) {
    std::cerr << "Lance error: " << e.what() << " (code: " << e.code() << ")" << std::endl;
} catch (const lance::InvalidInputError& e) {
    std::cerr << "Invalid input: " << e.what() << std::endl;
} catch (const lance::SchemaMismatchError& e) {
    std::cerr << "Schema mismatch: " << e.what() << std::endl;
}
```

## 性能优化

### 批量操作
- 使用适当的批次大小进行读写操作
- 避免频繁的小批量操作

### 内存管理
- 使用RAII模式自动管理资源
- 及时释放不需要的对象

### 查询优化
- 使用列投影减少数据传输
- 合理使用过滤条件

## 测试

运行测试套件：

```bash
cd build
make test
```

或者手动运行：

```bash
# 安装Google Test
sudo apt-get install libgtest-dev

# 编译测试
g++ -std=c++17 -lgtest -lgtest_main -lpthread \
    -I../include -L. -llance-cpp -larrow \
    ../tests/test_file.cpp -o test_file

# 运行测试
./test_file
```

## 故障排除

### 常见问题

1. **链接错误**: 确保正确链接Arrow库
2. **运行时错误**: 检查Lance库是否正确安装
3. **编译错误**: 确保使用C++17标准

### 调试信息

启用详细日志：

```bash
export ARROW_LOG_LEVEL=debug
```

## 贡献指南

1. Fork项目
2. 创建功能分支
3. 提交更改
4. 推送到分支
5. 创建Pull Request

## 许可证

Apache License 2.0

## 支持

- 文档: [https://lancedb.github.io/lance/](https://lancedb.github.io/lance/)
- 问题报告: [GitHub Issues](https://github.com/lancedb/lance/issues)
- 讨论: [Discord](https://discord.gg/zMM32dvNtd)
