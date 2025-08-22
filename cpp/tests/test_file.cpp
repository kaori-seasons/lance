#include <gtest/gtest.h>
#include <lance/lance.hpp>
#include <arrow/api.h>
#include <filesystem>

class LanceFileTest : public ::testing::Test {
protected:
    void SetUp() override {
        lance::init();
        
        schema_ = arrow::schema({
            arrow::field("a", arrow::int64()),
            arrow::field("b", arrow::utf8())
        });
        
        test_file_ = "test_file.lance";
    }
    
    void TearDown() override {
        if (std::filesystem::exists(test_file_)) {
            std::filesystem::remove(test_file_);
        }
        lance::cleanup();
    }
    
    std::shared_ptr<arrow::Schema> schema_;
    std::string test_file_;
};

TEST_F(LanceFileTest, BasicReadWrite) {
    // Write data
    {
        lance::LanceFileWriter writer(test_file_, schema_);
        
        auto a_array = arrow::Int64Array::Make({1, 2, 3}).ValueOrDie();
        auto b_array = arrow::StringArray::Make({"x", "y", "z"}).ValueOrDie();
        auto batch = arrow::RecordBatch::Make(schema_, 3, {a_array, b_array});
        
        writer.write_batch(batch);
        uint64_t rows = writer.finish();
        EXPECT_EQ(rows, 3);
    }
    
    // Read data
    {
        auto reader = lance::LanceFileReader::open(test_file_);
        EXPECT_EQ(reader.num_rows(), 3);
        
        auto table = reader.read_all();
        EXPECT_EQ(table->num_rows(), 3);
        EXPECT_EQ(table->num_columns(), 2);
    }
}

TEST_F(LanceFileTest, SchemaOnly) {
    {
        lance::LanceFileWriter writer(test_file_, schema_);
        writer.finish();
    }
    
    auto reader = lance::LanceFileReader::open(test_file_);
    auto read_schema = reader.schema();
    EXPECT_TRUE(schema_->Equals(read_schema));
}
