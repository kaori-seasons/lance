#include <iostream>
#include <lance/lance.hpp>
#include <arrow/api.h>

int main() {
    try {
        // Initialize Lance
        if (!lance::init()) {
            std::cerr << "Failed to initialize Lance" << std::endl;
            return 1;
        }

        // Create test data
        auto schema = arrow::schema({
            arrow::field("id", arrow::int64()),
            arrow::field("name", arrow::utf8()),
            arrow::field("value", arrow::float64())
        });

        // Write data to file
        {
            lance::LanceFileWriter writer("test.lance", schema);
            
            // Create test batch
            auto id_array = arrow::Int64Array::Make({1, 2, 3}).ValueOrDie();
            auto name_array = arrow::StringArray::Make({"Alice", "Bob", "Charlie"}).ValueOrDie();
            auto value_array = arrow::DoubleArray::Make({1.1, 2.2, 3.3}).ValueOrDie();
            
            auto batch = arrow::RecordBatch::Make(schema, 3, {id_array, name_array, value_array});
            writer.write_batch(batch);
            
            uint64_t rows_written = writer.finish();
            std::cout << "Wrote " << rows_written << " rows" << std::endl;
        }

        // Read data from file
        {
            auto reader = lance::LanceFileReader::open("test.lance");
            std::cout << "File has " << reader.num_rows() << " rows" << std::endl;
            
            auto table = reader.read_all();
            std::cout << "Read table with " << table->num_rows() << " rows and " 
                      << table->num_columns() << " columns" << std::endl;
        }

        // Work with dataset
        {
            auto dataset = lance::Dataset::open("test.lance");
            std::cout << "Dataset has " << dataset.count_rows() << " rows" << std::endl;
            
            auto scanner = dataset.scanner()
                .project({"id", "name"})
                .filter("id > 1");
            
            auto result = scanner.to_table();
            std::cout << "Filtered result has " << result->num_rows() << " rows" << std::endl;
        }

        lance::cleanup();
        return 0;
        
    } catch (const lance::LanceError& e) {
        std::cerr << "Lance error: " << e.what() << " (code: " << e.code() << ")" << std::endl;
        return 1;
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
}
