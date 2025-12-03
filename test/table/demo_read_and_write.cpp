#include "common/db_common.h"
#include "common/path.h"
#include "common/record.h"
#include "common/row_record.h"
#include "common/schema.h"
#include "common/tablet.h"
#include "file/write_file.h"
#include "reader/expression.h"
#include "reader/filter/filter.h"
#include "reader/qds_with_timegenerator.h"
#include "reader/qds_without_timegenerator.h"
#include "reader/tsfile_reader.h"
#include "writer/tsfile_table_writer.h"
#include "writer/tsfile_writer.h"
#include "cwrapper/tsfile_cwrapper.h"
#include "cwrapper/errno_define_c.h"
#include <cstdint>
#include <iostream>
#include <ostream>
#include <writer/tsfile_table_writer.h>
#include <string>
#include <filesystem>

#define HANDLE_ERROR(err_no)                  \
    do {                                      \
        if (err_no != 0) {                    \
            printf("get err no: %d", err_no); \
            return err_no;                    \
        }                                     \
    } while (0)

int demo_write() {

    storage::libtsfile_init();

    std::string table_name = "table1";

    // Create a file with specify path to write tsfile.
    storage::WriteFile file;
    int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef _WIN32
    flags |= O_BINARY;
#endif
    mode_t mode = 0666;
    file.create("test_cpp.tsfile", flags, mode);

    // Create table schema to describe a table in a tsfile.
    auto* schema = new storage::TableSchema(
        table_name,
        {
            common::ColumnSchema("tag1", common::TSDataType::STRING, common::ColumnCategory::TAG),
            common::ColumnSchema("TAG2", common::TSDataType::STRING, common::ColumnCategory::TAG),
            common::ColumnSchema("F1", common::TSDataType::BOOLEAN, common::ColumnCategory::FIELD),
            common::ColumnSchema("f2", common::TSDataType::INT32, common::ColumnCategory::FIELD),
            common::ColumnSchema("f3", common::TSDataType::INT64, common::ColumnCategory::FIELD),
            common::ColumnSchema("f4", common::TSDataType::FLOAT, common::ColumnCategory::FIELD),
            common::ColumnSchema("f5", common::TSDataType::DOUBLE, common::ColumnCategory::FIELD),
        });

    // Create a file with specify path to write tsfile.
    auto* writer = new storage::TsFileTableWriter(&file, schema);

    // Create tablet to insert data.
    storage::Tablet tablet(
        table_name,
        {
            "tag1", 
            "TAG2", 
            "F1", 
            "f2", 
            "f3", 
            "f4",
            "f5", 
        },
        {
            common::TSDataType::STRING, 
            common::TSDataType::STRING, 
            common::TSDataType::BOOLEAN, 
            common::TSDataType::INT32, 
            common::TSDataType::INT64, 
            common::TSDataType::FLOAT, 
            common::TSDataType::DOUBLE, 
        },
        {
            common::ColumnCategory::TAG, 
            common::ColumnCategory::TAG,
            common::ColumnCategory::FIELD,
            common::ColumnCategory::FIELD,
            common::ColumnCategory::FIELD,
            common::ColumnCategory::FIELD,
            common::ColumnCategory::FIELD,
        },
        1024);


    for (int row = 0; row < 10; row++) {
        long timestamp = row;
        tablet.add_timestamp(row, timestamp);
        if (row % 2 == 0) {
            tablet.add_value(row, "tag1", "tag1");
            tablet.add_value(row, "TAG2", "TAG2");
            tablet.add_value(row, "F1", true);
            tablet.add_value(row, "f2", static_cast<int32_t>(12345));
            tablet.add_value(row, "f3", static_cast<int64_t>(12345));
            tablet.add_value(row, "f4", static_cast<float>(12345.12345));
            // tablet.add_value(row, "f5", static_cast<double>(12345.12345));
        }
        
    }

    // Write tablet data.
    HANDLE_ERROR(writer->write_table(tablet));

    // Flush data
    HANDLE_ERROR(writer->flush());

    // Close writer.
    HANDLE_ERROR(writer->close());

    delete writer;
    delete schema;

    return 0;
}

int demo_read() {
    int code = 0;
    storage::libtsfile_init();
    std::string table_name = "table1";

    // Create tsfile reader and open tsfile with specify path.
    storage::TsFileReader reader;
    reader.open("test_cpp.tsfile");

    // Query data with tsfile reader.
    storage::ResultSet* temp_ret = nullptr;
    std::vector<std::string> columns;
    columns.emplace_back("tag1");
    columns.emplace_back("TAG2");
    columns.emplace_back("F1");
    columns.emplace_back("f2");
    columns.emplace_back("f3");
    columns.emplace_back("f4");
    columns.emplace_back("f5");

    // Column vector contains the columns you want to select.
    HANDLE_ERROR(reader.query(table_name, columns, INT64_MIN, INT64_MAX, temp_ret));

    // Get query handler.
    auto ret = dynamic_cast<storage::TableResultSet*>(temp_ret);

    // Metadata in query handler.
    auto metadata = ret->get_metadata();
    int column_num = metadata->get_column_count();
    for (int i = 1; i <= column_num; i++) {
        std::cout << "column name: " << metadata->get_column_name(i)
                  << " ";
        std::cout << "column type: "
                  << std::to_string(metadata->get_column_type(i)) 
                  << std::endl;
    }

    int num = 1;
    // Check and get next data.
    bool has_next = false;
    while ((code = ret->next(has_next)) == common::E_OK && has_next) {
        std::cout << "num:" << num++ << ": ";
        // Timestamp at column 1 and column index begin from 1.
        Timestamp timestamp = ret->get_value<Timestamp>(1);
        for (int i = 1; i <= column_num; i++) {
            if (ret->is_null(i)) {
                std::cout << metadata->get_column_name(i) << "：";
                std::cout << "null" << " ";
            } else {
                std::cout << metadata->get_column_name(i) << "：";
                switch (metadata->get_column_type(i)) {
                    case common::BOOLEAN:
                        std::cout << ret->get_value<bool>(i) << "      ";
                        break;
                    case common::INT32:
                        std::cout << ret->get_value<int32_t>(i) << "      ";
                        break;
                    case common::INT64:
                        std::cout << ret->get_value<int64_t>(i) << "      ";
                        break;
                    case common::FLOAT:
                        std::cout << ret->get_value<float>(i) << "      ";
                        break;
                    case common::DOUBLE:
                        std::cout << ret->get_value<double>(i) << "      ";
                        break;
                    case common::STRING:
                        std::cout << ret->get_value<common::String*>(i)
                                         ->to_std_string()
                                  << "      ";
                        break;
                    default:;
                }
            }
        }
        std::cout << std::endl;
    }

    // Close query result set.
    ret->close();

    // Close reader.
    reader.close();
    return 0;
}


int main() {
    demo_write();
    demo_read();
    return 0;
}