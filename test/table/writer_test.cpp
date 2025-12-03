#include "gtest/gtest.h"

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

// 文件路径
std::string file_path = "test_cpp.tsfile";

// 验证数据正确性
void query_data(std::string table_name, std::vector<std::string> columns) {
    // 初始化 TsFile 存储模块
    storage::libtsfile_init();
    
    // 创建 TsFile 读取器对象，用于读取 TsFile 文件
    storage::TsFileReader reader;
    reader.open(file_path);

    // 查询
    std::int64_t start_time = INT64_MIN;
    std::int64_t end_time = INT64_MAX;
    storage::ResultSet* temp_ret = nullptr;
    ASSERT_EQ(0, reader.query(table_name, columns, start_time, end_time, temp_ret));

    // 强制转换为 TableResultSet 类型
    auto ret = dynamic_cast<storage::TableResultSet*>(temp_ret);
    // 获取查询结果的元数据信息
    auto metadata = ret->get_metadata();
    // 获取查询结果的列数
    int column_num = metadata->get_column_count();
    // 获取列名
    for (int i = 1; i <= column_num; i++) {
        std::cout << metadata->get_column_name(i) << " ";
    }
    std::cout << std::endl;
    // 获取列类型
    for (int i = 1; i <= column_num; i++) {
        auto type = metadata->get_column_type(i);
        switch (type) {
            case common::TSDataType::INT32: 
                std::cout << "INT32 ";
                break;
            case common::TSDataType::INT64: 
                std::cout << "INT64 ";
                break;
            case common::TSDataType::FLOAT: 
                std::cout << "FLOAT ";
                break;
            case common::TSDataType::DOUBLE: 
                std::cout << "DOUBLE ";
                break;
            case common::TSDataType::STRING: 
                std::cout << "STRING ";
                break;
            default: std::cout << "UNKNOWN: " << metadata->get_column_type(i) << " ";break;
        }
    }
    std::cout << std::endl;

    // 定义一个布尔变量，用于标记是否有更多数据
    bool has_next = false;
    // 循环读取查询结果中的数据
    while ((ret->next(has_next)) == common::E_OK && has_next) {
        // 获取时间戳
        if (ret->is_null("time")){
            std::cout << "null" << " ";
        } else {
            Timestamp timestamp1 = ret->get_value<Timestamp>(1);
            std::cout << timestamp1 << " ";
            Timestamp timestamp2 = ret->get_value<Timestamp>("time");
            std::cout << timestamp2 << " ";
        }
        // 获取值
        for (int i = 1; i <= column_num; i++) {
            if (ret->is_null(i)) {
                std::cout << "null" << " ";
            } else {
                switch (metadata->get_column_type(i)) {
                    case common::INT32:
                        std::cout << ret->get_value<int32_t>(i) << " ";
                        break;
                    case common::INT64:
                        std::cout << ret->get_value<int64_t>(i) << " ";
                        break;
                    case common::FLOAT:
                        std::cout << ret->get_value<float>(i) << " ";
                        break;
                    case common::DOUBLE:
                        std::cout << ret->get_value<double>(i) << " ";
                        break;
                    case common::STRING:
                        std::cout << ret->get_value<common::String*>(i)
                                         ->to_std_string()
                                  << " ";
                        break;
                    default:;
                }
            }
        }
        std::cout << std::endl;
    }

    // 关闭查询结果集，释放资源
    ret->close();

    // 关闭查询接口，释放资源.
    reader.close();
}

// 输出数据格式定义
std::string get_column_definition(
    std::optional<std::string> column_name = std::nullopt,
    std::optional<common::TSDataType> data_type = std::nullopt,
    std::optional<common::ColumnCategory> column_category = std::nullopt) {
        std::string result;

        if (column_name) {
            result += "Column Name: " + *column_name + ", ";
        } else {
            result += "Column Name: (empty), ";
        }

        if (data_type) {
            result += "Data Type: ";
            switch (*data_type) {
                case common::TSDataType::INT64:
                    result += "INT64, ";
                    break;
                case common::TSDataType::INT32:
                    result += "INT32, ";
                    break;
                case common::TSDataType::FLOAT:
                    result += "FLOAT, ";
                    break;
                case common::TSDataType::DOUBLE:
                    result += "DOUBLE, ";
                    break;
                case common::TSDataType::STRING:
                    result += "STRING, ";
                    break;
                default:
                    result += "UNKNOWN, ";
            }
        } else {
            result += "Data Type: (empty), ";
        }

        if (column_category) {
            result += "Column Category: ";
            switch (*column_category) {
                case common::ColumnCategory::TAG:
                    result += "TAG";
                    break;
                case common::ColumnCategory::FIELD:
                    result += "FIELD";
                    break;
                default:
                    result += "UNKNOWN";
            }
        } else {
            result += "Column Category: (empty)";
        }
    
        return result; 
}

// 每个测试用例前后调用：用于清理环境
class test_writer : public ::testing::Test {
    protected:
        // 在每个测试用例执行之前调用
        void SetUp() override {
            // 确认目录存在
            if (!std::filesystem::exists("../data/tsfile") || !std::filesystem::is_directory("../data/tsfile")) {
                std::cerr << "Directory does not exist: " << "../data" << std::endl;
                return;
            }
            // 删除文件
            for (const auto& entry : std::filesystem::directory_iterator("../data/tsfile")) {
                if (std::filesystem::is_regular_file(entry)) {
                    std::filesystem::remove(entry.path());
                    std::cout << "Deleted file: " << entry.path() << std::endl;
                }
            }
        }
};

// // 测试1：全部数据类型、列类别，顺序，不含空值，相同设备，相同时间分区
// TEST(test_writer, test_TsFileTableWriter1) {
//     // 初始化 TsFile 存储模块
//     storage::libtsfile_init();

//     // 创建一个具有指定路径的文件来写入tsfile
//     storage::WriteFile file;
//     int flags = O_WRONLY | O_CREAT | O_TRUNC;
//     mode_t mode = 0666;
//     file.create(file_path, flags, mode);

//     // 列名
//     std::vector<std::string> columnNames;
//     columnNames.emplace_back("tag1");
//     columnNames.emplace_back("tag2");
//     columnNames.emplace_back("f1");
//     columnNames.emplace_back("f2");
//     columnNames.emplace_back("f3");
//     columnNames.emplace_back("f4");
//     columnNames.emplace_back("f5");
//     columnNames.emplace_back("f6");
//     columnNames.emplace_back("f7");
//     columnNames.emplace_back("f8");

//     std::string table_name = "table1";
//     // 创建表模式来描述ts文件中的表
//     auto* schema = new storage::TableSchema(
//         table_name,
//         {
//             common::ColumnSchema("tag1", common::TSDataType::STRING, common::UNCOMPRESSED, common::PLAIN, common::ColumnCategory::TAG),
//             common::ColumnSchema("tag2", common::TSDataType::STRING, common::UNCOMPRESSED, common::PLAIN, common::ColumnCategory::TAG),
//             common::ColumnSchema("f1", common::TSDataType::INT64, common::ColumnCategory::FIELD),
//             common::ColumnSchema("f2", common::TSDataType::INT32, common::UNCOMPRESSED, common::RLE, common::ColumnCategory::FIELD),
//             common::ColumnSchema("f3", common::TSDataType::FLOAT, common::ColumnCategory::FIELD),
//             common::ColumnSchema("f4", common::TSDataType::DOUBLE, common::UNCOMPRESSED, common::RLE, common::ColumnCategory::FIELD),
//             common::ColumnSchema("f5", common::TSDataType::INT64),
//             common::ColumnSchema("f6", common::TSDataType::INT32,common::ColumnCategory::FIELD),
//             common::ColumnSchema("f7", common::TSDataType::FLOAT),
//             common::ColumnSchema("f8", common::TSDataType::DOUBLE,common::ColumnCategory::FIELD),
//         });

//     // 写入的缓存大小
//     uint64_t memory_threshold = 1024;
//     // 创建一个具有指定路径的文件来写入tsfile
//     auto* writer = new storage::TsFileTableWriter(&file, schema, memory_threshold);
//     // auto* writer = new storage::TsFileTableWriter(&file, schema);

//     // 创建 tablet 以插入数据。
//     storage::Tablet tablet(
//         {
//             "tag1", 
//             "tag2", 
//             "f1", 
//             "f2", 
//             "f3", 
//             "f4", 
//             "f5", 
//             "f6",
//             "f7", 
//             "f8", 
//         },
//         {
//             common::TSDataType::STRING, 
//             common::TSDataType::STRING, 
//             common::TSDataType::INT64, 
//             common::TSDataType::INT32, 
//             common::TSDataType::FLOAT, 
//             common::TSDataType::DOUBLE, 
//             common::TSDataType::INT64, 
//             common::TSDataType::INT32, 
//             common::TSDataType::FLOAT, 
//             common::TSDataType::DOUBLE, 
//         },
//         1024);
//     // 写入不含空值
//     int64_t timestamp = 0;
//     for (int row = 0; row < 100; row++) {
//         ASSERT_EQ(0, tablet.add_timestamp(row, timestamp++));
//         ASSERT_EQ(0, tablet.add_value(row, "tag1", "tag1"));
//         ASSERT_EQ(0, tablet.add_value(row, "tag2", "tag2"));
//         ASSERT_EQ(0, tablet.add_value(row, "f1", static_cast<int64_t>(row)));
//         ASSERT_EQ(0, tablet.add_value(row, "f2", static_cast<int32_t>(row)));
//         ASSERT_EQ(0, tablet.add_value(row, "f3", static_cast<float>(row)));
//         ASSERT_EQ(0, tablet.add_value(row, "f4", static_cast<double>(row)));    
//         ASSERT_EQ(0, tablet.add_value(row, "f5", static_cast<int64_t>(row)));
//         ASSERT_EQ(0, tablet.add_value(row, "f6", static_cast<int32_t>(row)));
//         ASSERT_EQ(0, tablet.add_value(row, "f7", static_cast<float>(row)));
//         ASSERT_EQ(0, tablet.add_value(row, "f8", static_cast<double>(row)));
//     }
//     // 写入数据
//     ASSERT_EQ(0, writer->write_table(tablet));
//     // 刷新数据
//     ASSERT_EQ(0, writer->flush());

//     // 关闭写入
//     ASSERT_EQ(0, writer->close());

//     // 释放动态分配的内存
//     delete writer;
//     delete schema;

//     query_data(table_name, columnNames);
// }

// // 测试2：全部数据类型、列类别，顺序，不含空值，不同设备，相同时间分区
// TEST(test_writer, test_TsFileTableWriter2) {
//     // 初始化 TsFile 存储模块
//     storage::libtsfile_init();

//     // 创建一个具有指定路径的文件来写入tsfile
//     storage::WriteFile file;
//     int flags = O_WRONLY | O_CREAT | O_TRUNC;
//     mode_t mode = 0666;
//     file.create(file_path, flags, mode);

//     // 列名
//     std::vector<std::string> columnNames;
//     columnNames.emplace_back("tag1");
//     columnNames.emplace_back("tag2");
//     columnNames.emplace_back("f1");
//     columnNames.emplace_back("f2");
//     columnNames.emplace_back("f3");
//     columnNames.emplace_back("f4");
//     columnNames.emplace_back("f5");
//     columnNames.emplace_back("f6");
//     columnNames.emplace_back("f7");
//     columnNames.emplace_back("f8");

//     std::string table_name = "table1";
//     // 创建表模式来描述ts文件中的表
//     auto* schema = new storage::TableSchema(
//         table_name,
//         {
//             common::ColumnSchema("tag1", common::TSDataType::STRING, common::UNCOMPRESSED, common::PLAIN, common::ColumnCategory::TAG),
//             common::ColumnSchema("tag2", common::TSDataType::STRING, common::UNCOMPRESSED, common::PLAIN, common::ColumnCategory::TAG),
//             common::ColumnSchema("f1", common::TSDataType::INT64, common::ColumnCategory::FIELD),
//             common::ColumnSchema("f2", common::TSDataType::INT32, common::UNCOMPRESSED, common::RLE, common::ColumnCategory::FIELD),
//             common::ColumnSchema("f3", common::TSDataType::FLOAT, common::ColumnCategory::FIELD),
//             common::ColumnSchema("f4", common::TSDataType::DOUBLE, common::UNCOMPRESSED, common::RLE, common::ColumnCategory::FIELD),
//             common::ColumnSchema("f5", common::TSDataType::INT64),
//             common::ColumnSchema("f6", common::TSDataType::INT32,common::ColumnCategory::FIELD),
//             common::ColumnSchema("f7", common::TSDataType::FLOAT),
//             common::ColumnSchema("f8", common::TSDataType::DOUBLE,common::ColumnCategory::FIELD),
//         });

//     // 写入的缓存大小
//     uint64_t memory_threshold = 1024;
//     // 创建一个具有指定路径的文件来写入tsfile
//     auto* writer = new storage::TsFileTableWriter(&file, schema, memory_threshold);
//     // auto* writer = new storage::TsFileTableWriter(&file, schema);

//     // 创建 tablet 以插入数据。
//     storage::Tablet tablet(
//         {
//             "tag1", 
//             "tag2", 
//             "f1", 
//             "f2", 
//             "f3", 
//             "f4", 
//             "f5", 
//             "f6",
//             "f7", 
//             "f8", 
//         },
//         {
//             common::TSDataType::STRING, 
//             common::TSDataType::STRING, 
//             common::TSDataType::INT64, 
//             common::TSDataType::INT32, 
//             common::TSDataType::FLOAT, 
//             common::TSDataType::DOUBLE, 
//             common::TSDataType::INT64, 
//             common::TSDataType::INT32, 
//             common::TSDataType::FLOAT, 
//             common::TSDataType::DOUBLE, 
//         },
//         1024);
//     // 写入不含空值
//     int64_t timestamp = 0;
//     for (int row = 0; row < 100; row++) {
//         ASSERT_EQ(0, tablet.add_timestamp(row, timestamp++));
//         ASSERT_EQ(0, tablet.add_value(row, "tag1", ("tag1_" + std::to_string(row)).c_str()));
//         ASSERT_EQ(0, tablet.add_value(row, "tag2", ("tag2_" + std::to_string(row)).c_str()));
//         ASSERT_EQ(0, tablet.add_value(row, "f1", static_cast<int64_t>(row)));
//         ASSERT_EQ(0, tablet.add_value(row, "f2", static_cast<int32_t>(row)));
//         ASSERT_EQ(0, tablet.add_value(row, "f3", static_cast<float>(row)));
//         ASSERT_EQ(0, tablet.add_value(row, "f4", static_cast<double>(row)));    
//         ASSERT_EQ(0, tablet.add_value(row, "f5", static_cast<int64_t>(row)));
//         ASSERT_EQ(0, tablet.add_value(row, "f6", static_cast<int32_t>(row)));
//         ASSERT_EQ(0, tablet.add_value(row, "f7", static_cast<float>(row)));
//         ASSERT_EQ(0, tablet.add_value(row, "f8", static_cast<double>(row)));
//     }
//     // 写入数据
//     ASSERT_EQ(0, writer->write_table(tablet));
//     // 刷新数据
//     ASSERT_EQ(0, writer->flush());

//     // 关闭写入
//     ASSERT_EQ(0, writer->close());

//     // 释放动态分配的内存
//     delete writer;
//     delete schema;

//     query_data(table_name, columnNames);
// }

// // 测试3：全部数据类型、列类别，顺序，不含空值，相同设备，不同时间分区
// TEST(test_writer, test_TsFileTableWriter3) {
//     // 初始化 TsFile 存储模块
//     storage::libtsfile_init();

//     // 创建一个具有指定路径的文件来写入tsfile
//     storage::WriteFile file;
//     int flags = O_WRONLY | O_CREAT | O_TRUNC;
//     mode_t mode = 0666;
//     file.create(file_path, flags, mode);

//     // 列名
//     std::vector<std::string> columnNames;
//     columnNames.emplace_back("tag1");
//     columnNames.emplace_back("tag2");
//     columnNames.emplace_back("f1");
//     columnNames.emplace_back("f2");
//     columnNames.emplace_back("f3");
//     columnNames.emplace_back("f4");
//     columnNames.emplace_back("f5");
//     columnNames.emplace_back("f6");
//     columnNames.emplace_back("f7");
//     columnNames.emplace_back("f8");

//     std::string table_name = "table1";
//     // 创建表模式来描述ts文件中的表
//     auto* schema = new storage::TableSchema(
//         table_name,
//         {
//             common::ColumnSchema("tag1", common::TSDataType::STRING, common::UNCOMPRESSED, common::PLAIN, common::ColumnCategory::TAG),
//             common::ColumnSchema("tag2", common::TSDataType::STRING, common::UNCOMPRESSED, common::PLAIN, common::ColumnCategory::TAG),
//             common::ColumnSchema("f1", common::TSDataType::INT64, common::ColumnCategory::FIELD),
//             common::ColumnSchema("f2", common::TSDataType::INT32, common::UNCOMPRESSED, common::RLE, common::ColumnCategory::FIELD),
//             common::ColumnSchema("f3", common::TSDataType::FLOAT, common::ColumnCategory::FIELD),
//             common::ColumnSchema("f4", common::TSDataType::DOUBLE, common::UNCOMPRESSED, common::RLE, common::ColumnCategory::FIELD),
//             common::ColumnSchema("f5", common::TSDataType::INT64),
//             common::ColumnSchema("f6", common::TSDataType::INT32,common::ColumnCategory::FIELD),
//             common::ColumnSchema("f7", common::TSDataType::FLOAT),
//             common::ColumnSchema("f8", common::TSDataType::DOUBLE,common::ColumnCategory::FIELD),
//         });

//     // 写入的缓存大小
//     uint64_t memory_threshold = 1024;
//     // 创建一个具有指定路径的文件来写入tsfile
//     auto* writer = new storage::TsFileTableWriter(&file, schema, memory_threshold);
//     // auto* writer = new storage::TsFileTableWriter(&file, schema);

//     // 创建 tablet 以插入数据。
//     storage::Tablet tablet(
//         {
//             "tag1", 
//             "tag2", 
//             "f1", 
//             "f2", 
//             "f3", 
//             "f4", 
//             "f5", 
//             "f6",
//             "f7", 
//             "f8", 
//         },
//         {
//             common::TSDataType::STRING, 
//             common::TSDataType::STRING, 
//             common::TSDataType::INT64, 
//             common::TSDataType::INT32, 
//             common::TSDataType::FLOAT, 
//             common::TSDataType::DOUBLE, 
//             common::TSDataType::INT64, 
//             common::TSDataType::INT32, 
//             common::TSDataType::FLOAT, 
//             common::TSDataType::DOUBLE, 
//         },
//         1024);
//     // 写入不含空值
//     int64_t timestamp[] = {
//         -9223372036854775807LL,        
//         -9223372036854775LL,           
//         -92233720368547LL,            
//         -9223372036LL,                
//         -2147483648LL,               
//         0,                         
//         2147483647LL,               
//         9223372036854LL,           
//         9223372036854775LL,         
//         9223372036854775807LL      
//     };
//     for (int row = 0; row < 10; row++) {
//         ASSERT_EQ(0, tablet.add_timestamp(row, timestamp[row]));
//         ASSERT_EQ(0, tablet.add_value(row, "tag1", "tag1"));
//         ASSERT_EQ(0, tablet.add_value(row, "tag2", "tag2"));
//         ASSERT_EQ(0, tablet.add_value(row, "f1", static_cast<int64_t>(row)));
//         ASSERT_EQ(0, tablet.add_value(row, "f2", static_cast<int32_t>(row)));
//         ASSERT_EQ(0, tablet.add_value(row, "f3", static_cast<float>(row)));
//         ASSERT_EQ(0, tablet.add_value(row, "f4", static_cast<double>(row)));    
//         ASSERT_EQ(0, tablet.add_value(row, "f5", static_cast<int64_t>(row)));
//         ASSERT_EQ(0, tablet.add_value(row, "f6", static_cast<int32_t>(row)));
//         ASSERT_EQ(0, tablet.add_value(row, "f7", static_cast<float>(row)));
//         ASSERT_EQ(0, tablet.add_value(row, "f8", static_cast<double>(row)));
//     }
//     // 写入数据
//     ASSERT_EQ(0, writer->write_table(tablet));
//     // 刷新数据
//     ASSERT_EQ(0, writer->flush());

//     // 关闭写入
//     ASSERT_EQ(0, writer->close());

//     // 释放动态分配的内存
//     delete writer;
//     delete schema;

//     query_data(table_name, columnNames);
// }

// // 测试4：全部数据类型、列类别，顺序，不含空值，不同设备，不同时间分区
// TEST(test_writer, test_TsFileTableWriter4) {
//     // 初始化 TsFile 存储模块
//     storage::libtsfile_init();

//     // 创建一个具有指定路径的文件来写入tsfile
//     storage::WriteFile file;
//     int flags = O_WRONLY | O_CREAT | O_TRUNC;
//     mode_t mode = 0666;
//     file.create(file_path, flags, mode);

//     // 列名
//     std::vector<std::string> columnNames;
//     columnNames.emplace_back("tag1");
//     columnNames.emplace_back("tag2");
//     columnNames.emplace_back("f1");
//     columnNames.emplace_back("f2");
//     columnNames.emplace_back("f3");
//     columnNames.emplace_back("f4");
//     columnNames.emplace_back("f5");
//     columnNames.emplace_back("f6");
//     columnNames.emplace_back("f7");
//     columnNames.emplace_back("f8");

//     std::string table_name = "table1";
//     // 创建表模式来描述ts文件中的表
//     auto* schema = new storage::TableSchema(
//         table_name,
//         {
//             common::ColumnSchema("tag1", common::TSDataType::STRING, common::UNCOMPRESSED, common::PLAIN, common::ColumnCategory::TAG),
//             common::ColumnSchema("tag2", common::TSDataType::STRING, common::UNCOMPRESSED, common::PLAIN, common::ColumnCategory::TAG),
//             common::ColumnSchema("f1", common::TSDataType::INT64, common::ColumnCategory::FIELD),
//             common::ColumnSchema("f2", common::TSDataType::INT32, common::UNCOMPRESSED, common::RLE, common::ColumnCategory::FIELD),
//             common::ColumnSchema("f3", common::TSDataType::FLOAT, common::ColumnCategory::FIELD),
//             common::ColumnSchema("f4", common::TSDataType::DOUBLE, common::UNCOMPRESSED, common::RLE, common::ColumnCategory::FIELD),
//             common::ColumnSchema("f5", common::TSDataType::INT64),
//             common::ColumnSchema("f6", common::TSDataType::INT32,common::ColumnCategory::FIELD),
//             common::ColumnSchema("f7", common::TSDataType::FLOAT),
//             common::ColumnSchema("f8", common::TSDataType::DOUBLE,common::ColumnCategory::FIELD),
//         });

//     // 写入的缓存大小
//     uint64_t memory_threshold = 1024;
//     // 创建一个具有指定路径的文件来写入tsfile
//     auto* writer = new storage::TsFileTableWriter(&file, schema, memory_threshold);
//     // auto* writer = new storage::TsFileTableWriter(&file, schema);

//     // 创建 tablet 以插入数据。
//     storage::Tablet tablet(
//         {
//             "tag1", 
//             "tag2", 
//             "f1", 
//             "f2", 
//             "f3", 
//             "f4", 
//             "f5", 
//             "f6",
//             "f7", 
//             "f8", 
//         },
//         {
//             common::TSDataType::STRING, 
//             common::TSDataType::STRING, 
//             common::TSDataType::INT64, 
//             common::TSDataType::INT32, 
//             common::TSDataType::FLOAT, 
//             common::TSDataType::DOUBLE, 
//             common::TSDataType::INT64, 
//             common::TSDataType::INT32, 
//             common::TSDataType::FLOAT, 
//             common::TSDataType::DOUBLE, 
//         },
//         1024);
//     // 写入不含空值
//     int64_t timestamp[] = {
//         -9223372036854775807LL,        
//         -9223372036854775LL,           
//         -92233720368547LL,            
//         -9223372036LL,                
//         -2147483648LL,               
//         0,                         
//         2147483647LL,               
//         9223372036854LL,           
//         9223372036854775LL,         
//         9223372036854775807LL      
//     };
//     for (int row = 0; row < 10; row++) {
//         ASSERT_EQ(0, tablet.add_timestamp(row, timestamp[row]));
//         ASSERT_EQ(0, tablet.add_value(row, "tag1", ("tag1_" + std::to_string(row)).c_str()));
//         ASSERT_EQ(0, tablet.add_value(row, "tag2", ("tag2_" + std::to_string(row)).c_str()));
//         ASSERT_EQ(0, tablet.add_value(row, "f1", static_cast<int64_t>(row)));
//         ASSERT_EQ(0, tablet.add_value(row, "f2", static_cast<int32_t>(row)));
//         ASSERT_EQ(0, tablet.add_value(row, "f3", static_cast<float>(row)));
//         ASSERT_EQ(0, tablet.add_value(row, "f4", static_cast<double>(row)));    
//         ASSERT_EQ(0, tablet.add_value(row, "f5", static_cast<int64_t>(row)));
//         ASSERT_EQ(0, tablet.add_value(row, "f6", static_cast<int32_t>(row)));
//         ASSERT_EQ(0, tablet.add_value(row, "f7", static_cast<float>(row)));
//         ASSERT_EQ(0, tablet.add_value(row, "f8", static_cast<double>(row)));
//     }
//     // 写入数据
//     ASSERT_EQ(0, writer->write_table(tablet));
//     // 刷新数据
//     ASSERT_EQ(0, writer->flush());

//     // 关闭写入
//     ASSERT_EQ(0, writer->close());

//     // 释放动态分配的内存
//     delete writer;
//     delete schema;

//     query_data(table_name, columnNames);
// }

// // 测试5：含空值，TAG或FIELD列全空，全空
// TEST(test_writer, test_TsFileTableWriter5) {
//     storage::libtsfile_init();

//     storage::WriteFile file;
//     int flags = O_WRONLY | O_CREAT | O_TRUNC;
//     mode_t mode = 0666;
//     file.create(file_path, flags, mode);

//     std::vector<std::string> columnNames;
//     columnNames.emplace_back("tag1");
//     columnNames.emplace_back("tag2");
//     columnNames.emplace_back("f1");
//     columnNames.emplace_back("f2");
//     columnNames.emplace_back("f3");
//     columnNames.emplace_back("f4");
//     columnNames.emplace_back("f5");
//     columnNames.emplace_back("f6");
//     columnNames.emplace_back("f7");
//     columnNames.emplace_back("f8");

//     std::string table_name = "table1";
//     auto* schema = new storage::TableSchema(
//         table_name,
//         {
//             common::ColumnSchema("tag1", common::TSDataType::STRING, common::UNCOMPRESSED, common::PLAIN, common::ColumnCategory::TAG),
//             common::ColumnSchema("tag2", common::TSDataType::STRING, common::UNCOMPRESSED, common::PLAIN, common::ColumnCategory::TAG),
//             common::ColumnSchema("f1", common::TSDataType::INT64, common::ColumnCategory::FIELD),
//             common::ColumnSchema("f2", common::TSDataType::INT32, common::UNCOMPRESSED, common::RLE, common::ColumnCategory::FIELD),
//             common::ColumnSchema("f3", common::TSDataType::FLOAT, common::ColumnCategory::FIELD),
//             common::ColumnSchema("f4", common::TSDataType::DOUBLE, common::UNCOMPRESSED, common::RLE, common::ColumnCategory::FIELD),
//             common::ColumnSchema("f5", common::TSDataType::INT64),
//             common::ColumnSchema("f6", common::TSDataType::INT32,common::ColumnCategory::FIELD),
//             common::ColumnSchema("f7", common::TSDataType::FLOAT),
//             common::ColumnSchema("f8", common::TSDataType::DOUBLE,common::ColumnCategory::FIELD),
//         });
    
//     auto* writer = new storage::TsFileTableWriter(&file, schema);
//     storage::Tablet tablet(
//         {
//             "tag1", 
//             "tag2", 
//             "f1", 
//             "f2", 
//             "f3", 
//             "f4", 
//             "f5", 
//             "f6",
//             "f7", 
//             "f8", 
//         },
//         {
//             common::TSDataType::STRING, 
//             common::TSDataType::STRING, 
//             common::TSDataType::INT64, 
//             common::TSDataType::INT32, 
//             common::TSDataType::FLOAT, 
//             common::TSDataType::DOUBLE, 
//             common::TSDataType::INT64, 
//             common::TSDataType::INT32, 
//             common::TSDataType::FLOAT, 
//             common::TSDataType::DOUBLE, 
//         },
//         1024);
//     int64_t timestamp = 0;
//     // 写入含空值
//     for (int row = 0; row < 100; row++) {
//         ASSERT_EQ(0, tablet.add_timestamp(row, timestamp++));
//         if (row % 2 == 0) {
//             ASSERT_EQ(0, tablet.add_value(row, "tag1", "tag1"));
//             ASSERT_EQ(0, tablet.add_value(row, "f1", static_cast<int64_t>(row)));
//             ASSERT_EQ(0, tablet.add_value(row, "f2", static_cast<int32_t>(row)));
//             ASSERT_EQ(0, tablet.add_value(row, "f3", static_cast<float>(row)));
//             ASSERT_EQ(0, tablet.add_value(row, "f4", static_cast<double>(row))); 
//         } else {
//             ASSERT_EQ(0, tablet.add_value(row, "tag2", "tag2"));
//             ASSERT_EQ(0, tablet.add_value(row, "f5", static_cast<int64_t>(row)));
//             ASSERT_EQ(0, tablet.add_value(row, "f6", static_cast<int32_t>(row)));
//             ASSERT_EQ(0, tablet.add_value(row, "f7", static_cast<float>(row)));
//             ASSERT_EQ(0, tablet.add_value(row, "f8", static_cast<double>(row)));
//         }
//     }
//     ASSERT_EQ(0, writer->write_table(tablet));
//     ASSERT_EQ(0, writer->flush());
//     // 写入TAG列或FILED列全空值
//     for (int row = 0; row < 100; row++) {
//         ASSERT_EQ(0, tablet.add_timestamp(row, timestamp++));
//         if (row % 2 == 0) {
//             ASSERT_EQ(0, tablet.add_value(row, "tag1", "tag1"));
//             ASSERT_EQ(0, tablet.add_value(row, "tag2", "tag2"));
//         } else {
//             ASSERT_EQ(0, tablet.add_value(row, "f1", static_cast<int64_t>(row)));
//             ASSERT_EQ(0, tablet.add_value(row, "f2", static_cast<int32_t>(row)));
//             ASSERT_EQ(0, tablet.add_value(row, "f3", static_cast<float>(row)));
//             ASSERT_EQ(0, tablet.add_value(row, "f4", static_cast<double>(row))); 
//             ASSERT_EQ(0, tablet.add_value(row, "f5", static_cast<int64_t>(row)));
//             ASSERT_EQ(0, tablet.add_value(row, "f6", static_cast<int32_t>(row)));
//             ASSERT_EQ(0, tablet.add_value(row, "f7", static_cast<float>(row)));
//             ASSERT_EQ(0, tablet.add_value(row, "f8", static_cast<double>(row)));
//         }
//     }
//     ASSERT_EQ(0, writer->write_table(tablet));
//     ASSERT_EQ(0, writer->flush());
//     // 全空值
//     for (int row = 0; row < 100; row++) {
//         ASSERT_EQ(0, tablet.add_timestamp(row, timestamp++));
//     }
//     ASSERT_EQ(0, writer->write_table(tablet));
//     ASSERT_EQ(0, writer->flush());
//     ASSERT_EQ(0, writer->close());

//     delete writer;
//     delete schema;

//     // 验证数据正确性
//     query_data(table_name, columnNames);
// }

// // 测试6：1万TAG和FIELD列，1行
// TEST(test_writer, test_TsFileTableWriter6) {
//     // 初始化 TsFile 存储模块
//     storage::libtsfile_init();

//     // 创建一个具有指定路径的文件来写入tsfile
//     storage::WriteFile file;
//     int flags = O_WRONLY | O_CREAT | O_TRUNC;
//     mode_t mode = 0666;
//     file.create(file_path, flags, mode);

    
//     std::string table_name = "table1"; // 表名
//     std::vector<std::string> columnNames; // 列名
//     std::vector<common::ColumnSchema> column_schemas; // 列的元数据信息
//     std::vector<common::TSDataType> data_types; // 数据类型
//     std::size_t tag_count = 1000; // tag列数量
//     std::size_t field_count = 1000; // field列数量
//     std::size_t column_count = tag_count + field_count * 4; // 总列数
//     // Tag 列
//     for (size_t i = 0; i < tag_count; i++)
//     {
//         std::string tag_name = "tag_" + std::to_string(i);
//         columnNames.emplace_back(tag_name);
//         data_types.emplace_back(common::TSDataType::STRING);
//         column_schemas.emplace_back(tag_name, common::TSDataType::STRING, common::UNCOMPRESSED, common::PLAIN, common::ColumnCategory::TAG);
//     }
//     // Field 列
//     for (size_t i = 0; i < field_count; i++)
//     {
//         std::string field_name1 = "int64_" + std::to_string(i);
//         columnNames.emplace_back(field_name1);
//         data_types.emplace_back(common::TSDataType::INT64);
//         column_schemas.emplace_back(field_name1, common::TSDataType::INT64, common::UNCOMPRESSED, common::PLAIN, common::ColumnCategory::FIELD);
//         std::string field_name2 = "int32_" + std::to_string(i);
//         columnNames.emplace_back(field_name2);
//         data_types.emplace_back(common::TSDataType::INT32);
//         column_schemas.emplace_back(field_name2, common::TSDataType::INT32, common::UNCOMPRESSED, common::PLAIN, common::ColumnCategory::FIELD);
//         std::string field_name3 = "float_" + std::to_string(i);
//         columnNames.emplace_back(field_name3);
//         data_types.emplace_back(common::TSDataType::FLOAT);
//         column_schemas.emplace_back(field_name3, common::TSDataType::FLOAT, common::UNCOMPRESSED, common::PLAIN, common::ColumnCategory::FIELD);
//         std::string field_name4 = "double_" + std::to_string(i);
//         columnNames.emplace_back(field_name4);
//         data_types.emplace_back(common::TSDataType::DOUBLE);
//         column_schemas.emplace_back(field_name4, common::TSDataType::DOUBLE, common::UNCOMPRESSED, common::PLAIN, common::ColumnCategory::FIELD);
//     }
//     // 构建表的元数据
//     auto* schema = new storage::TableSchema(table_name,column_schemas); 

//     // 构建写入器
//     auto* writer = new storage::TsFileTableWriter(&file,schema); 

//     // 创建 tablet 以插入数据。
//     int max_rows = 1; // 最大行数
//     storage::Tablet tablet(columnNames,data_types,max_rows);
//     int64_t timestamp = 0;
//     for (int row = 0; row < max_rows; row++) {
//         ASSERT_EQ(0, tablet.add_timestamp(row, timestamp++));
//         for (size_t i = 0; i < data_types.size(); i++)
//         {
//             switch (data_types[i])
//             {
//                 case common::TSDataType::INT64:
//                     ASSERT_EQ(0, tablet.add_value(row, columnNames[i], static_cast<int64_t>(row)));
//                     break;
//                 case common::TSDataType::INT32:
//                     ASSERT_EQ(0, tablet.add_value(row, columnNames[i], static_cast<int32_t>(row)));
//                     break;
//                 case common::TSDataType::FLOAT:
//                     ASSERT_EQ(0, tablet.add_value(row, columnNames[i], static_cast<float>(row)));
//                     break;
//                 case common::TSDataType::DOUBLE:
//                     ASSERT_EQ(0, tablet.add_value(row, columnNames[i], static_cast<double>(row)));
//                     break;
//                 case common::TSDataType::STRING:
//                     ASSERT_EQ(0, tablet.add_value(row, columnNames[i], ("tag" + std::to_string(row)).c_str()));
//                     break;
//                 default:
//                     break;
//             }
//         }  
//     }
//     ASSERT_EQ(0, writer->write_table(tablet)); // 写入数据
//     ASSERT_EQ(0, writer->flush()); // 刷新数据
//     ASSERT_EQ(0, writer->close()); // 关闭写入

//     // 释放动态分配的内存
//     delete writer;
//     delete schema;

//     // 验证数据正确性
//     query_data(table_name, columnNames);
// }

// 测试7：10个TAG和FIELD列，1万行写入
// TEST(test_writer, test_TsFileTableWriter7) {
//     // 初始化 TsFile 存储模块
//     storage::libtsfile_init();
//     // 创建一个具有指定路径的文件来写入tsfile
//     storage::WriteFile file;
//     int flags = O_WRONLY | O_CREAT | O_TRUNC;
//     mode_t mode = 0666;
//     file.create(file_path, flags, mode); 
//     std::string table_name = "table7"; // 表名
//     std::vector<std::string> columnNames; // 列名
//     std::vector<common::ColumnSchema> column_schemas; // 列的元数据信息
//     std::vector<common::TSDataType> data_types; // 数据类型
//     std::size_t tag_count = 2; // tag列数量
//     std::size_t field_count = 1; // field列数量
//     std::size_t column_count = tag_count + field_count * 4; // 总列数
//     // Tag 列
//     for (size_t i = 0; i < tag_count; i++)
//     {
//         std::string tag_name = "tag_" + std::to_string(i);
//         columnNames.emplace_back(tag_name);
//         data_types.emplace_back(common::TSDataType::STRING);
//         column_schemas.emplace_back(tag_name, common::TSDataType::STRING, common::UNCOMPRESSED, common::PLAIN, common::ColumnCategory::TAG);
//     }
//     // Field 列
//     for (size_t i = 0; i < field_count; i++)
//     {
//         std::string field_name1 = "int64_" + std::to_string(i);
//         columnNames.emplace_back(field_name1);
//         data_types.emplace_back(common::TSDataType::INT64);
//         column_schemas.emplace_back(field_name1, common::TSDataType::INT64, common::UNCOMPRESSED, common::PLAIN, common::ColumnCategory::FIELD);
//         std::string field_name2 = "int32_" + std::to_string(i);
//         columnNames.emplace_back(field_name2);
//         data_types.emplace_back(common::TSDataType::INT32);
//         column_schemas.emplace_back(field_name2, common::TSDataType::INT32, common::UNCOMPRESSED, common::PLAIN, common::ColumnCategory::FIELD);
//         std::string field_name3 = "float_" + std::to_string(i);
//         columnNames.emplace_back(field_name3);
//         data_types.emplace_back(common::TSDataType::FLOAT);
//         column_schemas.emplace_back(field_name3, common::TSDataType::FLOAT, common::UNCOMPRESSED, common::PLAIN, common::ColumnCategory::FIELD);
//         std::string field_name4 = "double_" + std::to_string(i);
//         columnNames.emplace_back(field_name4);
//         data_types.emplace_back(common::TSDataType::DOUBLE);
//         column_schemas.emplace_back(field_name4, common::TSDataType::DOUBLE, common::UNCOMPRESSED, common::PLAIN, common::ColumnCategory::FIELD);
//     }
//     // 构建表的元数据
//     auto* schema = new storage::TableSchema(table_name,column_schemas); 
//     // 构建写入器
//     auto* writer = new storage::TsFileTableWriter(&file,schema); 
//     // 创建 tablet 以插入数据。
//     int max_rows = 10000; // 最大行数
//     storage::Tablet tablet(columnNames,data_types,max_rows);
//     int64_t timestamp = 0;
//     for (int row = 0; row < max_rows; row++) {
//         ASSERT_EQ(0, tablet.add_timestamp(row, timestamp++));
//         for (size_t i = 0; i < data_types.size(); i++)
//         {
//             switch (data_types[i])
//             {
//                 case common::TSDataType::INT64:
//                     ASSERT_EQ(0, tablet.add_value(row, columnNames[i], static_cast<int64_t>(row)));
//                     break;
//                 case common::TSDataType::INT32:
//                     ASSERT_EQ(0, tablet.add_value(row, columnNames[i], static_cast<int32_t>(row)));
//                     break;
//                 case common::TSDataType::FLOAT:
//                     ASSERT_EQ(0, tablet.add_value(row, columnNames[i], static_cast<float>(row)));
//                     break;
//                 case common::TSDataType::DOUBLE:
//                     ASSERT_EQ(0, tablet.add_value(row, columnNames[i], static_cast<double>(row)));
//                     break;
//                 case common::TSDataType::STRING:
//                     ASSERT_EQ(0, tablet.add_value(row, columnNames[i], ("tag" + std::to_string(row)).c_str()));
//                     break;
//                 default:
//                     break;
//             }
//         }  
//     }
//     ASSERT_EQ(0, writer->write_table(tablet)); // 写入数据
//     ASSERT_EQ(0, writer->flush()); // 刷新数据
//     ASSERT_EQ(0, writer->close()); // 关闭写入
//     // 释放动态分配的内存
//     delete writer;
//     delete schema;
//     // 验证数据正确性
//     query_data(table_name, columnNames);
// }

// // 测试8：1万设备（TAG列），10测点（FIELD列），1万行写入
// TEST(test_writer, test_TsFileTableWriter8) {
//     // 初始化 TsFile 存储模块
//     storage::libtsfile_init();
//     // 创建一个具有指定路径的文件来写入tsfile
//     storage::WriteFile file;
//     int flags = O_WRONLY | O_CREAT | O_TRUNC;
//     mode_t mode = 0666;
//     file.create(file_path, flags, mode);
//     std::string table_name = "table1"; // 表名
//     std::vector<std::string> columnNames; // 列名
//     std::vector<common::ColumnSchema> column_schemas; // 列的元数据信息
//     std::vector<common::TSDataType> data_types; // 数据类型
//     std::size_t tag_count = 10000; // tag列数量
//     std::size_t field_count = 10; // field列数量
//     std::size_t column_count = tag_count + field_count * 4; // 总列数
//     // Tag 列
//     for (size_t i = 0; i < tag_count; i++)
//     {
//         std::string tag_name = "tag_" + std::to_string(i);
//         columnNames.emplace_back(tag_name);
//         data_types.emplace_back(common::TSDataType::STRING);
//         column_schemas.emplace_back(tag_name, common::TSDataType::STRING, common::UNCOMPRESSED, common::PLAIN, common::ColumnCategory::TAG);
//     }
//     // Field 列
//     for (size_t i = 0; i < field_count; i++)
//     {
//         std::string field_name1 = "int64_" + std::to_string(i);
//         columnNames.emplace_back(field_name1);
//         data_types.emplace_back(common::TSDataType::INT64);
//         column_schemas.emplace_back(field_name1, common::TSDataType::INT64, common::UNCOMPRESSED, common::PLAIN, common::ColumnCategory::FIELD);
//         std::string field_name2 = "int32_" + std::to_string(i);
//         columnNames.emplace_back(field_name2);
//         data_types.emplace_back(common::TSDataType::INT32);
//         column_schemas.emplace_back(field_name2, common::TSDataType::INT32, common::UNCOMPRESSED, common::PLAIN, common::ColumnCategory::FIELD);
//         std::string field_name3 = "float_" + std::to_string(i);
//         columnNames.emplace_back(field_name3);
//         data_types.emplace_back(common::TSDataType::FLOAT);
//         column_schemas.emplace_back(field_name3, common::TSDataType::FLOAT, common::UNCOMPRESSED, common::PLAIN, common::ColumnCategory::FIELD);
//         std::string field_name4 = "double_" + std::to_string(i);
//         columnNames.emplace_back(field_name4);
//         data_types.emplace_back(common::TSDataType::DOUBLE);
//         column_schemas.emplace_back(field_name4, common::TSDataType::DOUBLE, common::UNCOMPRESSED, common::PLAIN, common::ColumnCategory::FIELD);
//     }
//     // 构建表的元数据
//     auto* schema = new storage::TableSchema(table_name,column_schemas); 
//     // 构建写入器
//     auto* writer = new storage::TsFileTableWriter(&file,schema); 
//     // 创建 tablet 以插入数据。
//     int max_rows = 10000; // 最大行数
//     storage::Tablet tablet(columnNames,data_types,max_rows);
//     int64_t timestamp = 0;
//     for (int row = 0; row < max_rows; row++) {
//         ASSERT_EQ(0, tablet.add_timestamp(row, timestamp++));
//         for (size_t i = 0; i < data_types.size(); i++)
//         {
//             switch (data_types[i])
//             {
//                 case common::TSDataType::INT64:
//                     ASSERT_EQ(0, tablet.add_value(row, columnNames[i], static_cast<int64_t>(row)));
//                     break;
//                 case common::TSDataType::INT32:
//                     ASSERT_EQ(0, tablet.add_value(row, columnNames[i], static_cast<int32_t>(row)));
//                     break;
//                 case common::TSDataType::FLOAT:
//                     ASSERT_EQ(0, tablet.add_value(row, columnNames[i], static_cast<float>(row)));
//                     break;
//                 case common::TSDataType::DOUBLE:
//                     ASSERT_EQ(0, tablet.add_value(row, columnNames[i], static_cast<double>(row)));
//                     break;
//                 case common::TSDataType::STRING:
//                     ASSERT_EQ(0, tablet.add_value(row, columnNames[i], ("tag" + std::to_string(row)).c_str()));
//                     break;
//                 default:
//                     break;
//             }
//         }  
//     }
//     ASSERT_EQ(0, writer->write_table(tablet)); // 写入数据
//     ASSERT_EQ(0, writer->flush()); // 刷新数据
//     ASSERT_EQ(0, writer->close()); // 关闭写入
//     // 释放动态分配的内存
//     delete writer;
//     delete schema;
//     // 验证数据正确性
//     query_data(table_name, columnNames);
// }

// // 测试9：10设备（TAG列），1万测点（FIELD列），1万行写入
// TEST(test_writer, test_TsFileTableWriter9) {
//     // 初始化 TsFile 存储模块
//     storage::libtsfile_init();
//     // 创建一个具有指定路径的文件来写入tsfile
//     storage::WriteFile file;
//     int flags = O_WRONLY | O_CREAT | O_TRUNC;
//     mode_t mode = 0666;
//     file.create(file_path, flags, mode);
//     std::string table_name = "table10"; // 表名
//     std::vector<std::string> columnNames; // 列名
//     std::vector<common::ColumnSchema> column_schemas; // 列的元数据信息
//     std::vector<common::TSDataType> data_types; // 数据类型
//     std::vector<common::ColumnCategory> column_category; // 列类别
//     std::size_t tag_count = 10; // tag列数量
//     std::size_t field_count = 10000; // field列数量
//     std::size_t column_count = tag_count + field_count * 4; // 总列数
//     // Tag 列
//     for (size_t i = 0; i < tag_count; i++)
//     {
//         std::string tag_name = "tag_" + std::to_string(i);
//         columnNames.emplace_back(tag_name);
//         data_types.emplace_back(common::TSDataType::STRING);
//         column_category.emplace_back(common::ColumnCategory::TAG);
//         column_schemas.emplace_back(tag_name, common::TSDataType::STRING, common::UNCOMPRESSED, common::PLAIN, common::ColumnCategory::TAG);
//     }
//     // Field 列
//     for (size_t i = 0; i < field_count; i++)
//     {
//         std::string field_name1 = "int64_" + std::to_string(i);
//         columnNames.emplace_back(field_name1);
//         data_types.emplace_back(common::TSDataType::INT64);
//         column_category.emplace_back(common::ColumnCategory::FIELD);
//         column_schemas.emplace_back(field_name1, common::TSDataType::INT64, common::UNCOMPRESSED, common::PLAIN, common::ColumnCategory::FIELD);
//         std::string field_name2 = "int32_" + std::to_string(i);
//         columnNames.emplace_back(field_name2);
//         data_types.emplace_back(common::TSDataType::INT32);
//         column_category.emplace_back(common::ColumnCategory::FIELD);
//         column_schemas.emplace_back(field_name2, common::TSDataType::INT32, common::UNCOMPRESSED, common::PLAIN, common::ColumnCategory::FIELD);
//         std::string field_name3 = "float_" + std::to_string(i);
//         columnNames.emplace_back(field_name3);
//         data_types.emplace_back(common::TSDataType::FLOAT);
//         column_category.emplace_back(common::ColumnCategory::FIELD);
//         column_schemas.emplace_back(field_name3, common::TSDataType::FLOAT, common::UNCOMPRESSED, common::PLAIN, common::ColumnCategory::FIELD);
//         std::string field_name4 = "double_" + std::to_string(i);
//         columnNames.emplace_back(field_name4);
//         data_types.emplace_back(common::TSDataType::DOUBLE);
//         column_category.emplace_back(common::ColumnCategory::FIELD);
//         column_schemas.emplace_back(field_name4, common::TSDataType::DOUBLE, common::UNCOMPRESSED, common::PLAIN, common::ColumnCategory::FIELD);
//     }
//     // 构建表的元数据
//     auto* schema = new storage::TableSchema(table_name,column_schemas); 
//     // 构建写入器
//     auto* writer = new storage::TsFileTableWriter(&file,schema); 
//     // 创建 tablet 以插入数据。
//     int max_rows = 10000; // 最大行数
//     storage::Tablet tablet(table_name,columnNames,data_types,column_category,max_rows);
//     int64_t timestamp = 0;
//     for (int row = 0; row < max_rows; row++) {
//         ASSERT_EQ(0, tablet.add_timestamp(row, timestamp++));
//         for (size_t i = 0; i < data_types.size(); i++)
//         {
//             switch (data_types[i])
//             {
//                 case common::TSDataType::INT64:
//                     ASSERT_EQ(0, tablet.add_value(row, columnNames[i], static_cast<int64_t>(row)));
//                     break;
//                 case common::TSDataType::INT32:
//                     ASSERT_EQ(0, tablet.add_value(row, columnNames[i], static_cast<int32_t>(row)));
//                     break;
//                 case common::TSDataType::FLOAT:
//                     ASSERT_EQ(0, tablet.add_value(row, columnNames[i], static_cast<float>(row)));
//                     break;
//                 case common::TSDataType::DOUBLE:
//                     ASSERT_EQ(0, tablet.add_value(row, columnNames[i], static_cast<double>(row)));
//                     break;
//                 case common::TSDataType::STRING:
//                     ASSERT_EQ(0, tablet.add_value(row, columnNames[i], ("tag" + std::to_string(row)).c_str()));
//                     break;
//                 default:
//                     break;
//             }
//         }  
//     }
//     ASSERT_EQ(0, writer->write_table(tablet)); // 写入数据
//     ASSERT_EQ(0, writer->flush()); // 刷新数据
//     ASSERT_EQ(0, writer->close()); // 关闭写入
//     // 释放动态分配的内存
//     delete writer;
//     delete schema;
//     // 验证数据正确性
//     query_data(table_name, columnNames);
// }

// // 测试10：默认编码压缩
// TEST(test_writer, test_TsFileTableWriter10) {
//     // 初始化 TsFile 存储模块
//     storage::libtsfile_init();

//     // 创建一个具有指定路径的文件来写入tsfile
//     storage::WriteFile file;
//     int flags = O_WRONLY | O_CREAT | O_TRUNC;
//     mode_t mode = 0666;
//     file.create(file_path, flags, mode);

//     std::string table_name = "table10"; // 表名
//     // 列名
//     std::vector<std::string> columnNames = 
//     {
//         "tag1", 
//         "tag2", 
//         "f1", 
//         "f2", 
//         "f3", 
//         "f4", 
//         "f5", 
//         "f6", 
//         "f7", 
//         "f8"
//     };
//     // 数据类型
//     std::vector<common::TSDataType> data_types = 
//     {
//         common::TSDataType::STRING, 
//         common::TSDataType::STRING, 
//         common::TSDataType::INT64, 
//         common::TSDataType::INT32, 
//         common::TSDataType::FLOAT, 
//         common::TSDataType::DOUBLE, 
//         common::TSDataType::INT64, 
//         common::TSDataType::INT32, 
//         common::TSDataType::FLOAT, 
//         common::TSDataType::DOUBLE
//     };
//     // 列类别
//     std::vector<common::ColumnCategory> column_categories = 
//     {
//         common::ColumnCategory::TAG, 
//         common::ColumnCategory::TAG, 
//         common::ColumnCategory::FIELD, 
//         common::ColumnCategory::FIELD, 
//         common::ColumnCategory::FIELD, 
//         common::ColumnCategory::FIELD, 
//         common::ColumnCategory::FIELD, 
//         common::ColumnCategory::FIELD, 
//         common::ColumnCategory::FIELD, 
//         common::ColumnCategory::FIELD, 
//     };
//     // 列的元数据信息
//     std::vector<common::ColumnSchema> column_schemas;
//     for (size_t i = 0; i < columnNames.size(); ++i) {
//         column_schemas.emplace_back(
//             columnNames[i], 
//             data_types[i], 
//             column_categories[i]);
//     }
//     // 表的元数据信息
//     auto* schema = new storage::TableSchema(
//         table_name,
//         column_schemas);


//     // 写入数据
//     auto* writer = new storage::TsFileTableWriter(&file, schema);
//     int max_rows = 1024; // 最大行数
//     storage::Tablet tablet(
//         table_name,
//         columnNames,
//         data_types,
//         column_categories,
//         max_rows);
//     // 写入不含空值
//     int64_t timestamp = 0;
//     for (int row = 0; row < 100; row++) {
//         ASSERT_EQ(0, tablet.add_timestamp(row, timestamp++));
//         ASSERT_EQ(0, tablet.add_value(row, "tag1", "tag1"));
//         ASSERT_EQ(0, tablet.add_value(row, "tag2", "tag2"));
//         ASSERT_EQ(0, tablet.add_value(row, "f1", static_cast<int64_t>(row * -12345)));
//         ASSERT_EQ(0, tablet.add_value(row, "f2", static_cast<int32_t>(row * 12345)));
//         ASSERT_EQ(0, tablet.add_value(row, "f3", static_cast<float>(row * -12345.12345F)));
//         ASSERT_EQ(0, tablet.add_value(row, "f4", static_cast<double>(row * -12345.12345)));    
//         ASSERT_EQ(0, tablet.add_value(row, "f5", static_cast<int64_t>(row * 123456789)));
//         ASSERT_EQ(0, tablet.add_value(row, "f6", static_cast<int32_t>(row * 123456789)));
//         ASSERT_EQ(0, tablet.add_value(row, "f7", static_cast<float>(row * 123456789.123456789F)));
//         ASSERT_EQ(0, tablet.add_value(row, "f8", static_cast<double>(row * 123456789.123456789)));
//     }
//     // 写入数据
//     ASSERT_EQ(0, writer->write_table(tablet));
//     // 刷新数据
//     ASSERT_EQ(0, writer->flush());

//     // 关闭写入
//     ASSERT_EQ(0, writer->close());

//     // 释放动态分配的内存
//     delete writer;
//     delete schema;

//     query_data(table_name, columnNames);
// }

// // 测试11：多次执行flush
// TEST(test_writer, test_TsFileTableWriter11) {
//     // 初始化 TsFile 存储模块
//     storage::libtsfile_init();

//     // 创建一个具有指定路径的文件来写入tsfile
//     storage::WriteFile file;
//     int flags = O_WRONLY | O_CREAT | O_TRUNC;
//     mode_t mode = 0666;
//     file.create(file_path, flags, mode);

//     std::string table_name = "table11"; // 表名
//     // 列名
//     std::vector<std::string> columnNames = 
//     {
//         "tag1", 
//         "tag2", 
//         "f1", 
//         "f2", 
//         "f3", 
//         "f4", 
//         "f5", 
//         "f6", 
//         "f7", 
//         "f8"
//     };
//     // 数据类型
//     std::vector<common::TSDataType> data_types = 
//     {
//         common::TSDataType::STRING, 
//         common::TSDataType::STRING, 
//         common::TSDataType::INT64, 
//         common::TSDataType::INT32, 
//         common::TSDataType::FLOAT, 
//         common::TSDataType::DOUBLE, 
//         common::TSDataType::INT64, 
//         common::TSDataType::INT32, 
//         common::TSDataType::FLOAT, 
//         common::TSDataType::DOUBLE
//     };
//     // 列类别
//     std::vector<common::ColumnCategory> column_categories = 
//     {
//         common::ColumnCategory::TAG, 
//         common::ColumnCategory::TAG, 
//         common::ColumnCategory::FIELD, 
//         common::ColumnCategory::FIELD, 
//         common::ColumnCategory::FIELD, 
//         common::ColumnCategory::FIELD, 
//         common::ColumnCategory::FIELD, 
//         common::ColumnCategory::FIELD, 
//         common::ColumnCategory::FIELD, 
//         common::ColumnCategory::FIELD, 
//     };
//     // 列的元数据信息
//     std::vector<common::ColumnSchema> column_schemas;
//     for (size_t i = 0; i < columnNames.size(); ++i) {
//         column_schemas.emplace_back(
//             columnNames[i], 
//             data_types[i], 
//             column_categories[i]);
//     }
//     // 表的元数据信息
//     auto* schema = new storage::TableSchema(
//         table_name,
//         column_schemas);


//     // 写入数据
//     auto* writer = new storage::TsFileTableWriter(&file, schema);
//     int max_rows = 1024; // 最大行数
//     storage::Tablet tablet(
//         table_name,
//         columnNames,
//         data_types,
//         column_categories,
//         max_rows);
    
//     int64_t timestamp = 0;
//     int row_index = 0;
//     // 持续写入：10次，每次写入10行数据
//     for (int row_num = 0; row_num < 10; row_num++) {
//         ASSERT_EQ(0, tablet.add_timestamp(row_index, timestamp));
//         ASSERT_EQ(0, tablet.add_value(row_index, "tag1", "tag1"));
//         ASSERT_EQ(0, tablet.add_value(row_index, "tag2", "tag2"));
//         ASSERT_EQ(0, tablet.add_value(row_index, "f1", static_cast<int64_t>(row_num)));
//         ASSERT_EQ(0, tablet.add_value(row_index, "f2", static_cast<int32_t>(row_num)));
//         ASSERT_EQ(0, tablet.add_value(row_index, "f3", static_cast<float>(row_num)));
//         ASSERT_EQ(0, tablet.add_value(row_index, "f4", static_cast<double>(row_num)));    
//         ASSERT_EQ(0, tablet.add_value(row_index, "f5", static_cast<int64_t>(row_num)));
//         ASSERT_EQ(0, tablet.add_value(row_index, "f6", static_cast<int32_t>(row_num)));
//         ASSERT_EQ(0, tablet.add_value(row_index, "f7", static_cast<float>(row_num)));
//         ASSERT_EQ(0, tablet.add_value(row_index, "f8", static_cast<double>(row_num)));
//         timestamp++;
//         row_index++;
//     }
//     ASSERT_EQ(0, writer->write_table(tablet));
//     for (size_t i = 0; i < 1; i++)
//     {
//         ASSERT_EQ(0, writer->flush());
//     }
    
//     ASSERT_EQ(0, writer->close());

//     // 释放动态分配的内存
//     delete writer;
//     delete schema;

//     query_data(table_name, columnNames);
// }
