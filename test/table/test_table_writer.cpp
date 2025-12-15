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

using namespace storage;
using namespace common;
using namespace std;

// 文件路径
string file_path = "/data/iotdb-test/test/cpp-tsfile-api-test-v4/data/tsfile/test_table.tsfile";
// 创建一个具有指定路径的文件来写入tsfile
storage::WriteFile writer_file_;

/**
 * 将数据类型转换为字符串
 */
string datatype_to_string(common::TSDataType type) {
    switch (type) {
        case common::TSDataType::BOOLEAN:
            return "BOOLEAN";
        case common::TSDataType::INT32:
            return "INT32";
        case common::TSDataType::INT64:
            return "INT64";
        case common::TSDataType::FLOAT:
            return "FLOAT";
        case common::TSDataType::DOUBLE:
            return "DOUBLE";
        case common::TSDataType::TEXT:
            return "TEXT";
        case common::TSDataType::STRING:
            return "STRING";
        case common::TSDataType::BLOB:
            return "BLOB";
        case common::TSDataType::DATE:
            return "DATE";
        case common::TSDataType::TIMESTAMP:
            return "TIMESTAMP";
        default:
            return "INVALID_TYPE";
    }
}

// 验证数据正确性
void query_data(string table_name, vector<string> column_names, vector<common::TSDataType> data_types, int expect_row_num) {
    // 创建 TsFile 读取器对象，用于读取 TsFile 文件
    storage::TsFileReader reader;
    reader.open(file_path);

    // 查询
    int64_t start_time = INT64_MIN;
    int64_t end_time = INT64_MAX;
    storage::ResultSet* temp_ret = nullptr;
    ASSERT_EQ(reader.query(table_name, column_names, start_time, end_time, temp_ret), E_OK);

    // 强制转换为 TableResultSet 类型
    auto ret = dynamic_cast<storage::TableResultSet*>(temp_ret);
    // 获取查询结果的元数据信息
    auto metadata = ret->get_metadata();
    // 验证列名和数据类型是否与预期一致
    ASSERT_EQ(metadata->get_column_name(1), "time");
    ASSERT_EQ(metadata->get_column_type(1), common::TSDataType::INT64) << "Data type mismatch expect data type: INT64, actual data type: " << datatype_to_string(metadata->get_column_type(1)) << endl;
    for (int i = 2; i <= metadata->get_column_count(); i++) {
        cout << metadata->get_column_name(i-1) << "[" << datatype_to_string(metadata->get_column_type(i-1)) << "] ";
        ASSERT_EQ(metadata->get_column_name(i), column_names[i-2]);
        ASSERT_EQ(metadata->get_column_type(i), data_types[i-2]) << "Data type mismatch expect data type: " << datatype_to_string(data_types[i-2]) << ", actual data type: " << datatype_to_string(metadata->get_column_type(i)) << endl;
    }
    cout << endl;

    // 定义一个布尔变量，用于标记是否有更多数据
    bool has_next = false;
    int actual_row_num = 0;
    // 查询数据
    while ((ret->next(has_next)) == common::E_OK && has_next) {
        try
        {
            cout << ret->get_value<Timestamp>("time") << " ";
            for (int i = 2; i <= metadata->get_column_count(); i++) {
                if (ret->is_null(i)) {
                    cout << "null" << " ";
                } else {
                    switch (metadata->get_column_type(i)) {
                        case common::DATE:
                        case common::INT32:
                            cout << ret->get_value<int32_t>(i) << " ";
                            break;
                        case common::TIMESTAMP:
                        case common::INT64:
                            cout << ret->get_value<int64_t>(i) << " ";
                            break;
                        case common::FLOAT:
                            cout << ret->get_value<float>(i) << " ";
                            break;
                        case common::DOUBLE:
                            cout << ret->get_value<double>(i) << " ";
                            break;
                        case common::BLOB:
                        case common::TEXT:
                        case common::STRING:
                            cout << ret->get_value<common::String*>(i)
                                                 ->to_std_string()
                                      << " ";
                            break;
                        case common::BOOLEAN:
                            cout << (ret->get_value<bool>(i) == 0 ? "false" : "true") << " ";
                            break;
                        default:
                            ASSERT_TRUE(false) << "Unsupported data type: " << metadata->get_column_type(i);
                    }
                }
            }
            cout << endl;
            actual_row_num++;
        }
        catch(const exception& e)
        {
            FAIL() << e.what() << '\n';
        }
    }
    // 验证行数是否与预期一致
    ASSERT_EQ(actual_row_num, expect_row_num);
    // 关闭查询结果集，释放资源
    ret->close();
    // 关闭查询接口，释放资源.
    ASSERT_EQ(reader.close(), E_OK);
}



// 每个测试用例前后调用：用于清理环境
class TsFileWriterTest : public ::testing::Test {
    protected:
        // 在每个测试用例执行之前调用
        void SetUp() override {
            // 确认目录存在
            if (!filesystem::exists("/data/iotdb-test/test/cpp-tsfile-api-test-v4/data/tsfile") || !filesystem::is_directory("/data/iotdb-test/test/cpp-tsfile-api-test-v4/data/tsfile")) {
                cerr << "Directory does not exist: " << "/data/iotdb-test/test/cpp-tsfile-api-test-v4/data/tsfile" << endl;
                return;
            }
            // 删除文件
            for (const auto& entry : filesystem::directory_iterator("/data/iotdb-test/test/cpp-tsfile-api-test-v4/data/tsfile")) {
                if (filesystem::is_regular_file(entry)) {
                    filesystem::remove(entry.path());
                }
            }
            // 初始化 TsFile 存储模块
            storage::libtsfile_init();
            // 创建文件
            int flags = O_WRONLY | O_CREAT | O_TRUNC;
            mode_t mode = 0666;
            writer_file_.create(file_path, flags, mode);
        }

        // 在每个测试用例执行之后调用
        void TearDown() override {
        }
};

// 测试写入1：全数据类型，不含空值，大小写列名、表名，,跨时间分区，不同设备
TEST_F(TsFileWriterTest, TestTsFileTableWriter1) {
    // 声明表名
    string table_name_ = "Table1";
    // 声明列名
    vector<string> column_names_ = {
        "DeviceId1",
        "DeviceId2",
        "Field1",
        "Field2",
        "Field3",
        "Field4",
        "Field5",
        "Field6",
        "Field7",
        "Field8",
        "Field9",
        "Field10",
    };
    // 声明数据类型
    vector<common::TSDataType> data_types_ = {
        common::TSDataType::STRING,
        common::TSDataType::STRING,
        common::TSDataType::INT64,
        common::TSDataType::INT32,
        common::TSDataType::FLOAT,
        common::TSDataType::DOUBLE,
        common::TSDataType::BOOLEAN,
        common::TSDataType::TEXT,
        common::TSDataType::STRING,
        common::TSDataType::BLOB,
        common::TSDataType::DATE,
        common::TSDataType::TIMESTAMP,
    };
    // 列类别
    vector<common::ColumnCategory> column_categories_ = {
        common::ColumnCategory::TAG,
        common::ColumnCategory::TAG,
        common::ColumnCategory::FIELD,
        common::ColumnCategory::FIELD,
        common::ColumnCategory::FIELD,
        common::ColumnCategory::FIELD,
        common::ColumnCategory::FIELD,
        common::ColumnCategory::FIELD,
        common::ColumnCategory::FIELD,
        common::ColumnCategory::FIELD,
        common::ColumnCategory::FIELD,
        common::ColumnCategory::FIELD,
    };
    // 声明列的元数据
    vector<common::ColumnSchema> column_schemas;
    for (size_t i = 0; i < column_names_.size(); i++) {
        column_schemas.push_back(
            common::ColumnSchema(column_names_[i], data_types_[i], column_categories_[i]));
    }

    // 声明表的元数据
    auto* table_schema_ = new storage::TableSchema(table_name_, column_schemas);
    // 构造写入器
    auto* tsfile_table_writer_ = new storage::TsFileTableWriter(&writer_file_, table_schema_);

    // 构造 tablet
    storage::Tablet tablet(column_names_, data_types_);
    // 构造数据
    int64_t timestamp = 0;
    for (int row = 0; row < 50; row++) {
        ASSERT_EQ(tablet.add_timestamp(row, timestamp++), E_OK);
        for (int i = 0; i < column_names_.size(); i++) {
            switch (data_types_[i]) {
                case common::TSDataType::STRING:
                    if (i < 2) { // TAG列
                        ASSERT_EQ(tablet.add_value(row, column_names_[i], ("d" + to_string(i+1) + "_" + to_string(row)).c_str()), E_OK);
                    } else { // FIELD列
                        ASSERT_EQ(tablet.add_value(row, column_names_[i], ("string" + to_string(row)).c_str()), E_OK);
                    }
                    break;
                case common::TSDataType::INT64:
                    ASSERT_EQ(tablet.add_value(row, column_names_[i], static_cast<int64_t>(row)), E_OK);
                    break;
                case common::TSDataType::INT32:
                    ASSERT_EQ(tablet.add_value(row, column_names_[i], static_cast<int32_t>(row)), E_OK);
                    break;
                case common::TSDataType::FLOAT:
                    ASSERT_EQ(tablet.add_value(row, column_names_[i], static_cast<float>(row)), E_OK);
                    break;
                case common::TSDataType::DOUBLE:
                    ASSERT_EQ(tablet.add_value(row, column_names_[i], static_cast<double>(row)), E_OK);
                    break;
                case common::TSDataType::BOOLEAN:
                    ASSERT_EQ(tablet.add_value(row, column_names_[i], row % 2 == 0), E_OK);
                    break;
                case common::TSDataType::TEXT:
                    ASSERT_EQ(tablet.add_value(row, column_names_[i], ("text" + to_string(row)).c_str()), E_OK);
                    break;
                case common::TSDataType::BLOB:
                    ASSERT_EQ(tablet.add_value(row, column_names_[i], ("blob" + to_string(row)).c_str()), E_OK);
                    break;
                case common::TSDataType::DATE:
                    ASSERT_EQ(tablet.add_value(row, column_names_[i], static_cast<int32_t>(row)), E_OK);
                    break;
                case common::TSDataType::TIMESTAMP:
                    ASSERT_EQ(tablet.add_value(row, column_names_[i], timestamp), E_OK);
                    break;
                default:
                    ASSERT_TRUE(false) << "Unsupported data type: " << data_types_[i];
                    return;
            }
        }
    }
    for (int row = 50; row < 100; row++) {
        timestamp += 10000000;
        ASSERT_EQ(tablet.add_timestamp(row, timestamp), E_OK);
        for (int i = 0; i < column_names_.size(); i++) {
            switch (data_types_[i]) {
                case common::TSDataType::STRING:
                    if (i < 2) { // TAG列
                        ASSERT_EQ(tablet.add_value(row, column_names_[i], ("d" + to_string(i+1) + "_" + to_string(row)).c_str()), E_OK);
                    } else { // FIELD列
                        ASSERT_EQ(tablet.add_value(row, column_names_[i], ("string" + to_string(row)).c_str()), E_OK);
                    }
                    break;
                case common::TSDataType::INT64:
                    ASSERT_EQ(tablet.add_value(row, column_names_[i], static_cast<int64_t>(row)), E_OK);
                    break;
                case common::TSDataType::INT32:
                    ASSERT_EQ(tablet.add_value(row, column_names_[i], static_cast<int32_t>(row)), E_OK);
                    break;
                case common::TSDataType::FLOAT:
                    ASSERT_EQ(tablet.add_value(row, column_names_[i], static_cast<float>(row)), E_OK);
                    break;
                case common::TSDataType::DOUBLE:
                    ASSERT_EQ(tablet.add_value(row, column_names_[i], static_cast<double>(row)), E_OK);
                    break;
                case common::TSDataType::BOOLEAN:
                    ASSERT_EQ(tablet.add_value(row, column_names_[i], row % 2 == 0), E_OK);
                    break;
                case common::TSDataType::TEXT:
                    ASSERT_EQ(tablet.add_value(row, column_names_[i], ("text" + to_string(row)).c_str()), E_OK);
                    break;
                case common::TSDataType::BLOB:
                    ASSERT_EQ(tablet.add_value(row, column_names_[i], ("blob" + to_string(row)).c_str()), E_OK);
                    break;
                case common::TSDataType::DATE:
                    ASSERT_EQ(tablet.add_value(row, column_names_[i], static_cast<int32_t>(row)), E_OK);
                    break;
                case common::TSDataType::TIMESTAMP:
                    ASSERT_EQ(tablet.add_value(row, column_names_[i], timestamp), E_OK);
                    break;
                default:
                    ASSERT_TRUE(false) << "Unsupported data type: " << data_types_[i];
                    return;
            }
        }
    }
    // 写入数据
    ASSERT_EQ(tsfile_table_writer_->write_table(tablet), E_OK);
    // 刷新数据
    ASSERT_EQ(tsfile_table_writer_->flush(), E_OK);
    // 关闭写入
    ASSERT_EQ(tsfile_table_writer_->close(), E_OK);

    // 释放动态分配的内存
    delete tsfile_table_writer_;
    delete table_schema_;

    // 验证数据正确性
    query_data(table_name_, column_names_, data_types_, 100);
}

// 测试写入2：含空值（列部分空、TAG列全空、FIELD列全空和全空）,跨时间分区，不同设备
TEST_F(TsFileWriterTest, TestTsFileTableWriter2) {
    // 声明表名
    string table_name_ = "Table1";
    // 声明列名
    vector<string> column_names_ = {
        "DeviceId1",
        "DeviceId2",
        "INT64",
        "INT32",
        "FLOAT",
        "DOUBLE",
        "BOOLEAN",
        "TEXT",
        "STRING",
        "BLOB",
        "DATE",
        "TIMESTAMP",
    };
    // 声明数据类型
    vector<common::TSDataType> data_types_ = {
        common::TSDataType::STRING,
        common::TSDataType::STRING,
        common::TSDataType::INT64,
        common::TSDataType::INT32,
        common::TSDataType::FLOAT,
        common::TSDataType::DOUBLE,
        common::TSDataType::BOOLEAN,
        common::TSDataType::TEXT,
        common::TSDataType::STRING,
        common::TSDataType::BLOB,
        common::TSDataType::DATE,
        common::TSDataType::TIMESTAMP,
    };
    // 列类别
    vector<common::ColumnCategory> column_categories_ = {
        common::ColumnCategory::TAG,
        common::ColumnCategory::TAG,
        common::ColumnCategory::FIELD,
        common::ColumnCategory::FIELD,
        common::ColumnCategory::FIELD,
        common::ColumnCategory::FIELD,
        common::ColumnCategory::FIELD,
        common::ColumnCategory::FIELD,
        common::ColumnCategory::FIELD,
        common::ColumnCategory::FIELD,
        common::ColumnCategory::FIELD,
        common::ColumnCategory::FIELD,
    };
    // 声明列的元数据
    vector<common::ColumnSchema> column_schemas;
    for (size_t i = 0; i < column_names_.size(); i++) {
        column_schemas.push_back(
            common::ColumnSchema(column_names_[i], data_types_[i], column_categories_[i]));
    }

    // 声明表的元数据
    auto* table_schema_ = new storage::TableSchema(table_name_, column_schemas);
    // 构造写入器
    auto* tsfile_table_writer_ = new storage::TsFileTableWriter(&writer_file_, table_schema_);

    // 构造 tablet
    storage::Tablet tablet(column_names_, data_types_);
    // 构造数据
    int64_t timestamp = 0;
    // 全空
    for (int row = 0; row < 40; row++) {
        ASSERT_EQ(tablet.add_timestamp(row, timestamp++), E_OK);
        if (row % 2 == 0) { 
            for (int i = 0; i < column_names_.size(); i++) {
                switch (data_types_[i]) {
                    case common::TSDataType::STRING:
                        if (i < 2) { // TAG列
                            ASSERT_EQ(tablet.add_value(row, column_names_[i], ("d" + to_string(i+1) + "_" + to_string(row)).c_str()), E_OK);
                        } else { // FIELD列
                            ASSERT_EQ(tablet.add_value(row, column_names_[i], ("string" + to_string(row)).c_str()), E_OK);
                        }
                        break;
                    case common::TSDataType::INT64:
                        ASSERT_EQ(tablet.add_value(row, column_names_[i], static_cast<int64_t>(row)), E_OK);
                        break;
                    case common::TSDataType::INT32:
                        ASSERT_EQ(tablet.add_value(row, column_names_[i], static_cast<int32_t>(row)), E_OK);
                        break;
                    case common::TSDataType::FLOAT:
                        ASSERT_EQ(tablet.add_value(row, column_names_[i], static_cast<float>(row)), E_OK);
                        break;
                    case common::TSDataType::DOUBLE:
                        ASSERT_EQ(tablet.add_value(row, column_names_[i], static_cast<double>(row)), E_OK);
                        break;
                    case common::TSDataType::BOOLEAN:
                        ASSERT_EQ(tablet.add_value(row, column_names_[i], row % 2 == 0), E_OK);
                        break;
                    case common::TSDataType::TEXT:
                        ASSERT_EQ(tablet.add_value(row, column_names_[i], ("text" + to_string(row)).c_str()), E_OK);
                        break;
                    case common::TSDataType::BLOB:
                        ASSERT_EQ(tablet.add_value(row, column_names_[i], ("blob" + to_string(row)).c_str()), E_OK);
                        break;
                    case common::TSDataType::DATE:
                        ASSERT_EQ(tablet.add_value(row, column_names_[i], static_cast<int32_t>(row)), E_OK);
                        break;
                    case common::TSDataType::TIMESTAMP:
                        ASSERT_EQ(tablet.add_value(row, column_names_[i], timestamp), E_OK);
                        break;
                    default:
                        ASSERT_TRUE(false) << "Unsupported data type: " << data_types_[i];
                        return;
                }
            }
        }
    }
    // TAG列为空
    for (int row = 40; row < 70; row++) {
        ASSERT_EQ(tablet.add_timestamp(row, timestamp+=10000000), E_OK);
        if (row < 50) { 
            ASSERT_EQ(tablet.add_value(row, column_names_[0], ("d1")), E_OK);
        } else if (row < 60) {
            ASSERT_EQ(tablet.add_value(row, column_names_[1], ("d2")), E_OK);
        }
        for (int i = 2; i < column_names_.size(); i++) {
            switch (data_types_[i]) {
                case common::TSDataType::STRING:
                    ASSERT_EQ(tablet.add_value(row, column_names_[i], ("string" + to_string(row)).c_str()), E_OK);
                    break;
                case common::TSDataType::INT64:
                    ASSERT_EQ(tablet.add_value(row, column_names_[i], static_cast<int64_t>(row)), E_OK);
                    break;
                case common::TSDataType::INT32:
                    ASSERT_EQ(tablet.add_value(row, column_names_[i], static_cast<int32_t>(row)), E_OK);
                    break;
                case common::TSDataType::FLOAT:
                    ASSERT_EQ(tablet.add_value(row, column_names_[i], static_cast<float>(row)), E_OK);
                    break;
                case common::TSDataType::DOUBLE:
                    ASSERT_EQ(tablet.add_value(row, column_names_[i], static_cast<double>(row)), E_OK);
                    break;
                case common::TSDataType::BOOLEAN:
                    ASSERT_EQ(tablet.add_value(row, column_names_[i], row % 2 == 0), E_OK);
                    break;
                case common::TSDataType::TEXT:
                    ASSERT_EQ(tablet.add_value(row, column_names_[i], ("text" + to_string(row)).c_str()), E_OK);
                    break;
                case common::TSDataType::BLOB:
                    ASSERT_EQ(tablet.add_value(row, column_names_[i], ("blob" + to_string(row)).c_str()), E_OK);
                    break;
                case common::TSDataType::DATE:
                    ASSERT_EQ(tablet.add_value(row, column_names_[i], static_cast<int32_t>(row)), E_OK);
                    break;
                case common::TSDataType::TIMESTAMP:
                    ASSERT_EQ(tablet.add_value(row, column_names_[i], timestamp), E_OK);
                    break;
                default:
                    ASSERT_TRUE(false) << "Unsupported data type: " << data_types_[i];
                    return;
            }
        }
    }
    // FIELD列为空
    for (int row = 70; row < 100; row++) {
        ASSERT_EQ(tablet.add_timestamp(row,  timestamp+=10000000), E_OK);
        ASSERT_EQ(tablet.add_value(row, column_names_[0], ("d1")), E_OK);
        ASSERT_EQ(tablet.add_value(row, column_names_[1], ("d2")), E_OK);
        if (row < 80) { // TAG列
            ASSERT_EQ(tablet.add_value(row, "STRING", ("string" + to_string(row)).c_str()), E_OK);
            ASSERT_EQ(tablet.add_value(row, "TEXT", ("text" + to_string(row)).c_str()), E_OK);
            ASSERT_EQ(tablet.add_value(row, "BLOB", ("blob" + to_string(row)).c_str()), E_OK);
            ASSERT_EQ(tablet.add_value(row, "DATE", static_cast<int32_t>(row)), E_OK);
            ASSERT_EQ(tablet.add_value(row, "TIMESTAMP", timestamp), E_OK);
        } else if (row < 90) {
            ASSERT_EQ(tablet.add_value(row, "INT64", static_cast<int64_t>(row)), E_OK);
            ASSERT_EQ(tablet.add_value(row, "INT32", static_cast<int32_t>(row)), E_OK);
            ASSERT_EQ(tablet.add_value(row, "FLOAT", static_cast<float>(row)), E_OK);
            ASSERT_EQ(tablet.add_value(row, "DOUBLE", static_cast<double>(row)), E_OK);
            ASSERT_EQ(tablet.add_value(row, "BOOLEAN", row % 2 == 0), E_OK);
        }
    }
    // 写入数据
    ASSERT_EQ(tsfile_table_writer_->write_table(tablet), E_OK);
    // 刷新数据
    ASSERT_EQ(tsfile_table_writer_->flush(), E_OK);
    // 关闭写入
    ASSERT_EQ(tsfile_table_writer_->close(), E_OK);

    // 释放动态分配的内存
    delete tsfile_table_writer_;
    delete table_schema_;

    // 验证数据正确性
    query_data(table_name_, column_names_, data_types_, 100);
}


// // 测试6：1万TAG和FIELD列，1行
// TEST_F(TsFileWriterTest, TestTsFileTableWriter6) {
//     string table_name = "table1"; // 表名
//     vector<string> columnNames; // 列名
//     vector<common::ColumnSchema> column_schemas; // 列的元数据信息
//     vector<common::TSDataType> data_types; // 数据类型
//     size_t tag_count = 1000; // tag列数量
//     size_t field_count = 1000; // field列数量
//     size_t column_count = tag_count + field_count * 4; // 总列数
//     // Tag 列
//     for (size_t i = 0; i < tag_count; i++)
//     {
//         string tag_name = "tag_" + to_string(i);
//         columnNames.emplace_back(tag_name);
//         data_types.emplace_back(common::TSDataType::STRING);
//         column_schemas.emplace_back(tag_name, common::TSDataType::STRING, common::UNCOMPRESSED, common::PLAIN, common::ColumnCategory::TAG);
//     }
//     // Field 列
//     for (size_t i = 0; i < field_count; i++)
//     {
//         string field_name1 = "int64_" + to_string(i);
//         columnNames.emplace_back(field_name1);
//         data_types.emplace_back(common::TSDataType::INT64);
//         column_schemas.emplace_back(field_name1, common::TSDataType::INT64, common::UNCOMPRESSED, common::PLAIN, common::ColumnCategory::FIELD);
//         string field_name2 = "int32_" + to_string(i);
//         columnNames.emplace_back(field_name2);
//         data_types.emplace_back(common::TSDataType::INT32);
//         column_schemas.emplace_back(field_name2, common::TSDataType::INT32, common::UNCOMPRESSED, common::PLAIN, common::ColumnCategory::FIELD);
//         string field_name3 = "float_" + to_string(i);
//         columnNames.emplace_back(field_name3);
//         data_types.emplace_back(common::TSDataType::FLOAT);
//         column_schemas.emplace_back(field_name3, common::TSDataType::FLOAT, common::UNCOMPRESSED, common::PLAIN, common::ColumnCategory::FIELD);
//         string field_name4 = "double_" + to_string(i);
//         columnNames.emplace_back(field_name4);
//         data_types.emplace_back(common::TSDataType::DOUBLE);
//         column_schemas.emplace_back(field_name4, common::TSDataType::DOUBLE, common::UNCOMPRESSED, common::PLAIN, common::ColumnCategory::FIELD);
//     }
//     // 构建表的元数据
//     auto* schema = new storage::TableSchema(table_name,column_schemas); 

//     // 构建写入器
//     auto* writer = new storage::TsFileTableWriter(&writer_file_,schema); 

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
//                     ASSERT_EQ(0, tablet.add_value(row, columnNames[i], ("tag" + to_string(row)).c_str()));
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

// // 测试7：10个TAG和FIELD列，1万行写入
// TEST_F(TsFileWriterTest, TestTsFileTableWriter7) {
//     string table_name = "table7"; // 表名
//     vector<string> columnNames; // 列名
//     vector<common::ColumnSchema> column_schemas; // 列的元数据信息
//     vector<common::TSDataType> data_types; // 数据类型
//     size_t tag_count = 2; // tag列数量
//     size_t field_count = 1; // field列数量
//     size_t column_count = tag_count + field_count * 4; // 总列数
//     // Tag 列
//     for (size_t i = 0; i < tag_count; i++)
//     {
//         string tag_name = "tag_" + to_string(i);
//         columnNames.emplace_back(tag_name);
//         data_types.emplace_back(common::TSDataType::STRING);
//         column_schemas.emplace_back(tag_name, common::TSDataType::STRING, common::UNCOMPRESSED, common::PLAIN, common::ColumnCategory::TAG);
//     }
//     // Field 列
//     for (size_t i = 0; i < field_count; i++)
//     {
//         string field_name1 = "int64_" + to_string(i);
//         columnNames.emplace_back(field_name1);
//         data_types.emplace_back(common::TSDataType::INT64);
//         column_schemas.emplace_back(field_name1, common::TSDataType::INT64, common::UNCOMPRESSED, common::PLAIN, common::ColumnCategory::FIELD);
//         string field_name2 = "int32_" + to_string(i);
//         columnNames.emplace_back(field_name2);
//         data_types.emplace_back(common::TSDataType::INT32);
//         column_schemas.emplace_back(field_name2, common::TSDataType::INT32, common::UNCOMPRESSED, common::PLAIN, common::ColumnCategory::FIELD);
//         string field_name3 = "float_" + to_string(i);
//         columnNames.emplace_back(field_name3);
//         data_types.emplace_back(common::TSDataType::FLOAT);
//         column_schemas.emplace_back(field_name3, common::TSDataType::FLOAT, common::UNCOMPRESSED, common::PLAIN, common::ColumnCategory::FIELD);
//         string field_name4 = "double_" + to_string(i);
//         columnNames.emplace_back(field_name4);
//         data_types.emplace_back(common::TSDataType::DOUBLE);
//         column_schemas.emplace_back(field_name4, common::TSDataType::DOUBLE, common::UNCOMPRESSED, common::PLAIN, common::ColumnCategory::FIELD);
//     }
//     // 构建表的元数据
//     auto* schema = new storage::TableSchema(table_name,column_schemas); 
//     // 构建写入器
//     auto* writer = new storage::TsFileTableWriter(&writer_file_,schema); 
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
//                     ASSERT_EQ(0, tablet.add_value(row, columnNames[i], ("tag" + to_string(row)).c_str()));
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

// // 测试8：1万设备（TAG列），10测点（FIELD列），100行写入
// TEST_F(TsFileWriterTest, TestTsFileTableWriter8) {
//     string table_name = "table1"; // 表名
//     vector<string> columnNames; // 列名
//     vector<common::ColumnSchema> column_schemas; // 列的元数据信息
//     vector<common::TSDataType> data_types; // 数据类型
//     size_t tag_count = 10000; // tag列数量
//     size_t field_count = 10; // field列数量
//     size_t column_count = tag_count + field_count * 4; // 总列数
//     // Tag 列
//     for (size_t i = 0; i < tag_count; i++)
//     {
//         string tag_name = "tag_" + to_string(i);
//         columnNames.emplace_back(tag_name);
//         data_types.emplace_back(common::TSDataType::STRING);
//         column_schemas.emplace_back(tag_name, common::TSDataType::STRING, common::UNCOMPRESSED, common::PLAIN, common::ColumnCategory::TAG);
//     }
//     // Field 列
//     for (size_t i = 0; i < field_count; i++)
//     {
//         string field_name1 = "int64_" + to_string(i);
//         columnNames.emplace_back(field_name1);
//         data_types.emplace_back(common::TSDataType::INT64);
//         column_schemas.emplace_back(field_name1, common::TSDataType::INT64, common::UNCOMPRESSED, common::PLAIN, common::ColumnCategory::FIELD);
//         string field_name2 = "int32_" + to_string(i);
//         columnNames.emplace_back(field_name2);
//         data_types.emplace_back(common::TSDataType::INT32);
//         column_schemas.emplace_back(field_name2, common::TSDataType::INT32, common::UNCOMPRESSED, common::PLAIN, common::ColumnCategory::FIELD);
//         string field_name3 = "float_" + to_string(i);
//         columnNames.emplace_back(field_name3);
//         data_types.emplace_back(common::TSDataType::FLOAT);
//         column_schemas.emplace_back(field_name3, common::TSDataType::FLOAT, common::UNCOMPRESSED, common::PLAIN, common::ColumnCategory::FIELD);
//         string field_name4 = "double_" + to_string(i);
//         columnNames.emplace_back(field_name4);
//         data_types.emplace_back(common::TSDataType::DOUBLE);
//         column_schemas.emplace_back(field_name4, common::TSDataType::DOUBLE, common::UNCOMPRESSED, common::PLAIN, common::ColumnCategory::FIELD);
//     }
//     // 构建表的元数据
//     auto* schema = new storage::TableSchema(table_name,column_schemas); 
//     // 构建写入器
//     auto* writer = new storage::TsFileTableWriter(&writer_file_,schema); 
//     // 创建 tablet 以插入数据。
//     int max_rows = 100; // 最大行数
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
//                     ASSERT_EQ(0, tablet.add_value(row, columnNames[i], ("tag" + to_string(row)).c_str()));
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

// // 测试9：10设备（TAG列），1万测点（FIELD列），100行写入
// TEST_F(TsFileWriterTest, TestTsFileTableWriter9) {
//     string table_name = "table10"; // 表名
//     vector<string> columnNames; // 列名
//     vector<common::ColumnSchema> column_schemas; // 列的元数据信息
//     vector<common::TSDataType> data_types; // 数据类型
//     vector<common::ColumnCategory> column_category; // 列类别
//     size_t tag_count = 10; // tag列数量
//     size_t field_count = 10000; // field列数量
//     size_t column_count = tag_count + field_count * 4; // 总列数
//     // Tag 列
//     for (size_t i = 0; i < tag_count; i++)
//     {
//         string tag_name = "tag_" + to_string(i);
//         columnNames.emplace_back(tag_name);
//         data_types.emplace_back(common::TSDataType::STRING);
//         column_category.emplace_back(common::ColumnCategory::TAG);
//         column_schemas.emplace_back(tag_name, common::TSDataType::STRING, common::UNCOMPRESSED, common::PLAIN, common::ColumnCategory::TAG);
//     }
//     // Field 列
//     for (size_t i = 0; i < field_count; i++)
//     {
//         string field_name1 = "int64_" + to_string(i);
//         columnNames.emplace_back(field_name1);
//         data_types.emplace_back(common::TSDataType::INT64);
//         column_category.emplace_back(common::ColumnCategory::FIELD);
//         column_schemas.emplace_back(field_name1, common::TSDataType::INT64, common::UNCOMPRESSED, common::PLAIN, common::ColumnCategory::FIELD);
//         string field_name2 = "int32_" + to_string(i);
//         columnNames.emplace_back(field_name2);
//         data_types.emplace_back(common::TSDataType::INT32);
//         column_category.emplace_back(common::ColumnCategory::FIELD);
//         column_schemas.emplace_back(field_name2, common::TSDataType::INT32, common::UNCOMPRESSED, common::PLAIN, common::ColumnCategory::FIELD);
//         string field_name3 = "float_" + to_string(i);
//         columnNames.emplace_back(field_name3);
//         data_types.emplace_back(common::TSDataType::FLOAT);
//         column_category.emplace_back(common::ColumnCategory::FIELD);
//         column_schemas.emplace_back(field_name3, common::TSDataType::FLOAT, common::UNCOMPRESSED, common::PLAIN, common::ColumnCategory::FIELD);
//         string field_name4 = "double_" + to_string(i);
//         columnNames.emplace_back(field_name4);
//         data_types.emplace_back(common::TSDataType::DOUBLE);
//         column_category.emplace_back(common::ColumnCategory::FIELD);
//         column_schemas.emplace_back(field_name4, common::TSDataType::DOUBLE, common::UNCOMPRESSED, common::PLAIN, common::ColumnCategory::FIELD);
//     }
//     // 构建表的元数据
//     auto* schema = new storage::TableSchema(table_name,column_schemas); 
//     // 构建写入器
//     auto* writer = new storage::TsFileTableWriter(&writer_file_,schema); 
//     // 创建 tablet 以插入数据。
//     int max_rows = 100; // 最大行数
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
//                     ASSERT_EQ(0, tablet.add_value(row, columnNames[i], ("tag" + to_string(row)).c_str()));
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

// 测试10：各种编码和压缩方式
// TEST_F(TsFileWriterTest, TestTsFileTableWriter10) {
//     // 声明表名
//     string table_name_ = "Table1";
//     // 声明列名
//     vector<string> column_names_ = {
//         "DeviceId1",
//         "DeviceId2",
//         "Field1",
//         "Field2",
//         "Field3",
//         "Field4",
//         "Field5",
//         "Field6",
//         "Field7",
//         "Field8",
//         "Field9",
//         "Field10",
//     };
//     // 声明数据类型
//     vector<common::TSDataType> data_types_ = {
//         common::TSDataType::STRING,
//         common::TSDataType::STRING,
//         common::TSDataType::INT64,
//         common::TSDataType::INT32,
//         common::TSDataType::FLOAT,
//         common::TSDataType::DOUBLE,
//         common::TSDataType::BOOLEAN,
//         common::TSDataType::TEXT,
//         common::TSDataType::STRING,
//         common::TSDataType::BLOB,
//         common::TSDataType::DATE,
//         common::TSDataType::TIMESTAMP,
//     };
//     // 压缩方式
//     vector<common::CompressionType> compression_types_ = {
//         common::CompressionType::LZ4,
//         common::CompressionType::LZ4,
//         common::CompressionType::LZ4,
//         common::CompressionType::LZ4,
//         common::CompressionType::LZ4,
//         common::CompressionType::LZ4,
//         common::CompressionType::LZ4,
//         common::CompressionType::LZ4,
//         common::CompressionType::LZ4,
//         common::CompressionType::LZ4,
//         common::CompressionType::LZ4,
//         common::CompressionType::LZ4,
//     };
//     // 编码方式
//     vector<common::TSEncoding> encoding_types_ = {
//         common::TSEncoding::PLAIN,
//         common::TSEncoding::PLAIN,
//         common::TSEncoding::PLAIN,
//         common::TSEncoding::PLAIN,
//         common::TSEncoding::PLAIN,
//         common::TSEncoding::PLAIN,
//         common::TSEncoding::PLAIN,
//         common::TSEncoding::PLAIN,
//         common::TSEncoding::PLAIN,
//         common::TSEncoding::PLAIN,
//         common::TSEncoding::PLAIN,
//         common::TSEncoding::PLAIN,
//     };
//     // 列类别
//     vector<common::ColumnCategory> column_categories_ = {
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
//         common::ColumnCategory::FIELD,
//         common::ColumnCategory::FIELD,
//     };
//     // 声明列的元数据
//     vector<common::ColumnSchema> column_schemas;
//     for (size_t i = 0; i < column_names_.size(); i++) {
//         column_schemas.push_back(
//             common::ColumnSchema(column_names_[i], data_types_[i], compression_types_[i], encoding_types_[i], column_categories_[i]));
//     }

//     // 声明表的元数据
//     auto* table_schema_ = new storage::TableSchema(table_name_, column_schemas);
//     // 构造写入器
//     auto* tsfile_table_writer_ = new storage::TsFileTableWriter(&writer_file_, table_schema_);

//     // 构造 tablet
//     storage::Tablet tablet(column_names_, data_types_);
//     // 构造数据
//     int64_t timestamp = 0;
//     for (int row = 0; row < 50; row++) {
//         ASSERT_EQ(tablet.add_timestamp(row, timestamp++), E_OK);
//         for (int i = 0; i < column_names_.size(); i++) {
//             switch (data_types_[i]) {
//                 case common::TSDataType::STRING:
//                     if (i < 2) { // TAG列
//                         ASSERT_EQ(tablet.add_value(row, column_names_[i], ("d" + to_string(i+1) + "_" + to_string(row)).c_str()), E_OK);
//                     } else { // FIELD列
//                         ASSERT_EQ(tablet.add_value(row, column_names_[i], ("string" + to_string(row)).c_str()), E_OK);
//                     }
//                     break;
//                 case common::TSDataType::INT64:
//                     ASSERT_EQ(tablet.add_value(row, column_names_[i], static_cast<int64_t>(row)), E_OK);
//                     break;
//                 case common::TSDataType::INT32:
//                     ASSERT_EQ(tablet.add_value(row, column_names_[i], static_cast<int32_t>(row)), E_OK);
//                     break;
//                 case common::TSDataType::FLOAT:
//                     ASSERT_EQ(tablet.add_value(row, column_names_[i], static_cast<float>(row)), E_OK);
//                     break;
//                 case common::TSDataType::DOUBLE:
//                     ASSERT_EQ(tablet.add_value(row, column_names_[i], static_cast<double>(row)), E_OK);
//                     break;
//                 case common::TSDataType::BOOLEAN:
//                     ASSERT_EQ(tablet.add_value(row, column_names_[i], row % 2 == 0), E_OK);
//                     break;
//                 case common::TSDataType::TEXT:
//                     ASSERT_EQ(tablet.add_value(row, column_names_[i], ("text" + to_string(row)).c_str()), E_OK);
//                     break;
//                 case common::TSDataType::BLOB:
//                     ASSERT_EQ(tablet.add_value(row, column_names_[i], ("blob" + to_string(row)).c_str()), E_OK);
//                     break;
//                 case common::TSDataType::DATE:
//                     ASSERT_EQ(tablet.add_value(row, column_names_[i], static_cast<int32_t>(row)), E_OK);
//                     break;
//                 case common::TSDataType::TIMESTAMP:
//                     ASSERT_EQ(tablet.add_value(row, column_names_[i], timestamp), E_OK);
//                     break;
//                 default:
//                     ASSERT_TRUE(false) << "Unsupported data type: " << data_types_[i];
//                     return;
//             }
//         }
//     }
//     for (int row = 50; row < 100; row++) {
//         timestamp += 10000000;
//         ASSERT_EQ(tablet.add_timestamp(row, timestamp), E_OK);
//         for (int i = 0; i < column_names_.size(); i++) {
//             switch (data_types_[i]) {
//                 case common::TSDataType::STRING:
//                     if (i < 2) { // TAG列
//                         ASSERT_EQ(tablet.add_value(row, column_names_[i], ("d" + to_string(i+1) + "_" + to_string(row)).c_str()), E_OK);
//                     } else { // FIELD列
//                         ASSERT_EQ(tablet.add_value(row, column_names_[i], ("string" + to_string(row)).c_str()), E_OK);
//                     }
//                     break;
//                 case common::TSDataType::INT64:
//                     ASSERT_EQ(tablet.add_value(row, column_names_[i], static_cast<int64_t>(row)), E_OK);
//                     break;
//                 case common::TSDataType::INT32:
//                     ASSERT_EQ(tablet.add_value(row, column_names_[i], static_cast<int32_t>(row)), E_OK);
//                     break;
//                 case common::TSDataType::FLOAT:
//                     ASSERT_EQ(tablet.add_value(row, column_names_[i], static_cast<float>(row)), E_OK);
//                     break;
//                 case common::TSDataType::DOUBLE:
//                     ASSERT_EQ(tablet.add_value(row, column_names_[i], static_cast<double>(row)), E_OK);
//                     break;
//                 case common::TSDataType::BOOLEAN:
//                     ASSERT_EQ(tablet.add_value(row, column_names_[i], row % 2 == 0), E_OK);
//                     break;
//                 case common::TSDataType::TEXT:
//                     ASSERT_EQ(tablet.add_value(row, column_names_[i], ("text" + to_string(row)).c_str()), E_OK);
//                     break;
//                 case common::TSDataType::BLOB:
//                     ASSERT_EQ(tablet.add_value(row, column_names_[i], ("blob" + to_string(row)).c_str()), E_OK);
//                     break;
//                 case common::TSDataType::DATE:
//                     ASSERT_EQ(tablet.add_value(row, column_names_[i], static_cast<int32_t>(row)), E_OK);
//                     break;
//                 case common::TSDataType::TIMESTAMP:
//                     ASSERT_EQ(tablet.add_value(row, column_names_[i], timestamp), E_OK);
//                     break;
//                 default:
//                     ASSERT_TRUE(false) << "Unsupported data type: " << data_types_[i];
//                     return;
//             }
//         }
//     }
//     // 写入数据
//     ASSERT_EQ(tsfile_table_writer_->write_table(tablet), E_OK);
//     // 刷新数据
//     ASSERT_EQ(tsfile_table_writer_->flush(), E_OK);
//     // 关闭写入
//     ASSERT_EQ(tsfile_table_writer_->close(), E_OK);

//     // 释放动态分配的内存
//     delete tsfile_table_writer_;
//     delete table_schema_;

//     // 验证数据正确性
//     query_data(table_name_, column_names_, data_types_, 100);
// }

// // 测试11：多次执行flush
// TEST_F(TsFileWriterTest, TestTsFileTableWriter11) {
//     string table_name = "table11"; // 表名
//     // 列名
//     vector<string> columnNames = 
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
//     vector<common::TSDataType> data_types = 
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
//     vector<common::ColumnCategory> column_categories = 
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
//     vector<common::ColumnSchema> column_schemas;
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
//     auto* writer = new storage::TsFileTableWriter(&writer_file_, schema);
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
