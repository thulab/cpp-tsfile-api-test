#include <iterator>
#include <time.h>
#include "gtest/gtest.h"

#include <iostream>
#include <random>
#include <string>
#include <filesystem>

#include "common/db_common.h"
#include "common/path.h"
#include "common/record.h"
#include "common/row_record.h"
#include "common/schema.h"
#include "common/tablet.h"
#include "file/write_file.h"
#include "reader/expression.h"
#include "reader/filter/filter.h"
#include "reader/filter/tag_filter.h"
#include "reader/qds_with_timegenerator.h"
#include "reader/qds_without_timegenerator.h"
#include "reader/tsfile_reader.h"
#include "writer/tsfile_table_writer.h"
#include "writer/tsfile_writer.h"

using namespace storage;
using namespace common;
using namespace std;

// 文件路径
string file_path = "/data/iotdb-test/test/cpp-tsfile-api-test-v4/data/tsfile/test_tree.tsfile";

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
        }

        // 在每个测试用例执行之后调用
        void TearDown() override {
        }
};

/**
 * 将数据类型转换为字符串
 */
string datatype_to_string(TSDataType type) {
    switch (type) {
        case TSDataType::BOOLEAN:
            return "BOOLEAN";
        case TSDataType::INT32:
            return "INT32";
        case TSDataType::INT64:
            return "INT64";
        case TSDataType::FLOAT:
            return "FLOAT";
        case TSDataType::DOUBLE:
            return "DOUBLE";
        case TSDataType::TEXT:
            return "TEXT";
        case TSDataType::STRING:
            return "STRING";
        case TSDataType::BLOB:
            return "BLOB";
        case TSDataType::DATE:
            return "DATE";
        case TSDataType::TIMESTAMP:
            return "TIMESTAMP";
        default:
            return "INVALID_TYPE";
    }
}

/**
 * 读取数据进行验证
 */
void reader(vector<string> path_list, vector<TSDataType> data_types, int64_t start_time, int64_t end_time, int expect_num) { 
    int actual_num = 0;
    TsFileReader reader;
    ASSERT_EQ(reader.open(file_path), E_OK);
    ResultSet* result_set = nullptr;

    ASSERT_EQ(reader.query(path_list, start_time, end_time, result_set), E_OK);
    auto* qds = (QDSWithoutTimeGenerator*)result_set;
    shared_ptr<ResultSetMetadata> result_set_metadata = qds->get_metadata();
    for (int i = 1; i < result_set_metadata->get_column_count() + 1; i++) {
        // cout << result_set_metadata->get_column_name(i) << "[" << datatype_to_string(result_set_metadata->get_column_type(i)) << "]  ";
        ASSERT_EQ(result_set_metadata->get_column_name(i+1), path_list[i-1]) << "Column name mismatch at index " << i << ": expected '" << path_list[i-1] << "', but got '" << result_set_metadata->get_column_name(i) << "'";
        ASSERT_EQ(result_set_metadata->get_column_type(i+1), data_types[i-1]) << "Data type mismatch at index " << i << ": expected " << datatype_to_string(data_types[i-1]) << ", but got " << datatype_to_string(result_set_metadata->get_column_type(i));
    }
    cout << endl;
    RowRecord *record = qds->get_row_record();
    record->reset();
    reader.destroy_query_data_set(qds);
    ASSERT_EQ(reader.close(), E_OK);
}

/**
 * 测试注册时间序列1：int TsFileWriter::register_timeseries(const string &device_id, const MeasurementSchema &measurement_schema)
 */
TEST_F(TsFileWriterTest, RegisterTimeSeries1) {
    // 创建一个TsFileWriter对象
    TsFileWriter* tsfile_writer_ = new TsFileWriter();
    // 初始化libtsfile库
    libtsfile_init();
    // 生成一个文件名
    string file_name_ = string(file_path);

    // 设置文件打开的标志:只写模式打开|文件不存在会创建一个新的文件|文件已经存在且能够成功打开文件那么内容会被清空
    int flags = O_WRONLY | O_CREAT | O_TRUNC;
    // 设置文件权限模式
    mode_t mode = 0666;
    // 使用TsFileWriter打开文件
    tsfile_writer_->open(file_name_, flags, mode);

    // 注册时间序列（使用默认配压缩编码）
    string device_id = "root.db1.d1";
    vector<string> measurement_names = {
        "BOOLEAN", 
        "INT32", 
        "INT64", 
        "FLOAT", 
        "DOUBLE",
        "TEXT",
        "STRING",
        "BLOB",
        "DATE",
        "TIMESTAMP",
    };
    vector<TSDataType> data_types = {
        TSDataType::BOOLEAN,
        TSDataType::INT32, 
        TSDataType::INT64,
        TSDataType::FLOAT,
        TSDataType::DOUBLE,
        TSDataType::TEXT,
        TSDataType::STRING,
        TSDataType::BLOB,
        TSDataType::DATE,
        TSDataType::TIMESTAMP,
    };
    for (int i = 0; i < measurement_names.size(); i++) {
        MeasurementSchema schema(measurement_names[i], data_types[i]);
        ASSERT_EQ(tsfile_writer_->register_timeseries(device_id,schema), E_OK);
    }
    ASSERT_EQ(tsfile_writer_->flush(), E_OK);
    ASSERT_EQ(tsfile_writer_->close(), E_OK);
}

/**
 * 测试写入表格数据1：int TsFileWriter::write_table(Tablet &tablet)
 */
TEST_F(TsFileWriterTest, WriteTable1) {
    // 创建一个TsFileWriter对象
    TsFileWriter* tsfile_writer_ = new TsFileWriter();
    // 初始化libtsfile库
    libtsfile_init();
    // 生成一个文件名
    string file_name_ = string(file_path);

    // 设置文件打开的标志:只写模式打开|文件不存在会创建一个新的文件|文件已经存在且能够成功打开文件那么内容会被清空
    int flags = O_WRONLY | O_CREAT | O_TRUNC;
    // 设置文件权限模式
    mode_t mode = 0666;
    // 使用TsFileWriter打开文件
    tsfile_writer_->open(file_name_, flags, mode);

    // 注册时间序列（使用默认配压缩编码）
    string device_id = "root.db1.d1";
    vector<string> measurement_names = {
        "BOOLEAN", 
        "INT32", 
        "INT64", 
        "FLOAT", 
        "DOUBLE",
        // "TEXT",
    };
    vector<TSDataType> data_types = {
        TSDataType::BOOLEAN,
        TSDataType::INT32, 
        TSDataType::INT64,
        TSDataType::FLOAT,
        TSDataType::DOUBLE,
        // TSDataType::TEXT,
    };
    vector<MeasurementSchema> schema_vecs;
    vector<string> path_list;
    for (int i = 0; i < measurement_names.size(); i++) {
        MeasurementSchema schema(measurement_names[i], data_types[i]);
        ASSERT_EQ(tsfile_writer_->register_timeseries(device_id,schema), E_OK);
        path_list.push_back(device_id + "." + measurement_names[i]);
        schema_vecs.push_back(schema);
    }
    // 定义最大行数
    int max_rows = 100;
    // 构造Tablet
    auto schema_ptr = make_shared<vector<MeasurementSchema>>(schema_vecs);
    Tablet tablet(device_id, schema_ptr, max_rows);
    // 添加数据
    for (int row = 0; row < max_rows; row++) {
            // 设置时间戳
            ASSERT_EQ(tablet.add_timestamp(row, row), E_OK);
            // 设置值
            for (int i = 0; i < measurement_names.size(); i++) {
                // 根据数据类型写入对应的值
                switch (data_types[i]) {
                    case BOOLEAN:
                        ASSERT_EQ(tablet.add_value(row, i, ((row % 2) != 0? true : false)), E_OK);
                        break;
                    case INT32:
                        ASSERT_EQ(tablet.add_value(row, i, row), E_OK);
                        break;
                    case INT64:
                        ASSERT_EQ(tablet.add_value(row, i, row), E_OK);
                        break;
                    case FLOAT:
                        ASSERT_EQ(tablet.add_value(row, i, static_cast<float>(row)), E_OK);
                        break;
                    case DOUBLE:
                        ASSERT_EQ(tablet.add_value(row, i, static_cast<double>(row)), E_OK);
                        break;
                    case TEXT:
                        ASSERT_EQ(tablet.add_value(row, i, to_string(row).c_str()), E_OK);
                        break;
                    default:
                        ASSERT(false); // 断言失败，表示代码中出现了未处理的类型
                        break;
                }
            }
        }
    // 写入tablet
    ASSERT_EQ(tsfile_writer_->write_tablet(tablet), E_OK);
    // 刷新文件
    ASSERT_EQ(tsfile_writer_->flush(), E_OK);
    // 关闭TsFileWriter
    ASSERT_EQ(tsfile_writer_->close(), E_OK);

    // 读取数据进行验证
    reader(path_list, data_types, 0, max_rows, max_rows);
}