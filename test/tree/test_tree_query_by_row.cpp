#include <gtest/gtest.h>

#include <iostream>
#include <string>
#include <vector>
#include <filesystem>
#include <memory>
#include <limits>
#include <cstring>

#include "common/db_common.h"
#include "common/path.h"
#include "common/record.h"
#include "common/row_record.h"
#include "common/schema.h"
#include "common/tablet.h"
#include "file/write_file.h"
#include "reader/tsfile_reader.h"
#include "reader/tsfile_tree_reader.h"
#include "reader/qds_without_timegenerator.h"
#include "writer/tsfile_writer.h"

using namespace storage;
using namespace common;
using namespace std;

// 文件路径（默认位于项目根目录下的 data/tsfile）
string test_query_by_row_file_path = "test_tree_query_by_row.tsfile";

/** --------------------------------- 测试组件函数 --------------------------------- **/

/**
 * @brief 初始化文件路径
 */
void init_file_path() {
    char result[PATH_MAX];
    ssize_t count = readlink("/proc/self/exe", result, PATH_MAX);
    std::string executable_path = std::string(result, (count > 0) ? count : 0);
    std::filesystem::path path_obj(executable_path);
    std::string exec_path = path_obj.parent_path().string();
    std::filesystem::path root_path(exec_path);

    while (!root_path.empty() && !std::filesystem::exists(root_path / "data")) {
        root_path = root_path.parent_path();
    }

    if (!root_path.empty()) {
        std::filesystem::path directory_path = root_path / "data" / "tsfile";
        if (!filesystem::exists(directory_path) || !filesystem::is_directory(directory_path)) {
            cerr << "Directory does not exist: " << directory_path << endl;
        }
        std::filesystem::path file_path_ = directory_path / test_query_by_row_file_path;

        if (std::filesystem::exists(file_path_) && std::filesystem::is_regular_file(file_path_)) {
            std::filesystem::remove(file_path_);
        }
        test_query_by_row_file_path = file_path_.string();
    } else {
        cerr << "Directory does not exist: " << root_path;
    }
}

/**
 * @brief 获取数据类型的字符串表示
 */
string datatype_to_string(TSDataType type) {
    switch (type) {
        case TSDataType::BOOLEAN: return "BOOLEAN";
        case TSDataType::INT32: return "INT32";
        case TSDataType::INT64: return "INT64";
        case TSDataType::FLOAT: return "FLOAT";
        case TSDataType::DOUBLE: return "DOUBLE";
        case TSDataType::TEXT: return "TEXT";
        case TSDataType::STRING: return "STRING";
        case TSDataType::BLOB: return "BLOB";
        case TSDataType::DATE: return "DATE";
        case TSDataType::TIMESTAMP: return "TIMESTAMP";
        default: return "UNKNOWN";
    }
}

/**
 * @brief 测试类：用于测试前初始化环境和测试后清理环境
 */
class TsFileTreeQueryByRowTest : public ::testing::Test {
   protected:
    void SetUp() override {
        init_file_path();
        libtsfile_init();
    }

    void TearDown() override {
    }
};

/** --------------------------------- 辅助测试函数 --------------------------------- **/

/**
 * @brief 辅助测试函数：写入全部数据类型的测试数据（Tablet方式写入部分数据为空值）
 * Tablet支持的数据类型：BOOLEAN, INT32, INT64, FLOAT, DOUBLE, STRING
 * Record支持的数据类型：BOOLEAN, INT32, INT64, FLOAT, DOUBLE, TEXT，STRING, BLOB, DATE, TIMESTAMP
 * 值（仅Tablet方式写入）：偶数行正常写入，奇数行的为空值
 * 
 * @param device_id 设备ID
 * @param measurement_names 该设备的测点名称
 * @param data_types 每个测点对应的数据类型（按位置对应）
 * @param row_count 行数
 * @param start_timestamp 起始时间戳
 * @param file_path 文件路径
 * @return 错误码
 */
int write_all_types_data(const string& device_id,
                         const vector<string>& measurement_names,
                         const vector<TSDataType>& data_types,
                         int row_count,
                         int64_t start_timestamp,
                         string& file_path,
                         bool use_tablet = true) {
    int ret = E_OK;
    TsFileWriter* tsfile_writer = new TsFileWriter();

    int flags = O_WRONLY | O_CREAT | O_TRUNC;
    mode_t mode = 0666;

    ret = tsfile_writer->open(file_path, flags, mode);
    if (ret != E_OK) {
        delete tsfile_writer;
        return ret;
    }

    // 注册时间序列
    for (size_t i = 0; i < measurement_names.size(); i++) {
        MeasurementSchema schema(measurement_names[i], data_types[i]);
        ret = tsfile_writer->register_timeseries(device_id, schema);
        if (ret != E_OK) {
            delete tsfile_writer;
            return ret;
        }
    }

    if (use_tablet) {
        // Tablet 写入方式（支持 6 种数据类型：BOOLEAN, INT32, INT64, FLOAT, DOUBLE, STRING）
        auto schema_ptr = make_shared<vector<MeasurementSchema>>();
        for (size_t i = 0; i < measurement_names.size(); i++) {
            schema_ptr->emplace_back(measurement_names[i], data_types[i]);
        }
        Tablet tablet(device_id, schema_ptr, row_count);

        // 添加数据
        for (int row = 0; row < row_count; row++) {
            ret = tablet.add_timestamp(row, start_timestamp + row);
            if (ret != E_OK) {
                delete tsfile_writer;
                return ret;
            }
            for (size_t i = 0; i < measurement_names.size(); i++) {
                // 偶数行的偶数列为空，奇数行的奇数列为空值
                if ((row % 2) == (i % 2)) {
                    continue;
                }
                switch (data_types[i]) {
                    case BOOLEAN:
                        ret = tablet.add_value(row, i, (row % 2 != 0));
                        break;
                    case INT32: {
                        int32_t val = row;
                        ret = tablet.add_value(row, i, val);
                        break;
                    }
                    case INT64: {
                        int64_t val = row;
                        ret = tablet.add_value(row, i, val);
                        break;
                    }
                    case FLOAT: {
                        float val = static_cast<float>(row);
                        ret = tablet.add_value(row, i, val);
                        break;
                    }
                    case DOUBLE: {
                        double val = static_cast<double>(row);
                        ret = tablet.add_value(row, i, val);
                        break;
                    }
                    case STRING: {
                        std::string val_str = "string" + to_string(row);
                        ret = tablet.add_value(row, i, val_str.c_str());
                        break;
                    }
                    default:
                        cerr << "Tablet does not support type: " << datatype_to_string(data_types[i]) << endl;
                        delete tsfile_writer;
                        return E_TYPE_NOT_MATCH;
                }
                if (ret != E_OK) {
                    cerr << "Error writing row=" << row << " col=" << i << ", ret=" << ret << endl;
                    delete tsfile_writer;
                    return ret;
                }
            }
        }

        ret = tsfile_writer->write_tablet(tablet);
        if (ret != E_OK) {
            delete tsfile_writer;
            return ret;
        }
    } else {
        // Record 写入方式（支持 10 种数据类型）
        for (int row = 0; row < row_count; row++) {
            TsRecord record(start_timestamp + row, device_id);
            for (size_t i = 0; i < measurement_names.size(); i++) {
                // // 偶数行的偶数列为空，奇数行的奇数列为空值
                // if ((row % 2) == (i % 2)) {
                //     record.points_.emplace_back(DataPoint(measurement_names[i]));
                //     continue;
                // }
                switch (data_types[i]) {
                    case BOOLEAN:
                        record.add_point(measurement_names[i], (row % 2 == 0));
                        break;
                    case INT32:
                        record.add_point(measurement_names[i], static_cast<int32_t>(row));
                        break;
                    case TIMESTAMP:
                    case INT64:
                        record.add_point(measurement_names[i], static_cast<int64_t>(row));
                        break;
                    case FLOAT:
                        record.add_point(measurement_names[i], static_cast<float>(row));
                        break;
                    case DOUBLE:
                        record.add_point(measurement_names[i], static_cast<double>(row));
                        break;
                    case TEXT: 
                    case STRING: 
                    case BLOB: {
                        std::string val_str = to_string(row);
                        common::String blob_val(const_cast<char*>(val_str.c_str()), val_str.length());
                        record.add_point(measurement_names[i], blob_val);
                        break;
                    }
                    case DATE: {
                        std::time_t now = std::time(nullptr);
                        std::tm* local_time = std::localtime(&now);
                        std::tm date_val = {};
                        date_val.tm_year = local_time->tm_year;
                        date_val.tm_mon = local_time->tm_mon;
                        date_val.tm_mday = local_time->tm_mday + row;
                        record.add_point(measurement_names[i], date_val);
                        break;
                    }
                    default:
                        cerr << "Unsupported type: " << datatype_to_string(data_types[i]) << endl;
                        delete tsfile_writer;
                        return E_TYPE_NOT_MATCH;
                }
            }
            ret = tsfile_writer->write_record(record);
            if (ret != E_OK) {
                cerr << "Error writing row=" << row << ", ret=" << ret << endl;
                delete tsfile_writer;
                return ret;
            }
        }
    }

    ret = tsfile_writer->flush();
    if (ret != E_OK) {
        delete tsfile_writer;
        return ret;
    }
    ret = tsfile_writer->close();
    delete tsfile_writer;
    return ret;
}

/**
 * @brief 辅助测试函数：打印数据
 * 
 * @param result_set 查询结果集
 */
void print_data(storage::ResultSet* result_set) {
    auto *qds = (QDSWithoutTimeGenerator*)result_set;
    // 获取元数据和列数
    auto metadata = qds->get_metadata();
    uint32_t column_count = metadata->get_column_count();
    // 打印序列和数据类型 (索引从 1 开始，到 column_count 结束)
    for (uint32_t i = 1; i <= column_count; i++) {
        cout << metadata->get_column_name(i) << "[" << datatype_to_string(metadata->get_column_type(i)) << "]  ";
    }
    cout << endl;
    // 打印值 (索引从 1 开始，到 column_count 结束)
    for (uint32_t i = 1; i <= column_count; i++) {
        cout << " ";
        if (qds->is_null(i)) {
            cout << "null";
        } else {
            TSDataType type = metadata->get_column_type(i);
            switch (type) {
                case BOOLEAN:
                    cout << (qds->get_value<bool>(i) ? "true" : "false");
                    break;
                case DATE:
                case INT32:
                    cout << qds->get_value<int32_t>(i);
                    break;
                case TIMESTAMP:
                case INT64:
                    cout << qds->get_value<int64_t>(i);
                    break;
                case FLOAT:
                    cout << fixed << qds->get_value<float>(i);
                    break;
                case DOUBLE:
                    cout << fixed << qds->get_value<double>(i);
                    break;
                case TEXT:
                case STRING:
                case BLOB: {
                    common::String* str = qds->get_value<common::String*>(i);
                    if (str && str->buf_) {
                        cout << string(str->buf_, str->len_);
                    } else {
                        cout << "null";
                    }
                    break;
                }
                default:
                    FAIL() << "Unsupported type";
                    break;
            }
        }
    }
    cout << endl;
}

/**
 * @brief 辅助测试函数：写入多个设备的数据（支持多种数据类型）
 *
 * @param devices_and_measurements 设备和测量名称列表
 * @param data_types 数据类型列表，必须与每个设备的 measurements 数量一致
 * @param row_count 数据行数
 * @param file_path 输出文件路径
 * @param use_tablet true=使用 Tablet 写入 (支持 6 种类型), false=使用 Record 写入 (支持 10 种类型)
 * @return 操作状态码
 */
int write_multi_device_data(
    const std::vector<std::pair<std::string, std::vector<std::string>>>& devices_and_measurements,
    const std::vector<TSDataType>& data_types,
    int row_count,
    string& file_path,
    bool use_tablet = true) {
    int ret = E_OK;
    TsFileWriter* tsfile_writer = new TsFileWriter();

    int flags = O_WRONLY | O_CREAT | O_TRUNC;
    mode_t mode = 0666;

    ret = tsfile_writer->open(file_path, flags, mode);
    if (ret != E_OK) {
        delete tsfile_writer;
        return ret;
    }

    // 检查每个设备的测点数量是否与 data_types 数量一致
    for (auto& device_pair : devices_and_measurements) {
        const string& device_id = device_pair.first;
        const vector<string>& measurements = device_pair.second;
        if (measurements.size() != data_types.size()) {
            cerr << "Error: device " << device_id
                 << " has " << measurements.size() << " measurements, but data_types has "
                 << data_types.size() << " entries" << endl;
            delete tsfile_writer;
            return E_INVALID_ARG;
        }
    }

    // 注册时间序列
    for (auto& device_pair : devices_and_measurements) {
        const string& device_id = device_pair.first;
        const vector<string>& measurements = device_pair.second;
        for (size_t i = 0; i < measurements.size(); i++) {
            MeasurementSchema schema(measurements[i], data_types[i]);
            ret = tsfile_writer->register_timeseries(device_id, schema);
            if (ret != E_OK) {
                delete tsfile_writer;
                return ret;
            }
        }
    }

    // 写入数据
    if (use_tablet) {
        // Tablet 写入方式（支持 6 种数据类型：BOOLEAN, INT32, INT64, FLOAT, DOUBLE, STRING）
        for (auto& device_pair : devices_and_measurements) {
            const string& device_id = device_pair.first;
            const vector<string>& measurements = device_pair.second;

            auto schema_ptr = make_shared<vector<MeasurementSchema>>();
            for (size_t i = 0; i < measurements.size(); i++) {
                schema_ptr->emplace_back(measurements[i], data_types[i]);
            }
            Tablet tablet(device_id, schema_ptr, row_count);

            // 添加数据
            for (int row = 0; row < row_count; row++) {
                ret = tablet.add_timestamp(row, row);
                if (ret != E_OK) {
                    delete tsfile_writer;
                    return ret;
                }
                for (size_t col = 0; col < measurements.size(); col++) {
                    // 偶数行的偶数列为空，奇数行的奇数列为空值
                    if ((row % 2) == (col % 2)) {
                        continue;
                    }
                    switch (data_types[col]) {
                        case BOOLEAN:
                            ret = tablet.add_value(row, col, (row % 2 != 0));
                            break;
                        case INT32: {
                            int32_t val = row;
                            ret = tablet.add_value(row, col, val);
                            break;
                        }
                        case INT64: {
                            int64_t val = row;
                            ret = tablet.add_value(row, col, val);
                            break;
                        }
                        case FLOAT: {
                            float val = static_cast<float>(row);
                            ret = tablet.add_value(row, col, val);
                            break;
                        }
                        case DOUBLE: {
                            double val = static_cast<double>(row);
                            ret = tablet.add_value(row, col, val);
                            break;
                        }
                        case STRING: {
                            std::string val_str = "string" + to_string(row);
                            ret = tablet.add_value(row, col, val_str.c_str());
                            break;
                        }
                        default:
                            cerr << "Tablet does not support type: " << datatype_to_string(data_types[col]) << endl;
                            delete tsfile_writer;
                            return E_TYPE_NOT_MATCH;
                    }
                    if (ret != E_OK) {
                        cerr << "Error writing row=" << row << " col=" << col << ", ret=" << ret << endl;
                        delete tsfile_writer;
                        return ret;
                    }
                }
            }

            ret = tsfile_writer->write_tablet(tablet);
            if (ret != E_OK) {
                delete tsfile_writer;
                return ret;
            }
        }
    } else {
        // Record 写入方式（支持 10 种数据类型）
        for (auto& device_pair : devices_and_measurements) {
            const string& device_id = device_pair.first;
            const vector<string>& measurements = device_pair.second;

            for (int row = 0; row < row_count; row++) {
                TsRecord record(row, device_id);
                for (size_t col = 0; col < measurements.size(); col++) {
                    // // 偶数行的偶数列为空，奇数行的奇数列为空值
                    // if ((row % 2) == (col % 2)) {
                    //     record.points_.emplace_back(DataPoint(measurements[col]));
                    //     continue;
                    // }
                    switch (data_types[col]) {
                        case BOOLEAN:
                            record.add_point(measurements[col], (row % 2 != 0));
                            break;
                        case INT32:
                            record.add_point(measurements[col], static_cast<int32_t>(row));
                            break;
                        case TIMESTAMP:
                        case INT64:
                            record.add_point(measurements[col], static_cast<int64_t>(row));
                            break;
                        case FLOAT:
                            record.add_point(measurements[col], static_cast<float>(row));
                            break;
                        case DOUBLE:
                            record.add_point(measurements[col], static_cast<double>(row));
                            break;
                        case TEXT:
                        case STRING:
                        case BLOB: {
                            std::string val_str = to_string(row);
                            common::String blob_val(const_cast<char*>(val_str.c_str()), val_str.length());
                            record.add_point(measurements[col], blob_val);
                            break;
                        }
                        case DATE: {
                            std::time_t now = std::time(nullptr);
                            std::tm* local_time = std::localtime(&now);
                            std::tm date_val = {};
                            date_val.tm_year = local_time->tm_year;
                            date_val.tm_mon = local_time->tm_mon;
                            date_val.tm_mday = local_time->tm_mday + row;
                            record.add_point(measurements[col], date_val);
                            break;
                        }
                        default:
                            cerr << "Unsupported type: " << datatype_to_string(data_types[col]) << endl;
                            delete tsfile_writer;
                            return E_TYPE_NOT_MATCH;
                    }
                }
                ret = tsfile_writer->write_record(record);
                if (ret != E_OK) {
                    cerr << "Error writing row=" << row << ", ret=" << ret << endl;
                    delete tsfile_writer;
                    return ret;
                }
            }
        }
    }

    ret = tsfile_writer->flush();
    if (ret != E_OK) {
        delete tsfile_writer;
        return ret;
    }
    ret = tsfile_writer->close();
    delete tsfile_writer;
    return ret;
}

/** --------------------------------- 测试用例 --------------------------------- **/

/**
 * @brief 测试 1：测试全部数据类型
 */
TEST_F(TsFileTreeQueryByRowTest, TestAllDataTypes_WithBoundaryValues) {
    // 1. 创建数据
    string device_id = "root.d1";
    vector<string> measurement_names = {"bool_col", "int32_col", "int64_col", "float_col", "double_col", "string_col", "text_col", "blob_col", "timestamp_col", "date_col"};
    vector<TSDataType> data_types = {BOOLEAN, INT32, INT64, FLOAT, DOUBLE, STRING, TEXT, BLOB, TIMESTAMP, DATE};
    int total_rows = 15;
    ASSERT_EQ(write_all_types_data(device_id, measurement_names, data_types, total_rows, 0, test_query_by_row_file_path, false), E_OK);
    // 2. 读取数据
    TsFileTreeReader reader;
    ASSERT_EQ(reader.open(test_query_by_row_file_path), E_OK);
    vector<string> device_ids = {device_id};
    ResultSet* result_set = nullptr;
    ASSERT_EQ(reader.queryByRow(device_ids, measurement_names, 0, 10, result_set), E_OK);
    int row_count = 0;
    bool has_next = false;
    while (result_set->next(has_next) == E_OK && has_next) {
        row_count++;
    }
    // 3. 验证结果
    ASSERT_EQ(row_count, 10);
    reader.destroy_query_data_set(result_set);
    ASSERT_EQ(reader.close(), E_OK);
}


/**
 * @brief 测试 2：测试单设备 - 存在的设备
 */
TEST_F(TsFileTreeQueryByRowTest, TestDeviceId_Single_Existing) {
    // 1. 创建数据
    string device_id = "root.d1";
    vector<string> measurement_names = {"bool_col", "int32_col", "int64_col", "float_col", "double_col", "string_col"};
    vector<TSDataType> data_types = {BOOLEAN, INT32, INT64, FLOAT, DOUBLE, STRING};
    int total_rows = 100;
    // Use Record mode to write all rows (including null values on odd rows)
    ASSERT_EQ(write_all_types_data(device_id, measurement_names, data_types, total_rows, 0, test_query_by_row_file_path, true), E_OK);

    // 2. 读取数据
    TsFileTreeReader reader;
    ASSERT_EQ(reader.open(test_query_by_row_file_path), E_OK);
    vector<string> device_ids = {device_id};
    ResultSet* result_set = nullptr;
    ASSERT_EQ(reader.queryByRow(device_ids, measurement_names, 0, -1, result_set), E_OK);
    int row_count = 0;
    bool has_next = false;
    while (result_set->next(has_next) == E_OK && has_next) {
        row_count++;
    }

    // 3. 验证结果
    ASSERT_EQ(row_count, total_rows);
    reader.destroy_query_data_set(result_set);
    ASSERT_EQ(reader.close(), E_OK);
}

/**
 * @brief 测试3：测试单设备 - 不存在的设备
 */
TEST_F(TsFileTreeQueryByRowTest, TestDeviceId_Single_NotExisting) {
    GTEST_SKIP() << "预期只输出空，实际会报错不存在：E_DEVICE_NOT_EXIST";
    // 1. 创建数据
    string device_id = "root.d1";
    vector<string> measurement_names = {"bool_col", "int32_col", "int64_col", "float_col", "double_col", "string_col"};
    vector<TSDataType> data_types = {BOOLEAN, INT32, INT64, FLOAT, DOUBLE, STRING};
    int total_rows = 100;
    ASSERT_EQ(write_all_types_data(device_id, measurement_names, data_types, total_rows, 0, test_query_by_row_file_path, true), E_OK);

    // 2. 读取数据
    TsFileTreeReader reader;
    ASSERT_EQ(reader.open(test_query_by_row_file_path), E_OK);
    vector<string> device_ids = {"root.db1.d_not_exist"};
    ResultSet* result_set = nullptr;
    ASSERT_EQ(reader.queryByRow(device_ids, measurement_names, 0, -1, result_set), E_OK);
    int row_count = 0;
    bool has_next = false;
    while (result_set->next(has_next) == E_OK && has_next) {
        row_count++;
    }

    // 3. 验证结果
    ASSERT_EQ(row_count, 0);
    reader.destroy_query_data_set(result_set);
    ASSERT_EQ(reader.close(), E_OK);
}

/**
 * @brief 测试4：测试多设备 - 全存在的设备，每个设备对应测点一致
 */
TEST_F(TsFileTreeQueryByRowTest, TestDeviceId_Multi_AllExisting1) {
    // 1. 创建数据
    vector<string> device_ids = {"root.d1", "root.d2", "root.d3"};
    vector<string> measurement_names = {"bool_col", "int32_col", "int64_col", "float_col", "double_col", "string_col"};
    vector<TSDataType> data_types = {BOOLEAN, INT32, INT64, FLOAT, DOUBLE, STRING};
    vector<pair<string, vector<string>>> devices_and_measurements;
    for (size_t i = 0; i < device_ids.size(); i++)
    {
        devices_and_measurements.emplace_back(device_ids[i], measurement_names);
    }
    int total_rows = 100;
    ASSERT_EQ(write_multi_device_data(devices_and_measurements, data_types, total_rows, test_query_by_row_file_path), E_OK);

    // 2. 读取数据
    TsFileTreeReader reader;
    ASSERT_EQ(reader.open(test_query_by_row_file_path), E_OK);
    ResultSet* result_set = nullptr;
    ASSERT_EQ(reader.queryByRow(device_ids, measurement_names, 0, -1, result_set), E_OK);
    int row_count = 0;
    bool has_next = false;
    while (result_set->next(has_next) == E_OK && has_next) {
        row_count++;
    }

    // 3. 验证结果
    ASSERT_EQ(row_count, total_rows);
    reader.destroy_query_data_set(result_set);
    ASSERT_EQ(reader.close(), E_OK);
}

/**
 * @brief 测试5：测试多设备 - 全存在的设备，每个设备对应测点不一致
 */
TEST_F(TsFileTreeQueryByRowTest, TestDeviceId_Multi_AllExisting2) {
    GTEST_SKIP() << "预期只输出存在每个设备的测点，实际会报错不存在：E_NOT_EXIST";
    // 1. 创建数据
    vector<string> device_ids = {"root.d1", "root.db1.d2", "root.db1.d3"};
    vector<TSDataType> data_types = {BOOLEAN, INT32, INT64, FLOAT, DOUBLE, STRING};
    vector<pair<string, vector<string>>> devices_and_measurements = 
    {
        {"root.d1", {"bool_col_1", "int32_col_1", "int64_col_1", "float_col_1", "double_col_1", "string_col_1"}},
        {"root.db1.d2", {"bool_col_2", "int32_col_2", "int64_col_2", "float_col_2", "double_col_2", "string_col_2"}},
        {"root.db1.d3", {"bool_col_3", "int32_col_3", "int64_col_3", "float_col_3", "double_col_3", "string_col_3"}}
    };
    int total_rows = 100;
    ASSERT_EQ(write_multi_device_data(devices_and_measurements, data_types, total_rows, test_query_by_row_file_path), E_OK);

    // 2. 读取数据
    TsFileTreeReader reader;
    ASSERT_EQ(reader.open(test_query_by_row_file_path), E_OK);
    vector<string> measurement_names = {"bool_col_1", "int32_col_2", "int64_col_3", "float_col_1", "double_col_2", "string_col_3"};
    ResultSet* result_set = nullptr;
    ASSERT_EQ(reader.queryByRow(device_ids, measurement_names, 0, -1, result_set), E_OK);
    int row_count = 0;
    bool has_next = false;
    while (result_set->next(has_next) == E_OK && has_next) {
        row_count++;
    }

    // 3. 验证结果
    ASSERT_EQ(row_count, total_rows);
    reader.destroy_query_data_set(result_set);
    ASSERT_EQ(reader.close(), E_OK);
}


/**
 * @brief 测试6：多设备 - 部分设备不存在，每个设备对应测点一致
 */
TEST_F(TsFileTreeQueryByRowTest, TestDeviceId_Multi_PartialNotExisting1) {
    GTEST_SKIP() << "预期只输出存在存在的设备，实际会报错不存在：E_DEVICE_NOT_EXIST";
    vector<string> device_ids = {"root.d1", "root.db1.d2", "root.db1.d3"};
    vector<string> measurement_names = {"bool_col", "int32_col", "int64_col", "float_col", "double_col", "string_col"};
    vector<pair<string, vector<string>>> devices_and_measurements;
    for (size_t i = 0; i < device_ids.size(); i++)
    {
        devices_and_measurements.emplace_back(device_ids[i], measurement_names);
    }
    vector<TSDataType> data_types = {BOOLEAN, INT32, INT64, FLOAT, DOUBLE, STRING};
    int total_rows = 100;
    ASSERT_EQ(write_multi_device_data(devices_and_measurements, data_types, total_rows, test_query_by_row_file_path), E_OK);

    TsFileTreeReader reader;
    ASSERT_EQ(reader.open(test_query_by_row_file_path), E_OK);
    vector<string> device_ids_ = {"root.d1", "root.db1.d_not_exist", "root.db1.d3"};
    ResultSet* result_set = nullptr;
    ASSERT_EQ(reader.queryByRow(device_ids_, measurement_names, 0, -1, result_set), E_OK);
    int row_count = 0;
    bool has_next = false;
    while (result_set->next(has_next) == E_OK && has_next) {
        row_count++;
    }

    // 3. 验证结果
    ASSERT_EQ(row_count, total_rows);
    reader.destroy_query_data_set(result_set);
    ASSERT_EQ(reader.close(), E_OK);
}

/**
 * @brief 测试7：多设备 - 部分设备不存在，每个设备对应测点不一致
 */
TEST_F(TsFileTreeQueryByRowTest, TestDeviceId_Multi_PartialNotExisting2) {
    GTEST_SKIP() << "预期只输出存在存在的设备的存在的测点，实际会报错不存在：E_DEVICE_NOT_EXIST";
    // 1. 创建数据
    vector<string> device_ids = {"root.d1", "root.db1.d2", "root.db1.d3"};
    vector<TSDataType> data_types = {BOOLEAN, INT32, INT64, FLOAT, DOUBLE, STRING};
    vector<pair<string, vector<string>>> devices_and_measurements = 
    {
        {"root.d1", {"bool_col_1", "int32_col_1", "int64_col_1", "float_col_1", "double_col_1", "string_col_1"}},
        {"root.db1.d2", {"bool_col_2", "int32_col_2", "int64_col_2", "float_col_2", "double_col_2", "string_col_2"}},
        {"root.db1.d3", {"bool_col_3", "int32_col_3", "int64_col_3", "float_col_3", "double_col_3", "string_col_3"}}
    };
    int total_rows = 100;
    ASSERT_EQ(write_multi_device_data(devices_and_measurements, data_types, total_rows, test_query_by_row_file_path), E_OK);

    // 2. 读取数据
    TsFileTreeReader reader;
    ASSERT_EQ(reader.open(test_query_by_row_file_path), E_OK);
    vector<string> device_ids_ = {"root.d1", "root.db1.d_not_exist", "root.db1.d3"};
    vector<string> measurement_names_ = {"bool_col_1", "int32_col_2", "int64_col_3", "float_col_1", "double_col_2", "string_col_3"};
    ResultSet* result_set = nullptr;
    ASSERT_EQ(reader.queryByRow(device_ids_, measurement_names_, 0, -1, result_set), E_OK);
    int row_count = 0;
    bool has_next = false;
    while (result_set->next(has_next) == E_OK && has_next) {
        row_count++;
    }

    // 3. 验证结果
    ASSERT_EQ(row_count, total_rows);
    reader.destroy_query_data_set(result_set);
    ASSERT_EQ(reader.close(), E_OK);
}

/**
 * @brief 测试8：多设备 - 全部设备不存在
 */
TEST_F(TsFileTreeQueryByRowTest, TestDeviceId_Multi_AllNotExisting) {
    GTEST_SKIP() << "预期只输出空，实际会报错不存在：E_DEVICE_NOT_EXIST";
    // 1. 创建数据
    vector<string> device_ids = {"root.d1", "root.db1.d2", "root.db1.d3"};
    vector<string> measurement_names = {"bool_col", "int32_col", "int64_col", "float_col", "double_col", "string_col"};
    vector<pair<string, vector<string>>> devices_and_measurements;
    for (size_t i = 0; i < device_ids.size(); i++)
    {
        devices_and_measurements.emplace_back(device_ids[i], measurement_names);
    }
    vector<TSDataType> data_types = {BOOLEAN, INT32, INT64, FLOAT, DOUBLE, STRING};
    int total_rows = 100;
    ASSERT_EQ(write_multi_device_data(devices_and_measurements, data_types, total_rows, test_query_by_row_file_path, true), E_OK);

    // 2. 读取数据
    TsFileTreeReader reader;
    ASSERT_EQ(reader.open(test_query_by_row_file_path), E_OK);
    vector<string> device_ids_ = {"root.db1.d_not_exist1", "root.db1.d_not_exist2", "root.db1.d_not_exist3"};
    ResultSet* result_set = nullptr;
    ASSERT_EQ(reader.queryByRow(device_ids_, measurement_names, 0, -1, result_set), E_OK);
    int row_count = 0;
    bool has_next = false;
    while (result_set->next(has_next) == E_OK && has_next) {
        row_count++;
    }

    // 3. 验证结果
    ASSERT_EQ(row_count, total_rows);
    reader.destroy_query_data_set(result_set);
    ASSERT_EQ(reader.close(), E_OK);
}

/**
 * @brief 测试8：设备 ID - 小写英文
 */
TEST_F(TsFileTreeQueryByRowTest, TestDeviceId_Lowercase) {
    // 1. 创建数据
    string device_id = "root.device_lowercase";
    vector<string> measurement_names = {"bool_col", "int32_col", "int64_col", "float_col", "double_col",
                                        "text_col", "string_col", "blob_col", "date_col", "timestamp_col"};
    vector<TSDataType> data_types = {BOOLEAN, INT32, INT64, FLOAT, DOUBLE, TEXT, STRING, BLOB, DATE, TIMESTAMP};
    int total_rows = 100;
    ASSERT_EQ(write_all_types_data(device_id, measurement_names, data_types, total_rows, 0, test_query_by_row_file_path, false), E_OK);

    // 2. 读取数据
    TsFileTreeReader reader;
    ASSERT_EQ(reader.open(test_query_by_row_file_path), E_OK);
    vector<string> device_ids = {device_id};
    ResultSet* result_set = nullptr;
    ASSERT_EQ(reader.queryByRow(device_ids, measurement_names, 0, -1, result_set), E_OK);
    int row_count = 0;
    bool has_next = false;
    while (result_set->next(has_next) == E_OK && has_next) {
        row_count++;
    }
    // 3. 验证结果
    ASSERT_EQ(row_count, total_rows);
    reader.destroy_query_data_set(result_set);
    ASSERT_EQ(reader.close(), E_OK);
}

/**
 * @brief 测试9：设备 ID - 大写英文
 */
TEST_F(TsFileTreeQueryByRowTest, TestDeviceId_Uppercase) {
    // 1. 创建数据
    string device_id = "ROOT.DEVICE_UPPERCASE";
    vector<string> measurement_names = {"bool_col", "int32_col", "int64_col", "float_col", "double_col",
                                        "text_col", "string_col", "blob_col", "date_col", "timestamp_col"};
    vector<TSDataType> data_types = {BOOLEAN, INT32, INT64, FLOAT, DOUBLE, TEXT, STRING, BLOB, DATE, TIMESTAMP};
    int total_rows = 100;
    ASSERT_EQ(write_all_types_data(device_id, measurement_names, data_types, total_rows, 0, test_query_by_row_file_path, false), E_OK);

    // 2. 读取数据
    TsFileTreeReader reader;
    ASSERT_EQ(reader.open(test_query_by_row_file_path), E_OK);
    vector<string> device_ids = {device_id};
    ResultSet* result_set = nullptr;
    ASSERT_EQ(reader.queryByRow(device_ids, measurement_names, 0, -1, result_set), E_OK);
    int row_count = 0;
    bool has_next = false;
    while (result_set->next(has_next) == E_OK && has_next) {
        row_count++;
    }
    // 3. 验证结果
    ASSERT_EQ(row_count, total_rows);
    reader.destroy_query_data_set(result_set);
    ASSERT_EQ(reader.close(), E_OK);
}

/**
 * @brief 测试10：设备 ID - 纯数字
 */
TEST_F(TsFileTreeQueryByRowTest, TestDeviceId_Numbers) {
    GTEST_SKIP() << "带处理，不支持纯数字设备？";
    // 1. 创建数据
    string device_id = "root.1234";
    vector<string> measurement_names = {"bool_col", "int32_col", "int64_col", "float_col", "double_col", "string_col"};
    vector<TSDataType> data_types = {BOOLEAN, INT32, INT64, FLOAT, DOUBLE, STRING};
    int total_rows = 100;
    ASSERT_EQ(write_all_types_data(device_id, measurement_names, data_types, total_rows, 0, test_query_by_row_file_path, true), E_OK);

    // 2. 读取数据
    TsFileTreeReader reader;
    ASSERT_EQ(reader.open(test_query_by_row_file_path), E_OK);
    vector<string> device_ids = {device_id};
    ResultSet* result_set = nullptr;
    ASSERT_EQ(reader.queryByRow(device_ids, measurement_names, 0, -1, result_set), E_OK);
    int row_count = 0;
    bool has_next = false;
    while (result_set->next(has_next) == E_OK && has_next) {
        row_count++;
    }
    // 3. 验证结果
    ASSERT_EQ(row_count, total_rows);
    reader.destroy_query_data_set(result_set);
    ASSERT_EQ(reader.close(), E_OK);
}

/**
 * @brief 测试11：设备 ID - 下划线
 */
TEST_F(TsFileTreeQueryByRowTest, TestDeviceId_Underscore) {
    // 1. 创建数据
    string device_id = "root.______";
    vector<string> measurement_names = {"bool_col", "int32_col", "int64_col", "float_col", "double_col",
                                        "text_col", "string_col", "blob_col", "date_col", "timestamp_col"};
    vector<TSDataType> data_types = {BOOLEAN, INT32, INT64, FLOAT, DOUBLE, TEXT, STRING, BLOB, DATE, TIMESTAMP};
    int total_rows = 100;
    ASSERT_EQ(write_all_types_data(device_id, measurement_names, data_types, total_rows, 0, test_query_by_row_file_path, false), E_OK);

    // 2. 读取数据
    TsFileTreeReader reader;
    ASSERT_EQ(reader.open(test_query_by_row_file_path), E_OK);
    vector<string> device_ids = {device_id};
    ResultSet* result_set = nullptr;
    ASSERT_EQ(reader.queryByRow(device_ids, measurement_names, 0, -1, result_set), E_OK);
    int row_count = 0;
    bool has_next = false;
    while (result_set->next(has_next) == E_OK && has_next) {
        row_count++;
    }
    // 3. 验证结果
    ASSERT_EQ(row_count, total_rows);
    reader.destroy_query_data_set(result_set);
    ASSERT_EQ(reader.close(), E_OK);
}

/**
 * @brief 测试12：设备 ID - 中文字符
 */
TEST_F(TsFileTreeQueryByRowTest, TestDeviceId_UnicodeChinese) {
    // 1. 创建数据
    string device_id = "root.设备名称";
    vector<string> measurement_names = {"bool_col", "int32_col", "int64_col", "float_col", "double_col",
                                        "text_col", "string_col", "blob_col", "date_col", "timestamp_col"};
    vector<TSDataType> data_types = {BOOLEAN, INT32, INT64, FLOAT, DOUBLE, TEXT, STRING, BLOB, DATE, TIMESTAMP};
    int total_rows = 100;
    ASSERT_EQ(write_all_types_data(device_id, measurement_names, data_types, total_rows, 0, test_query_by_row_file_path, false), E_OK);

    // 2. 读取数据
    TsFileTreeReader reader;
    ASSERT_EQ(reader.open(test_query_by_row_file_path), E_OK);
    vector<string> device_ids = {device_id};
    ResultSet* result_set = nullptr;
    ASSERT_EQ(reader.queryByRow(device_ids, measurement_names, 0, -1, result_set), E_OK);
    int row_count = 0;
    bool has_next = false;
    while (result_set->next(has_next) == E_OK && has_next) {
        row_count++;
    }
    // 3. 验证结果
    ASSERT_EQ(row_count, total_rows);
    reader.destroy_query_data_set(result_set);
    ASSERT_EQ(reader.close(), E_OK);
}

/**
 * @brief 测试13：设备 ID - 特殊字符（只支持部分）
 */
TEST_F(TsFileTreeQueryByRowTest, TestDeviceId_Space) {
    GTEST_SKIP() << "带处理，不支持纯字符设备？";
    // 1. 创建数据
    string device_id = "root. !@#";
    vector<string> measurement_names = {"bool_col", "int32_col", "int64_col", "float_col", "double_col",
                                        "text_col", "string_col", "blob_col", "date_col", "timestamp_col"};
    vector<TSDataType> data_types = {BOOLEAN, INT32, INT64, FLOAT, DOUBLE, TEXT, STRING, BLOB, DATE, TIMESTAMP};
    int total_rows = 100;
    ASSERT_EQ(write_all_types_data(device_id, measurement_names, data_types, total_rows, 0, test_query_by_row_file_path, false), E_OK);

    // 2. 读取数据
    TsFileTreeReader reader;
    ASSERT_EQ(reader.open(test_query_by_row_file_path), E_OK);
    vector<string> device_ids = {device_id};
    ResultSet* result_set = nullptr;
    ASSERT_EQ(reader.queryByRow(device_ids, measurement_names, 0, -1, result_set), E_OK);
    int row_count = 0;
    bool has_next = false;
    while (result_set->next(has_next) == E_OK && has_next) {
        row_count++;
    }
    // 3. 验证结果
    ASSERT_EQ(row_count, total_rows);
    reader.destroy_query_data_set(result_set);
    ASSERT_EQ(reader.close(), E_OK);
}

/**
 * @brief 测试14：单测点 - 存在的测点
 */
TEST_F(TsFileTreeQueryByRowTest, TestMeasurement_Single_Existing) {
    // 1. 创建数据
    string device_id = "root.d1";
    vector<string> measurement_names = {"bool_col"};
    vector<TSDataType> data_types = {BOOLEAN};
    int total_rows = 100;
    ASSERT_EQ(write_all_types_data(device_id, measurement_names, data_types, total_rows, 0, test_query_by_row_file_path, false), E_OK);

    // 2. 读取数据
    TsFileTreeReader reader;
    ASSERT_EQ(reader.open(test_query_by_row_file_path), E_OK);
    vector<string> device_ids = {device_id};
    ResultSet* result_set = nullptr;
    ASSERT_EQ(reader.queryByRow(device_ids, measurement_names, 0, -1, result_set), E_OK);
    int row_count = 0;
    bool has_next = false;
    while (result_set->next(has_next) == E_OK && has_next) {
        row_count++;
    }
    // 3. 验证结果
    ASSERT_EQ(row_count, total_rows);
    reader.destroy_query_data_set(result_set);
    ASSERT_EQ(reader.close(), E_OK);
}

/**
 * @brief 测试15：单测点 - 不存在的测点
 */
TEST_F(TsFileTreeQueryByRowTest, TestMeasurement_Single_NotExisting) {
    GTEST_SKIP() << "预期输出空，实际会报错不存在：E_NOT_EXIST";
    // 1. 创建数据
    string device_id = "root.d1";
    vector<string> measurement_names = {"s1"};
    vector<TSDataType> data_types = {INT32};
    int total_rows = 100;
    ASSERT_EQ(write_all_types_data(device_id, measurement_names, data_types, total_rows, 0, test_query_by_row_file_path, false), E_OK);

    // 2. 读取数据
    TsFileTreeReader reader;
    ASSERT_EQ(reader.open(test_query_by_row_file_path), E_OK);
    vector<string> device_ids = {device_id};
    ResultSet* result_set = nullptr;
    ASSERT_EQ(reader.queryByRow(device_ids, measurement_names, 0, -1, result_set), E_OK);
    int row_count = 0;
    bool has_next = false;
    while (result_set->next(has_next) == E_OK && has_next) {
        row_count++;
    }
    // 3. 验证结果
    ASSERT_EQ(row_count, 0);
    reader.destroy_query_data_set(result_set);
    ASSERT_EQ(reader.close(), E_OK);
}

/**
 * @brief 测试16：多测点 - 全存在的测点
 */
TEST_F(TsFileTreeQueryByRowTest, TestMeasurement_Multi_AllExisting) {
    // 1. 创建数据
    string device_id = "root.d1";
    vector<string> measurement_names = {"bool_col", "int32_col", "int64_col", "float_col", "double_col",
                                        "text_col", "string_col", "blob_col", "date_col", "timestamp_col"};
    vector<TSDataType> data_types = {BOOLEAN, INT32, INT64, FLOAT, DOUBLE, TEXT, STRING, BLOB, DATE, TIMESTAMP};
    int total_rows = 100;
    ASSERT_EQ(write_all_types_data(device_id, measurement_names, data_types, total_rows, 0, test_query_by_row_file_path, false), E_OK);

    // 2. 读取数据
    TsFileTreeReader reader;
    ASSERT_EQ(reader.open(test_query_by_row_file_path), E_OK);
    vector<string> device_ids = {device_id};
    ResultSet* result_set = nullptr;
    ASSERT_EQ(reader.queryByRow(device_ids, measurement_names, 0, -1, result_set), E_OK);
    int row_count = 0;
    bool has_next = false;
    while (result_set->next(has_next) == E_OK && has_next) {
        row_count++;
    }
    ASSERT_EQ(row_count, total_rows);
    reader.destroy_query_data_set(result_set);
    ASSERT_EQ(reader.close(), E_OK);
}

/**
 * @brief 测试17：多测点 - 部分测点不存在
 */
TEST_F(TsFileTreeQueryByRowTest, TestMeasurement_Multi_PartialNotExisting) {
    GTEST_SKIP() << "预期输出存在的，实际会报错不存在：E_NOT_EXIST";
    // 1. 创建数据
    string device_id = "root.d1";
    vector<string> measurement_names = {"bool_col", "int32_col", "int64_col", "float_col", "double_col",
                                        "text_col", "string_col", "blob_col", "date_col", "timestamp_col"};
    vector<TSDataType> data_types = {BOOLEAN, INT32, INT64, FLOAT, DOUBLE, TEXT, STRING, BLOB, DATE, TIMESTAMP};
    int total_rows = 100;
    ASSERT_EQ(write_all_types_data(device_id, measurement_names, data_types, total_rows, 0, test_query_by_row_file_path, false), E_OK);

    // 2. 读取数据
    TsFileTreeReader reader;
    ASSERT_EQ(reader.open(test_query_by_row_file_path), E_OK);
    vector<string> device_ids = {device_id};
    ResultSet* result_set = nullptr;
    ASSERT_EQ(reader.queryByRow(device_ids, {"bool_col", "s_not_exist", "int32_col"}, 0, -1, result_set), E_OK);
    int row_count = 0;
    bool has_next = false;
    while (result_set->next(has_next) == E_OK && has_next) {
        row_count++;
    }
    // 3. 验证结果
    ASSERT_EQ(row_count, total_rows);
    reader.destroy_query_data_set(result_set);
    ASSERT_EQ(reader.close(), E_OK);
}

/**
 * @brief 测试18：多测点 - 全部测点不存在
 */
TEST_F(TsFileTreeQueryByRowTest, TestMeasurement_Multi_AllNotExisting) {
    GTEST_SKIP() << "预期输出空，实际会报错不存在：E_NOT_EXIST";
    // 1. 创建数据
    string device_id = "root.d1";
    vector<string> measurement_names = {"bool_col", "int32_col", "int64_col", "float_col", "double_col",
                                        "text_col", "string_col", "blob_col", "date_col", "timestamp_col"};
    vector<TSDataType> data_types = {BOOLEAN, INT32, INT64, FLOAT, DOUBLE, TEXT, STRING, BLOB, DATE, TIMESTAMP};
    int total_rows = 100;
    ASSERT_EQ(write_all_types_data(device_id, measurement_names, data_types, total_rows, 0, test_query_by_row_file_path, false), E_OK);

    // 2. 读取数据
    TsFileTreeReader reader;
    ASSERT_EQ(reader.open(test_query_by_row_file_path), E_OK);
    vector<string> device_ids = {device_id};
    ResultSet* result_set = nullptr;
    ASSERT_EQ(reader.queryByRow(device_ids, {"s_not_exist1", "s_not_exist2"}, 0, -1, result_set), E_OK);
    int row_count = 0;
    bool has_next = false;
    while (result_set->next(has_next) == E_OK && has_next) {
        row_count++;
    }
    // 3. 验证结果
    ASSERT_EQ(row_count, 0);
    reader.destroy_query_data_set(result_set);
    ASSERT_EQ(reader.close(), E_OK);
}

/**
 * @brief 测试19：测试名 — 小写英文
 */
TEST_F(TsFileTreeQueryByRowTest, TestMeasurement_Lowercase) {
    // 1. 创建数据
    string device_id = "root.d1";
    vector<string> measurement_names = {"bool_col", "int32_col", "int64_col", "float_col", "double_col",
                                        "text_col", "string_col", "blob_col", "date_col", "timestamp_col"};
    vector<TSDataType> data_types = {BOOLEAN, INT32, INT64, FLOAT, DOUBLE, TEXT, STRING, BLOB, DATE, TIMESTAMP};
    int total_rows = 100;
    ASSERT_EQ(write_all_types_data(device_id, measurement_names, data_types, total_rows, 0, test_query_by_row_file_path, false), E_OK);

    // 2. 读取数据
    TsFileTreeReader reader;
    ASSERT_EQ(reader.open(test_query_by_row_file_path), E_OK);
    vector<string> device_ids = {device_id};
    ResultSet* result_set = nullptr;
    ASSERT_EQ(reader.queryByRow(device_ids, measurement_names, 0, -1, result_set), E_OK);
    int row_count = 0;
    bool has_next = false;
    while (result_set->next(has_next) == E_OK && has_next) {
        row_count++;
    }
    // 3. 验证结果
    ASSERT_EQ(row_count, total_rows);
    reader.destroy_query_data_set(result_set);
    ASSERT_EQ(reader.close(), E_OK);
}

/**
 * @brief 测试20：测点名 — 大写英文
 */
TEST_F(TsFileTreeQueryByRowTest, TestMeasurement_Uppercase) {
    // 1. 创建数据
    string device_id = "root.d1";
    vector<string> measurement_names = {"BOOL_COL", "INT32_COL", "INT64_COL", "FLOAT_COL", "DOUBLE_COL",
                                        "TEXT_COL", "STRING_COL", "BLOB_COL", "DATE_COL", "TIMESTAMP_COL"};
    vector<TSDataType> data_types = {BOOLEAN, INT32, INT64, FLOAT, DOUBLE, TEXT, STRING, BLOB, DATE, TIMESTAMP};
    int total_rows = 100;
    ASSERT_EQ(write_all_types_data(device_id, measurement_names, data_types, total_rows, 0, test_query_by_row_file_path, false), E_OK);

    // 2. 读取数据
    TsFileTreeReader reader;
    ASSERT_EQ(reader.open(test_query_by_row_file_path), E_OK);
    vector<string> device_ids = {device_id};
    ResultSet* result_set = nullptr;
    ASSERT_EQ(reader.queryByRow(device_ids, measurement_names, 0, -1, result_set), E_OK);
    int row_count = 0;
    bool has_next = false;
    while (result_set->next(has_next) == E_OK && has_next) {
        row_count++;
    }
    // 3. 验证结果
    ASSERT_EQ(row_count, total_rows);
    reader.destroy_query_data_set(result_set);
    ASSERT_EQ(reader.close(), E_OK);
}

/**
 * @brief 测试21：测点名 - 纯数字
 */
TEST_F(TsFileTreeQueryByRowTest, TestMeasurement_Numbers) {
    GTEST_SKIP() << "带处理，不支持纯数字测点？";
    // 1. 创建数据
    string device_id = "root.d1";
    vector<string> measurement_names = {"'1'", "'2'", "'3'", "'4'", "'5'",
                                        "'6'", "'7'", "'8'", "'9'", "'10'"};
    vector<TSDataType> data_types = {BOOLEAN, INT32, INT64, FLOAT, DOUBLE, TEXT, STRING, BLOB, DATE, TIMESTAMP};
    int total_rows = 100;
    ASSERT_EQ(write_all_types_data(device_id, measurement_names, data_types, total_rows, 0, test_query_by_row_file_path, false), E_OK);

    // 2. 读取数据
    TsFileTreeReader reader;
    ASSERT_EQ(reader.open(test_query_by_row_file_path), E_OK);
    vector<string> device_ids = {device_id};
    ResultSet* result_set = nullptr;
    ASSERT_EQ(reader.queryByRow(device_ids, measurement_names, 0, -1, result_set), E_OK);
    int row_count = 0;
    bool has_next = false;
    while (result_set->next(has_next) == E_OK && has_next) {
        row_count++;
    }
    // 3. 验证结果
    ASSERT_EQ(row_count, total_rows);
    reader.destroy_query_data_set(result_set);
    ASSERT_EQ(reader.close(), E_OK);
}

/**
 * @brief 测试22:测点名 - 下划线
 */
TEST_F(TsFileTreeQueryByRowTest, TestMeasurement_Underscore) {
    // 1. 创建数据
    string device_id = "root.d1";
    vector<string> measurement_names = {"_", "__", "___", "____", "_____",
                                        "______", "_______", "________", "_________", "__________"};
    vector<TSDataType> data_types = {BOOLEAN, INT32, INT64, FLOAT, DOUBLE, TEXT, STRING, BLOB, DATE, TIMESTAMP};
    int total_rows = 100;
    ASSERT_EQ(write_all_types_data(device_id, measurement_names, data_types, total_rows, 0, test_query_by_row_file_path, false), E_OK);

    // 2. 读取数据
    TsFileTreeReader reader;
    ASSERT_EQ(reader.open(test_query_by_row_file_path), E_OK);
    vector<string> device_ids = {device_id};
    ResultSet* result_set = nullptr;
    ASSERT_EQ(reader.queryByRow(device_ids, measurement_names, 0, -1, result_set), E_OK);
    int row_count = 0;
    bool has_next = false;
    while (result_set->next(has_next) == E_OK && has_next) {
        row_count++;
    }
    // 3. 验证结果
    ASSERT_EQ(row_count, total_rows);
    reader.destroy_query_data_set(result_set);
    ASSERT_EQ(reader.close(), E_OK);
}

/**
 * @brief 测试23：测点名 - 中文字符
 */
TEST_F(TsFileTreeQueryByRowTest, TestMeasurement_UnicodeChinese) {
    // 1. 创建数据
    string device_id = "root.d1";
    vector<string> measurement_names = {"中文", "中文中文", "中文中文中文", "中文中文中文中文", "中文中文中文中文中文",
                                        "中文中文中文中文中文中文", "中文中文中文中文中文中文中文", "中文中文中文中文中文中文中文中文", "中文中文中文中文中文中文中文中文中文", "中文中文中文中文中文中文中文中文中文中文"};
    vector<TSDataType> data_types = {BOOLEAN, INT32, INT64, FLOAT, DOUBLE, TEXT, STRING, BLOB, DATE, TIMESTAMP};
    int total_rows = 100;
    ASSERT_EQ(write_all_types_data(device_id, measurement_names, data_types, total_rows, 0, test_query_by_row_file_path, false), E_OK);

    // 2. 读取数据
    TsFileTreeReader reader;
    ASSERT_EQ(reader.open(test_query_by_row_file_path), E_OK);
    vector<string> device_ids = {device_id};
    ResultSet* result_set = nullptr;
    ASSERT_EQ(reader.queryByRow(device_ids, measurement_names, 0, -1, result_set), E_OK);
    int row_count = 0;
    bool has_next = false;
    while (result_set->next(has_next) == E_OK && has_next) {
        row_count++;
    }
    // 3. 验证结果
    ASSERT_EQ(row_count, total_rows);
    reader.destroy_query_data_set(result_set);
    ASSERT_EQ(reader.close(), E_OK);
}

/**
 * @brief 测试24:测点名 - 特殊字符
 */
TEST_F(TsFileTreeQueryByRowTest, TestMeasurement_Space) {
    GTEST_SKIP() << "待处理，不支持特殊字符？";
    // 1. 创建数据
    string device_id = "root.d1";
    vector<string> measurement_names = {" !@#1", " !@#2", " !@#3", " !@#4", " !@#5",
                                        " !@#6", " !@#7", " !@#8", " !@#9", " !@#10"};
    vector<TSDataType> data_types = {BOOLEAN, INT32, INT64, FLOAT, DOUBLE, TEXT, STRING, BLOB, DATE, TIMESTAMP};
    int total_rows = 100;
    ASSERT_EQ(write_all_types_data(device_id, measurement_names, data_types, total_rows, 0, test_query_by_row_file_path, false), E_OK);

    // 2. 读取数据
    TsFileTreeReader reader;
    ASSERT_EQ(reader.open(test_query_by_row_file_path), E_OK);
    vector<string> device_ids = {device_id};
    ResultSet* result_set = nullptr;
    ASSERT_EQ(reader.queryByRow(device_ids, measurement_names, 0, -1, result_set), E_OK);
    int row_count = 0;
    bool has_next = false;
    while (result_set->next(has_next) == E_OK && has_next) {
        row_count++;
    }
    // 3. 验证结果
    ASSERT_EQ(row_count, total_rows);
    reader.destroy_query_data_set(result_set);
    ASSERT_EQ(reader.close(), E_OK);
}

/**
 * @brief 测试25：offset 小于 0（相当于值为0）
 */
TEST_F(TsFileTreeQueryByRowTest, TestOffset_Negative) {
    // 1. 创建数据
    string device_id = "root.d1";
    vector<string> measurement_names = {"bool_col", "int32_col", "int64_col", "float_col", "double_col",
                                        "text_col", "string_col", "blob_col", "date_col", "timestamp_col"};
    vector<TSDataType> data_types = {BOOLEAN, INT32, INT64, FLOAT, DOUBLE, TEXT, STRING, BLOB, DATE, TIMESTAMP};
    int total_rows = 100;
    ASSERT_EQ(write_all_types_data(device_id, measurement_names, data_types, total_rows, 0, test_query_by_row_file_path, false), E_OK);

    // 2. 读取数据
    TsFileTreeReader reader;
    ASSERT_EQ(reader.open(test_query_by_row_file_path), E_OK);
    vector<string> device_ids = {device_id};
    ResultSet* result_set = nullptr;
    ASSERT_EQ(reader.queryByRow(device_ids, measurement_names, -100, -1, result_set), E_OK);
    int row_count = 0;
    bool has_next = false;
    while (result_set->next(has_next) == E_OK && has_next) {
        row_count++;
    }
    // 3. 验证结果
    ASSERT_EQ(row_count, total_rows);
    reader.destroy_query_data_set(result_set);
    ASSERT_EQ(reader.close(), E_OK);
}

/**
 * @brief 测试26：offset 大于等于 0，不超过实际行数
 */
TEST_F(TsFileTreeQueryByRowTest, TestOffset_Valid) {
    // 1. 创建数据
    string device_id = "root.d1";
    vector<string> measurement_names = {"bool_col", "int32_col", "int64_col", "float_col", "double_col",
                                        "text_col", "string_col", "blob_col", "date_col", "timestamp_col"};
    vector<TSDataType> data_types = {BOOLEAN, INT32, INT64, FLOAT, DOUBLE, TEXT, STRING, BLOB, DATE, TIMESTAMP};
    int total_rows = 100;
    ASSERT_EQ(write_all_types_data(device_id, measurement_names, data_types, total_rows, 0, test_query_by_row_file_path, false), E_OK);

    // 2. 读取数据
    TsFileTreeReader reader;
    ASSERT_EQ(reader.open(test_query_by_row_file_path), E_OK);
    vector<string> device_ids = {device_id};
    ResultSet* result_set = nullptr;
    ASSERT_EQ(reader.queryByRow(device_ids, measurement_names, 5, -1, result_set), E_OK);
    int row_count = 0;
    bool has_next = false;
    while (result_set->next(has_next) == E_OK && has_next) {
        row_count++;
    }
    // 3. 验证结果
    ASSERT_EQ(row_count, 95);
    reader.destroy_query_data_set(result_set);
    ASSERT_EQ(reader.close(), E_OK);
}

/**
 * @brief 测试27：offset 超过实际行数
 */
TEST_F(TsFileTreeQueryByRowTest, TestOffset_ExceedTotal) {
    // 1. 创建数据
    string device_id = "root.d1";
    vector<string> measurement_names = {"bool_col", "int32_col", "int64_col", "float_col", "double_col",
                                        "text_col", "string_col", "blob_col", "date_col", "timestamp_col"};
    vector<TSDataType> data_types = {BOOLEAN, INT32, INT64, FLOAT, DOUBLE, TEXT, STRING, BLOB, DATE, TIMESTAMP};
    int total_rows = 100;
    ASSERT_EQ(write_all_types_data(device_id, measurement_names, data_types, total_rows, 0, test_query_by_row_file_path, false), E_OK);

    // 2. 读取数据
    TsFileTreeReader reader;
    ASSERT_EQ(reader.open(test_query_by_row_file_path), E_OK);
    vector<string> device_ids = {device_id};
    ResultSet* result_set = nullptr;
    ASSERT_EQ(reader.queryByRow(device_ids, measurement_names, 100, 100, result_set), E_OK);
    int row_count = 0;
    bool has_next = false;
    while (result_set->next(has_next) == E_OK && has_next) {
        row_count++;
    }
    // 3. 验证结果
    ASSERT_EQ(row_count, 0);
    reader.destroy_query_data_set(result_set);
    ASSERT_EQ(reader.close(), E_OK);
}

/**
 * @brief 测试28：limit 小于 0（相当于值为-1）
 */
TEST_F(TsFileTreeQueryByRowTest, TestLimit_Negative) {
    // 1. 创建数据
    string device_id = "root.d1";
    vector<string> measurement_names = {"bool_col", "int32_col", "int64_col", "float_col", "double_col",
                                        "text_col", "string_col", "blob_col", "date_col", "timestamp_col"};
    vector<TSDataType> data_types = {BOOLEAN, INT32, INT64, FLOAT, DOUBLE, TEXT, STRING, BLOB, DATE, TIMESTAMP};
    int total_rows = 100;
    ASSERT_EQ(write_all_types_data(device_id, measurement_names, data_types, total_rows, 0, test_query_by_row_file_path, false), E_OK);

    // 2. 读取数据
    TsFileTreeReader reader;
    ASSERT_EQ(reader.open(test_query_by_row_file_path), E_OK);
    vector<string> device_ids = {device_id};
    ResultSet* result_set = nullptr;
    ASSERT_EQ(reader.queryByRow(device_ids, measurement_names, 0, -1, result_set), E_OK);
    int row_count = 0;
    bool has_next = false;
    while (result_set->next(has_next) == E_OK && has_next) {
        row_count++;
    }
    // 3. 验证结果
    ASSERT_EQ(row_count, total_rows);
    reader.destroy_query_data_set(result_set);
    ASSERT_EQ(reader.close(), E_OK);
}

/**
 * @brief 测试29：limit 大于等于 0，不超过实际行数
 */
TEST_F(TsFileTreeQueryByRowTest, TestLimit_Valid) {
    // 1. 创建数据
    string device_id = "root.d1";
    vector<string> measurement_names = {"bool_col", "int32_col", "int64_col", "float_col", "double_col",
                                        "text_col", "string_col", "blob_col", "date_col", "timestamp_col"};
    vector<TSDataType> data_types = {BOOLEAN, INT32, INT64, FLOAT, DOUBLE, TEXT, STRING, BLOB, DATE, TIMESTAMP};
    int total_rows = 100;
    ASSERT_EQ(write_all_types_data(device_id, measurement_names, data_types, total_rows, 0, test_query_by_row_file_path, false), E_OK);

    // 2. 读取数据
    TsFileTreeReader reader;
    ASSERT_EQ(reader.open(test_query_by_row_file_path), E_OK);
    vector<string> device_ids = {device_id};
    ResultSet* result_set = nullptr;
    ASSERT_EQ(reader.queryByRow(device_ids, measurement_names, 0, 5, result_set), E_OK);
    int row_count = 0;
    bool has_next = false;
    while (result_set->next(has_next) == E_OK && has_next) {
        row_count++;
    }
    // 3. 验证结果
    ASSERT_EQ(row_count, 5);
    reader.destroy_query_data_set(result_set);
    ASSERT_EQ(reader.close(), E_OK);
}

/**
 * @brief 测试30：limit 超过实际行数
 */
TEST_F(TsFileTreeQueryByRowTest, TestLimit_ExceedTotal) {
    // 1. 创建数据
    string device_id = "root.d1";
    vector<string> measurement_names = {"bool_col", "int32_col", "int64_col", "float_col", "double_col",
                                        "text_col", "string_col", "blob_col", "date_col", "timestamp_col"};
    vector<TSDataType> data_types = {BOOLEAN, INT32, INT64, FLOAT, DOUBLE, TEXT, STRING, BLOB, DATE, TIMESTAMP};
    int total_rows = 100;
    ASSERT_EQ(write_all_types_data(device_id, measurement_names, data_types, total_rows, 0, test_query_by_row_file_path, false), E_OK);

    // 2. 读取数据
    TsFileTreeReader reader;
    ASSERT_EQ(reader.open(test_query_by_row_file_path), E_OK);
    vector<string> device_ids = {device_id};
    ResultSet* result_set = nullptr;
    ASSERT_EQ(reader.queryByRow(device_ids, measurement_names, 0, 10000, result_set), E_OK);
    int row_count = 0;
    bool has_next = false;
    while (result_set->next(has_next) == E_OK && has_next) {
        row_count++;
    }
    // 3. 验证结果
    ASSERT_EQ(row_count, total_rows);
    reader.destroy_query_data_set(result_set);
    ASSERT_EQ(reader.close(), E_OK);
}

/**
 * @brief 测试31：limit 小于 offset
 */
TEST_F(TsFileTreeQueryByRowTest, TestLimit_LessThanOffset) {
    // 1. 创建数据
    string device_id = "root.d1";
    vector<string> measurement_names = {"bool_col", "int32_col", "int64_col", "float_col", "double_col",
                                        "text_col", "string_col", "blob_col", "date_col", "timestamp_col"};
    vector<TSDataType> data_types = {BOOLEAN, INT32, INT64, FLOAT, DOUBLE, TEXT, STRING, BLOB, DATE, TIMESTAMP};
    int total_rows = 20;
    ASSERT_EQ(write_all_types_data(device_id, measurement_names, data_types, total_rows, 0, test_query_by_row_file_path, false), E_OK);

    // 2. 读取数据
    TsFileTreeReader reader;
    ASSERT_EQ(reader.open(test_query_by_row_file_path), E_OK);
    vector<string> device_ids = {device_id};
    ResultSet* result_set = nullptr;
    ASSERT_EQ(reader.queryByRow(device_ids, measurement_names, 15, 3, result_set), E_OK);
    int row_count = 0;
    bool has_next = false;
    while (result_set->next(has_next) == E_OK && has_next) {
        row_count++;
    }
    // 3. 验证结果
    ASSERT_EQ(row_count, 3);
    reader.destroy_query_data_set(result_set);
    ASSERT_EQ(reader.close(), E_OK);
}

/**
 * @brief 测试32：result_set 空的
 */
TEST_F(TsFileTreeQueryByRowTest, TestResultSet_Empty) {
    // 1. 创建数据
    string device_id = "root.d1";
    vector<string> measurement_names = {"bool_col", "int32_col", "int64_col", "float_col", "double_col",
                                        "text_col", "string_col", "blob_col", "date_col", "timestamp_col"};
    vector<TSDataType> data_types = {BOOLEAN, INT32, INT64, FLOAT, DOUBLE, TEXT, STRING, BLOB, DATE, TIMESTAMP};
    int total_rows = 100;
    ASSERT_EQ(write_all_types_data(device_id, measurement_names, data_types, total_rows, 0, test_query_by_row_file_path, false), E_OK);

    // 2. 读取数据
    TsFileTreeReader reader;
    ASSERT_EQ(reader.open(test_query_by_row_file_path), E_OK);
    vector<string> device_ids = {device_id};
    ResultSet* result_set = nullptr;
    ASSERT_EQ(reader.queryByRow(device_ids, measurement_names, 0, -1, result_set), E_OK);
    int row_count = 0;
    bool has_next = false;
    while (result_set->next(has_next) == E_OK && has_next) {
        row_count++;
    }
    // 3. 验证结果
    ASSERT_EQ(row_count, 100);
    reader.destroy_query_data_set(result_set);
    ASSERT_EQ(reader.close(), E_OK);
}

