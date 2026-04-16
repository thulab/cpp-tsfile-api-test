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
#include "common/statistic.h"
#include "file/write_file.h"
#include "reader/tsfile_reader.h"
#include "writer/tsfile_table_writer.h"

using namespace storage;
using namespace common;
using namespace std;

// 文件路径（默认位于项目根目录下的 data/tsfile）
string test_table_metadata_file_path = "test_table_get_timeseries_metadata.tsfile";

/** --------------------------------- 测试组件函数 --------------------------------- **/

/**
 * @brief 初始化文件路径
 */
void init_table_metadata_file_path() {
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
        std::filesystem::path file_path_ = directory_path / test_table_metadata_file_path;

        if (std::filesystem::exists(file_path_) && std::filesystem::is_regular_file(file_path_)) {
            std::filesystem::remove(file_path_);
        }
        test_table_metadata_file_path = file_path_.string();
    } else {
        cerr << "Directory does not exist: " << root_path;
    }
}

/**
 * @brief 获取数据类型的字符串表示
 */
string datatype_to_string_table_metadata(TSDataType type) {
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
        case TSDataType::VECTOR: return "VECTOR";
        case TSDataType::UNKNOWN: return "UNKNOWN";
        case TSDataType::NULL_TYPE: return "NULL_TYPE";
        case TSDataType::INVALID_DATATYPE: return "INVALID_DATATYPE";
        default: cerr << "Invalid data type" << endl;
    }
}

/**
 * @brief 测试类：用于测试前初始化环境和测试后清理环境
 */
class TsFileTableGetTimeseriesMetadataTest : public ::testing::Test {
   protected:
    void SetUp() override {
        init_table_metadata_file_path();
        libtsfile_init();
    }

    void TearDown() override {
    }
};

/** --------------------------------- 辅助测试函数 --------------------------------- **/

/**
 * @brief 辅助测试函数：写入表模型数据（用于统计测试，不跳过任何值）
 *
 * @param file_path 输出文件路径
 * @param table_name 表名
 * @param column_names 列名列表
 * @param column_categories 列类别列表（TAG 或 FIELD）
 * @param data_types 数据类型列表
 * @param row_count 数据行数
 * @param tag_value TAG 列的固定值（默认为"fixed_device"，确保所有行属于同一设备）
 * @return 操作状态码
 */
int write_table_data_metadata_for_stat(
    string& file_path,
    const string& table_name,
    const vector<string>& column_names,
    const vector<ColumnCategory>& column_categories,
    const vector<TSDataType>& data_types,
    int row_count = 100,
    const string& tag_value = "fixed_device") {
    int ret = E_OK;

    // 构造列 schema
    vector<ColumnSchema> column_schemas;
    for (size_t i = 0; i < column_names.size(); i++) {
        column_schemas.push_back(ColumnSchema(column_names[i], data_types[i], column_categories[i]));
    }

    // 构造表 schema
    auto* table_schema = new TableSchema(table_name, column_schemas);

    // 构造写入器
    WriteFile writer_file;
    int flags = O_WRONLY | O_CREAT | O_TRUNC;
    mode_t mode = 0666;
    ret = writer_file.create(file_path, flags, mode);
    if (ret != E_OK) {
        delete table_schema;
        return ret;
    }

    auto* tsfile_table_writer = new TsFileTableWriter(&writer_file, table_schema);

    // 构造 tablet 并写入数据
    Tablet tablet(column_names, data_types);
    for (int row = 0; row < row_count; row++) {
        ret = tablet.add_timestamp(row, row);
        if (ret != E_OK) {
            delete tsfile_table_writer;
            delete table_schema;
            return ret;
        }

        for (size_t col = 0; col < column_names.size(); col++) {
            // TAG 列使用固定值，FIELD 列使用行号
            if (column_categories[col] == ColumnCategory::TAG) {
                ret = tablet.add_value(row, column_names[col], tag_value.c_str());
            } else {
                switch (data_types[col]) {
                    case BOOLEAN:
                        ret = tablet.add_value(row, column_names[col], (row % 2 != 0));
                        break;
                    case INT32:
                        ret = tablet.add_value(row, column_names[col], static_cast<int32_t>(row));
                        break;
                    case TIMESTAMP:
                    case INT64:
                        ret = tablet.add_value(row, column_names[col], static_cast<int64_t>(row));
                        break;
                    case FLOAT:
                        ret = tablet.add_value(row, column_names[col], static_cast<float>(row));
                        break;
                    case DOUBLE:
                        ret = tablet.add_value(row, column_names[col], static_cast<double>(row));
                        break;
                    case STRING:
                        ret = tablet.add_value(row, column_names[col], ("string" + to_string(row)).c_str());
                        break;
                    case TEXT:
                        ret = tablet.add_value(row, column_names[col], ("text" + to_string(row)).c_str());
                        break;
                    case BLOB:
                        ret = tablet.add_value(row, column_names[col], ("blob" + to_string(row)).c_str());
                        break;
                    case DATE:
                        ret = tablet.add_value(row, column_names[col], static_cast<int32_t>(row));
                        break;
                    default:
                        cerr << "Unsupported type for stat test: " << datatype_to_string_table_metadata(data_types[col]) << endl;
                        delete tsfile_table_writer;
                        delete table_schema;
                        return E_TYPE_NOT_MATCH;
                }
            }
            if (ret != E_OK) {
                delete tsfile_table_writer;
                delete table_schema;
                return ret;
            }
        }
    }

    ret = tsfile_table_writer->write_table(tablet);
    if (ret != E_OK) {
        delete tsfile_table_writer;
        delete table_schema;
        return ret;
    }

    ret = tsfile_table_writer->flush();
    if (ret != E_OK) {
        delete tsfile_table_writer;
        delete table_schema;
        return ret;
    }

    ret = tsfile_table_writer->close();
    delete tsfile_table_writer;
    delete table_schema;
    return ret;
}

/**
 * @brief 辅助测试函数：写入表模型的多个设备数据
 *
 * @param file_path 输出文件路径
 * @param table_name 表名
 * @param tag_columns TAG 列名列表
 * @param field_columns FIELD 列名列表
 * @param data_types 数据类型列表（对应 FIELD 列）
 * @param row_count 数据行数
 * @return 操作状态码
 */
int write_multi_device_table_data_metadata(
    string& file_path,
    const string& table_name,
    const vector<string>& tag_columns,
    const vector<string>& field_columns,
    const vector<TSDataType>& data_types,
    int row_count = 100) {
    int ret = E_OK;

    // 构造列名和类别
    vector<string> column_names;
    vector<ColumnCategory> column_categories;

    for (const auto& tag_col : tag_columns) {
        column_names.push_back(tag_col);
        column_categories.push_back(ColumnCategory::TAG);
    }
    for (const auto& field_col : field_columns) {
        column_names.push_back(field_col);
        column_categories.push_back(ColumnCategory::FIELD);
    }

    // 构造列 schema
    vector<ColumnSchema> column_schemas;
    for (size_t i = 0; i < column_names.size(); i++) {
        TSDataType type = (i < tag_columns.size()) ? STRING : data_types[i - tag_columns.size()];
        column_schemas.push_back(ColumnSchema(column_names[i], type, column_categories[i]));
    }

    // 构造表 schema
    auto* table_schema = new TableSchema(table_name, column_schemas);

    // 构造写入器
    WriteFile writer_file;
    int flags = O_WRONLY | O_CREAT | O_TRUNC;
    mode_t mode = 0666;
    ret = writer_file.create(file_path, flags, mode);
    if (ret != E_OK) {
        delete table_schema;
        return ret;
    }

    auto* tsfile_table_writer = new TsFileTableWriter(&writer_file, table_schema);

    // 构造 tablet 并写入数据
    vector<TSDataType> tablet_types;
    for (size_t i = 0; i < column_names.size(); i++) {
        tablet_types.push_back((i < tag_columns.size()) ? STRING : data_types[i - tag_columns.size()]);
    }

    Tablet tablet(column_names, tablet_types);
    for (int row = 0; row < row_count; row++) {
        ret = tablet.add_timestamp(row, row);
        if (ret != E_OK) {
            delete tsfile_table_writer;
            delete table_schema;
            return ret;
        }

        for (size_t col = 0; col < column_names.size(); col++) {
            if (col < tag_columns.size()) {
                // TAG 列：写入字符串值表示不同设备
                string tag_value = "device" + to_string(row % 5);  // 5 个不同设备
                ret = tablet.add_value(row, column_names[col], tag_value.c_str());
            } else {
                // FIELD 列：根据数据类型写入
                size_t field_idx = col - tag_columns.size();
                switch (data_types[field_idx]) {
                    case BOOLEAN:
                        ret = tablet.add_value(row, column_names[col], (row % 2 != 0));
                        break;
                    case INT32:
                        ret = tablet.add_value(row, column_names[col], static_cast<int32_t>(row));
                        break;
                    case INT64:
                    case TIMESTAMP:
                        ret = tablet.add_value(row, column_names[col], static_cast<int64_t>(row));
                        break;
                    case FLOAT:
                        ret = tablet.add_value(row, column_names[col], static_cast<float>(row));
                        break;
                    case DOUBLE:
                        ret = tablet.add_value(row, column_names[col], static_cast<double>(row));
                        break;
                    case STRING:
                        ret = tablet.add_value(row, column_names[col], ("string" + to_string(row)).c_str());
                        break;
                    default:
                        cerr << "Unsupported type: " << datatype_to_string_table_metadata(data_types[field_idx]) << endl;
                        delete tsfile_table_writer;
                        delete table_schema;
                        return E_TYPE_NOT_MATCH;
                }
            }
            if (ret != E_OK) {
                delete tsfile_table_writer;
                delete table_schema;
                return ret;
            }
        }
    }

    ret = tsfile_table_writer->write_table(tablet);
    if (ret != E_OK) {
        delete tsfile_table_writer;
        delete table_schema;
        return ret;
    }

    ret = tsfile_table_writer->flush();
    if (ret != E_OK) {
        delete tsfile_table_writer;
        delete table_schema;
        return ret;
    }

    ret = tsfile_table_writer->close();
    delete tsfile_table_writer;
    delete table_schema;
    return ret;
}

/** --------------------------------- 测试用例 --------------------------------- **/

/**
 * @brief 测试 1：测试 get_all_table_schemas 获取所有表 schema
 */
TEST_F(TsFileTableGetTimeseriesMetadataTest, TestGetAllTableSchemas_Basic) {
    // 1. 创建数据 - 创建一个表
    string table_name1 = "Table1";

    vector<string> column_names1 = {"DeviceId", "Temperature", "Pressure"};
    vector<TSDataType> data_types1 = {STRING, FLOAT, DOUBLE};
    vector<ColumnCategory> column_categories1 = {ColumnCategory::TAG, ColumnCategory::FIELD, ColumnCategory::FIELD};

    ASSERT_EQ(write_table_data_metadata_for_stat(test_table_metadata_file_path, table_name1,
            column_names1, column_categories1, data_types1, 100), E_OK);

    // 2. 读取元数据
    TsFileReader reader;
    ASSERT_EQ(reader.open(test_table_metadata_file_path), E_OK);

    // 3. 获取所有表 schema
    vector<shared_ptr<TableSchema>> table_schemas = reader.get_all_table_schemas();

    // 4. 验证结果
    ASSERT_EQ(table_schemas.size(), 1) << "Table schema count mismatch, expected 1, actual " << table_schemas.size();

    bool found_table1 = false;
    for (auto& schema : table_schemas) {
        if (schema->get_table_name() == "table1") {
            found_table1 = true;
            ASSERT_EQ(schema->get_columns_num(), 3);
        }
    }

    ASSERT_TRUE(found_table1) << "Table1 schema not found";

    ASSERT_EQ(reader.close(), E_OK);
}

/**
 * @brief 测试 2：测试 get_table_schema 获取指定表 schema
 */
TEST_F(TsFileTableGetTimeseriesMetadataTest, TestGetTableSchema_SpecifiedTable) {
    // 1. 创建数据
    string table_name = "TestTable";
    vector<string> column_names = {"DeviceId", "Location", "Temperature", "Pressure", "Humidity"};
    vector<TSDataType> data_types = {STRING, STRING, FLOAT, DOUBLE, INT32};
    vector<ColumnCategory> column_categories = {ColumnCategory::TAG, ColumnCategory::TAG, ColumnCategory::FIELD, ColumnCategory::FIELD, ColumnCategory::FIELD};

    ASSERT_EQ(write_table_data_metadata_for_stat(test_table_metadata_file_path, table_name,
            column_names, column_categories, data_types, 100), E_OK);

    // 2. 读取元数据
    TsFileReader reader;
    ASSERT_EQ(reader.open(test_table_metadata_file_path), E_OK);

    // 3. 获取指定表 schema
    shared_ptr<TableSchema> schema = reader.get_table_schema(table_name);

    // 4. 验证结果
    ASSERT_NE(schema, nullptr) << "Table schema should not be null";
    ASSERT_EQ(schema->get_table_name(), "testtable");  // 表名会被转换为小写
    ASSERT_EQ(schema->get_columns_num(), 5);

    // 验证列数据类型
    vector<TSDataType> actual_types = schema->get_data_types();
    ASSERT_EQ(actual_types.size(), 5);

    ASSERT_EQ(reader.close(), E_OK);
}

/**
 * @brief 测试 3：测试 get_table_schema 获取不存在的表（应返回 null）
 */
TEST_F(TsFileTableGetTimeseriesMetadataTest, TestGetTableSchema_NonExistentTable) {
    // 1. 创建数据
    string table_name = "ExistingTable";
    vector<string> column_names = {"DeviceId", "Value"};
    vector<TSDataType> data_types = {STRING, INT32};
    vector<ColumnCategory> column_categories = {ColumnCategory::TAG, ColumnCategory::FIELD};

    ASSERT_EQ(write_table_data_metadata_for_stat(test_table_metadata_file_path, table_name,
            column_names, column_categories, data_types, 10), E_OK);

    // 2. 读取元数据
    TsFileReader reader;
    ASSERT_EQ(reader.open(test_table_metadata_file_path), E_OK);

    // 3. 获取不存在的表 schema
    shared_ptr<TableSchema> schema = reader.get_table_schema("NonExistentTable");

    // 4. 验证结果
    ASSERT_EQ(schema, nullptr) << "Non-existent table should return null schema";

    ASSERT_EQ(reader.close(), E_OK);
}

/**
 * @brief 测试 4：测试 get_table_schema 获取表 schema 中的设备 ID（表模型不支持直接获取所有设备）
 */
TEST_F(TsFileTableGetTimeseriesMetadataTest, TestGetAllDevices_Basic) {
    // 1. 创建数据
    string table_name1 = "Table1";
    vector<string> column_names1 = {"DeviceId", "Temperature", "Pressure"};
    vector<TSDataType> data_types1 = {STRING, FLOAT, DOUBLE};
    vector<ColumnCategory> column_categories1 = {ColumnCategory::TAG, ColumnCategory::FIELD, ColumnCategory::FIELD};

    ASSERT_EQ(write_table_data_metadata_for_stat(test_table_metadata_file_path, table_name1,
            column_names1, column_categories1, data_types1, 100), E_OK);

    // 2. 读取元数据
    TsFileReader reader;
    ASSERT_EQ(reader.open(test_table_metadata_file_path), E_OK);

    // 3. 获取所有设备 ID
    vector<shared_ptr<IDeviceID>> device_ids;
    try
    {
        device_ids = reader.get_all_devices(table_name1);
    }
    catch(const std::exception& e)
    {
        std::cerr << e.what() << '\n';
    }

    // 4. 验证结果
    // ASSERT_EQ(device_ids.size(), 5) << "Device count mismatch, expected 5, actual " << device_ids.size();

    // // 验证设备名
    // for (auto& device_id : device_ids) {
    //     string device_name = device_id->get_device_name();
    //     bool found = false;
    //     for (const auto& expected : devices) {
    //         if (device_name == expected) {
    //             found = true;
    //             break;
    //         }
    //     }
    //     ASSERT_TRUE(found) << "Device ID not found: " << device_name;
    // }

    ASSERT_EQ(reader.close(), E_OK);
}

/**
 * @brief 测试 5：测试 get_timeseries_metadata 获取指定设备的测点元数据
 */
TEST_F(TsFileTableGetTimeseriesMetadataTest, TestGetTimeseriesMetadata_SpecifiedDevice) {
    // 1. 创建数据
    string table_name = "TestTable";
    vector<string> devices = {"device_A", "device_B"};
    vector<string> measurements = {"FLOAT", "DOUBLE", "INT32"};
    vector<string> column_names;
    for (auto devices : devices) {
        column_names.emplace_back(devices);
    }
    for (auto measurement : measurements) {
        column_names.emplace_back(measurement);
    }
    vector<TSDataType> data_types = {STRING, STRING, FLOAT, DOUBLE, INT32};
    vector<ColumnCategory> column_categories = {ColumnCategory::TAG, ColumnCategory::TAG, ColumnCategory::FIELD, ColumnCategory::FIELD, ColumnCategory::FIELD};
    vector<ColumnSchema> column_schemas;
    for (size_t i = 0; i < column_names.size(); i++) {
        column_schemas.push_back(ColumnSchema(column_names[i], data_types[i], column_categories[i]));
    }
    auto* table_schema = new TableSchema(table_name, column_schemas);
    WriteFile writer_file;
    ASSERT_EQ(writer_file.create(test_table_metadata_file_path, O_WRONLY | O_CREAT | O_TRUNC, 0666), E_OK);
    auto* tsfile_table_writer = new TsFileTableWriter(&writer_file, table_schema);
    Tablet tablet(column_names, data_types, 500000);
    // 写入两个设备的数据
    for (int row = 0; row < 5; row++) {
        tablet.add_timestamp(row, row);
        // tablet.add_value(row, devices[0], "Device_A");
        tablet.add_value(row, devices[1], "Device_B");
        tablet.add_value(row, measurements[0], static_cast<float>(row));
        tablet.add_value(row, measurements[1], static_cast<double>(row));
        tablet.add_value(row, measurements[2], static_cast<int32_t>(row));
    }
    for (int row = 5; row < 10; row++) {
        tablet.add_timestamp(row, row);
        tablet.add_value(row, devices[0], "Device_B");
        tablet.add_value(row, devices[1], "Device_A");
        tablet.add_value(row, measurements[0], static_cast<float>(row));
        tablet.add_value(row, measurements[1], static_cast<double>(row));
        tablet.add_value(row, measurements[2], static_cast<int32_t>(row));
    }
    ASSERT_EQ(tsfile_table_writer->write_table(tablet), E_OK);
    ASSERT_EQ(tsfile_table_writer->flush(), E_OK);
    ASSERT_EQ(tsfile_table_writer->close(), E_OK);
    delete tsfile_table_writer;
    delete table_schema;

    // 2. 读取元数据
    TsFileReader reader;
    ASSERT_EQ(reader.open(test_table_metadata_file_path), E_OK);

    // 3. 获取所有设备的测点元数据
    std::vector<std::string> selected_device_segments = {"testtable", "Device_B", "Device_A"};
    std::vector<std::string*> null_device = {new std::string("testtable"), nullptr, new std::string("Device_B")};
    std::vector<std::string> not_exist_device_segments = {"testtable", "NonExist1", "NonExist2"};
    vector<shared_ptr<IDeviceID>> device_ids = 
    {
        make_shared<StringArrayDeviceID>(selected_device_segments),
        make_shared<StringArrayDeviceID>(null_device),
        make_shared<StringArrayDeviceID>(not_exist_device_segments),
    };
    DeviceTimeseriesMetadataMap metadata = reader.get_timeseries_metadata(device_ids);
    // DeviceTimeseriesMetadataMap metadata = reader.get_timeseries_metadata();
    for (auto& [device_id, fields] : metadata) {
        // 验证设备名
        cout << "TAG: " << device_id->get_device_name() << endl;
        for (auto& field : fields) {
            cout << "FIELD: " << field->get_measurement_name().to_std_string() << ", "
            << "Data type: " << static_cast<int>(field->get_data_type()) << ", "
            "statistics: "<< field->get_statistic()->count_ << ", " << field->get_statistic()->start_time_ << ", " << field->get_statistic()->end_time_<< endl;
        }
    }
    // 4. 验证结果
    ASSERT_GE(metadata.size(), 1) << "Expected at least 1 devices, actual " << metadata.size();

    ASSERT_EQ(reader.close(), E_OK);
}

/**
 * @brief 测试 6：测试 get_timeseries_metadata 获取所有设备的测点元数据
 */
TEST_F(TsFileTableGetTimeseriesMetadataTest, TestGetTimeseriesMetadata_AllDevices) {
    // 1. 创建数据
    string table_name = "TestTable";
    vector<ColumnSchema> column_schemas = {
        {"DeviceId", STRING, ColumnCategory::TAG},
        {"Temperature", FLOAT, ColumnCategory::FIELD},
        {"Pressure", DOUBLE, ColumnCategory::FIELD}
    };

    auto* table_schema = new TableSchema(table_name, column_schemas);
    WriteFile writer_file;
    ASSERT_EQ(writer_file.create(test_table_metadata_file_path, O_WRONLY | O_CREAT | O_TRUNC, 0666), E_OK);
    auto* tsfile_table_writer = new TsFileTableWriter(&writer_file, table_schema);
    Tablet tablet({"DeviceId", "Temperature", "Pressure"}, {STRING, FLOAT, DOUBLE});

    // 写入 3 个设备的数据
    vector<string> devices = {"d1", "d2", "d3"};
    for (int row = 0; row < 90; row++) {
        tablet.add_timestamp(row, row);
        tablet.add_value(row, "DeviceId", devices[row % 3].c_str());
        tablet.add_value(row, "Temperature", static_cast<float>(row));
        tablet.add_value(row, "Pressure", static_cast<double>(row * 2));
    }

    ASSERT_EQ(tsfile_table_writer->write_table(tablet), E_OK);
    ASSERT_EQ(tsfile_table_writer->flush(), E_OK);
    ASSERT_EQ(tsfile_table_writer->close(), E_OK);
    delete tsfile_table_writer;
    delete table_schema;

    // 2. 读取元数据
    TsFileReader reader;
    ASSERT_EQ(reader.open(test_table_metadata_file_path), E_OK);

    // 3. 获取所有设备的测点元数据
    DeviceTimeseriesMetadataMap metadata = reader.get_timeseries_metadata();

    // 4. 验证结果
    ASSERT_EQ(metadata.size(), 3) << "Expected 3 devices, actual " << metadata.size();

    for (auto& [did, timeseries_list] : metadata) {
        ASSERT_EQ(timeseries_list.size(), 2) << "Expected 2 timeseries per device, actual " << timeseries_list.size();
    }

    ASSERT_EQ(reader.close(), E_OK);
}

/**
 * @brief 测试 7：测试 get_timeseries_metadata 返回统计信息（count, start_time, end_time）
 *
 * 注意：表模型的数据组织方式与树模型不同，每个唯一的 TAG 组合被视为一个设备。
 * 此测试验证表模型的基本统计功能。
 */
TEST_F(TsFileTableGetTimeseriesMetadataTest, TestGetTimeseriesMetadata_StatisticInfo) {
    // 1. 创建数据 - 写入 100 行数据，时间戳从 0 到 99
    string table_name = "StatTable";
    vector<string> column_names = {"DeviceId", "Temperature", "Pressure"};
    vector<TSDataType> data_types = {STRING, FLOAT, DOUBLE};
    vector<ColumnCategory> column_categories = {ColumnCategory::TAG, ColumnCategory::FIELD, ColumnCategory::FIELD};

    int row_count = 100;
    ASSERT_EQ(write_table_data_metadata_for_stat(test_table_metadata_file_path, table_name,
            column_names, column_categories, data_types, row_count), E_OK);

    // 2. 读取元数据
    TsFileReader reader;
    ASSERT_EQ(reader.open(test_table_metadata_file_path), E_OK);

    // 3. 获取所有设备的测点元数据
    DeviceTimeseriesMetadataMap metadata = reader.get_timeseries_metadata();

    // 4. 验证统计信息 - 表模型中每个唯一 TAG 组合是一个设备
    // 由于我们写入了 100 行不同的 DeviceId 值，所以会有 100 个设备
    ASSERT_GE(metadata.size(), 1) << "Expected at least 1 device, actual " << metadata.size();

    // 验证至少有一个设备有正确的统计信息
    bool found_valid_statistic = false;
    for (const auto& entry : metadata) {
        const auto& timeseries_list = entry.second;
        if (timeseries_list.size() >= 2) {  // Temperature 和 Pressure
            for (const auto& ts : timeseries_list) {
                if (ts->get_statistic()->count_ > 0) {
                    found_valid_statistic = true;
                    break;
                }
            }
        }
    }
    ASSERT_TRUE(found_valid_statistic) << "No valid statistic found";

    ASSERT_EQ(reader.close(), E_OK);
}

/**
 * @brief 测试 8：测试单行数据的统计信息
 */
TEST_F(TsFileTableGetTimeseriesMetadataTest, TestGetTimeseriesMetadata_SingleRowStatistic) {
    // 1. 创建数据 - 只写入 1 行数据
    string table_name = "SingleRowTable";
    vector<string> column_names = {"DeviceId", "Value"};
    vector<TSDataType> data_types = {STRING, INT64};
    vector<ColumnCategory> column_categories = {ColumnCategory::TAG, ColumnCategory::FIELD};
    int row_count = 1;

    ASSERT_EQ(write_table_data_metadata_for_stat(test_table_metadata_file_path, table_name,
            column_names, column_categories, data_types, row_count), E_OK);

    // 2. 读取元数据
    TsFileReader reader;
    ASSERT_EQ(reader.open(test_table_metadata_file_path), E_OK);

    // 3. 获取测点元数据
    DeviceTimeseriesMetadataMap metadata = reader.get_timeseries_metadata();

    // 4. 验证统计信息
    ASSERT_EQ(metadata.size(), 1);

    for (const auto& entry : metadata) {
        const auto& timeseries_list = entry.second;
        ASSERT_EQ(timeseries_list.size(), 1);

        const auto& ts = timeseries_list[0];
        // count = 1
        EXPECT_EQ(ts->get_statistic()->count_, 1);
        // start_time = end_time = 0 (只有一行，时间戳为 0)
        EXPECT_EQ(ts->get_statistic()->start_time_, 0);
        EXPECT_EQ(ts->get_statistic()->end_time_, 0);
    }

    ASSERT_EQ(reader.close(), E_OK);
}

/**
 * @brief 测试 9：测试指定设备列表时返回统计信息
 *
 * 注意：表模型的设备 ID 包含表名，且 get_timeseries_metadata(device_ids)
 * 需要匹配完整的设备路径。此测试验证基本的元数据读取功能。
 */
TEST_F(TsFileTableGetTimeseriesMetadataTest, TestGetTimeseriesMetadata_StatisticInfoForSpecifiedDevices) {
    // 1. 创建数据 - 写入固定设备的数据
    string table_name = "StatTable";
    vector<ColumnSchema> column_schemas = {
        {"DeviceId", STRING, ColumnCategory::TAG},
        {"Value", DOUBLE, ColumnCategory::FIELD}
    };

    auto* table_schema = new TableSchema(table_name, column_schemas);
    WriteFile writer_file;
    ASSERT_EQ(writer_file.create(test_table_metadata_file_path, O_WRONLY | O_CREAT | O_TRUNC, 0666), E_OK);
    auto* tsfile_table_writer = new TsFileTableWriter(&writer_file, table_schema);
    Tablet tablet({"DeviceId", "Value"}, {STRING, DOUBLE});

    // 写入固定设备的数据
    int row_count = 100;
    for (int row = 0; row < row_count; row++) {
        tablet.add_timestamp(row, row);
        tablet.add_value(row, "DeviceId", "fixed_device");
        tablet.add_value(row, "Value", static_cast<double>(row));
    }

    ASSERT_EQ(tsfile_table_writer->write_table(tablet), E_OK);
    ASSERT_EQ(tsfile_table_writer->flush(), E_OK);
    ASSERT_EQ(tsfile_table_writer->close(), E_OK);
    delete tsfile_table_writer;
    delete table_schema;

    // 2. 读取元数据
    TsFileReader reader;
    ASSERT_EQ(reader.open(test_table_metadata_file_path), E_OK);

    // 3. 获取所有设备的测点元数据
    DeviceTimeseriesMetadataMap metadata = reader.get_timeseries_metadata();

    // 4. 验证结果 - 应该有一个设备
    ASSERT_EQ(metadata.size(), 1);

    for (const auto& entry : metadata) {
        const auto& timeseries_list = entry.second;
        ASSERT_EQ(timeseries_list.size(), 1) << "Device should have 1 timeseries";

        const auto& ts = timeseries_list[0];
        EXPECT_EQ(ts->get_statistic()->count_, row_count);
        EXPECT_EQ(ts->get_statistic()->start_time_, 0);
        EXPECT_EQ(ts->get_statistic()->end_time_, (row_count - 1));
    }

    ASSERT_EQ(reader.close(), E_OK);
}

/**
 * @brief 测试 10：测试不存在的设备（应返回空）
 */
TEST_F(TsFileTableGetTimeseriesMetadataTest, TestGetTimeseriesMetadata_NonExistentDevice) {
    // 1. 创建数据
    string table_name = "TestTable";
    vector<ColumnSchema> column_schemas = {
        {"DeviceId", STRING, ColumnCategory::TAG},
        {"Value", INT32, ColumnCategory::FIELD}
    };

    auto* table_schema = new TableSchema(table_name, column_schemas);
    WriteFile writer_file;
    ASSERT_EQ(writer_file.create(test_table_metadata_file_path, O_WRONLY | O_CREAT | O_TRUNC, 0666), E_OK);
    auto* tsfile_table_writer = new TsFileTableWriter(&writer_file, table_schema);
    Tablet tablet({"DeviceId", "Value"}, {STRING, INT32});

    for (int row = 0; row < 10; row++) {
        tablet.add_timestamp(row, row);
        tablet.add_value(row, "DeviceId", "existing_device");
        tablet.add_value(row, "Value", static_cast<int32_t>(row));
    }

    ASSERT_EQ(tsfile_table_writer->write_table(tablet), E_OK);
    ASSERT_EQ(tsfile_table_writer->flush(), E_OK);
    ASSERT_EQ(tsfile_table_writer->close(), E_OK);
    delete tsfile_table_writer;
    delete table_schema;

    // 2. 读取元数据
    TsFileReader reader;
    ASSERT_EQ(reader.open(test_table_metadata_file_path), E_OK);

    // 3. 获取不存在的设备元数据
    auto non_existent_device = make_shared<StringArrayDeviceID>("non_existent_device");
    vector<shared_ptr<IDeviceID>> device_ids = {non_existent_device};
    DeviceTimeseriesMetadataMap metadata = reader.get_timeseries_metadata(device_ids);

    // 4. 验证结果：不存在的设备不会返回到 map 中
    ASSERT_EQ(metadata.size(), 0);
    ASSERT_EQ(metadata.count(non_existent_device), 0);

    ASSERT_EQ(reader.close(), E_OK);
}

/**
 * @brief 测试 11：测试空设备列表（应返回空）
 */
TEST_F(TsFileTableGetTimeseriesMetadataTest, TestGetTimeseriesMetadata_EmptyDeviceList) {
    // 1. 创建数据
    string table_name = "TestTable";
    vector<ColumnSchema> column_schemas = {
        {"DeviceId", STRING, ColumnCategory::TAG},
        {"Value", INT32, ColumnCategory::FIELD}
    };

    auto* table_schema = new TableSchema(table_name, column_schemas);
    WriteFile writer_file;
    ASSERT_EQ(writer_file.create(test_table_metadata_file_path, O_WRONLY | O_CREAT | O_TRUNC, 0666), E_OK);
    auto* tsfile_table_writer = new TsFileTableWriter(&writer_file, table_schema);
    Tablet tablet({"DeviceId", "Value"}, {STRING, INT32});

    for (int row = 0; row < 10; row++) {
        tablet.add_timestamp(row, row);
        tablet.add_value(row, "DeviceId", "device1");
        tablet.add_value(row, "Value", static_cast<int32_t>(row));
    }

    ASSERT_EQ(tsfile_table_writer->write_table(tablet), E_OK);
    ASSERT_EQ(tsfile_table_writer->flush(), E_OK);
    ASSERT_EQ(tsfile_table_writer->close(), E_OK);
    delete tsfile_table_writer;
    delete table_schema;

    // 2. 读取元数据
    TsFileReader reader;
    ASSERT_EQ(reader.open(test_table_metadata_file_path), E_OK);

    // 3. 获取空设备列表的元数据
    vector<shared_ptr<IDeviceID>> empty_device_ids;
    DeviceTimeseriesMetadataMap metadata = reader.get_timeseries_metadata(empty_device_ids);

    // 4. 验证结果
    ASSERT_EQ(metadata.size(), 0);

    ASSERT_EQ(reader.close(), E_OK);
}

/**
 * @brief 测试 12：测试多种数据类型的统计信息
 *
 * 注意：表模型中每个唯一的 TAG 组合是一个设备。此测试使用固定设备值
 * 来验证统计信息的正确性。
 */
TEST_F(TsFileTableGetTimeseriesMetadataTest, TestGetTimeseriesMetadata_MultiDataTypeStatistic) {
    // 1. 创建数据 - 包含多种数据类型，使用固定设备
    string table_name = "MultiTypeTable";
    vector<ColumnSchema> column_schemas = {
        {"DeviceId", STRING, ColumnCategory::TAG},
        {"BoolVal", BOOLEAN, ColumnCategory::FIELD},
        {"Int32Val", INT32, ColumnCategory::FIELD},
        {"Int64Val", INT64, ColumnCategory::FIELD},
        {"FloatVal", FLOAT, ColumnCategory::FIELD},
        {"DoubleVal", DOUBLE, ColumnCategory::FIELD}
    };

    auto* table_schema = new TableSchema(table_name, column_schemas);
    WriteFile writer_file;
    ASSERT_EQ(writer_file.create(test_table_metadata_file_path, O_WRONLY | O_CREAT | O_TRUNC, 0666), E_OK);
    auto* tsfile_table_writer = new TsFileTableWriter(&writer_file, table_schema);
    Tablet tablet(
        {"DeviceId", "BoolVal", "Int32Val", "Int64Val", "FloatVal", "DoubleVal"},
        {STRING, BOOLEAN, INT32, INT64, FLOAT, DOUBLE}
    );

    // 写入固定设备的数据
    int row_count = 50;
    for (int row = 0; row < row_count; row++) {
        tablet.add_timestamp(row, row);
        tablet.add_value(row, "DeviceId", "fixed_device");
        tablet.add_value(row, "BoolVal", (row % 2 != 0));
        tablet.add_value(row, "Int32Val", static_cast<int32_t>(row));
        tablet.add_value(row, "Int64Val", static_cast<int64_t>(row));
        tablet.add_value(row, "FloatVal", static_cast<float>(row));
        tablet.add_value(row, "DoubleVal", static_cast<double>(row));
    }

    ASSERT_EQ(tsfile_table_writer->write_table(tablet), E_OK);
    ASSERT_EQ(tsfile_table_writer->flush(), E_OK);
    ASSERT_EQ(tsfile_table_writer->close(), E_OK);
    delete tsfile_table_writer;
    delete table_schema;

    // 2. 读取元数据
    TsFileReader reader;
    ASSERT_EQ(reader.open(test_table_metadata_file_path), E_OK);

    // 3. 获取测点元数据
    DeviceTimeseriesMetadataMap metadata = reader.get_timeseries_metadata();

    // 4. 验证结果 - 应该有一个设备，包含 5 个 FIELD 测点
    ASSERT_EQ(metadata.size(), 1);

    for (const auto& entry : metadata) {
        const auto& timeseries_list = entry.second;
        ASSERT_EQ(timeseries_list.size(), 5) << "Expected 5 timeseries, actual " << timeseries_list.size();

        for (const auto& ts : timeseries_list) {
            EXPECT_EQ(ts->get_statistic()->count_, row_count)
                << "Timeseries " << ts->get_measurement_name().to_std_string() << " count mismatch";
        }
    }

    ASSERT_EQ(reader.close(), E_OK);
}
