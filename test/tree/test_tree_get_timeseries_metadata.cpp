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
#include "common/device_id.h"
#include "common/statistic.h"
#include "file/write_file.h"
#include "reader/tsfile_reader.h"
#include "reader/tsfile_tree_reader.h"
#include "writer/tsfile_writer.h"

using namespace storage;
using namespace common;
using namespace std;

// 文件路径（默认位于项目根目录下的 data/tsfile）
string test_metadata_file_path = "test_tree_get_timeseries_metadata.tsfile";

/** --------------------------------- 测试组件函数 --------------------------------- **/

/**
 * @brief 初始化文件路径
 */
void init_metadata_file_path() {
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
        std::filesystem::path file_path_ = directory_path / test_metadata_file_path;

        if (std::filesystem::exists(file_path_) && std::filesystem::is_regular_file(file_path_)) {
            std::filesystem::remove(file_path_);
        }
        test_metadata_file_path = file_path_.string();
    } else {
        cerr << "Directory does not exist: " << root_path;
    }
}

/**
 * @brief 获取数据类型的字符串表示
 */
string datatype_to_string_metadata(TSDataType type) {
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
class TsFileTreeGetTimeseriesMetadataTest : public ::testing::Test {
   protected:
    void SetUp() override {
        init_metadata_file_path();
        libtsfile_init();
    }

    void TearDown() override {
    }
};

/** --------------------------------- 辅助测试函数 --------------------------------- **/

/**
 * @brief 辅助测试函数：写入多设备数据（用于统计测试，不跳过任何值，使用 Record 方式）
 *
 * @param file_path 输出文件路径
 * @param devices_and_measurements 设备和测量名称列表
 * @param data_types 数据类型列表，必须与每个设备的 measurements 数量一致
 * @param row_count 数据行数
 * @param time_multiplier 时间戳乘数（默认 604800000，即时间戳 = row * 1000）
 * @return 操作状态码
 */
int write_multi_device_data_metadata_for_stat(
    string& file_path,
    const vector<pair<string, vector<string>>>& devices_and_measurements,
    const vector<TSDataType>& data_types,
    int row_count = 100,
    int time_multiplier = 1) {
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

    // 写入数据（使用 Record 方式，确保所有值都写入）
    for (auto& device_pair : devices_and_measurements) {
        const string& device_id = device_pair.first;
        const vector<string>& measurements = device_pair.second;

        for (int row = 0; row < row_count; row++) {
            TsRecord record(row * time_multiplier, device_id);
            for (size_t col = 0; col < measurements.size(); col++) {
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
                    default:
                        cerr << "Unsupported type for stat test: " << datatype_to_string_metadata(data_types[col]) << endl;
                        delete tsfile_writer;
                        return E_TYPE_NOT_MATCH;
                }
            }
            ret = tsfile_writer->write_record(record);
            if (ret != E_OK) {
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
 * @brief 辅助测试函数：写入多个设备的数据
 *
 * @param file_path 输出文件路径
 * @param devices_and_measurements 设备和测量名称列表
 * @param data_types 数据类型列表，必须与每个设备的 measurements 数量一致
 * @param row_count 数据行数
 * @param use_tablet true=使用 Tablet 写入，false=使用 Record 写入
 * @param time_multiplier 时间戳乘数（默认 604800000，即时间戳 = row * 1000）
 * @return 操作状态码
 */
int write_multi_device_data_metadata(
    string& file_path,
    const vector<pair<string, vector<string>>>& devices_and_measurements,
    const vector<TSDataType>& data_types,
    int row_count = 100,
    bool use_tablet = true,
    int time_multiplier = 604800000) {
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
                            cerr << "Tablet does not support type: " << datatype_to_string_metadata(data_types[col]) << endl;
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
        for (auto& device_pair : devices_and_measurements) {
            const string& device_id = device_pair.first;
            const vector<string>& measurements = device_pair.second;

            for (int row = 0; row < row_count; row++) {
                TsRecord record(row, device_id);
                for (size_t col = 0; col < measurements.size(); col++) {
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
                            cerr << "Unsupported type: " << datatype_to_string_metadata(data_types[col]) << endl;
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
 * @brief 测试 1：测试 get_all_device_ids 获取所有设备 ID，设备名包含大小写英文、中文、字符、数字、特殊字符等
 */
TEST_F(TsFileTreeGetTimeseriesMetadataTest, TestGetAllDeviceIds_Basic) {
    // 1. 创建数据
    vector<string> devices = 
    {
        "root.db.device1", 
        "root.DB.Devices2", 
        "root.db.设备3", 
        "root.d4", 
        "root.db.a.b.c.d.e.f.g.h.i.j.k.l.m.n.o.p.q.r.s.t.u.v.w.x.y.z", 
        "root.db.`123`", 
        "root.db.`!@#   $%^&*()_+-=[]|{};:'\",<.>/?`"
    };
    vector<string> measurements = {"s1", "s2", "s3", "s4", "s5", "s6"};
    vector<TSDataType> data_types = {BOOLEAN, INT32, INT64, FLOAT, DOUBLE, STRING};
    vector<pair<string, vector<string>>> devices_and_measurements;
    for (auto& device : devices) {
        devices_and_measurements.push_back(make_pair(device, measurements));
    }
    ASSERT_EQ(write_multi_device_data_metadata(test_metadata_file_path, devices_and_measurements, data_types), E_OK);
    
    // 2. 读取元数据
    TsFileTreeReader reader;
    ASSERT_EQ(reader.open(test_metadata_file_path), E_OK);

    // 3. 获取所有设备 ID
    vector<string> device_ids = reader.get_all_device_ids();

    // 4. 验证结果
    ASSERT_EQ(device_ids.size(), devices.size()) << "Device ID count mismatch, expected " << devices.size() << ", actual " << device_ids.size();
    for (auto& device_id : device_ids) {
       EXPECT_TRUE(std::find(devices.begin(), devices.end(), device_id) != devices.end()) << "Device ID not found: " << device_id;
    }

    ASSERT_EQ(reader.close(), E_OK);
}

/**
 * @brief 测试 2：测试 get_all_devices 获取所有设备 ID，设备名包含大小写英文、中文、字符、数字、特殊字符等
 */
TEST_F(TsFileTreeGetTimeseriesMetadataTest, TestGetAllDevices_IDeviceIDForm) {
    // 1. 创建数据
    vector<string> devices = 
    {
        "root.db.device1", 
        "root.DB.Devices2", 
        "root.db.设备3", 
        "root.d4", 
        "root.db.a.b.c.d.e.f.g.h.i.j.k.l.m.n.o.p.q.r.s.t.u.v.w.x.y.z", 
        "root.db.`123`", 
        "root.db.`!@#   $%^&*()_+-=[]|{};:'\",<.>/?`"
    };
    vector<string> measurements = {"s1", "s2", "s3", "s4", "s5", "s6"};
    vector<TSDataType> data_types = {BOOLEAN, INT32, INT64, FLOAT, DOUBLE, STRING};
    vector<pair<string, vector<string>>> devices_and_measurements;
    for (auto& device : devices) {
        devices_and_measurements.push_back(make_pair(device, measurements));
    }
    ASSERT_EQ(write_multi_device_data_metadata(test_metadata_file_path, devices_and_measurements, data_types), E_OK);

    // 2. 读取元数据
    TsFileTreeReader reader;
    ASSERT_EQ(reader.open(test_metadata_file_path), E_OK);

    // 3. 获取所有设备
    vector<shared_ptr<IDeviceID>> device_ids = reader.get_all_devices();

    // 4. 验证结果
    ASSERT_EQ(devices.size(), devices.size()) << "Device ID size mismatch, expected: " << devices.size() << ", actual: " << device_ids.size() << endl;
    for (auto& device_id : device_ids) {
        EXPECT_TRUE(std::find(devices.begin(), devices.end(), device_id->get_device_name()) != devices.end()) << "Device ID not found: " << device_id->get_device_name();
    }

    ASSERT_EQ(reader.close(), E_OK);
}

/**
 * @brief 测试 3：测试 get_timeseries_metadata 获取指定设备的测点元数据（一次一个个设备）
 */
TEST_F(TsFileTreeGetTimeseriesMetadataTest, TestGetTimeseriesMetadata_SpecifiedDevice1) {
    // 1. 创建数据
    vector<string> devices = 
    {
        "root.db.device1", 
        "root.DB.Devices2", 
        "root.db.设备3", 
        "root.d4", 
        "root.db.a.b.c.d.e.f.g.h.i.j.k.l.m.n.o.p.q.r.s.t.u.v.w.x.y.z", 
        "root.db.`123`", 
        "root.db.`!@#   $%^&*()_+-=[]|{};:'\",<.>/?`"
    };
    vector<string> measurements = {"measurement1", "Measurement2", "测点3", "12345", "!@#   $%^&*()_+-=[]|{};:'\",<.>/?", "m1.m2", "m7", "m8", "m9", "m10"};
    vector<TSDataType> data_types = {BOOLEAN, INT32, INT64, FLOAT, DOUBLE, STRING, TEXT, BLOB, DATE, TIMESTAMP};
    vector<pair<string, vector<string>>> devices_and_measurements;
    for (auto& device : devices) {
        devices_and_measurements.push_back(make_pair(device, measurements));
    }
    ASSERT_EQ(write_multi_device_data_metadata(test_metadata_file_path, devices_and_measurements, data_types, 10, false), E_OK);

    // 2. 读取元数据
    TsFileTreeReader reader;
    ASSERT_EQ(reader.open(test_metadata_file_path), E_OK);

    // 3. 获取指定设备的测点元数据
    for (auto& device : devices) { 
        vector<shared_ptr<IDeviceID>> device_ids = {make_shared<StringArrayDeviceID>(device)};
        DeviceTimeseriesMetadataMap metadata = reader.get_timeseries_metadata(device_ids);
        // 4. 验证结果
        ASSERT_EQ(metadata.size(), 1) << "Device timeseries metadata count mismatch, expected: " << 1 << ", actual: " << metadata.size() << endl;
        for (auto& [device_id, timeseries_list] : metadata) {
            // 验证设备名
            ASSERT_EQ(device_id->get_device_name(), device) << "Device name mismatch, expected: " << device << ", actual: " << device_id->get_device_name() << endl;
            for (auto& ts : timeseries_list) {
                // 验证测点名
                auto measurement = std::find(measurements.begin(), measurements.end(), ts->get_measurement_name().to_std_string());
                ASSERT_TRUE(measurement != measurements.end()) << "Measurement not found: " << ts->get_measurement_name().to_std_string();
                // 验证数据类型
                ASSERT_EQ(ts->get_data_type(), data_types[measurement - measurements.begin()]) << "Data type mismatch for measurement, Expected: " << get_data_type_name(data_types[measurement - measurements.begin()]) << ", Actual: " << get_data_type_name(ts->get_data_type()) << endl;
            }
        }
    }

    ASSERT_EQ(reader.close(), E_OK);
}

/**
 * @brief 测试 4：测试 get_timeseries_metadata 获取指定设备的测点元数据（一次多个存在的设备，设备都存在）
 */
TEST_F(TsFileTreeGetTimeseriesMetadataTest, TestGetTimeseriesMetadata_AllDevice) {
    // 1. 创建数据
    vector<string> devices = 
    {
        "root.db.device1", 
        "root.DB.Devices2", 
        "root.db.设备3", 
        "root.d4", 
        "root.db.a.b.c.d.e.f.g.h.i.j.k.l.m.n.o.p.q.r.s.t.u.v.w.x.y.z", 
        "root.db.`123`", 
        "root.db.`!@#   $%^&*()_+-=[]|{};:'\",<.>/?`"
    };
    vector<string> measurements = {"measurement1", "Measurement2", "测点3", "12345", "!@#   $%^&*()_+-=[]|{};:'\",<.>/?", "m1.m2", "m7", "m8", "m9", "m10"};
    vector<TSDataType> data_types = {BOOLEAN, INT32, INT64, FLOAT, DOUBLE, STRING, TEXT, BLOB, DATE, TIMESTAMP};
    vector<pair<string, vector<string>>> devices_and_measurements;
    for (auto& device : devices) {
        devices_and_measurements.push_back(make_pair(device, measurements));
    }
    ASSERT_EQ(write_multi_device_data_metadata(test_metadata_file_path, devices_and_measurements, data_types, 10, false), E_OK);

    // 2. 读取元数据
    TsFileTreeReader reader;
    ASSERT_EQ(reader.open(test_metadata_file_path), E_OK);

    // 3. 获取指定设备的测点元数据
    vector<shared_ptr<IDeviceID>> device_ids;
    for (auto& device : devices) {
        device_ids.push_back(make_shared<StringArrayDeviceID>(device));
    }
    DeviceTimeseriesMetadataMap metadata;
    try
    {
        metadata = reader.get_timeseries_metadata(device_ids);
    }
    catch(const std::exception& e)
    {
        std::cerr << e.what() << '\n';
    }

    // 4. 验证结果
    ASSERT_EQ(metadata.size(), devices.size()) << "Device ID size mismatch, expected: " << devices.size() << ", actual: " << metadata.size() << endl;
    for (auto& [device_id, timeseries_list] : metadata) {
        // 验证设备名
        ASSERT_TRUE(find(devices.begin(), devices.end(), device_id->get_device_name()) != devices.end());
        for (auto& ts : timeseries_list) {
            // 验证测点名
            auto measurement = find(measurements.begin(), measurements.end(), ts->get_measurement_name().to_std_string());
            ASSERT_TRUE(measurement != measurements.end()) << "Measurement not found: " << ts->get_measurement_name().to_std_string();
            // 验证数据类型
            ASSERT_EQ(ts->get_data_type(), data_types[measurement - measurements.begin()]) << "Data type mismatch for measurement, Expected: " << get_data_type_name(data_types[measurement - measurements.begin()]) << ", Actual: " << get_data_type_name(ts->get_data_type()) << endl;
        }
    }
    ASSERT_EQ(reader.close(), E_OK);
}

/**
 * @brief 测试 5：测试 get_timeseries_metadata 获取指定设备的测点元数据（一次多个存在的设备，部分设备存在）
 */
TEST_F(TsFileTreeGetTimeseriesMetadataTest, TestGetTimeseriesMetadata_PartialDevicesExist) {
    // 1. 创建数据
    vector<string> devices = 
    {
        "root.db.device1", 
        "root.DB.Devices2", 
        "root.db.设备3", 
        "root.d4", 
        "root.db.a.b.c.d.e.f.g.h.i.j.k.l.m.n.o.p.q.r.s.t.u.v.w.x.y.z", 
        "root.db.`123`", 
        "root.db.`!@#   $%^&*()_+-=[]|{};:'\",<.>/?`"
    };
    vector<string> measurements = {"measurement1", "Measurement2", "测点3", "12345", "!@#   $%^&*()_+-=[]|{};:'\",<.>/?", "m1.m2", "m7", "m8", "m9", "m10"};
    vector<TSDataType> data_types = {BOOLEAN, INT32, INT64, FLOAT, DOUBLE, STRING, TEXT, BLOB, DATE, TIMESTAMP};
    vector<pair<string, vector<string>>> devices_and_measurements;
    for (auto& device : devices) {
        devices_and_measurements.push_back(make_pair(device, measurements));
    }
    ASSERT_EQ(write_multi_device_data_metadata(test_metadata_file_path, devices_and_measurements, data_types, 10, false), E_OK);

    // 2. 读取元数据
    TsFileTreeReader reader;
    ASSERT_EQ(reader.open(test_metadata_file_path), E_OK);

    // 3. 获取指定设备的测点元数据
    vector<string> devices2 = 
    {
        "root.db.device1", 
        "root.DB.Devices2", 
        "root.db.不存在的设备",
    };
    vector<shared_ptr<IDeviceID>> device_ids;
    for (auto& device : devices2) {
        device_ids.push_back(make_shared<StringArrayDeviceID>(device));
    }
    DeviceTimeseriesMetadataMap metadata;
    try
    {
        metadata = reader.get_timeseries_metadata(device_ids);
    }
    catch(const std::exception& e)
    {
        std::cerr << e.what() << '\n';
    }

    // 4. 验证结果，预期只查询存在的
    ASSERT_EQ(metadata.size(), devices2.size() - 1) << "Device ID size mismatch, expected: " << devices2.size() - 1 << ", actual: " << metadata.size() << endl;
    for (auto& [device_id, timeseries_list] : metadata) {
        // 验证设备名
        ASSERT_TRUE(find(devices2.begin(), devices2.end(), device_id->get_device_name()) != devices2.end());
        for (auto& ts : timeseries_list) {
            // 验证测点名
            auto measurement = find(measurements.begin(), measurements.end(), ts->get_measurement_name().to_std_string());
            ASSERT_TRUE(measurement != measurements.end()) << "Measurement not found: " << ts->get_measurement_name().to_std_string();
            // 验证数据类型
            ASSERT_EQ(ts->get_data_type(), data_types[measurement - measurements.begin()]) << "Data type mismatch for measurement, Expected: " << get_data_type_name(data_types[measurement - measurements.begin()]) << ", Actual: " << get_data_type_name(ts->get_data_type()) << endl;
        }
    }
    ASSERT_EQ(reader.close(), E_OK);
}

/**
 * @brief 测试 6：测试 get_timeseries_metadata 获取不存在的设备（应返回空）
 */
TEST_F(TsFileTreeGetTimeseriesMetadataTest, TestGetTimeseriesMetadata_NonExistentDevice) {
    // 1. 创建数据
    vector<pair<string, vector<string>>> devices_and_measurements = {
        {"root.d1", {"s1", "s2"}}
    };
    vector<TSDataType> data_types = {INT32, INT64};
    int row_count = 10;
    ASSERT_EQ(write_multi_device_data_metadata(test_metadata_file_path, devices_and_measurements, data_types), E_OK);

    // 2. 读取元数据
    TsFileTreeReader reader;
    ASSERT_EQ(reader.open(test_metadata_file_path), E_OK);

    // 3. 获取不存在的设备元数据
    auto non_existent_device = make_shared<StringArrayDeviceID>("root.non_existent_device");
    vector<shared_ptr<IDeviceID>> device_ids = {non_existent_device};
    DeviceTimeseriesMetadataMap metadata;
    try
    {
        metadata = reader.get_timeseries_metadata(device_ids);
    }
    catch(const std::exception& e)
    {
        std::cerr << e.what() << '\n';
    }

    // 4. 验证结果：不存在的设备不会返回到 map 中
    ASSERT_EQ(metadata.size(), 0);
    ASSERT_EQ(metadata.find(non_existent_device), metadata.end());

    ASSERT_EQ(reader.close(), E_OK);
}

/**
 * @brief 测试 7：测试 get_timeseries_metadata 获取文件中所有设备的所有测点元数据
 */
TEST_F(TsFileTreeGetTimeseriesMetadataTest, TestGetTimeseriesMetadata_AllDevice_Test) {
    // 1. 创建数据
    vector<string> devices = 
    {
        "root.db.device1", 
        "root.DB.Devices2", 
        "root.db.设备3", 
        "root.d4", 
        "root.db.a.b.c.d.e.f.g.h.i.j.k.l.m.n.o.p.q.r.s.t.u.v.w.x.y.z", 
        "root.db.`123`", 
        "root.db.`!@#   $%^&*()_+-=[]|{};:'\",<.>/?`"
    };
    vector<string> measurements = {"measurement1", "Measurement2", "测点3", "12345", "!@#   $%^&*()_+-=[]|{};:'\",<.>/?", "m1.m2"};
    vector<TSDataType> data_types = {BOOLEAN, INT32, INT64, FLOAT, DOUBLE, STRING};
    vector<pair<string, vector<string>>> devices_and_measurements;
    for (auto& device : devices) {
        devices_and_measurements.push_back(make_pair(device, measurements));
    }
    int row_count = 10000;
    int time_multiplier = 1;
    ASSERT_EQ(write_multi_device_data_metadata(test_metadata_file_path, devices_and_measurements, data_types, row_count, true, time_multiplier), E_OK);

    // 2. 读取元数据
    TsFileTreeReader reader;
    ASSERT_EQ(reader.open(test_metadata_file_path), E_OK);

    // 3. 获取指定设备的测点元数据
    DeviceTimeseriesMetadataMap metadata = reader.get_timeseries_metadata();
    
    // 4. 验证结果
    ASSERT_EQ(metadata.size(), devices.size()) << "Device timeseries metadata count mismatch, expected: " << devices.size() << ", actual: " << metadata.size() << endl;
    for (auto& [device_id, timeseries_list] : metadata) {
        // 验证设备名
        ASSERT_TRUE(find(devices.begin(), devices.end(), device_id->get_device_name()) != devices.end());
        for (auto& ts : timeseries_list) {
            // 验证测点名
            auto measurement = std::find(measurements.begin(), measurements.end(), ts->get_measurement_name().to_std_string());
            ASSERT_TRUE(measurement != measurements.end()) << "Measurement not found: " << ts->get_measurement_name().to_std_string();
            // 验证数据类型
            ASSERT_EQ(ts->get_data_type(), data_types[measurement - measurements.begin()]) << "Data type mismatch for measurement, Expected: " << get_data_type_name(data_types[measurement - measurements.begin()]) << ", Actual: " << get_data_type_name(ts->get_data_type()) << endl;
            ASSERT_EQ(ts->get_statistic()->count_, 5000) << "Expected: " << 5000 << ", Actual: " << ts->get_statistic()->count_ << endl;
        }
    }

    ASSERT_EQ(reader.close(), E_OK);
}

/**
 * @brief 测试 8：测试 get_timeseries_metadata 返回的统计信息（start_time, end_time, count）
 */
TEST_F(TsFileTreeGetTimeseriesMetadataTest, TestGetTimeseriesMetadata_StatisticInfo) {
    // 1. 创建数据 - 写入 100 行数据，时间戳从 0 到 99
    vector<pair<string, vector<string>>> devices_and_measurements = {
        {"root.d1", {"temperature", "pressure"}},
        {"root.d2", {"temperature", "pressure"}}
    };
    vector<TSDataType> data_types = {FLOAT, DOUBLE};
    int row_count = 100;
    // 使用专门的统计测试辅助函数（不跳过任何值）
    ASSERT_EQ(write_multi_device_data_metadata_for_stat(test_metadata_file_path, devices_and_measurements, data_types, row_count), E_OK);

    // 2. 读取元数据
    TsFileTreeReader reader;
    ASSERT_EQ(reader.open(test_metadata_file_path), E_OK);

    // 3. 获取所有设备的测点元数据
    DeviceTimeseriesMetadataMap metadata = reader.get_timeseries_metadata();

    // 4. 验证统计信息
    ASSERT_EQ(metadata.size(), 2);

    for (const auto& entry : metadata) {
        const auto& device_id = entry.first;
        const auto& timeseries_list = entry.second;
        string device_name = device_id->get_device_name();

        // 每个设备应该有 2 个测点
        ASSERT_EQ(timeseries_list.size(), 2) << "Device " << device_name << " should have 2 timeseries";

        // 验证每个测点的统计信息
        for (const auto& ts : timeseries_list) {
            // 验证 count = 100
            EXPECT_EQ(ts->get_statistic()->count_, row_count)
                << "Device " << device_name << " timeseries "
                << ts->get_measurement_name().to_std_string() << " count mismatch";

            // 验证 start_time = 0
            EXPECT_EQ(ts->get_statistic()->start_time_, 0)
                << "Device " << device_name << " timeseries "
                << ts->get_measurement_name().to_std_string() << " start_time mismatch";

            // 验证 end_time = (row_count - 1) = 99
            EXPECT_EQ(ts->get_statistic()->end_time_, (row_count - 1))
                << "Device " << device_name << " timeseries "
                << ts->get_measurement_name().to_std_string() << " end_time mismatch";
        }
    }

    ASSERT_EQ(reader.close(), E_OK);
}

/**
 * @brief 测试 9：测试 get_timeseries_metadata 指定设备列表时返回统计信息
 */
TEST_F(TsFileTreeGetTimeseriesMetadataTest, TestGetTimeseriesMetadata_StatisticInfoForSpecifiedDevices) {
    // 1. 创建数据 - 写入 50 行数据
    vector<pair<string, vector<string>>> devices_and_measurements = {
        {"root.sensor.temp1", {"value"}},
        {"root.sensor.temp2", {"value"}},
        {"root.sensor.temp3", {"value"}}
    };
    vector<TSDataType> data_types = {DOUBLE};
    int row_count = 50;
    ASSERT_EQ(write_multi_device_data_metadata_for_stat(test_metadata_file_path, devices_and_measurements, data_types, row_count), E_OK);

    // 2. 读取元数据
    TsFileTreeReader reader;
    ASSERT_EQ(reader.open(test_metadata_file_path), E_OK);

    // 3. 获取指定设备的测点元数据
    vector<shared_ptr<IDeviceID>> device_ids = {
        make_shared<StringArrayDeviceID>("root.sensor.temp1"),
        make_shared<StringArrayDeviceID>("root.sensor.temp2")
    };
    DeviceTimeseriesMetadataMap metadata = reader.get_timeseries_metadata(device_ids);

    // 4. 验证结果
    ASSERT_EQ(metadata.size(), 2);

    int start_time = 0;
    int end_time = (row_count - 1);

    for (const auto& entry : metadata) {
        const auto& device_id = entry.first;
        const auto& timeseries_list = entry.second;
        string device_name = device_id->get_device_name();

        ASSERT_EQ(timeseries_list.size(), 1) << "Device " << device_name << " should have 1 timeseries";

        const auto& ts = timeseries_list[0];
        EXPECT_EQ(ts->get_statistic()->count_, row_count);
        EXPECT_EQ(ts->get_statistic()->start_time_, start_time);
        EXPECT_EQ(ts->get_statistic()->end_time_, end_time);
    }

    ASSERT_EQ(reader.close(), E_OK);
}

/**
 * @brief 测试 10：测试单行数据的统计信息
 */
TEST_F(TsFileTreeGetTimeseriesMetadataTest, TestGetTimeseriesMetadata_SingleRowStatistic) {
    // 1. 创建数据 - 只写入 1 行数据
    vector<pair<string, vector<string>>> devices_and_measurements = {
        {"root.single.d1", {"s1"}}
    };
    vector<TSDataType> data_types = {INT64};
    int row_count = 1;
    ASSERT_EQ(write_multi_device_data_metadata_for_stat(test_metadata_file_path, devices_and_measurements, data_types, row_count), E_OK);

    // 2. 读取元数据
    TsFileTreeReader reader;
    ASSERT_EQ(reader.open(test_metadata_file_path), E_OK);

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
 * @brief 测试 11：测试使用 Record 写入时的统计信息（非 Tablet 写入）
 */
TEST_F(TsFileTreeGetTimeseriesMetadataTest, TestGetTimeseriesMetadata_RecordWriteStatistic) {
    // 1. 创建数据 - 使用 Record 方式写入
    vector<pair<string, vector<string>>> devices_and_measurements = {
        {"root.record.d1", {"s1", "s2"}}
    };
    vector<TSDataType> data_types = {INT32, FLOAT};
    int row_count = 20;
    ASSERT_EQ(write_multi_device_data_metadata_for_stat(test_metadata_file_path, devices_and_measurements, data_types, row_count), E_OK);

    // 2. 读取元数据
    TsFileTreeReader reader;
    ASSERT_EQ(reader.open(test_metadata_file_path), E_OK);

    // 3. 获取测点元数据
    auto device_id = make_shared<StringArrayDeviceID>("root.record.d1");
    vector<shared_ptr<IDeviceID>> device_ids = {device_id};
    DeviceTimeseriesMetadataMap metadata = reader.get_timeseries_metadata(device_ids);

    // 4. 验证统计信息
    ASSERT_EQ(metadata.size(), 1);
    const auto& timeseries_list = metadata[device_id];
    ASSERT_EQ(timeseries_list.size(), 2);

    for (const auto& ts : timeseries_list) {
        EXPECT_EQ(ts->get_statistic()->count_, row_count);
        EXPECT_EQ(ts->get_statistic()->start_time_, 0);
        EXPECT_EQ(ts->get_statistic()->end_time_, (row_count - 1));
    }

    ASSERT_EQ(reader.close(), E_OK);
}

/**
 * @brief 测试 12：测试 get_timeseries_metadata 验证最小/最大值统计
 */
TEST_F(TsFileTreeGetTimeseriesMetadataTest, TestGetTimeseriesMetadata_MinMaxStatistic) {
    // 1. 创建数据 - 写入特定值以验证 min/max 统计
    string device_id_str = "root.minmax.d1";
    vector<pair<string, vector<string>>> devices_and_measurements = {
        {device_id_str, {"int_val", "double_val"}}
    };
    vector<TSDataType> data_types = {INT32, DOUBLE};
    int row_count = 10;

    // 自定义写入：让值有明确的最小/最大值
    int ret = E_OK;
    TsFileWriter* tsfile_writer = new TsFileWriter();
    int flags = O_WRONLY | O_CREAT | O_TRUNC;
    mode_t mode = 0666;

    ret = tsfile_writer->open(test_metadata_file_path, flags, mode);
    ASSERT_EQ(ret, E_OK);

    // 注册时间序列
    ret = tsfile_writer->register_timeseries(device_id_str, MeasurementSchema("int_val", INT32));
    ASSERT_EQ(ret, E_OK);
    ret = tsfile_writer->register_timeseries(device_id_str, MeasurementSchema("double_val", DOUBLE));
    ASSERT_EQ(ret, E_OK);

    // 写入数据：int_val = 0,2,4,6,8,10,12,14,16,18 (min=0, max=18)
    //           double_val = 1, 2, ..., 10 (min=1, max=10)
    for (int row = 0; row < row_count; row++) {
        TsRecord record(row * 1000, device_id_str);
        record.add_point("int_val", static_cast<int32_t>(row * 2));
        record.add_point("double_val", static_cast<double>(row + 1));  // 1.0, 2.0, ..., 10.0
        ret = tsfile_writer->write_record(record);
        ASSERT_EQ(ret, E_OK);
    }

    ret = tsfile_writer->flush();
    ASSERT_EQ(ret, E_OK);
    ret = tsfile_writer->close();
    delete tsfile_writer;
    ASSERT_EQ(ret, E_OK);

    // 2. 读取元数据
    TsFileTreeReader reader;
    ASSERT_EQ(reader.open(test_metadata_file_path), E_OK);

    // 3. 获取测点元数据
    auto device_id = make_shared<StringArrayDeviceID>(device_id_str);
    vector<shared_ptr<IDeviceID>> device_ids = {device_id};
    DeviceTimeseriesMetadataMap metadata = reader.get_timeseries_metadata(device_ids);

    // 4. 验证统计信息
    ASSERT_EQ(metadata.size(), 1);
    const auto& timeseries_list = metadata[device_id];
    ASSERT_EQ(timeseries_list.size(), 2);

    // 查找 int_val 和 double_val 的统计信息
    const ITimeseriesIndex* int_ts = nullptr;
    const ITimeseriesIndex* double_ts = nullptr;

    for (const auto& ts : timeseries_list) {
        string name = ts->get_measurement_name().to_std_string();
        if (name == "int_val") {
            int_ts = ts.get();
        } else if (name == "double_val") {
            double_ts = ts.get();
        }
    }

    ASSERT_NE(int_ts, nullptr);
    ASSERT_NE(double_ts, nullptr);

    // 验证 int_val 的 min/max (需要转换为 Int32Statistic)
    const Int32Statistic* int32_stat = dynamic_cast<const Int32Statistic*>(int_ts->get_statistic());
    ASSERT_NE(int32_stat, nullptr);
    EXPECT_EQ(int32_stat->min_value_, 0);
    EXPECT_EQ(int32_stat->max_value_, 18);

    // 验证 double_val 的 min/max (需要转换为 DoubleStatistic)
    const DoubleStatistic* double_stat = dynamic_cast<const DoubleStatistic*>(double_ts->get_statistic());
    ASSERT_NE(double_stat, nullptr);
    EXPECT_DOUBLE_EQ(double_stat->min_value_, 1.0);
    EXPECT_DOUBLE_EQ(double_stat->max_value_, 10.0);

    ASSERT_EQ(reader.close(), E_OK);
}
