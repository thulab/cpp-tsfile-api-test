/**
 * @file test_restorable_tsfile_writer.cpp
 * @brief Unit tests for RestorableTsFileIOWriter - TsFile broken file recovery feature
 *
 * 根据需求文档 V2.2.2-TsFile-TsFileCPP 破损文件恢复 - 需求分析
 * 测试 RestorableTsFileIOWriter 类的破损文件恢复功能
 *
 * 测试覆盖场景：
 * 1. 空文件打开与恢复
 * 2. 非法 Magic 文件处理
 * 3. 完整文件处理（不需要恢复）
 * 4. 截断/损坏文件恢复
 * 5. 仅 Header 文件处理
 * 6. 恢复后继续使用 TsFileWriter 写入
 * 7. 恢复后继续使用 TsFileTreeWriter 写入
 * 8. 多层设备名恢复与写入
 * 9. 对齐时间序列恢复与写入
 */

#include "file/restorable_tsfile_io_writer.h"

#include <gtest/gtest.h>

#include <fstream>
#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include "common/record.h"
#include "common/schema.h"
#include "common/tsfile_common.h"
#include "file/write_file.h"
#include "reader/tsfile_reader.h"
#include "reader/tsfile_tree_reader.h"
#include "writer/tsfile_tree_writer.h"
#include "writer/tsfile_writer.h"

using namespace storage;
using namespace common;
using namespace std;

// -----------------------------------------------------------------------------
// 辅助函数：获取文件大小、损坏文件尾部等
// -----------------------------------------------------------------------------

/**
 * @brief 获取写入文件的标志（跨平台）
 */
static int GetWriteCreateFlags() {
    int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef _WIN32
    flags |= O_BINARY;
#endif
    return flags;
}

/**
 * @brief 获取文件大小
 */
static int64_t GetFileSize(const std::string& path) {
    std::ifstream f(path, std::ios::binary | std::ios::ate);
    return static_cast<int64_t>(f.tellg());
}

/**
 * @brief 损坏文件尾部（用 0 覆盖最后 num_bytes 字节）
 */
static void CorruptFileTail(const std::string& path, int num_bytes) {
    const int64_t full_size = GetFileSize(path);
    std::ofstream out(path, std::ios::binary | std::ios::in);
    out.seekp(full_size - static_cast<std::streamoff>(num_bytes));
    for (int i = 0; i < num_bytes; ++i) {
        out.put(0);
    }
    out.close();
}

/**
 * @brief 查询树模型读取器并返回行数
 */
static int CountTreeReaderRows(
    TsFileTreeReader& reader, const std::vector<std::string>& measurement_ids) {
    auto device_ids = reader.get_all_device_ids();
    ResultSet* result = nullptr;
    int ret =
        reader.query(device_ids, measurement_ids, INT64_MIN, INT64_MAX, result);
    if (ret != E_OK || result == nullptr) {
        return -1;
    }
    int count = 0;
    for (auto it = result->iterator(); it.hasNext(); it.next()) {
        ++count;
    }
    reader.destroy_query_data_set(result);
    return count;
}

/**
 * @brief 生成随机字符串
 */
static std::string generate_random_string(int length) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, 61);
    const std::string chars =
        "0123456789"
        "abcdefghijklmnopqrstuvwxyz"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    std::string s;
    s.reserve(static_cast<size_t>(length));
    for (int i = 0; i < length; ++i) {
        s += chars[static_cast<size_t>(dis(gen))];
    }
    return s;
}

// -----------------------------------------------------------------------------
// 测试夹具
// -----------------------------------------------------------------------------

class RestorableTsFileWriterTest : public ::testing::Test {
   protected:
    void SetUp() override {
        libtsfile_init();
        file_name_ = std::string("restorable_tsfile_writer_test_") +
                     generate_random_string(10) + std::string(".tsfile");
        remove(file_name_.c_str());
    }

    void TearDown() override {
        remove(file_name_.c_str());
        libtsfile_destroy();
    }

    int64_t GetCurrentFileSize() const { return GetFileSize(file_name_); }

    void CorruptCurrentFileTail(int num_bytes) {
        CorruptFileTail(file_name_, num_bytes);
    }

    std::string file_name_;
};

// -----------------------------------------------------------------------------
// 测试用例 1：打开空文件
// 验证：空文件应该被视为 crashed 状态，可以写入
// -----------------------------------------------------------------------------

TEST_F(RestorableTsFileWriterTest, OpenEmptyFile) {
    RestorableTsFileIOWriter writer;
    ASSERT_EQ(writer.open(file_name_, true), E_OK);
    EXPECT_TRUE(writer.can_write());
    EXPECT_TRUE(writer.has_crashed());
    EXPECT_EQ(writer.get_truncated_size(), 0);
    EXPECT_NE(writer.get_tsfile_io_writer(), nullptr);
    writer.close();
}

// -----------------------------------------------------------------------------
// 测试用例 2：打开非法 Magic 文件
// 验证：Magic 字符串不匹配的文件应该返回错误
// -----------------------------------------------------------------------------

TEST_F(RestorableTsFileWriterTest, OpenBadMagicFile) {
    std::ofstream f(file_name_);
    f.write("BadFile", 7);
    f.close();

    RestorableTsFileIOWriter writer;
    EXPECT_NE(writer.open(file_name_, true), E_OK);
    EXPECT_EQ(writer.get_truncated_size(), TSFILE_CHECK_INCOMPATIBLE);
    writer.close();
}

// -----------------------------------------------------------------------------
// 测试用例 3：打开完整文件
// 验证：完整的 TsFile 不应该被标记为 crashed，不能继续写入
// -----------------------------------------------------------------------------

TEST_F(RestorableTsFileWriterTest, OpenCompleteFile) {
    TsFileWriter tw;
    ASSERT_EQ(tw.open(file_name_, GetWriteCreateFlags(), 0666), E_OK);
    tw.register_timeseries(
        "d1",
        MeasurementSchema("s1", FLOAT, GORILLA, CompressionType::UNCOMPRESSED));
    TsRecord record(1, "d1");
    record.add_point("s1", 1.0f);
    tw.write_record(record);
    record.timestamp_ = 2;
    tw.write_record(record);
    tw.flush();
    tw.close();

    RestorableTsFileIOWriter writer;
    ASSERT_EQ(writer.open(file_name_, true), E_OK);
    EXPECT_FALSE(writer.can_write());
    EXPECT_FALSE(writer.has_crashed());
    EXPECT_EQ(writer.get_truncated_size(), TSFILE_CHECK_COMPLETE);
    EXPECT_EQ(writer.get_tsfile_io_writer(), nullptr);
    writer.close();
}

// -----------------------------------------------------------------------------
// 测试用例 4：打开截断文件
// 验证：尾部损坏的文件应该被恢复，截断到安全位置
// -----------------------------------------------------------------------------

TEST_F(RestorableTsFileWriterTest, OpenTruncatedFile) {
    TsFileWriter tw;
    ASSERT_EQ(tw.open(file_name_, GetWriteCreateFlags(), 0666), E_OK);
    tw.register_timeseries(
        "d1",
        MeasurementSchema("s1", FLOAT, RLE, CompressionType::UNCOMPRESSED));
    TsRecord record(1, "d1");
    record.add_point("s1", 1.0f);
    tw.write_record(record);
    tw.flush();
    tw.close();

    const int64_t full_size = GetCurrentFileSize();
    CorruptCurrentFileTail(5);

    RestorableTsFileIOWriter writer;
    ASSERT_EQ(writer.open(file_name_, true), E_OK);
    EXPECT_TRUE(writer.can_write());
    EXPECT_TRUE(writer.has_crashed());
    EXPECT_GE(writer.get_truncated_size(),
              static_cast<int64_t>(MAGIC_STRING_TSFILE_LEN + 1));
    EXPECT_LE(writer.get_truncated_size(), full_size);
    EXPECT_NE(writer.get_tsfile_io_writer(), nullptr);
    writer.close();
}

// -----------------------------------------------------------------------------
// 测试用例 5：打开仅有 Header 的文件
// 验证：只包含 Magic String 和 Version 的文件应该可以恢复
// -----------------------------------------------------------------------------

TEST_F(RestorableTsFileWriterTest, OpenFileWithOnlyHeader) {
    int flags = O_RDWR | O_CREAT | O_TRUNC;
#ifdef _WIN32
    flags |= O_BINARY;
#endif
    WriteFile wf;
    ASSERT_EQ(wf.create(file_name_, flags, 0666), E_OK);
    wf.write(MAGIC_STRING_TSFILE, MAGIC_STRING_TSFILE_LEN);
    wf.write(&VERSION_NUM_BYTE, 1);
    wf.close();

    RestorableTsFileIOWriter writer;
    ASSERT_EQ(writer.open(file_name_, true), E_OK);
    EXPECT_TRUE(writer.can_write());
    EXPECT_TRUE(writer.has_crashed());
    EXPECT_EQ(writer.get_truncated_size(), MAGIC_STRING_TSFILE_LEN + 1);
    writer.close();
}

// -----------------------------------------------------------------------------
// 测试用例 6：恢复后继续使用 TsFileWriter 写入
// 验证：损坏文件恢复后可以继续使用 TsFileWriter 写入数据
// -----------------------------------------------------------------------------

TEST_F(RestorableTsFileWriterTest, TruncateRecoversAndProvidesWriter) {
    TsFileWriter tw;
    ASSERT_EQ(tw.open(file_name_, GetWriteCreateFlags(), 0666), E_OK);
    tw.register_timeseries(
        "d1",
        MeasurementSchema("s1", FLOAT, GORILLA, CompressionType::UNCOMPRESSED));
    TsRecord record(1, "d1");
    record.add_point("s1", 1.0f);
    tw.write_record(record);
    record.timestamp_ = 2;
    tw.write_record(record);
    tw.flush();
    tw.close();

    CorruptCurrentFileTail(3);

    RestorableTsFileIOWriter rw;
    ASSERT_EQ(rw.open(file_name_, true), E_OK);
    ASSERT_TRUE(rw.can_write());
    ASSERT_NE(rw.get_tsfile_io_writer(), nullptr);
    ASSERT_NE(rw.get_write_file(), nullptr);
    EXPECT_EQ(rw.get_file_path(), file_name_);

    TsFileWriter tw2;
    ASSERT_EQ(tw2.init(&rw), E_OK);
    TsRecord record2(3, "d1");
    record2.add_point("s1", 3.0f);
    ASSERT_EQ(tw2.write_record(record2), E_OK);
    tw2.close();
    rw.close();
}

// -----------------------------------------------------------------------------
// 测试用例 7：多层设备名恢复与写入
// 验证：多层设备路径（如 root.d1）在恢复后可以继续写入
// -----------------------------------------------------------------------------

TEST_F(RestorableTsFileWriterTest, TreeModelMultiSegmentDeviceRecoverAndWrite) {
    TsFileWriter tw;
    ASSERT_EQ(tw.open(file_name_, GetWriteCreateFlags(), 0666), E_OK);
    tw.register_timeseries(
        "root.d1",
        MeasurementSchema("s1", FLOAT, GORILLA, CompressionType::UNCOMPRESSED));
    TsRecord record(1, "root.d1");
    record.add_point("s1", 1.0f);
    ASSERT_EQ(tw.write_record(record), E_OK);
    tw.flush();
    tw.close();

    CorruptCurrentFileTail(3);

    RestorableTsFileIOWriter rw;
    ASSERT_EQ(rw.open(file_name_, true), E_OK);
    ASSERT_TRUE(rw.can_write());

    TsFileWriter tw2;
    ASSERT_EQ(tw2.init(&rw), E_OK);
    TsRecord record2(2, "root.d1");
    record2.add_point("s1", 2.0f);
    ASSERT_EQ(tw2.write_record(record2), E_OK);
    tw2.flush();
    tw2.close();
    rw.close();

    TsFileTreeReader reader;
    reader.open(file_name_);
    ASSERT_EQ(reader.get_all_device_ids().size(), 1u);
    ASSERT_EQ(CountTreeReaderRows(reader, {"s1"}), 2);
    reader.close();
}

// -----------------------------------------------------------------------------
// 测试用例 8：恢复后使用 TsFileTreeWriter 写入多设备数据
// 验证：损坏文件恢复后可以继续使用 TsFileTreeWriter 写入多设备数据
// -----------------------------------------------------------------------------

TEST_F(RestorableTsFileWriterTest, MultiDeviceRecoverAndWriteWithTreeWriter) {
    // 1. 写入数据
    TsFileWriter tw;
    ASSERT_EQ(tw.open(file_name_, GetWriteCreateFlags(), 0666), E_OK);
    vector<string> devices = {"d1", "d2"};
    for (auto& device : devices) {
        tw.register_timeseries(device, MeasurementSchema("s1", FLOAT));
        tw.register_timeseries(device, MeasurementSchema("s2", INT32));
        tw.register_timeseries(device, MeasurementSchema("s3", BOOLEAN));
        tw.register_timeseries(device, MeasurementSchema("s4", INT64));
        tw.register_timeseries(device, MeasurementSchema("s5", DOUBLE));
        tw.register_timeseries(device, MeasurementSchema("s6", TEXT));
        tw.register_timeseries(device, MeasurementSchema("s7", STRING));
        tw.register_timeseries(device, MeasurementSchema("s8", BLOB));
        tw.register_timeseries(device, MeasurementSchema("s9", DATE));
        tw.register_timeseries(device, MeasurementSchema("s10", TIMESTAMP));  
    }

    std::time_t now = std::time(nullptr);
    std::tm* local_time = std::localtime(&now);
    std::tm today = {};
    today.tm_year = local_time->tm_year;
    today.tm_mon = local_time->tm_mon;
    today.tm_mday = local_time->tm_mday;

    int row_num = 10;
    for (int i = 0; i < row_num; i++) { 
        for (auto device : devices) {
            TsRecord r1(i, device);
            r1.add_point("s1", 1.0f);
            r1.add_point("s2", 10);
            r1.add_point("s3", true);
            r1.add_point("s4", 10);
            r1.add_point("s5", 5.0);
            r1.add_point("s6", "hello");
            r1.add_point("s7", "hello");
            r1.add_point("s8", "hello");
            r1.add_point("s9", today);
            r1.add_point("s10", 10);
            ASSERT_EQ(tw.write_record(r1), E_OK);
        }
    }
    tw.flush();
    tw.close();

    // 2. 持续破坏写入
    int row_num2 = row_num + 10;
    for (int i = row_num; i < row_num2; i++) {
        CorruptCurrentFileTail(i);

        RestorableTsFileIOWriter rw;
        ASSERT_EQ(rw.open(file_name_, true), E_OK);
        ASSERT_TRUE(rw.can_write());
        EXPECT_TRUE(rw.has_crashed());
        EXPECT_GE(rw.get_truncated_size(), static_cast<int64_t>(MAGIC_STRING_TSFILE_LEN + 1));
        EXPECT_NE(rw.get_tsfile_io_writer(), nullptr);

        TsFileTreeWriter tree_writer(&rw);
        for (auto device : devices) {
            TsRecord r2(i, device);
            r2.add_point("s1", 1.0f);
            r2.add_point("s2", 10);
            r2.add_point("s3", true);
            r2.add_point("s4", 10);
            r2.add_point("s5", 5.0);
            r2.add_point("s6", "hello");
            r2.add_point("s7", "hello");
            r2.add_point("s8", "hello");
            r2.add_point("s9", today);
            r2.add_point("s10", 10);
            ASSERT_EQ(tree_writer.write(r2), E_OK);
        }
        tree_writer.flush();
        tree_writer.close();
    }

    // 3. 读取元数据
    TsFileTreeReader reader;
    ASSERT_EQ(reader.open(file_name_), E_OK);
    DeviceTimeseriesMetadataMap metadata = reader.get_timeseries_metadata();
    for (auto& [device_id, timeseries_list] : metadata) {
        for (auto& ts : timeseries_list) {
            ASSERT_EQ(ts->get_statistic()->count_, 20);
            ASSERT_EQ(ts->get_statistic()->start_time_, 0);
            ASSERT_EQ(ts->get_statistic()->end_time_, 19);
        }
    }
}

// -----------------------------------------------------------------------------
// 测试用例 9：对齐时间序列恢复与写入
// 验证：对齐时间序列文件可以恢复并继续写入
// -----------------------------------------------------------------------------

TEST_F(RestorableTsFileWriterTest, AlignedTimeseriesRecoverAndWrite) {
    // 1. 创建文件并写入数据
    TsFileWriter tw;
    ASSERT_EQ(tw.open(file_name_, GetWriteCreateFlags(), 0666), E_OK);
    std::vector<MeasurementSchema*> aligned_schemas;
    aligned_schemas.push_back(new MeasurementSchema("s1", BOOLEAN));
    aligned_schemas.push_back(new MeasurementSchema("s2", INT32));
    aligned_schemas.push_back(new MeasurementSchema("s3", INT64));
    aligned_schemas.push_back(new MeasurementSchema("s4", FLOAT));
    aligned_schemas.push_back(new MeasurementSchema("s5", DOUBLE));
    aligned_schemas.push_back(new MeasurementSchema("s6", TEXT));
    aligned_schemas.push_back(new MeasurementSchema("s7", STRING));
    aligned_schemas.push_back(new MeasurementSchema("s8", BLOB));
    aligned_schemas.push_back(new MeasurementSchema("s9", DATE));
    aligned_schemas.push_back(new MeasurementSchema("s10", TIMESTAMP));
    tw.register_aligned_timeseries("d1", aligned_schemas);

    std::time_t now = std::time(nullptr);
    std::tm* local_time = std::localtime(&now);
    std::tm today = {};
    today.tm_year = local_time->tm_year;
    today.tm_mon = local_time->tm_mon;
    today.tm_mday = local_time->tm_mday;

    int row_num = 10;
    for (int i = 0; i < row_num; i++) {
        TsRecord r1(i, "d1");
        r1.add_point("s1", true);
        r1.add_point("s2", 10);
        r1.add_point("s3", 10);
        r1.add_point("s4", 5.0);
        r1.add_point("s5", 5.0);
        r1.add_point("s6", "hello");
        r1.add_point("s7", "hello");
        r1.add_point("s8", "hello");
        r1.add_point("s9", today);
        r1.add_point("s10", 10);
        ASSERT_EQ(tw.write_record_aligned(r1), E_OK);
    }
    tw.flush();
    tw.close();

    // 2. 持续损坏文件并重新写入
    int row_num2 = row_num + 10;
    for (int i = row_num; i < row_num2; i++) {
        CorruptCurrentFileTail(i);

        RestorableTsFileIOWriter rw;
        ASSERT_EQ(rw.open(file_name_, true), E_OK);
        ASSERT_TRUE(rw.can_write());
        EXPECT_TRUE(rw.has_crashed());
        EXPECT_GE(rw.get_truncated_size(), static_cast<int64_t>(MAGIC_STRING_TSFILE_LEN + 1));
        EXPECT_NE(rw.get_tsfile_io_writer(), nullptr);
        
        TsFileTreeWriter tree_writer(&rw);
        TsRecord r2(i, "d1");
        r2.add_point("s1", true);
        r2.add_point("s2", 10);
        r2.add_point("s3", 10);
        r2.add_point("s4", 5.0);
        r2.add_point("s5", 5.0);
        r2.add_point("s6", "hello");
        r2.add_point("s7", "hello");
        r2.add_point("s8", "hello");
        r2.add_point("s9", today);
        r2.add_point("s10", 10);
        ASSERT_EQ(tree_writer.write(r2), E_OK);
        tree_writer.flush();
        tree_writer.close();
    }

    // 3. 验证数据行数
    TsFileTreeReader reader;
    ASSERT_EQ(reader.open(file_name_), E_OK);
    DeviceTimeseriesMetadataMap metadata = reader.get_timeseries_metadata();
    for (auto& [device_id, timeseries_list] : metadata) {
        for (auto& ts : timeseries_list) {
            // cout << "count: " << ts->get_statistic()->count_ << ", " << "start: " << ts->get_statistic()->start_time_ << ", " << "end: " << ts->get_statistic()->end_time_ << endl;
            ASSERT_EQ(ts->get_statistic()->count_, 20);
            ASSERT_EQ(ts->get_statistic()->start_time_, 0);
            ASSERT_EQ(ts->get_statistic()->end_time_, 19);
        }
    }
}

// -----------------------------------------------------------------------------
// 测试用例 10：恢复后文件状态验证
// 验证：恢复后可以正确获取文件状态信息（can_write, has_crashed, file_path）
// -----------------------------------------------------------------------------

TEST_F(RestorableTsFileWriterTest, RecoveredFileStateVerification) {
    // 创建初始文件
    TsFileWriter tw;
    ASSERT_EQ(tw.open(file_name_, GetWriteCreateFlags(), 0666), E_OK);
    tw.register_timeseries("device1", MeasurementSchema("sensor1", FLOAT));
    TsRecord record(1000, "device1");
    record.add_point("sensor1", 25.5f);
    tw.write_record(record);
    tw.flush();
    tw.close();

    // 损坏文件
    CorruptCurrentFileTail(3);

    // 恢复文件并验证状态
    RestorableTsFileIOWriter rw;
    ASSERT_EQ(rw.open(file_name_, true), E_OK);

    // 验证状态标志
    EXPECT_TRUE(rw.can_write());
    EXPECT_TRUE(rw.has_crashed());
    EXPECT_EQ(rw.get_file_path(), file_name_);

    // 验证截断位置有效
    EXPECT_GE(rw.get_truncated_size(), MAGIC_STRING_TSFILE_LEN + 1);

    rw.close();
}

// -----------------------------------------------------------------------------
// 测试用例 11：不同损坏程度的文件恢复
// 验证：不同程度的文件损坏都能正确恢复
// -----------------------------------------------------------------------------

TEST_F(RestorableTsFileWriterTest, DifferentCorruptionLevels) {
    // 测试不同程度的损坏
    vector<int> corruption_levels = {1, 3, 5, 10};

    for (int corrupt_bytes : corruption_levels) {
        // 重新生成文件名
        std::string test_file = file_name_ + "_corrupt_" + std::to_string(corrupt_bytes);
        remove(test_file.c_str());

        // 创建有更多数据的文件
        TsFileWriter tw;
        ASSERT_EQ(tw.open(test_file, GetWriteCreateFlags(), 0666), E_OK);
        tw.register_timeseries("d1", MeasurementSchema("s1", INT32));
        for (int i = 0; i < 10; i++) {
            TsRecord record(i * 1000, "d1");
            record.add_point("s1", i);
            tw.write_record(record);
        }
        tw.flush();
        tw.close();

        int64_t original_size = GetFileSize(test_file);

        // 损坏文件
        CorruptFileTail(test_file, corrupt_bytes);

        // 恢复文件
        RestorableTsFileIOWriter rw;
        ASSERT_EQ(rw.open(test_file, true), E_OK);
        EXPECT_TRUE(rw.can_write());
        EXPECT_TRUE(rw.has_crashed());
        EXPECT_LE(rw.get_truncated_size(), original_size);
        rw.close();

        // 清理测试文件
        remove(test_file.c_str());
    }
}

// -----------------------------------------------------------------------------
// 测试用例 12：恢复后写入的数据可读
// 验证：恢复后写入的新数据可以被正确读取
// -----------------------------------------------------------------------------

TEST_F(RestorableTsFileWriterTest, RecoveredAndWrittenDataIsReadable) {
    // 创建初始文件
    TsFileWriter tw;
    ASSERT_EQ(tw.open(file_name_, GetWriteCreateFlags(), 0666), E_OK);
    tw.register_timeseries("d1", MeasurementSchema("s1", FLOAT));

    // 写入初始数据
    TsRecord record1(1000, "d1");
    record1.add_point("s1", 1.0f);
    tw.write_record(record1);
    tw.flush();
    tw.close();

    // 损坏文件
    CorruptCurrentFileTail(3);

    // 恢复并写入新数据
    RestorableTsFileIOWriter rw;
    ASSERT_EQ(rw.open(file_name_, true), E_OK);

    TsFileTreeWriter tree_writer(&rw);
    TsRecord record2(2000, "d1");
    record2.add_point("s1", 2.0f);
    ASSERT_EQ(tree_writer.write(record2), E_OK);
    TsRecord record3(3000, "d1");
    record3.add_point("s1", 3.0f);
    ASSERT_EQ(tree_writer.write(record3), E_OK);
    tree_writer.flush();
    tree_writer.close();
    rw.close();

    // 读取并验证数据
    TsFileTreeReader reader;
    reader.open(file_name_);

    auto device_ids = reader.get_all_device_ids();
    ASSERT_EQ(device_ids.size(), 1u);
    EXPECT_EQ(device_ids[0], "d1");

    ResultSet* result = nullptr;
    int ret = reader.query(device_ids, {"s1"}, INT64_MIN, INT64_MAX, result);
    ASSERT_EQ(ret, E_OK);
    ASSERT_NE(result, nullptr);

    int row_count = 0;
    for (auto it = result->iterator(); it.hasNext(); it.next()) {
        row_count++;
    }

    // 应该有 3 行数据（初始 1 行 + 恢复后写入 2 行）
    EXPECT_EQ(row_count, 3);

    reader.destroy_query_data_set(result);
    reader.close();
}
