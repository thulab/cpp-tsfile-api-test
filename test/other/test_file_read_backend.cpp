/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

// RN-144「TsFile 支持使用 mmap 作为文件后端」——读后端契约用例（移植版）
//
// 来源（apache/tsfile rc/2.5.0；用例名逐字保留，便于与产品 gtest 及 PLM 台账对齐）：
//   cpp/test/file/local_random_access_read_file_test.cc -> LocalRandomAccessReadFileTest（8 条）
//   cpp/test/file/utf8_path_test.cc                     -> Utf8PathTest.LocalRandomAccessReadFileOpensUtf8Path
//   cpp/test/cwrapper/cwrapper_test.cc                  -> CWrapperTest.FileReadBackendConfigurationRoundTripsAndValidates
//   cpp/test/reader/tsfile_reader_test.cc               -> TsFileReaderTest.ReadsThroughRandomAccessReadFile
//
// 移植差异：每条自带 fixture、只依赖已发布的 SDK 头文件与 libtsfile.so；
//           产品测试文件内的本地 helper（InMemoryRandomAccessReadFile）随用例一并搬入。

// 产品的 C++ 测试树以 -DENABLE_TEST 编译（cpp/CMakeLists.txt: add_definitions(-DENABLE_TEST)），
// 注入辅助函数 common::enable_injection / disable_injection 仅在该宏下声明；
// libtsfile.so 本身即按该口径构建（已导出这两个符号）。本文件按同一口径定义，
// 不修改测试程序的全局编译参数。
#ifndef ENABLE_TEST
#define ENABLE_TEST
#endif

#include <gtest/gtest.h>

#include <cstdio>
#include <cstring>
#include <fstream>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#ifdef _WIN32
#include <io.h>
#include <process.h>
#else
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

#include "common/global.h"
#include "common/record.h"
#include "common/schema.h"
#include "common/tsfile_common.h"
#include "file/local_random_access_read_file.h"
#include "file/random_access_read_file.h"
#include "reader/result_set.h"
#include "reader/tsfile_reader.h"
#include "utils/injection.h"
#include "utils/errno_define.h"
#include "writer/tsfile_writer.h"

extern "C" {
#include "cwrapper/errno_define_c.h"
#include "cwrapper/tsfile_cwrapper.h"
}

namespace {

using storage::LocalRandomAccessReadFile;
using storage::MeasurementSchema;
using storage::RandomAccessReadFile;
using storage::ResultSet;
using storage::TsFileReader;
using storage::TsFileWriter;
using storage::TsRecord;

std::string ProcessTempPath(const char* stem) {
    std::ostringstream path;
    path << ::testing::TempDir() << stem << "_";
#ifdef _WIN32
    path << _getpid();
#else
    path << getpid();
#endif
    path << ".tsfile";
    return path.str();
}

int CreateWriteFlags() {
#ifdef _WIN32
    return O_WRONLY | O_CREAT | O_TRUNC | O_BINARY;
#else
    return O_WRONLY | O_CREAT | O_TRUNC;
#endif
}

// ---------------------------------------------------------------------------
// 1) LocalRandomAccessReadFileTest —— 默认后端 / 定位读 / MMAP 有界读 / AUTO / 注入 / 边界
// ---------------------------------------------------------------------------

class BackendGuard {
   public:
    BackendGuard() : original_(common::get_file_read_backend()) {}
    ~BackendGuard() { common::set_file_read_backend(original_); }

   private:
    common::FileReadBackend original_;
};

class InjectionGuard {
   public:
    explicit InjectionGuard(const char* point) : point_(point) {
        common::enable_injection(point_, 0);
    }
    ~InjectionGuard() { common::disable_injection(point_); }

   private:
    const char* point_;
};

class LocalRandomAccessReadFileTest : public ::testing::Test {
   protected:
    void SetUp() override {
        content_ = "TsFile";
        content_.push_back('\x03');
        content_ += "read-backend-payload";
        content_ += "TsFile";
        write_file(file_name_, content_);
    }

    void TearDown() override {
        std::remove(file_name_.c_str());
        std::remove(empty_file_name_.c_str());
    }

    static void write_file(const std::string& path,
                           const std::string& content) {
        std::ofstream output(
            path.c_str(), std::ios::out | std::ios::binary | std::ios::trunc);
        ASSERT_TRUE(output.is_open());
        output.write(content.data(),
                     static_cast<std::streamsize>(content.size()));
        ASSERT_TRUE(output.good());
    }

    BackendGuard backend_guard_;
    const std::string file_name_ = ProcessTempPath("read_file_backend_test");
    const std::string empty_file_name_ =
        ProcessTempPath("read_file_backend_empty");
    std::string content_;
};

TEST_F(LocalRandomAccessReadFileTest, PreadIsTheDefaultBackend) {
    EXPECT_EQ(common::get_file_read_backend(), common::FileReadBackend::PREAD);
}

TEST_F(LocalRandomAccessReadFileTest, PreadPreservesPositionedReadBehavior) {
    ASSERT_EQ(common::set_file_read_backend(common::FileReadBackend::PREAD),
              common::E_OK);
    LocalRandomAccessReadFile file;
    ASSERT_EQ(file.open(file_name_), common::E_OK);
    EXPECT_TRUE(file.is_opened());
    EXPECT_EQ(file.active_backend(), common::FileReadBackend::PREAD);
    ASSERT_EQ(common::set_file_read_backend(common::FileReadBackend::MMAP),
              common::E_OK);
    EXPECT_EQ(file.active_backend(), common::FileReadBackend::PREAD);

    std::vector<char> buffer(content_.size() + 8, '\0');
    int32_t read_len = -1;
    ASSERT_EQ(file.read(0, buffer.data(), static_cast<int32_t>(buffer.size()),
                        read_len),
              common::E_OK);
    EXPECT_EQ(read_len, static_cast<int32_t>(content_.size()));
    EXPECT_EQ(std::string(buffer.data(), static_cast<size_t>(read_len)),
              content_);

    EXPECT_EQ(file.read(-1, buffer.data(), 1, read_len), common::E_INVALID_ARG);
    EXPECT_EQ(file.read(0, nullptr, 1, read_len), common::E_INVALID_ARG);
    EXPECT_EQ(file.read(0, nullptr, 0, read_len), common::E_OK);
    EXPECT_EQ(read_len, 0);
}

TEST_F(LocalRandomAccessReadFileTest,
       MmapReadsBoundedRangesAndReleasesResources) {
    ASSERT_EQ(common::set_file_read_backend(common::FileReadBackend::MMAP),
              common::E_OK);
    LocalRandomAccessReadFile file;
    ASSERT_EQ(file.open(file_name_), common::E_OK);
    EXPECT_TRUE(file.is_opened());
    EXPECT_EQ(file.active_backend(), common::FileReadBackend::MMAP);

    char tail[16] = {};
    int32_t read_len = -1;
    const int64_t offset = static_cast<int64_t>(content_.size()) - 3;
    ASSERT_EQ(file.read(offset, tail, sizeof(tail), read_len), common::E_OK);
    EXPECT_EQ(read_len, 3);
    EXPECT_EQ(std::string(tail, static_cast<size_t>(read_len)),
              content_.substr(content_.size() - 3));

    uint64_t size = 0;
    uint64_t fingerprint = 0;
    // MMAP 在 open() 返回前已释放文件描述符，generation() 必须使用映射建立
    // 时抓取的快照。
    EXPECT_EQ(file.generation(size, fingerprint), common::E_OK);
    EXPECT_EQ(size, content_.size());
    EXPECT_NE(fingerprint, 0u);

    file.close();
    EXPECT_FALSE(file.is_opened());
    EXPECT_EQ(std::remove(file_name_.c_str()), 0);
}

TEST_F(LocalRandomAccessReadFileTest, AutoPrefersMmapAndCloseIsIdempotent) {
    ASSERT_EQ(common::set_file_read_backend(common::FileReadBackend::AUTO),
              common::E_OK);
    LocalRandomAccessReadFile file;
    ASSERT_EQ(file.open(file_name_), common::E_OK);
    EXPECT_EQ(file.active_backend(), common::FileReadBackend::MMAP);

    file.close();
    file.close();
    EXPECT_FALSE(file.is_opened());

    ASSERT_EQ(common::set_file_read_backend(common::FileReadBackend::PREAD),
              common::E_OK);
    ASSERT_EQ(file.open(file_name_), common::E_OK);
    EXPECT_EQ(file.active_backend(), common::FileReadBackend::PREAD);
}

TEST_F(LocalRandomAccessReadFileTest,
       AutoFallsBackButRequiredMmapReportsFailure) {
    InjectionGuard mmap_failure("read_file_mmap_fail");

    ASSERT_EQ(common::set_file_read_backend(common::FileReadBackend::AUTO),
              common::E_OK);
    LocalRandomAccessReadFile automatic;
    ASSERT_EQ(automatic.open(file_name_), common::E_OK);
    EXPECT_EQ(automatic.active_backend(), common::FileReadBackend::PREAD);
    automatic.close();

    ASSERT_EQ(common::set_file_read_backend(common::FileReadBackend::MMAP),
              common::E_OK);
    LocalRandomAccessReadFile required;
    EXPECT_EQ(required.open(file_name_), common::E_FILE_MAP_ERR);
    EXPECT_FALSE(required.is_opened());
}

TEST_F(LocalRandomAccessReadFileTest,
       AutoFallsBackButRequiredMmapReportsUnsupported) {
    InjectionGuard mmap_unsupported("read_file_mmap_unsupported");

    ASSERT_EQ(common::set_file_read_backend(common::FileReadBackend::AUTO),
              common::E_OK);
    LocalRandomAccessReadFile automatic;
    ASSERT_EQ(automatic.open(file_name_), common::E_OK);
    EXPECT_EQ(automatic.active_backend(), common::FileReadBackend::PREAD);
    automatic.close();

    ASSERT_EQ(common::set_file_read_backend(common::FileReadBackend::MMAP),
              common::E_OK);
    LocalRandomAccessReadFile required;
    EXPECT_EQ(required.open(file_name_), common::E_NOT_SUPPORT);
    EXPECT_FALSE(required.is_opened());
}

TEST_F(LocalRandomAccessReadFileTest, EmptyFileIsRejectedBeforeMapping) {
    write_file(empty_file_name_, "");
    ASSERT_EQ(common::set_file_read_backend(common::FileReadBackend::MMAP),
              common::E_OK);
    LocalRandomAccessReadFile file;
    EXPECT_EQ(file.open(empty_file_name_), common::E_TSFILE_CORRUPTED);
    EXPECT_FALSE(file.is_opened());
}

TEST_F(LocalRandomAccessReadFileTest, InvalidConfigurationDoesNotChangeBackend) {
    ASSERT_EQ(common::set_file_read_backend(common::FileReadBackend::PREAD),
              common::E_OK);
    EXPECT_EQ(
        common::set_file_read_backend(static_cast<common::FileReadBackend>(99)),
        common::E_INVALID_ARG);
    EXPECT_EQ(common::get_file_read_backend(), common::FileReadBackend::PREAD);
}

// ---------------------------------------------------------------------------
// 2) Utf8PathTest —— UTF-8 路径下打开本地随机读文件
// ---------------------------------------------------------------------------

// 路径字节用显式转义拼写，避免源码字符集导致编译器二次编码。
// U+6D4B U+8BD5（"测试"）+ U+00E9（é）。
const char* kUtf8Stem = "\xE6\xB5\x8B\xE8\xAF\x95_\xC3\xA9_";

std::string Utf8Name() {
    const ::testing::TestInfo* info =
        ::testing::UnitTest::GetInstance()->current_test_info();
    return std::string(kUtf8Stem) +
           (info != nullptr ? info->name() : "unknown") + ".tsfile";
}

#ifdef _WIN32
std::wstring WideName() {
    const ::testing::TestInfo* info =
        ::testing::UnitTest::GetInstance()->current_test_info();
    std::wstring wide(L"\x6D4B\x8BD5_\x00E9_");
    const char* tag = info != nullptr ? info->name() : "unknown";
    while (*tag != '\0') {
        wide += static_cast<wchar_t>(*tag++);
    }
    wide += L".tsfile";
    return wide;
}
#endif

// LocalRandomAccessReadFile::open() 要求的最小完整 TsFile：头 magic + 版本字节 + 尾 magic
const std::string& MinimalTsFile() {
    static const std::string bytes = std::string(storage::MAGIC_STRING_TSFILE) +
                                     storage::VERSION_NUM_BYTE +
                                     storage::MAGIC_STRING_TSFILE;
    return bytes;
}

void RemoveTestFiles() {
#ifdef _WIN32
    ::_wunlink(WideName().c_str());
    ::_unlink(Utf8Name().c_str());
#else
    ::unlink(Utf8Name().c_str());
#endif
}

// 存在性检查不走窄字符 CRT，避免被测试要暴露的编码问题影响
bool ExistsByWidePath() {
#ifdef _WIN32
    return ::GetFileAttributesW(WideName().c_str()) != INVALID_FILE_ATTRIBUTES;
#else
    struct stat st;
    return ::stat(Utf8Name().c_str(), &st) == 0;
#endif
}

bool CreateFixtureByWidePath() {
#ifdef _WIN32
    const int fd = ::_wopen(WideName().c_str(),
                            _O_WRONLY | _O_CREAT | _O_TRUNC | _O_BINARY,
                            _S_IREAD | _S_IWRITE);
#else
    const int fd =
        ::open(Utf8Name().c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0666);
#endif
    if (fd < 0) {
        return false;
    }
    const std::string& content = MinimalTsFile();
    const int written = static_cast<int>(
        ::write(fd, content.data(), static_cast<unsigned>(content.size())));
#ifdef _WIN32
    ::_close(fd);
#else
    ::close(fd);
#endif
    return written == static_cast<int>(content.size());
}

class Utf8PathTest : public ::testing::Test {
   protected:
    void SetUp() override { RemoveTestFiles(); }
    void TearDown() override { RemoveTestFiles(); }
};

TEST_F(Utf8PathTest, LocalRandomAccessReadFileOpensUtf8Path) {
    ASSERT_TRUE(CreateFixtureByWidePath());
    ASSERT_TRUE(ExistsByWidePath());

    LocalRandomAccessReadFile read_file;
    EXPECT_EQ(read_file.open(Utf8Name()), common::E_OK)
        << "an existing file with a non-ASCII name could not be opened";
    EXPECT_TRUE(read_file.is_opened());
    read_file.close();
}

// ---------------------------------------------------------------------------
// 3) CWrapperTest —— C 接口的文件后端配置往返与取值校验
// ---------------------------------------------------------------------------

class CWrapperTest : public ::testing::Test {};

TEST_F(CWrapperTest, FileReadBackendConfigurationRoundTripsAndValidates) {
    const TsFileReadBackend original = tsfile_get_file_read_backend();

    EXPECT_EQ(tsfile_set_file_read_backend(TSFILE_READ_BACKEND_MMAP), RET_OK);
    EXPECT_EQ(tsfile_get_file_read_backend(), TSFILE_READ_BACKEND_MMAP);
    EXPECT_EQ(tsfile_set_file_read_backend(TSFILE_READ_BACKEND_PREAD), RET_OK);
    EXPECT_EQ(tsfile_get_file_read_backend(), TSFILE_READ_BACKEND_PREAD);
    EXPECT_EQ(tsfile_set_file_read_backend(99), RET_INVALID_ARG);
    EXPECT_EQ(tsfile_set_file_read_backend(256), RET_INVALID_ARG);
    EXPECT_EQ(tsfile_get_file_read_backend(), TSFILE_READ_BACKEND_PREAD);

    EXPECT_EQ(tsfile_set_file_read_backend(original), RET_OK);
}

// ---------------------------------------------------------------------------
// 4) TsFileReaderTest —— TsFileReader 经 RandomAccessReadFile 抽象读取
// ---------------------------------------------------------------------------

// 产品测试文件内的本地 helper，随用例一并搬入（SDK 不导出此类）
class InMemoryRandomAccessReadFile : public RandomAccessReadFile {
   public:
    explicit InMemoryRandomAccessReadFile(std::vector<char> bytes)
        : bytes_(std::move(bytes)), opened_(true), name_("memory://test") {}

    bool is_opened() const override { return opened_; }

    int64_t file_size() const override {
        return static_cast<int64_t>(bytes_.size());
    }

    const std::string& file_path() const override { return name_; }

    int generation(uint64_t& size, uint64_t& fingerprint) const override {
        if (!opened_) {
            return common::E_FILE_READ_ERR;
        }
        size = bytes_.size();
        fingerprint = 0;
        return common::E_OK;
    }

    int read(int64_t offset, char* buffer, int32_t size,
             int32_t& read_size) override {
        read_size = 0;
        if (!opened_ || offset < 0 || size < 0 ||
            (buffer == nullptr && size > 0)) {
            return common::E_INVALID_ARG;
        }
        if (size == 0 || static_cast<size_t>(offset) >= bytes_.size()) {
            return common::E_OK;
        }
        const size_t available = bytes_.size() - static_cast<size_t>(offset);
        read_size = static_cast<int32_t>(
            std::min(available, static_cast<size_t>(size)));
        std::memcpy(buffer, bytes_.data() + offset,
                    static_cast<size_t>(read_size));
        return common::E_OK;
    }

    void close() override { opened_ = false; }

   private:
    std::vector<char> bytes_;
    bool opened_;
    std::string name_;
};

class TsFileReaderTest : public ::testing::Test {
   protected:
    void SetUp() override {
        // 写路径需要库级初始化（产品 fixture 与本仓库既有 fixture 同样调用）
        ASSERT_EQ(storage::libtsfile_init(), common::E_OK);
        file_name_ = ProcessTempPath("read_backend_reader_test");
        std::remove(file_name_.c_str());
    }

    void TearDown() override {
        std::remove(file_name_.c_str());
        storage::libtsfile_destroy();
    }

    std::string file_name_;
};

TEST_F(TsFileReaderTest, ReadsThroughRandomAccessReadFile) {
    const std::string device = "root.sg.device";
    const std::string measurement = "temperature";

    {
        TsFileWriter writer;
        ASSERT_EQ(writer.open(file_name_, CreateWriteFlags(), 0666),
                  common::E_OK);
        ASSERT_EQ(writer.register_timeseries(
                      device,
                      MeasurementSchema(measurement, common::TSDataType::INT32,
                                        common::TSEncoding::PLAIN,
                                        common::CompressionType::UNCOMPRESSED)),
                  common::E_OK);
        TsRecord record(100, device);
        record.add_point(measurement, static_cast<int32_t>(42));
        ASSERT_EQ(writer.write_record(record), common::E_OK);
        ASSERT_EQ(writer.flush(), common::E_OK);
        ASSERT_EQ(writer.close(), common::E_OK);
    }

    std::ifstream input(file_name_, std::ios::binary);
    ASSERT_TRUE(input.is_open());
    std::vector<char> bytes((std::istreambuf_iterator<char>(input)),
                            std::istreambuf_iterator<char>());
    ASSERT_FALSE(bytes.empty());

    std::unique_ptr<RandomAccessReadFile> source(
        new InMemoryRandomAccessReadFile(std::move(bytes)));
    TsFileReader reader;
    ASSERT_EQ(reader.open(std::move(source)), common::E_OK);
    EXPECT_EQ(reader.get_file_version(),
              static_cast<unsigned char>(storage::VERSION_NUM_BYTE));

    std::vector<std::string> paths = {device + "." + measurement};
    ResultSet* result = nullptr;
    ASSERT_EQ(reader.query(paths, 0, 200, result), common::E_OK);
    ASSERT_NE(result, nullptr);
    bool has_next = false;
    ASSERT_EQ(result->next(has_next), common::E_OK);
    ASSERT_TRUE(has_next);
    EXPECT_EQ(result->get_value<int32_t>(2), 42);
    ASSERT_EQ(result->next(has_next), common::E_OK);
    EXPECT_FALSE(has_next);

    reader.destroy_query_data_set(result);
    reader.close();
}

}  // namespace
