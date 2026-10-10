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

// RN-146 批次 4：C++ 侧接入与契约（用例 42 / 43 / 44 / 59 / 60），tester-only 探针。
//
// 42 RandomAccessReadFile 基线：自定义实现读取结果 == 路径读取结果（行/schema）
// 43 自定义实现按 §4.1.2 契约「内部连续读满短读」
// 44 跨接口：把 C++ 侧行数据导出 JSON，与 Python 侧 file-like source 结果比对（脚本比对）
// 59 并发 read() 安全：8 线程 × 3 轮并发读同一实例不同区间 + 同一实例查询
// 60 记录型：非 EOF 短读实现（调用方违约）→ 如实记录现象，不判产品缺陷
//
// 环境变量：RN146_CPP_FIXTURE（样本路径）、RN146_CPP_JSON（44 导出路径）

#ifndef ENABLE_TEST
#define ENABLE_TEST
#endif

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <limits>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "common/global.h"
#include "common/schema.h"
#include "file/random_access_read_file.h"
#include "reader/result_set.h"
#include "reader/tsfile_reader.h"
#include "utils/errno_define.h"
#include "rn146_fixture.h"

namespace {

using storage::RandomAccessReadFile;
using storage::ResultSet;
using storage::TsFileReader;

struct RowData {
    std::vector<int64_t> times;
    std::vector<std::string> s0;
    std::vector<std::string> s2;  // 已序列化：数字字符串或 "null"（NULL 口径与 Python None 对齐）
};

std::string JsonOf(const RowData& rows) {
    std::ostringstream os;
    os << "{\"rows\":" << rows.times.size() << ",\"times\":[";
    for (size_t i = 0; i < rows.times.size(); ++i) {
        if (i) os << ",";
        os << rows.times[i];
    }
    os << "],\"s0\":[";
    for (size_t i = 0; i < rows.s0.size(); ++i) {
        if (i) os << ",";
        os << "\"" << rows.s0[i] << "\"";
    }
    os << "],\"s2\":[";
    for (size_t i = 0; i < rows.s2.size(); ++i) {
        if (i) os << ",";
        os << rows.s2[i];
    }
    os << "]}";
    return os.str();
}

std::vector<char> ReadFileBytes(const std::string& path) {
    std::ifstream input(path, std::ios::binary);
    return std::vector<char>(std::istreambuf_iterator<char>(input),
                             std::istreambuf_iterator<char>());
}

// 43：合规实现 —— 底层每次最多返回 chunk 字节（短读），实现内部循环读满
class FillingShortReadSource : public RandomAccessReadFile {
   public:
    FillingShortReadSource(const std::vector<char>& bytes, int32_t chunk,
                           std::string name)
        : bytes_(bytes), chunk_(chunk), name_(std::move(name)), opened_(true) {}

    bool is_opened() const override { return opened_; }
    int64_t file_size() const override {
        return static_cast<int64_t>(bytes_.size());
    }
    const std::string& file_path() const override { return name_; }
    int generation(uint64_t& size, uint64_t& fingerprint) const override {
        if (!opened_) return common::E_FILE_READ_ERR;
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
        if (size == 0 || offset >= static_cast<int64_t>(bytes_.size())) {
            return common::E_OK;  // EOF
        }
        while (read_size < size) {
            const int32_t piece = std::min(chunk_, size - read_size);
            const int64_t off = offset + read_size;
            if (off >= static_cast<int64_t>(bytes_.size())) break;
            const int32_t avail = static_cast<int32_t>(std::min<int64_t>(
                piece, static_cast<int64_t>(bytes_.size()) - off));
            std::memcpy(buffer + read_size, bytes_.data() + off, avail);
            read_size += avail;
            if (avail < piece) break;  // 真 EOF
        }
        return common::E_OK;
    }
    void close() override { opened_ = false; }

   private:
    std::vector<char> bytes_;
    int32_t chunk_;
    std::string name_;
    bool opened_;
};

// 59：线程安全实现（互斥保护共享状态；数据只读）
class ConcurrentMemorySource : public RandomAccessReadFile {
   public:
    ConcurrentMemorySource(const std::vector<char>& bytes, std::string name)
        : bytes_(bytes), name_(std::move(name)), opened_(true) {}
    bool is_opened() const override { return opened_; }
    int64_t file_size() const override {
        return static_cast<int64_t>(bytes_.size());
    }
    const std::string& file_path() const override { return name_; }
    int generation(uint64_t& size, uint64_t& fingerprint) const override {
        std::lock_guard<std::mutex> guard(mutex_);
        if (!opened_) return common::E_FILE_READ_ERR;
        size = bytes_.size();
        fingerprint = 0;
        return common::E_OK;
    }
    int read(int64_t offset, char* buffer, int32_t size,
             int32_t& read_size) override {
        std::lock_guard<std::mutex> guard(mutex_);
        read_size = 0;
        if (!opened_ || offset < 0 || size < 0 ||
            (buffer == nullptr && size > 0)) {
            return common::E_INVALID_ARG;
        }
        if (size == 0 || offset >= static_cast<int64_t>(bytes_.size())) {
            return common::E_OK;
        }
        const int32_t avail = static_cast<int32_t>(std::min<int64_t>(
            size, static_cast<int64_t>(bytes_.size()) - offset));
        std::memcpy(buffer, bytes_.data() + offset, avail);
        read_size = avail;
        return common::E_OK;
    }
    void close() override {
        std::lock_guard<std::mutex> guard(mutex_);
        opened_ = false;
    }

   private:
    std::vector<char> bytes_;
    std::string name_;
    bool opened_;
    mutable std::mutex mutex_;
};

// 60（记录型）：非 EOF 短读 —— 每次最多 8 字节，绝不补满
class NonConformantShortReadSource : public RandomAccessReadFile {
   public:
    NonConformantShortReadSource(const std::vector<char>& bytes, std::string name)
        : bytes_(bytes), name_(std::move(name)), opened_(true) {}
    bool is_opened() const override { return opened_; }
    int64_t file_size() const override {
        return static_cast<int64_t>(bytes_.size());
    }
    const std::string& file_path() const override { return name_; }
    int generation(uint64_t& size, uint64_t& fingerprint) const override {
        if (!opened_) return common::E_FILE_READ_ERR;
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
        if (size == 0 || offset >= static_cast<int64_t>(bytes_.size())) {
            return common::E_OK;
        }
        const int32_t avail = static_cast<int32_t>(std::min<int64_t>(
            std::min<int32_t>(size, 8),
            static_cast<int64_t>(bytes_.size()) - offset));
        std::memcpy(buffer, bytes_.data() + offset, avail);
        read_size = avail;
        return common::E_OK;
    }
    void close() override { opened_ = false; }

   private:
    std::vector<char> bytes_;
    std::string name_;
    bool opened_;
};

struct ReadOutcome {
    int open_rc = -1;
    int query_rc = -1;
    int iter_rc = -1;
    int schema_cols = -1;
    RowData rows;
};

ReadOutcome ReadViaSource(std::unique_ptr<RandomAccessReadFile> source) {
    ReadOutcome out;
    TsFileReader reader;
    out.open_rc = reader.open(std::move(source));
    if (out.open_rc != common::E_OK) return out;
    std::shared_ptr<storage::TableSchema> schema = reader.get_table_schema("test");
    if (schema) {
        out.schema_cols = schema->get_columns_num();
    }
    std::vector<std::string> columns = {"s0", "s2"};
    ResultSet* result = nullptr;
    out.query_rc = reader.query("test", columns, 0,
                                std::numeric_limits<int64_t>::max(), result, -1);
    if (out.query_rc == common::E_OK && result != nullptr) {
        out.iter_rc = common::E_OK;
        bool has_next = false;
        while (true) {
            const int rc = result->next(has_next);
            if (rc != common::E_OK) {
                out.iter_rc = rc;
                break;
            }
            if (!has_next) break;
            out.rows.times.push_back(result->get_value<int64_t>(1));
            // 字符串列（TAG/STRING）经 Field::get_string_value() 取（该 SDK 不支持
            // get_value<std::string>，预检已实测：模板仅覆盖数值类型）
            storage::Field* s0_field = result->get_row_record()->get_field(1);
            common::String* s0_str =
                s0_field != nullptr ? s0_field->get_string_value() : nullptr;
            out.rows.s0.push_back(s0_str != nullptr ? s0_str->to_std_string()
                                                    : std::string());
            // NULL 必须显式判（C++ 对 NULL 字段调数值 getter 会得到 0，与 Python None 不等价）
            if (result->is_null(3)) {
                out.rows.s2.push_back("null");
            } else {
                out.rows.s2.push_back(
                    std::to_string(result->get_value<int32_t>(3)));
            }
        }
        reader.destroy_query_data_set(result);
    }
    reader.close();
    return out;
}

ReadOutcome ReadViaPath(const std::string& path) {
    ReadOutcome out;
    TsFileReader reader;
    out.open_rc = reader.open(path);
    if (out.open_rc != common::E_OK) return out;
    std::shared_ptr<storage::TableSchema> schema = reader.get_table_schema("test");
    if (schema) {
        out.schema_cols = schema->get_columns_num();
    }
    std::vector<std::string> columns = {"s0", "s2"};
    ResultSet* result = nullptr;
    out.query_rc = reader.query("test", columns, 0,
                                std::numeric_limits<int64_t>::max(), result, -1);
    if (out.query_rc == common::E_OK && result != nullptr) {
        out.iter_rc = common::E_OK;
        bool has_next = false;
        while (true) {
            const int rc = result->next(has_next);
            if (rc != common::E_OK) {
                out.iter_rc = rc;
                break;
            }
            if (!has_next) break;
            out.rows.times.push_back(result->get_value<int64_t>(1));
            storage::Field* s0_field = result->get_row_record()->get_field(1);
            common::String* s0_str =
                s0_field != nullptr ? s0_field->get_string_value() : nullptr;
            out.rows.s0.push_back(s0_str != nullptr ? s0_str->to_std_string()
                                                    : std::string());
            if (result->is_null(3)) {
                out.rows.s2.push_back("null");
            } else {
                out.rows.s2.push_back(
                    std::to_string(result->get_value<int32_t>(3)));
            }
        }
        reader.destroy_query_data_set(result);
    }
    reader.close();
    return out;
}

bool SameRows(const ReadOutcome& a, const ReadOutcome& b) {
    return a.rows.times == b.rows.times && a.rows.s0 == b.rows.s0 &&
           a.rows.s2 == b.rows.s2 && a.schema_cols == b.schema_cols;
}

class RandomAccessReadContract : public ::testing::Test {
   protected:
    void SetUp() override {
        ASSERT_EQ(storage::libtsfile_init(), common::E_OK);
        fixture_ = rn146_fixture::TableFixturePath("RN146_CPP_FIXTURE");
        ASSERT_FALSE(fixture_.empty()) << "auto fixture build failed";
        bytes_ = ReadFileBytes(fixture_);
        ASSERT_FALSE(bytes_.empty()) << "fixture missing: " << fixture_;
    }
    void TearDown() override { storage::libtsfile_destroy(); }
    std::string fixture_;
    std::vector<char> bytes_;
};

TEST_F(RandomAccessReadContract, Case42BaselineMatchesPathReader) {
    const ReadOutcome path = ReadViaPath(fixture_);
    ASSERT_EQ(path.open_rc, common::E_OK);
    ASSERT_EQ(path.query_rc, common::E_OK);
    const ReadOutcome custom = ReadViaSource(std::unique_ptr<RandomAccessReadFile>(
        new ConcurrentMemorySource(bytes_, "memory://rn146-case42")));
    std::printf("[probe] case42 path_rows=%zu custom_rows=%zu schema=%d/%d\n",
                path.rows.times.size(), custom.rows.times.size(), path.schema_cols,
                custom.schema_cols);
    EXPECT_TRUE(SameRows(path, custom)) << "custom source must match path reader";
    EXPECT_GT(path.rows.times.size(), 0u);
}

TEST_F(RandomAccessReadContract, Case43ShortReadFilledInternally) {
    const ReadOutcome path = ReadViaPath(fixture_);
    const ReadOutcome filled = ReadViaSource(std::unique_ptr<RandomAccessReadFile>(
        new FillingShortReadSource(bytes_, 7, "memory://rn146-case43")));
    std::printf("[probe] case43 chunk=7 path_rows=%zu filled_rows=%zu\n",
                path.rows.times.size(), filled.rows.times.size());
    EXPECT_TRUE(SameRows(path, filled)) << "short-read filled source must match path";
}

TEST_F(RandomAccessReadContract, Case44CrossInterfaceJson) {
    const ReadOutcome custom = ReadViaSource(std::unique_ptr<RandomAccessReadFile>(
        new FillingShortReadSource(bytes_, 64, "memory://rn146-case44")));
    const char* env = std::getenv("RN146_CPP_JSON");
    const std::string out_path =
        env != nullptr ? env : rn146_fixture::FixtureDir() + "/cpp_rows.json";
    std::ofstream os(out_path, std::ios::binary | std::ios::trunc);
    os << JsonOf(custom.rows) << "\n";
    os.close();
    std::printf("[probe] case44 rows=%zu json=%s\n", custom.rows.times.size(),
                out_path.c_str());
    EXPECT_GT(custom.rows.times.size(), 0u);
}

TEST_F(RandomAccessReadContract, Case59ConcurrentReadsThreadSafe) {
    std::shared_ptr<ConcurrentMemorySource> source =
        std::make_shared<ConcurrentMemorySource>(bytes_, "memory://rn146-case59");
    std::atomic<int> failures{0};
    for (int round = 0; round < 3; ++round) {
        std::vector<std::thread> threads;
        for (int t = 0; t < 8; ++t) {
            threads.emplace_back([&, t]() {
                const int64_t base =
                    static_cast<int64_t>(t) * 97 %
                    static_cast<int64_t>(bytes_.size() - 64);
                for (int i = 0; i < 50; ++i) {
                    const int64_t off =
                        (base + i * 31) % static_cast<int64_t>(bytes_.size() - 16);
                    const int32_t len = 16;
                    char buf[16];
                    int32_t got = 0;
                    const int rc = source->read(off, buf, len, got);
                    if (rc != common::E_OK || got != len ||
                        std::memcmp(buf, bytes_.data() + off, len) != 0) {
                        ++failures;
                    }
                }
            });
        }
        for (auto& th : threads) th.join();
    }
    // 同一实现类再开一个实例交给 reader（reader 取 unique_ptr 所有权）
    const ReadOutcome via_shared = ReadViaSource(std::unique_ptr<RandomAccessReadFile>(
        new ConcurrentMemorySource(bytes_, "memory://rn146-case59-query")));
    const ReadOutcome path = ReadViaPath(fixture_);
    std::printf("[probe] case59 rounds=3 threads=8 failures=%d shared_rows=%zu\n",
                failures.load(), via_shared.rows.times.size());
    EXPECT_EQ(failures.load(), 0);
    EXPECT_TRUE(SameRows(path, via_shared));
}

TEST_F(RandomAccessReadContract, Case60NonEofShortReadRecorded) {
    const ReadOutcome out = ReadViaSource(std::unique_ptr<RandomAccessReadFile>(
        new NonConformantShortReadSource(bytes_, "memory://rn146-case60")));
    std::printf(
        "[probe] case60 nonconformant open=%d query=%d iter=%d rows=%zu "
        "(记录型，不判产品)\n",
        out.open_rc, out.query_rc, out.iter_rc, out.rows.times.size());
    SUCCEED();
}

}  // namespace
