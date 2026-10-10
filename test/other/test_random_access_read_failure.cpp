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

// RN-146（支持直接读取远程文件）批次 2e —— C++ 侧注入式读失败矩阵（tester-only 探针）
//
// 目的：判定 Python 侧实测到的「读失败被静默吞没（空结果 / partial）或降级为
//      E_TABLE_NOT_EXIST=49」是否为共享 C++ 读取核心的行为，而不仅是 Python 适配层。
//      对应问题记录：records/rn146-defect-read-failure-swallowed-20260924.md
// 说明：本文件是「跨接口对照」探针，不是 RN-146 判定用例；判定仍由
//      python-tsfile-api-test/rn146_tests/ 编号 26–30、35 承担。
// 注入口径与 Python 侧 rn146_tests/helpers.py::FailingBytesIO 对齐：
//      fail_at   —— 第 N 次 read() 返回 E_FILE_READ_ERR（Python：第 N 次 read 抛 OSError）
//      cut_bytes —— 已服务累计字节 >= cut 时的下一次 read() 返回 E_FILE_READ_ERR
// 载体：/data/test/test-program/cpp-tsfile-api-test（只新增本文件 + CMake 注册）

#ifndef ENABLE_TEST
#define ENABLE_TEST
#endif

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iterator>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "common/global.h"
#include "file/random_access_read_file.h"
#include "reader/result_set.h"
#include "reader/tsfile_reader.h"
#include "utils/errno_define.h"
#include "rn146_fixture.h"

namespace {

using storage::RandomAccessReadFile;
using storage::ResultSet;
using storage::TsFileReader;

constexpr int64_t kNoFailAt = -1;
constexpr int64_t kNoCut = -1;

struct ProbeStats {
    int read_calls = 0;
    int64_t served_bytes = 0;
    std::vector<int32_t> read_sizes;
};

class FailingReadSource : public RandomAccessReadFile {
   public:
    FailingReadSource(const std::vector<char>& bytes, int64_t fail_at,
                      int64_t cut_bytes, ProbeStats* stats)
        : bytes_(bytes),
          opened_(true),
          name_("memory://rn146-batch2e"),
          fail_at_(fail_at),
          cut_bytes_(cut_bytes),
          stats_(stats) {}

    bool is_opened() const override { return opened_; }

    int64_t file_size() const override {
        return static_cast<int64_t>(bytes_.size());
    }

    const std::string& file_path() const override { return name_; }

    int generation(uint64_t& size, uint64_t& fingerprint) const override {
        if (!opened_) {
            return common::E_FILE_READ_ERR;
        }
        size = static_cast<uint64_t>(bytes_.size());
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
        ++stats_->read_calls;
        stats_->read_sizes.push_back(size);
        if (fail_at_ >= 0 && stats_->read_calls == fail_at_) {
            return common::E_FILE_READ_ERR;  // 注入：第 N 次读失败
        }
        if (cut_bytes_ >= 0 && stats_->served_bytes >= cut_bytes_) {
            return common::E_FILE_READ_ERR;  // 注入：累计服务已达 cut
        }
        if (size == 0 || static_cast<size_t>(offset) >= bytes_.size()) {
            return common::E_OK;  // EOF
        }
        const size_t available = bytes_.size() - static_cast<size_t>(offset);
        read_size = static_cast<int32_t>(
            std::min(available, static_cast<size_t>(size)));
        std::memcpy(buffer, bytes_.data() + offset,
                    static_cast<size_t>(read_size));
        stats_->served_bytes += read_size;
        return common::E_OK;
    }

    void close() override { opened_ = false; }

   private:
    std::vector<char> bytes_;
    bool opened_;
    std::string name_;
    int64_t fail_at_;
    int64_t cut_bytes_;
    ProbeStats* stats_;
};

struct ProbeOutcome {
    int open_rc = -1;
    int query_rc = -1;
    int iter_rc = -1;  // -1 = 未迭代（打开/查询未成功）
    int64_t rows = -1;
    int read_calls = 0;
    int64_t served_bytes = 0;
};

ProbeOutcome RunProbe(const std::vector<char>& bytes, int64_t fail_at,
                      int64_t cut_bytes, const std::string& table_name,
                      const std::vector<std::string>& columns) {
    ProbeOutcome out;
    ProbeStats stats;
    TsFileReader reader;
    out.open_rc = reader.open(std::unique_ptr<RandomAccessReadFile>(
        new FailingReadSource(bytes, fail_at, cut_bytes, &stats)));
    if (out.open_rc == common::E_OK) {
        ResultSet* result = nullptr;
        out.query_rc = reader.query(table_name, columns, 0,
                                    std::numeric_limits<int64_t>::max(),
                                    result, -1);
        if (out.query_rc == common::E_OK && result != nullptr) {
            out.rows = 0;
            out.iter_rc = common::E_OK;
            bool has_next = false;
            while (true) {
                const int rc = result->next(has_next);
                if (rc != common::E_OK) {
                    out.iter_rc = rc;
                    break;
                }
                if (!has_next) {
                    break;
                }
                ++out.rows;
            }
            reader.destroy_query_data_set(result);
        }
        reader.close();
    }
    out.read_calls = stats.read_calls;
    out.served_bytes = stats.served_bytes;
    return out;
}

bool IsSwallowed(const ProbeOutcome& out, int64_t baseline_rows) {
    return out.open_rc == common::E_OK && out.query_rc == common::E_OK &&
           out.iter_rc == common::E_OK && out.rows < baseline_rows;
}

const char* VerdictOf(const ProbeOutcome& out, int64_t baseline_rows) {
    if (out.open_rc != common::E_OK) {
        return "OPEN_ERR";
    }
    if (out.query_rc != common::E_OK) {
        return "QUERY_ERR";
    }
    if (out.iter_rc != common::E_OK) {
        return "ITER_ERR";
    }
    if (out.rows == baseline_rows) {
        return "FULL";
    }
    if (out.rows == 0) {
        return "EMPTY(swallowed)";
    }
    return "PARTIAL(swallowed)";
}

class RandomAccessReadFailureProbe : public ::testing::Test {
   protected:
    void SetUp() override {
        ASSERT_EQ(storage::libtsfile_init(), common::E_OK);
        fixture_path_ = rn146_fixture::TableFixturePath("RN146_FIXTURE_PATH");
        ASSERT_FALSE(fixture_path_.empty()) << "auto fixture build failed";
        std::ifstream input(fixture_path_, std::ios::binary);
        ASSERT_TRUE(input.is_open()) << "fixture not found: " << fixture_path_;
        bytes_.assign(std::istreambuf_iterator<char>(input),
                      std::istreambuf_iterator<char>());
        ASSERT_FALSE(bytes_.empty()) << "fixture is empty: " << fixture_path_;
    }

    void TearDown() override { storage::libtsfile_destroy(); }

    std::string fixture_path_;
    std::vector<char> bytes_;
};

TEST_F(RandomAccessReadFailureProbe, ReadFailureMatrix) {
    const std::vector<std::string> columns = {"s0", "s2"};
    const ProbeOutcome baseline =
        RunProbe(bytes_, kNoFailAt, kNoCut, "test", columns);
    std::printf("[probe] fixture=%s file_bytes=%zu\n", fixture_path_.c_str(),
                bytes_.size());
    std::printf(
        "[probe] baseline open=%d query=%d iter=%d rows=%lld reads=%d "
        "served=%lld\n",
        baseline.open_rc, baseline.query_rc, baseline.iter_rc,
        static_cast<long long>(baseline.rows), baseline.read_calls,
        static_cast<long long>(baseline.served_bytes));
    EXPECT_EQ(baseline.open_rc, common::E_OK) << "clean source must open";
    EXPECT_EQ(baseline.query_rc, common::E_OK) << "clean source must query";
    EXPECT_GT(baseline.rows, 0) << "clean source must return rows";

    // 控制组：错误码空间是否可区分（对照 Python 批次 2d control-run）
    const ProbeOutcome no_table =
        RunProbe(bytes_, kNoFailAt, kNoCut, "no_such_table", columns);
    const ProbeOutcome no_column =
        RunProbe(bytes_, kNoFailAt, kNoCut, "test", {"no_such_col"});
    std::printf(
        "[probe] control no_such_table query=%d | no_such_col query=%d\n",
        no_table.query_rc, no_column.query_rc);

    int fail_at_points = 0;
    int fail_at_swallowed = 0;
    int fail_at_table_not_exist = 0;
    const int sweep_max = baseline.read_calls + 3;
    std::printf("[probe] --- fail_at matrix (1..%d) ---\n", sweep_max);
    for (int n = 1; n <= sweep_max; ++n) {
        const ProbeOutcome out = RunProbe(bytes_, n, kNoCut, "test", columns);
        ++fail_at_points;
        if (IsSwallowed(out, baseline.rows)) {
            ++fail_at_swallowed;
        }
        if (out.query_rc == common::E_TABLE_NOT_EXIST) {
            ++fail_at_table_not_exist;
        }
        std::printf(
            "[probe] fail_at=%2d reads=%2d open=%2d query=%2d iter=%2d "
            "rows=%lld -> %s\n",
            n, out.read_calls, out.open_rc, out.query_rc, out.iter_rc,
            static_cast<long long>(out.rows),
            VerdictOf(out, baseline.rows));
    }

    int cut_points = 0;
    int cut_swallowed = 0;
    int cut_table_not_exist = 0;
    std::printf("[probe] --- cut_bytes sweep (256 step 256) ---\n");
    for (int64_t cut = 256; cut < static_cast<int64_t>(bytes_.size());
         cut += 256) {
        const ProbeOutcome out = RunProbe(bytes_, kNoFailAt, cut, "test",
                                          columns);
        ++cut_points;
        if (IsSwallowed(out, baseline.rows)) {
            ++cut_swallowed;
        }
        if (out.query_rc == common::E_TABLE_NOT_EXIST) {
            ++cut_table_not_exist;
        }
        std::printf(
            "[probe] cut=%6lld reads=%2d open=%2d query=%2d iter=%2d "
            "rows=%lld -> %s\n",
            static_cast<long long>(cut), out.read_calls, out.open_rc,
            out.query_rc, out.iter_rc, static_cast<long long>(out.rows),
            VerdictOf(out, baseline.rows));
    }

    const int swallowed = fail_at_swallowed + cut_swallowed;
    const int table_not_exist = fail_at_table_not_exist + cut_table_not_exist;
    std::printf(
        "[probe] summary baseline_reads=%d baseline_rows=%lld "
        "fail_at_points=%d cut_points=%d swallowed=%d table_not_exist=%d\n",
        baseline.read_calls, static_cast<long long>(baseline.rows),
        fail_at_points, cut_points, swallowed, table_not_exist);
    std::printf("[probe] verdict=%s\n", (swallowed > 0 || table_not_exist > 0)
                                             ? "SHARED_CORE_DEFECT"
                                             : "PYTHON_ADAPTER_ONLY");
    EXPECT_GT(fail_at_points, 0);
    EXPECT_GT(cut_points, 0);
}

}  // namespace
