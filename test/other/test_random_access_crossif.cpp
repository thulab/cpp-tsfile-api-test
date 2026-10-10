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

// RN-146 批次 7：跨接口复验（C++ 侧）—— 用例 26–30 的 C++ 语义矩阵（tester-only）
// kind: table（表模型查询）/ tree（树模型查询）/ tag（TagEq 过滤，走设备索引）/ devices（设备列表）
// 判据：任一点 open=0 且 query=0 且 iter=0 且 rows<基线，或 query=E_TABLE_NOT_EXIST(49) ⇒ SWALLOW

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
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "common/global.h"
#include "common/schema.h"
#include "file/random_access_read_file.h"
#include "reader/filter/tag_filter.h"
#include "reader/result_set.h"
#include "reader/tsfile_reader.h"
#include "utils/errno_define.h"
#include "rn146_fixture.h"

namespace {

using storage::RandomAccessReadFile;
using storage::ResultSet;
using storage::TsFileReader;

struct Stats {
    int calls = 0;
    int64_t served = 0;
};

class FailingSource : public RandomAccessReadFile {
   public:
    FailingSource(const std::vector<char>& bytes, int fail_at, int64_t cut,
                  Stats* stats)
        : bytes_(bytes),
          opened_(true),
          name_("memory://rn146-c7"),
          fail_at_(fail_at),
          cut_(cut),
          stats_(stats) {}
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
        ++stats_->calls;
        if (fail_at_ > 0 && stats_->calls == fail_at_) {
            return common::E_FILE_READ_ERR;
        }
        if (cut_ >= 0 && stats_->served >= cut_) {
            return common::E_FILE_READ_ERR;
        }
        if (size == 0 || offset >= static_cast<int64_t>(bytes_.size())) {
            return common::E_OK;
        }
        const size_t avail = bytes_.size() - static_cast<size_t>(offset);
        read_size = static_cast<int32_t>(
            std::min(avail, static_cast<size_t>(size)));
        std::memcpy(buffer, bytes_.data() + offset,
                    static_cast<size_t>(read_size));
        stats_->served += read_size;
        return common::E_OK;
    }
    void close() override { opened_ = false; }

   private:
    std::vector<char> bytes_;
    bool opened_;
    std::string name_;
    int fail_at_;
    int64_t cut_;
    Stats* stats_;
};

enum class Kind { kTable, kTree, kTag, kDevices };

const char* KindName(Kind kind) {
    switch (kind) {
        case Kind::kTable: return "table";
        case Kind::kTree: return "tree";
        case Kind::kTag: return "tag";
        default: return "devices";
    }
}

struct Outcome {
    int open_rc = -1;
    int query_rc = -1;
    int iter_rc = -1;
    int64_t rows = -1;
    int calls = 0;
};

Outcome RunProbe(const std::vector<char>& bytes, Kind kind, int fail_at,
                 int64_t cut) {
    Outcome out;
    Stats stats;
    TsFileReader reader;
    out.open_rc = reader.open(std::unique_ptr<RandomAccessReadFile>(
        new FailingSource(bytes, fail_at, cut, &stats)));
    if (out.open_rc == common::E_OK) {
        if (kind == Kind::kDevices) {
            std::vector<std::shared_ptr<storage::IDeviceID>> devices;
            const int rc = reader.get_all_devices(devices);
            out.query_rc = rc;
            out.iter_rc = common::E_OK;
            out.rows = rc == common::E_OK ? static_cast<int64_t>(devices.size())
                                          : -1;
        } else {
            ResultSet* result = nullptr;
            if (kind == Kind::kTable) {
                std::vector<std::string> columns = {"s0", "s2"};
                out.query_rc = reader.query(
                    "test", columns, 0,
                    std::numeric_limits<int64_t>::max(), result, -1);
            } else if (kind == Kind::kTag) {
                std::vector<std::string> columns = {"s0", "s2"};
                storage::TagEq tag_eq(0, "a");
                out.query_rc = reader.query(
                    "test", columns, 0,
                    std::numeric_limits<int64_t>::max(), result, &tag_eq, 0);
            } else {
                std::vector<std::string> paths = {
                    "root.ln.wf01.wt01.temperature",
                    "root.ln.wf01.wt01.status"};
                out.query_rc = reader.query(
                    paths, 0, std::numeric_limits<int64_t>::max(), result);
            }
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
                    if (!has_next) break;
                    ++out.rows;
                }
                reader.destroy_query_data_set(result);
            }
        }
        reader.close();
    }
    out.calls = stats.calls;
    return out;
}

bool Swallowed(const Outcome& out, int64_t baseline_rows) {
    const bool rc_ok = out.open_rc == common::E_OK &&
                       out.query_rc == common::E_OK &&
                       out.iter_rc == common::E_OK;
    return (rc_ok && out.rows < baseline_rows) ||
           out.query_rc == common::E_TABLE_NOT_EXIST;
}

class RandomAccessCrossInterface : public ::testing::Test {
   protected:
    void SetUp() override {
        ASSERT_EQ(storage::libtsfile_init(), common::E_OK);
        table_path_ = rn146_fixture::TableFixturePath("RN146_TABLE");
        tree_path_ = rn146_fixture::TreeFixturePath("RN146_TREE");
        ASSERT_FALSE(table_path_.empty()) << "auto table fixture build failed";
        ASSERT_FALSE(tree_path_.empty()) << "auto tree fixture build failed";
        table_bytes_ = ReadAll(table_path_);
        tree_bytes_ = ReadAll(tree_path_);
        ASSERT_FALSE(table_bytes_.empty()) << table_path_;
        ASSERT_FALSE(tree_bytes_.empty()) << tree_path_;
    }
    void TearDown() override { storage::libtsfile_destroy(); }

    static std::vector<char> ReadAll(const std::string& path) {
        std::ifstream input(path, std::ios::binary);
        return std::vector<char>(std::istreambuf_iterator<char>(input),
                                 std::istreambuf_iterator<char>());
    }

    std::string table_path_;
    std::string tree_path_;
    std::vector<char> table_bytes_;
    std::vector<char> tree_bytes_;
};

TEST_F(RandomAccessCrossInterface, CrossInterfaceMatrix) {
    const char* json_env = std::getenv("RN146_CROSSIF_CSV");
    const std::string csv_path =
        json_env != nullptr ? json_env : rn146_fixture::FixtureDir() + "/cpp.csv";
    std::ofstream csv(csv_path, std::ios::binary | std::ios::trunc);
    csv << "kind,point,open,query,iter,rows,calls\n";
    const Kind kinds[4] = {Kind::kTable, Kind::kTree, Kind::kTag,
                           Kind::kDevices};
    for (Kind kind : kinds) {
        const std::vector<char>& bytes =
            kind == Kind::kTree ? tree_bytes_ : table_bytes_;
        const Outcome base = RunProbe(bytes, kind, 0, -1);
        int bad = 0;
        int points = 0;
        std::printf("[probe] kind=%s baseline open=%d query=%d rows=%lld calls=%d\n",
                    KindName(kind), base.open_rc, base.query_rc,
                    static_cast<long long>(base.rows), base.calls);
        for (int n = 1; n <= base.calls + 2; ++n) {
            const Outcome out = RunProbe(bytes, kind, n, -1);
            ++points;
            if (Swallowed(out, base.rows)) ++bad;
            std::printf("[probe] %s fail_at=%2d open=%2d query=%2d iter=%2d rows=%lld -> %s\n",
                        KindName(kind), n, out.open_rc, out.query_rc,
                        out.iter_rc, static_cast<long long>(out.rows),
                        Swallowed(out, base.rows) ? "SWALLOW" : "err/full");
            csv << KindName(kind) << ",fail_at=" << n << "," << out.open_rc
                << "," << out.query_rc << "," << out.iter_rc << ","
                << out.rows << "," << out.calls << "\n";
        }
        for (int64_t cut = 256; cut < static_cast<int64_t>(bytes.size());
             cut += 256) {
            const Outcome out = RunProbe(bytes, kind, 0, cut);
            ++points;
            if (Swallowed(out, base.rows)) ++bad;
            std::printf("[probe] %s cut=%6lld open=%2d query=%2d iter=%2d rows=%lld -> %s\n",
                        KindName(kind), static_cast<long long>(cut),
                        out.open_rc, out.query_rc, out.iter_rc,
                        static_cast<long long>(out.rows),
                        Swallowed(out, base.rows) ? "SWALLOW" : "err/full");
            csv << KindName(kind) << ",cut=" << cut << "," << out.open_rc
                << "," << out.query_rc << "," << out.iter_rc << ","
                << out.rows << "," << out.calls << "\n";
        }
        std::printf("[probe] %s points=%d swallowed=%d verdict=%s\n",
                    KindName(kind), points, bad,
                    bad > 0 ? "CXX_SWALLOWS" : "CXX_OK");
    }
    csv.close();
    EXPECT_TRUE(true);
}

}  // namespace
