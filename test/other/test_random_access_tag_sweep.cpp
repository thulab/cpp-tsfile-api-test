// RN-146 批次 7b：TagEq 列索引口径扫描 + 修正后重跑 Tag 过滤矩阵（编号 28），tester-only
#ifndef ENABLE_TEST
#define ENABLE_TEST
#endif
#include <gtest/gtest.h>
#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <limits>
#include <memory>
#include <string>
#include <vector>
#include "common/global.h"
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

struct Stats { int calls = 0; int64_t served = 0; };

class FailingSource : public RandomAccessReadFile {
   public:
    FailingSource(const std::vector<char>& b, int fail_at, int64_t cut, Stats* s)
        : bytes_(b), opened_(true), name_("memory://rn146-c7b"), fail_at_(fail_at),
          cut_(cut), stats_(s) {}
    bool is_opened() const override { return opened_; }
    int64_t file_size() const override { return static_cast<int64_t>(bytes_.size()); }
    const std::string& file_path() const override { return name_; }
    int generation(uint64_t& size, uint64_t& fingerprint) const override {
        if (!opened_) return common::E_FILE_READ_ERR;
        size = bytes_.size(); fingerprint = 0; return common::E_OK;
    }
    int read(int64_t offset, char* buffer, int32_t size, int32_t& read_size) override {
        read_size = 0;
        if (!opened_ || offset < 0 || size < 0 || (buffer == nullptr && size > 0))
            return common::E_INVALID_ARG;
        ++stats_->calls;
        if (fail_at_ > 0 && stats_->calls == fail_at_) return common::E_FILE_READ_ERR;
        if (cut_ >= 0 && stats_->served >= cut_) return common::E_FILE_READ_ERR;
        if (size == 0 || offset >= static_cast<int64_t>(bytes_.size())) return common::E_OK;
        const size_t avail = bytes_.size() - static_cast<size_t>(offset);
        read_size = static_cast<int32_t>(std::min(avail, static_cast<size_t>(size)));
        std::memcpy(buffer, bytes_.data() + offset, static_cast<size_t>(read_size));
        stats_->served += read_size;
        return common::E_OK;
    }
    void close() override { opened_ = false; }
   private:
    std::vector<char> bytes_; bool opened_; std::string name_; int fail_at_; int64_t cut_; Stats* stats_;
};

struct Outcome { int open_rc = -1; int query_rc = -1; int iter_rc = -1; int64_t rows = -1; int calls = 0; };

Outcome RunTag(const std::vector<char>& bytes, int col_idx, int fail_at, int64_t cut) {
    Outcome out; Stats stats; TsFileReader reader;
    out.open_rc = reader.open(std::unique_ptr<RandomAccessReadFile>(
        new FailingSource(bytes, fail_at, cut, &stats)));
    if (out.open_rc == common::E_OK) {
        std::vector<std::string> columns = {"s0", "s2"};
        storage::TagEq tag_eq(col_idx, "a");
        ResultSet* result = nullptr;
        out.query_rc = reader.query("test", columns, 0,
                                    std::numeric_limits<int64_t>::max(), result,
                                    &tag_eq, 0);
        if (out.query_rc == common::E_OK && result != nullptr) {
            out.rows = 0; out.iter_rc = common::E_OK;
            bool has_next = false;
            while (true) {
                const int rc = result->next(has_next);
                if (rc != common::E_OK) { out.iter_rc = rc; break; }
                if (!has_next) break;
                ++out.rows;
            }
            reader.destroy_query_data_set(result);
        }
        reader.close();
    }
    out.calls = stats.calls;
    return out;
}

TEST(TagSweepProbe, ColIndexAndMatrix) {
    ASSERT_EQ(storage::libtsfile_init(), common::E_OK);
    const std::string path = rn146_fixture::TableFixturePath("RN146_TABLE");
    ASSERT_FALSE(path.empty()) << "auto fixture build failed";
    std::ifstream in(path, std::ios::binary);
    const std::vector<char> bytes((std::istreambuf_iterator<char>(in)),
                                  std::istreambuf_iterator<char>());
    ASSERT_FALSE(bytes.empty());
    int best_idx = -1; int64_t best_rows = -1;
    for (int idx = 0; idx <= 2; ++idx) {
        for (const char* value : {"a", "b"}) {
            Stats stats; TsFileReader reader;
            const int open_rc = reader.open(std::unique_ptr<RandomAccessReadFile>(
                new FailingSource(bytes, 0, -1, &stats)));
            int64_t rows = -1; int query_rc = -1;
            if (open_rc == common::E_OK) {
                std::vector<std::string> columns = {"s0", "s2"};
                storage::TagEq tag_eq(idx, value);
                ResultSet* result = nullptr;
                query_rc = reader.query("test", columns, 0,
                                        std::numeric_limits<int64_t>::max(), result,
                                        &tag_eq, 0);
                if (query_rc == common::E_OK && result != nullptr) {
                    rows = 0; bool has_next = false;
                    while (result->next(has_next) == common::E_OK && has_next) ++rows;
                    reader.destroy_query_data_set(result);
                }
                reader.close();
            }
            std::printf("[probe] sweep col_idx=%d value=%s open=%d query=%d rows=%lld\n",
                        idx, value, open_rc, query_rc, static_cast<long long>(rows));
            if (strcmp(value, "a") == 0 && rows > best_rows) { best_rows = rows; best_idx = idx; }
        }
    }
    std::printf("[probe] tag baseline col_idx=%d rows=%lld\n", best_idx,
                static_cast<long long>(best_rows));
    if (best_idx >= 0 && best_rows > 0) {
        int swallowed = 0; int points = 0;
        for (int n = 1; n <= 20; ++n) {
            const Outcome out = RunTag(bytes, best_idx, n, -1);
            ++points;
            const bool bad = (out.open_rc == common::E_OK && out.query_rc == common::E_OK &&
                              out.iter_rc == common::E_OK && out.rows < best_rows) ||
                             out.query_rc == common::E_TABLE_NOT_EXIST;
            if (bad) ++swallowed;
            std::printf("[probe] tag fail_at=%2d open=%2d query=%2d iter=%2d rows=%lld -> %s\n",
                        n, out.open_rc, out.query_rc, out.iter_rc,
                        static_cast<long long>(out.rows), bad ? "SWALLOW" : "err/full");
        }
        for (int64_t cut = 256; cut < static_cast<int64_t>(bytes.size()); cut += 256) {
            const Outcome out = RunTag(bytes, best_idx, 0, cut);
            ++points;
            const bool bad = (out.open_rc == common::E_OK && out.query_rc == common::E_OK &&
                              out.iter_rc == common::E_OK && out.rows < best_rows) ||
                             out.query_rc == common::E_TABLE_NOT_EXIST;
            if (bad) ++swallowed;
            std::printf("[probe] tag cut=%5lld open=%2d query=%2d iter=%2d rows=%lld -> %s\n",
                        static_cast<long long>(cut), out.open_rc, out.query_rc,
                        out.iter_rc, static_cast<long long>(out.rows),
                        bad ? "SWALLOW" : "err/full");
        }
        std::printf("[probe] tag points=%d swallowed=%d verdict=%s\n", points, swallowed,
                    swallowed > 0 ? "CXX_SWALLOWS" : "CXX_OK");
    } else {
        std::printf("[probe] tag baseline 未命中（口径待研发确认），跳过滤矩阵\n");
    }
    storage::libtsfile_destroy();
    SUCCEED();
}
}  // namespace
