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

// TsFile-168 batch 2: malformed/tampered ALP input protection (cases 22-25).
// Every malformed input must fail safely: no crash, no silently wrong data.

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <functional>
#include <iostream>
#include <iterator>
#include <string>
#include <vector>

#include <fcntl.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <unistd.h>

#include "common/record.h"
#include "common/schema.h"
#include "common/tsfile_common.h"
#include "encoding/alp/alp_constants.h"
#include "encoding/alp/alp_format.h"
#include "encoding/alp/alp_scalar.h"
#include "file/write_file.h"
#include "reader/result_set.h"
#include "reader/tsfile_reader.h"
#include "writer/tsfile_writer.h"

namespace {

using namespace storage;  // NOLINT

const int64_t kBaseTime = 1700000000000LL;

std::string WorkDir() {
    static const std::string dir = []() {
        char tmpl[] = "/tmp/tsfile168_sec_XXXXXX";
        char* made = ::mkdtemp(tmpl);
        return std::string(made == nullptr ? "/tmp" : made);
    }();
    return dir;
}

bool SameDoubleBits(double a, double b) {
    return std::memcmp(&a, &b, sizeof(double)) == 0;
}

int64_t FileSize(const std::string& path) {
    std::ifstream in(path.c_str(), std::ios::binary | std::ios::ate);
    return in.good() ? static_cast<int64_t>(in.tellg()) : -1;
}

void WriteU16(std::vector<uint8_t>& out, size_t offset, uint16_t value) {
    out[offset] = static_cast<uint8_t>(value & 0xff);
    out[offset + 1] = static_cast<uint8_t>((value >> 8) & 0xff);
}

void WriteU32(std::vector<uint8_t>& out, size_t offset, uint32_t value) {
    for (int i = 0; i < 4; ++i) {
        out[offset + static_cast<size_t>(i)] =
            static_cast<uint8_t>((value >> (8 * i)) & 0xff);
    }
}

struct Mismatch {
    int64_t count = 0;
    std::string first;

    void Add(const std::string& message) {
        if (count == 0) {
            first = message;
        }
        ++count;
    }

    void ExpectNone(const char* label) const {
        EXPECT_EQ(0, count) << label << ": mismatches=" << count
                            << ", first=" << first;
    }
};

std::vector<double> ExceptionValues() {
    std::vector<double> values(256);
    for (size_t i = 0; i < values.size(); ++i) {
        if (i % 37 == 0) {
            values[i] = (i % 74 == 0) ? 1e30 : -1e30;
        } else {
            values[i] = static_cast<double>(i % 100) * 0.25 - 12.5;
        }
    }
    return values;
}

std::vector<uint8_t> EncodePage(const std::vector<double>& values) {
    std::vector<uint8_t> encoded;
    const int status = alp::AlpEncodePage(
        values.data(), static_cast<uint32_t>(values.size()), encoded);
    EXPECT_EQ(alp::ALP_OK, status);
    return encoded;
}

void ParseHeaders(const std::vector<uint8_t>& page,
                  alp::AlpPageHeader& page_header,
                  alp::AlpBlockHeader& block_header, uint32_t& block_pos) {
    uint32_t pos = 0;
    ASSERT_TRUE(alp::AlpParsePageHeader(page.data(),
                                        static_cast<uint32_t>(page.size()),
                                        pos, page_header));
    block_pos = pos;
    ASSERT_TRUE(alp::AlpParseBlockHeader(page.data(),
                                         static_cast<uint32_t>(page.size()),
                                         pos, block_header));
}

// Tamper every page-header / block-header field once and require a safe
// failure (any non-OK status).  Returns nothing; records mismatches.
void ReplayHeaderTampering(const std::vector<uint8_t>& page,
                           Mismatch& mismatch, bool verbose) {
    alp::AlpPageHeader page_header;
    alp::AlpBlockHeader block_header;
    uint32_t block_pos = 0;
    ParseHeaders(page, page_header, block_header, block_pos);
    const size_t block_start = block_pos;  // already positioned at the header

    auto expect_rejected =
        [&](const char* label,
            const std::function<void(std::vector<uint8_t>&)>& mutate) {
            std::vector<uint8_t> copy = page;
            mutate(copy);
            std::vector<double> out;
            const int status = alp::AlpDecodePage(
                copy.data(), static_cast<uint32_t>(copy.size()), out);
            if (status == alp::ALP_OK) {
                mismatch.Add(std::string("tamper accepted: ") + label);
            }
            if (verbose) {
                std::cout << "[TSFILE168][case22] tamper " << label
                          << " status=" << status << " decoded=" << out.size()
                          << std::endl;
            }
        };

    expect_rejected("page.version=2",
                    [](std::vector<uint8_t>& p) { p[0] = 2; });
    expect_rejected("page.data_type=float",
                    [](std::vector<uint8_t>& p) { p[1] = 0; });
    expect_rejected("page.reserved=1", [](std::vector<uint8_t>& p) {
        WriteU16(p, 2, 1);
    });
    expect_rejected("page.value_count=UINT32_MAX",
                    [](std::vector<uint8_t>& p) {
                        WriteU32(p, 4, 0xFFFFFFFFu);
                    });
    expect_rejected("page.block_count=UINT32_MAX",
                    [](std::vector<uint8_t>& p) {
                        WriteU32(p, 8, 0xFFFFFFFFu);
                    });
    expect_rejected("page.block_count=0",
                    [](std::vector<uint8_t>& p) { WriteU32(p, 8, 0); });
    expect_rejected("page.reserved2=1",
                    [](std::vector<uint8_t>& p) { WriteU32(p, 12, 1); });
    expect_rejected("block.scheme=2", [block_start](std::vector<uint8_t>& p) {
        p[block_start] = 2;
    });
    expect_rejected("block.value_count=0",
                    [block_start](std::vector<uint8_t>& p) {
                        WriteU32(p, block_start + 4, 0);
                    });
    expect_rejected("block.value_count=1025",
                    [block_start](std::vector<uint8_t>& p) {
                        WriteU32(p, block_start + 4, 1025);
                    });
    expect_rejected("block.exception_count>value_count",
                    [&](std::vector<uint8_t>& p) {
                        WriteU32(p, block_start + 8,
                                 block_header.value_count + 1);
                    });
    expect_rejected("block.bit_width=65",
                    [block_start](std::vector<uint8_t>& p) {
                        p[block_start + 24] = 65;
                    });
    expect_rejected("block.factor=200", [block_start](std::vector<uint8_t>& p) {
        p[block_start + 1] = 200;
    });
    expect_rejected("block.exponent=200",
                    [block_start](std::vector<uint8_t>& p) {
                        p[block_start + 2] = 200;
                    });
    expect_rejected("block.factor>exponent",
                    [block_start, &block_header](std::vector<uint8_t>& p) {
                        const uint8_t invalid_factor =
                            block_header.exponent < 255
                                ? static_cast<uint8_t>(
                                      block_header.exponent + 1)
                                : static_cast<uint8_t>(254);
                        p[block_start + 1] = invalid_factor;
                    });
    expect_rejected("block.body_bytes+1",
                    [&](std::vector<uint8_t>& p) {
                        WriteU32(p, block_start + 12,
                                 block_header.body_bytes + 1);
                    });
    if (block_header.body_bytes > 0) {
        expect_rejected("block.body_bytes-1", [&](std::vector<uint8_t>& p) {
            WriteU32(p, block_start + 12, block_header.body_bytes - 1);
        });
    }
}

// Truncate the page at every possible length; all strict prefixes must fail.
int64_t ReplayTruncation(const std::vector<uint8_t>& page, Mismatch& mismatch,
                         bool verbose) {
    int64_t accepted = 0;
    for (size_t length = 0; length < page.size(); ++length) {
        std::vector<double> out;
        const int status = alp::AlpDecodePage(
            page.data(), static_cast<uint32_t>(length), out);
        if (status == alp::ALP_OK) {
            ++accepted;
            if (accepted <= 5) {
                mismatch.Add("truncation accepted at length " +
                             std::to_string(length));
            }
        }
    }
    std::vector<double> out;
    const int full_status = alp::AlpDecodePage(
        page.data(), static_cast<uint32_t>(page.size()), out);
    if (full_status != alp::ALP_OK) {
        mismatch.Add("full page rejected");
    }
    if (verbose) {
        std::cout << "[TSFILE168][case23] truncation_points=" << page.size()
                  << " accepted=" << accepted
                  << " full_status=" << full_status << std::endl;
    }
    return accepted;
}

// 1 MiB page header that declares the largest block/value counts the bounds
// check allows.  The decoder reserves value_count elements before parsing the
// first block; this measures that amplification.  It is a risk observation:
// the page must still be rejected, but the reservation itself is recorded for
// the user/R&D to decide whether a limit is needed.
void ReplayMemoryAmplification(Mismatch& mismatch, bool verbose) {
    const uint32_t size = 1024 * 1024;
    std::vector<uint8_t> page(size, 0);
    page[0] = alp::ALP_FORMAT_VERSION;
    page[1] = alp::ALP_DOUBLE;
    const uint32_t block_count =
        (size - alp::ALP_PAGE_HEADER_SIZE) / alp::ALP_BLOCK_HEADER_WIRE_SIZE;
    const uint32_t value_count = block_count * alp::ALP_BLOCK_SIZE;
    WriteU32(page, 4, value_count);
    WriteU32(page, 8, block_count);
    WriteU32(page, 12, 0);

    struct rusage before_usage;
    struct rusage after_usage;
    getrusage(RUSAGE_SELF, &before_usage);
    std::vector<double> out;
    const auto started = std::chrono::steady_clock::now();
    const int status =
        alp::AlpDecodePage(page.data(), size, out);
    const auto elapsed_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - started)
            .count();
    getrusage(RUSAGE_SELF, &after_usage);
    const int64_t maxrss_delta_kb =
        static_cast<int64_t>(after_usage.ru_maxrss) -
        static_cast<int64_t>(before_usage.ru_maxrss);
    if (status == alp::ALP_OK) {
        mismatch.Add("memory amplification page accepted");
    }
    if (verbose) {
        std::cout << "[TSFILE168][case22] memamp status=" << status
                  << " value_count=" << value_count
                  << " capacity=" << out.capacity()
                  << " reserved_bytes="
                  << static_cast<uint64_t>(out.capacity()) * sizeof(double)
                  << " maxrss_delta_kb=" << maxrss_delta_kb
                  << " elapsed_ms=" << elapsed_ms << std::endl;
    }
}

void WriteAlpFile(const std::string& path) {
    TsFileWriter writer;
    ASSERT_EQ(common::E_OK,
              writer.open(path, O_RDWR | O_CREAT | O_TRUNC, 0666));
    ASSERT_EQ(common::E_OK,
              writer.register_timeseries(
                  "root.alp.corrupt",
                  MeasurementSchema("d", common::DOUBLE, common::ALP,
                                    common::UNCOMPRESSED)));
    for (int r = 0; r < 1000; ++r) {
        TsRecord record(kBaseTime + r, "root.alp.corrupt");
        record.add_point("d", static_cast<double>(r) * 0.5 - 250.0);
        ASSERT_EQ(common::E_OK, writer.write_record(record));
    }
    ASSERT_EQ(common::E_OK, writer.close());
}

// Tamper a real ALP file's page header and read it through TsFileReader.
void ReplayCorruptFile(const std::string& good_path, Mismatch& mismatch,
                       bool verbose) {
    std::ifstream in(good_path.c_str(), std::ios::binary);
    ASSERT_TRUE(in.good());
    std::vector<uint8_t> bytes((std::istreambuf_iterator<char>(in)),
                               std::istreambuf_iterator<char>());
    // 1000 DOUBLE values in one block: version, type, reserved, value_count,
    // block_count, reserved2.
    const uint8_t signature[16] = {0x01, 0x01, 0x00, 0x00, 0xE8, 0x03,
                                   0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
                                   0x00, 0x00, 0x00, 0x00};
    auto found = std::search(bytes.begin(), bytes.end(), signature,
                             signature + sizeof(signature));
    if (found == bytes.end()) {
        mismatch.Add("ALP page header signature not found");
        return;
    }
    const size_t page_offset =
        static_cast<size_t>(found - bytes.begin());
    if (verbose) {
        std::cout << "[TSFILE168][case24] page_offset=" << page_offset
                  << " file_bytes=" << bytes.size() << std::endl;
    }

    struct Variant {
        const char* label;
        size_t offset;
        uint8_t value;
    };
    const Variant variants[] = {
        {"data_type=float", page_offset + 1, 0x00},
        {"value_count=999", page_offset + 4, 0xE7},
    };
    std::vector<std::string> paths = {"root.alp.corrupt.d"};
    for (const Variant& variant : variants) {
        std::vector<uint8_t> corrupted = bytes;
        corrupted[variant.offset] = variant.value;
        const std::string bad_path =
            WorkDir() + "/case24_" + variant.label + ".tsfile";
        {
            std::ofstream out(bad_path.c_str(),
                              std::ios::binary | std::ios::trunc);
            ASSERT_TRUE(out.good());
            out.write(reinterpret_cast<const char*>(corrupted.data()),
                      static_cast<std::streamsize>(corrupted.size()));
        }

        TsFileReader reader;
        int open_rc = common::E_INVALID_ARG;
        int query_rc = common::E_INVALID_ARG;
        int next_rc = common::E_INVALID_ARG;
        int rows = 0;
        open_rc = reader.open(bad_path);
        if (open_rc == common::E_OK) {
            ResultSet* result = nullptr;
            query_rc = reader.query(paths, kBaseTime, kBaseTime + 999, result);
            if (query_rc == common::E_OK && result != nullptr) {
                bool has_next = false;
                while ((next_rc = result->next(has_next)) == common::E_OK &&
                       has_next) {
                    ++rows;
                }
                reader.destroy_query_data_set(result);
            }
            reader.close();
        }
        const bool failed = open_rc != common::E_OK ||
                            query_rc != common::E_OK ||
                            next_rc != common::E_OK;
        if (!failed) {
            mismatch.Add(std::string("corrupt file accepted: ") +
                         variant.label);
        }
        if (verbose) {
            std::cout << "[TSFILE168][case24] " << variant.label
                      << " open=" << open_rc << " query=" << query_rc
                      << " next=" << next_rc << " rows=" << rows << std::endl;
        }
        std::remove(bad_path.c_str());
    }
}

class AlpSecurityTest : public ::testing::Test {
   protected:
    void SetUp() override { libtsfile_init(); }

    void TearDown() override {
        for (const std::string& file : files_) {
            std::remove(file.c_str());
        }
        files_.clear();
        libtsfile_destroy();
    }

    std::string NewFile(const std::string& name) {
        std::string path = WorkDir() + "/" + name;
        files_.push_back(path);
        return path;
    }

    std::vector<std::string> files_;
};

// Case 22: page/block header tampering + memory amplification observation.
TEST_F(AlpSecurityTest, CorruptHeadersRejected) {
    const std::vector<double> values = ExceptionValues();
    const std::vector<uint8_t> page = EncodePage(values);
    ASSERT_FALSE(page.empty());
    alp::AlpPageHeader page_header;
    alp::AlpBlockHeader block_header;
    uint32_t block_pos = 0;
    ParseHeaders(page, page_header, block_header, block_pos);
    ASSERT_EQ(alp::ALP_SCHEME_ALP, block_header.scheme);
    ASSERT_GT(block_header.exception_count, 0u);
    ASSERT_EQ(static_cast<uint32_t>(values.size()), page_header.value_count);

    Mismatch mismatch;
    // The untampered page must still decode correctly.
    {
        std::vector<double> out;
        ASSERT_EQ(alp::ALP_OK,
                  alp::AlpDecodePage(page.data(),
                                     static_cast<uint32_t>(page.size()),
                                     out));
        ASSERT_EQ(values.size(), out.size());
        for (size_t i = 0; i < values.size(); ++i) {
            if (!SameDoubleBits(values[i], out[i])) {
                mismatch.Add("original page value " + std::to_string(i));
            }
        }
    }
    ReplayHeaderTampering(page, mismatch, true);
    ReplayMemoryAmplification(mismatch, true);
    mismatch.ExpectNone("case22");
    std::cout << "[TSFILE168][case22] page_bytes=" << page.size()
              << " exception_count=" << block_header.exception_count
              << std::endl;
}

// Case 23: truncation at every structural position must be rejected.
TEST_F(AlpSecurityTest, TruncatedPagesRejected) {
    const std::vector<double> values = ExceptionValues();
    const std::vector<uint8_t> page = EncodePage(values);
    ASSERT_FALSE(page.empty());
    alp::AlpPageHeader page_header;
    alp::AlpBlockHeader block_header;
    uint32_t block_pos = 0;
    ParseHeaders(page, page_header, block_header, block_pos);
    ASSERT_EQ(alp::ALP_SCHEME_ALP, block_header.scheme);
    ASSERT_GT(block_header.exception_count, 0u);

    Mismatch mismatch;
    ReplayTruncation(page, mismatch, true);
    mismatch.ExpectNone("case23");
}

// Case 24: a tampered on-disk ALP page must fail through TsFileReader.
TEST_F(AlpSecurityTest, CorruptFileReadRejected) {
    const std::string good_path = NewFile("case24_good.tsfile");
    WriteAlpFile(good_path);
    Mismatch mismatch;
    ReplayCorruptFile(good_path, mismatch, true);
    mismatch.ExpectNone("case24");
    std::cout << "[TSFILE168][case24] file_bytes=" << FileSize(good_path)
              << std::endl;
}

// Case 25: replay the full malformed corpus; ASan/LSan must stay clean.
TEST_F(AlpSecurityTest, MalformedCleanUnderAsan) {
    Mismatch mismatch;
    const std::vector<double> values = ExceptionValues();
    const std::vector<uint8_t> page = EncodePage(values);
    ASSERT_FALSE(page.empty());

    ReplayHeaderTampering(page, mismatch, true);
    ReplayTruncation(page, mismatch, true);
    ReplayMemoryAmplification(mismatch, true);

    const std::string good_path = NewFile("case25_good.tsfile");
    WriteAlpFile(good_path);
    ReplayCorruptFile(good_path, mismatch, true);

    mismatch.ExpectNone("case25");
    std::cout << "[TSFILE168][case25] malformed corpus replayed" << std::endl;
}

}  // namespace
