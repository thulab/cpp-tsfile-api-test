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

// TsFile-168 batch 1: C++ ALP (FLOAT/DOUBLE) encoding verification.
// Cases 1-13, 26, 29, 30.  Every value check is a bit-pattern comparison.

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "common/db_common.h"
#include "common/device_id.h"
#include "common/record.h"
#include "common/row_record.h"
#include "common/schema.h"
#include "common/tablet.h"
#include "common/tsfile_common.h"
#include "encoding/alp/alp_constants.h"
#include "encoding/alp/alp_format.h"
#include "encoding/alp/alp_scalar.h"
#include "file/write_file.h"
#include "reader/qds_without_timegenerator.h"
#include "reader/result_set.h"
#include "reader/table_result_set.h"
#include "reader/tsfile_reader.h"
#include "writer/tsfile_table_writer.h"
#include "writer/tsfile_writer.h"

namespace {

using namespace storage;  // NOLINT

const int64_t kBaseTime = 1700000000000LL;

std::string WorkDir() {
    static const std::string dir = []() {
        char tmpl[] = "/tmp/tsfile168_alp_XXXXXX";
        char* made = ::mkdtemp(tmpl);
        return std::string(made == nullptr ? "/tmp" : made);
    }();
    return dir;
}

bool FileExists(const std::string& path) {
    std::ifstream in(path.c_str(), std::ios::binary);
    return in.good();
}

int64_t FileSize(const std::string& path) {
    std::ifstream in(path.c_str(), std::ios::binary | std::ios::ate);
    return in.good() ? static_cast<int64_t>(in.tellg()) : -1;
}

std::string Pad2(int n) {
    std::string s = std::to_string(n);
    return s.size() < 2 ? "0" + s : s;
}

std::string Pad3(int n) {
    std::string s = std::to_string(n);
    while (s.size() < 3) s = "0" + s;
    return s;
}

bool SameFloatBits(float a, float b) {
    return std::memcmp(&a, &b, sizeof(float)) == 0;
}

bool SameDoubleBits(double a, double b) {
    return std::memcmp(&a, &b, sizeof(double)) == 0;
}

float FloatBits(uint32_t bits) {
    float v = 0.0f;
    std::memcpy(&v, &bits, sizeof(v));
    return v;
}

double DoubleBits(uint64_t bits) {
    double v = 0.0;
    std::memcpy(&v, &bits, sizeof(v));
    return v;
}

std::string FloatBitsText(float v) {
    uint32_t b = 0;
    std::memcpy(&b, &v, sizeof(b));
    char buf[32];
    snprintf(buf, sizeof(buf), "0x%08x", b);
    return std::string(buf);
}

std::string DoubleBitsText(double v) {
    uint64_t b = 0;
    std::memcpy(&b, &v, sizeof(b));
    char buf[32];
    snprintf(buf, sizeof(buf), "0x%016llx",
             static_cast<unsigned long long>(b));
    return std::string(buf);
}

// Data with periodic specials, used by the large-scale tree cases.
float FloatValue(int64_t index) {
    const int m = static_cast<int>(index % 64);
    switch (m) {
        case 1:
            return std::numeric_limits<float>::max();
        case 2:
            return -std::numeric_limits<float>::max();
        case 3:
            return std::numeric_limits<float>::denorm_min();
        case 4:
            return -std::numeric_limits<float>::denorm_min();
        case 5:
            return std::numeric_limits<float>::quiet_NaN();
        case 6:
            return std::numeric_limits<float>::infinity();
        case 7:
            return -std::numeric_limits<float>::infinity();
        default:
            return static_cast<float>(m - 32) * 0.25f;
    }
}

double DoubleValue(int64_t index) {
    const int m = static_cast<int>(index % 64);
    switch (m) {
        case 1:
            return std::numeric_limits<double>::max();
        case 2:
            return -std::numeric_limits<double>::max();
        case 3:
            return std::numeric_limits<double>::denorm_min();
        case 4:
            return -std::numeric_limits<double>::denorm_min();
        case 5:
            return std::numeric_limits<double>::quiet_NaN();
        case 6:
            return std::numeric_limits<double>::infinity();
        case 7:
            return -std::numeric_limits<double>::infinity();
        default:
            return static_cast<double>(m - 32) * 0.5;
    }
}

// Fixed-scale decimals: exactly representable, ALP encodes them without
// exceptions so a mismatch cannot be blamed on the codec's exception path.
float DecimalFloat(int64_t index) {
    return static_cast<float>(index % 1000) * 0.01f - 5.0f;
}

double DecimalDouble(int64_t index) {
    return static_cast<double>(index % 2000) * 0.0001 - 0.1;
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

class AlpEncodingTest : public ::testing::Test {
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

// ---------------------------------------------------------------------------
// Case 1: non-aligned devices, 100 devices x 100 measurements x 100 rows.
// ---------------------------------------------------------------------------
TEST_F(AlpEncodingTest, NonAlignedLargeScaleRoundTrip) {
    const int kDevices = 100;
    const int kMeasurements = 100;
    const int kRows = 100;
    const std::string path = NewFile("case01_non_aligned_large.tsfile");

    auto measurement_name = [](int m) {
        return (m < 50 ? std::string("f_") + Pad2(m)
                       : std::string("d_") + Pad2(m - 50));
    };
    auto device_name = [](int d) {
        return std::string("root.alp.dev") + Pad3(d);
    };

    auto started = std::chrono::steady_clock::now();
    {
        TsFileWriter writer;
        ASSERT_EQ(common::E_OK,
                  writer.open(path, O_RDWR | O_CREAT | O_TRUNC, 0666));
        for (int d = 0; d < kDevices; ++d) {
            const std::string device = device_name(d);
            for (int m = 0; m < kMeasurements; ++m) {
                const common::TSDataType type =
                    m < 50 ? common::FLOAT : common::DOUBLE;
                ASSERT_EQ(common::E_OK,
                          writer.register_timeseries(
                              device, MeasurementSchema(measurement_name(m),
                                                        type, common::ALP,
                                                        common::LZ4)));
            }
        }
        for (int d = 0; d < kDevices; ++d) {
            const std::string device = device_name(d);
            for (int r = 0; r < kRows; ++r) {
                TsRecord record(kBaseTime + r, device);
                for (int m = 0; m < kMeasurements; ++m) {
                    if (r % 20 == 0 && m % 20 == 0) {
                        continue;  // 5% sparse rows
                    }
                    const int64_t index =
                        static_cast<int64_t>(r) * kMeasurements + m;
                    if (m < 50) {
                        record.add_point(measurement_name(m),
                                         FloatValue(index));
                    } else {
                        record.add_point(measurement_name(m),
                                         DoubleValue(index));
                    }
                }
                ASSERT_EQ(common::E_OK, writer.write_record(record));
            }
        }
        ASSERT_EQ(common::E_OK, writer.close());
    }

    TsFileReader reader;
    ASSERT_EQ(common::E_OK, reader.open(path));
    Mismatch mismatch;
    int64_t total_rows = 0;
    for (int d = 0; d < kDevices; ++d) {
        const std::string device = device_name(d);
        std::vector<std::string> paths;
        paths.reserve(kMeasurements);
        for (int m = 0; m < kMeasurements; ++m) {
            paths.push_back(device + "." + measurement_name(m));
        }
        ResultSet* result = nullptr;
        ASSERT_EQ(common::E_OK,
                  reader.query(paths, kBaseTime, kBaseTime + kRows - 1,
                               result));
        ASSERT_NE(nullptr, result);
        int rows = 0;
        bool has_next = false;
        while (IS_SUCC(result->next(has_next)) && has_next) {
            const int64_t timestamp = result->get_int64_at(1);
            const int r = static_cast<int>(timestamp - kBaseTime);
            if (r < 0 || r >= kRows) {
                mismatch.Add("device " + device + " unexpected timestamp " +
                             std::to_string(timestamp));
                break;
            }
            for (int m = 0; m < kMeasurements; ++m) {
                const uint32_t column = static_cast<uint32_t>(m + 2);
                const bool expect_null = (r % 20 == 0 && m % 20 == 0);
                if (result->is_null(column) != expect_null) {
                    mismatch.Add("device " + device + " row " +
                                 std::to_string(r) + " measurement " +
                                 measurement_name(m) + " null mismatch");
                    continue;
                }
                if (expect_null) {
                    continue;
                }
                const int64_t index =
                    static_cast<int64_t>(r) * kMeasurements + m;
                if (m < 50) {
                    const float actual = result->get_float_at(column);
                    const float expected = FloatValue(index);
                    if (!SameFloatBits(actual, expected)) {
                        mismatch.Add("device " + device + " row " +
                                     std::to_string(r) + " " +
                                     measurement_name(m) + " expected " +
                                     FloatBitsText(expected) + " actual " +
                                     FloatBitsText(actual));
                    }
                } else {
                    const double actual = result->get_double_at(column);
                    const double expected = DoubleValue(index);
                    if (!SameDoubleBits(actual, expected)) {
                        mismatch.Add("device " + device + " row " +
                                     std::to_string(r) + " " +
                                     measurement_name(m) + " expected " +
                                     DoubleBitsText(expected) + " actual " +
                                     DoubleBitsText(actual));
                    }
                }
            }
            ++rows;
        }
        EXPECT_EQ(kRows, rows) << "device " << device;
        total_rows += rows;
        reader.destroy_query_data_set(result);
    }
    ASSERT_EQ(common::E_OK, reader.close());
    mismatch.ExpectNone("case01");
    const auto elapsed_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - started)
            .count();
    std::cout << "[TSFILE168][case01] rows=" << total_rows
              << " file_bytes=" << FileSize(path)
              << " elapsed_ms=" << elapsed_ms << std::endl;
}

// ---------------------------------------------------------------------------
// Case 2: aligned devices, 100 devices x 100 measurements x 100 rows.
// ---------------------------------------------------------------------------
TEST_F(AlpEncodingTest, AlignedLargeScaleRoundTrip) {
    const int kDevices = 100;
    const int kMeasurements = 100;
    const int kRows = 100;
    const std::string path = NewFile("case02_aligned_large.tsfile");

    auto measurement_name = [](int m) {
        return (m < 50 ? std::string("f_") + Pad2(m)
                       : std::string("d_") + Pad2(m - 50));
    };
    auto device_name = [](int d) {
        return std::string("root.alp.adev") + Pad3(d);
    };

    auto started = std::chrono::steady_clock::now();
    {
        TsFileWriter writer;
        ASSERT_EQ(common::E_OK,
                  writer.open(path, O_RDWR | O_CREAT | O_TRUNC, 0666));
        for (int d = 0; d < kDevices; ++d) {
            std::vector<MeasurementSchema*> schemas;
            schemas.reserve(kMeasurements);
            for (int m = 0; m < kMeasurements; ++m) {
                const common::TSDataType type =
                    m < 50 ? common::FLOAT : common::DOUBLE;
                schemas.push_back(new MeasurementSchema(
                    measurement_name(m), type, common::ALP, common::LZ4));
            }
            ASSERT_EQ(common::E_OK, writer.register_aligned_timeseries(
                                        device_name(d), schemas));
        }
        for (int d = 0; d < kDevices; ++d) {
            const std::string device = device_name(d);
            for (int r = 0; r < kRows; ++r) {
                TsRecord record(kBaseTime + r, device);
                for (int m = 0; m < kMeasurements; ++m) {
                    if (r % 20 == 0 && m % 20 == 0) {
                        continue;
                    }
                    const int64_t index =
                        static_cast<int64_t>(r) * kMeasurements + m;
                    if (m < 50) {
                        record.add_point(measurement_name(m),
                                         FloatValue(index));
                    } else {
                        record.add_point(measurement_name(m),
                                         DoubleValue(index));
                    }
                }
                ASSERT_EQ(common::E_OK, writer.write_record_aligned(record));
            }
        }
        ASSERT_EQ(common::E_OK, writer.close());
    }

    TsFileReader reader;
    ASSERT_EQ(common::E_OK, reader.open(path));
    Mismatch mismatch;
    int64_t total_rows = 0;
    for (int d = 0; d < kDevices; ++d) {
        const std::string device = device_name(d);
        std::vector<std::string> paths;
        paths.reserve(kMeasurements);
        for (int m = 0; m < kMeasurements; ++m) {
            paths.push_back(device + "." + measurement_name(m));
        }
        ResultSet* result = nullptr;
        ASSERT_EQ(common::E_OK,
                  reader.query(paths, kBaseTime, kBaseTime + kRows - 1,
                               result));
        ASSERT_NE(nullptr, result);
        int rows = 0;
        bool has_next = false;
        while (IS_SUCC(result->next(has_next)) && has_next) {
            const int64_t timestamp = result->get_int64_at(1);
            const int r = static_cast<int>(timestamp - kBaseTime);
            if (r < 0 || r >= kRows) {
                mismatch.Add("device " + device + " unexpected timestamp " +
                             std::to_string(timestamp));
                break;
            }
            for (int m = 0; m < kMeasurements; ++m) {
                const uint32_t column = static_cast<uint32_t>(m + 2);
                const bool expect_null = (r % 20 == 0 && m % 20 == 0);
                if (result->is_null(column) != expect_null) {
                    mismatch.Add("device " + device + " row " +
                                 std::to_string(r) + " measurement " +
                                 measurement_name(m) + " null mismatch");
                    continue;
                }
                if (expect_null) {
                    continue;
                }
                const int64_t index =
                    static_cast<int64_t>(r) * kMeasurements + m;
                if (m < 50) {
                    const float actual = result->get_float_at(column);
                    const float expected = FloatValue(index);
                    if (!SameFloatBits(actual, expected)) {
                        mismatch.Add("device " + device + " row " +
                                     std::to_string(r) + " " +
                                     measurement_name(m) + " expected " +
                                     FloatBitsText(expected) + " actual " +
                                     FloatBitsText(actual));
                    }
                } else {
                    const double actual = result->get_double_at(column);
                    const double expected = DoubleValue(index);
                    if (!SameDoubleBits(actual, expected)) {
                        mismatch.Add("device " + device + " row " +
                                     std::to_string(r) + " " +
                                     measurement_name(m) + " expected " +
                                     DoubleBitsText(expected) + " actual " +
                                     DoubleBitsText(actual));
                    }
                }
            }
            ++rows;
        }
        EXPECT_EQ(kRows, rows) << "device " << device;
        total_rows += rows;
        reader.destroy_query_data_set(result);
    }
    ASSERT_EQ(common::E_OK, reader.close());
    mismatch.ExpectNone("case02");
    const auto elapsed_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - started)
            .count();
    std::cout << "[TSFILE168][case02] rows=" << total_rows
              << " file_bytes=" << FileSize(path)
              << " elapsed_ms=" << elapsed_ms << std::endl;
}

// ---------------------------------------------------------------------------
// Case 3: aligned and non-aligned devices in the same file.
// ---------------------------------------------------------------------------
TEST_F(AlpEncodingTest, MixedAlignedAndNonAligned) {
    const int kAligned = 10;
    const int kPlain = 10;
    const int kMeasurements = 10;
    const int kRows = 1000;
    const std::string path = NewFile("case03_mixed.tsfile");

    auto measurement_name = [](int m) { return "m" + Pad2(m); };
    auto measurement_type = [](int m) {
        return m % 2 == 0 ? common::FLOAT : common::DOUBLE;
    };
    {
        TsFileWriter writer;
        ASSERT_EQ(common::E_OK,
                  writer.open(path, O_RDWR | O_CREAT | O_TRUNC, 0666));
        for (int d = 0; d < kAligned; ++d) {
            std::vector<MeasurementSchema*> schemas;
            for (int m = 0; m < kMeasurements; ++m) {
                schemas.push_back(new MeasurementSchema(
                    measurement_name(m), measurement_type(m), common::ALP,
                    common::LZ4));
            }
            ASSERT_EQ(common::E_OK,
                      writer.register_aligned_timeseries(
                          "root.alp.aligned" + Pad2(d), schemas));
        }
        for (int d = 0; d < kPlain; ++d) {
            const std::string device = "root.alp.plain" + Pad2(d);
            for (int m = 0; m < kMeasurements; ++m) {
                ASSERT_EQ(common::E_OK, writer.register_timeseries(
                                            device,
                                            MeasurementSchema(
                                                measurement_name(m),
                                                measurement_type(m),
                                                common::ALP, common::LZ4)));
            }
        }
        for (int d = 0; d < kAligned; ++d) {
            const std::string device = "root.alp.aligned" + Pad2(d);
            for (int r = 0; r < kRows; ++r) {
                TsRecord record(kBaseTime + r, device);
                for (int m = 0; m < kMeasurements; ++m) {
                    const int64_t index =
                        static_cast<int64_t>(r) * kMeasurements + m;
                    if (m % 2 == 0) {
                        record.add_point(measurement_name(m),
                                         FloatValue(index));
                    } else {
                        record.add_point(measurement_name(m),
                                         DoubleValue(index));
                    }
                }
                ASSERT_EQ(common::E_OK, writer.write_record_aligned(record));
            }
        }
        for (int d = 0; d < kPlain; ++d) {
            const std::string device = "root.alp.plain" + Pad2(d);
            for (int r = 0; r < kRows; ++r) {
                TsRecord record(kBaseTime + r, device);
                for (int m = 0; m < kMeasurements; ++m) {
                    const int64_t index =
                        static_cast<int64_t>(r) * kMeasurements + m;
                    if (m % 2 == 0) {
                        record.add_point(measurement_name(m),
                                         FloatValue(index));
                    } else {
                        record.add_point(measurement_name(m),
                                         DoubleValue(index));
                    }
                }
                ASSERT_EQ(common::E_OK, writer.write_record(record));
            }
        }
        ASSERT_EQ(common::E_OK, writer.close());
    }

    TsFileReader reader;
    ASSERT_EQ(common::E_OK, reader.open(path));
    Mismatch mismatch;
    for (int group = 0; group < 2; ++group) {
        for (int d = 0; d < (group == 0 ? kAligned : kPlain); ++d) {
            const std::string device =
                (group == 0 ? "root.alp.aligned" : "root.alp.plain") + Pad2(d);
            std::vector<std::string> paths;
            for (int m = 0; m < kMeasurements; ++m) {
                paths.push_back(device + "." + measurement_name(m));
            }
            ResultSet* result = nullptr;
            ASSERT_EQ(common::E_OK,
                      reader.query(paths, kBaseTime, kBaseTime + kRows - 1,
                                   result));
            ASSERT_NE(nullptr, result);
            int rows = 0;
            bool has_next = false;
            while (IS_SUCC(result->next(has_next)) && has_next) {
                const int r =
                    static_cast<int>(result->get_int64_at(1) - kBaseTime);
                if (r < 0 || r >= kRows) {
                    mismatch.Add("device " + device + " bad timestamp");
                    break;
                }
                for (int m = 0; m < kMeasurements; ++m) {
                    const uint32_t column = static_cast<uint32_t>(m + 2);
                    const int64_t index =
                        static_cast<int64_t>(r) * kMeasurements + m;
                    if (m % 2 == 0) {
                        if (!SameFloatBits(result->get_float_at(column),
                                           FloatValue(index))) {
                            mismatch.Add("device " + device + " row " +
                                         std::to_string(r) + " " +
                                         measurement_name(m));
                        }
                    } else {
                        if (!SameDoubleBits(result->get_double_at(column),
                                            DoubleValue(index))) {
                            mismatch.Add("device " + device + " row " +
                                         std::to_string(r) + " " +
                                         measurement_name(m));
                        }
                    }
                }
                ++rows;
            }
            EXPECT_EQ(kRows, rows) << "device " << device;
            reader.destroy_query_data_set(result);
        }
    }
    ASSERT_EQ(common::E_OK, reader.close());
    mismatch.ExpectNone("case03");
    std::cout << "[TSFILE168][case03] devices=" << (kAligned + kPlain)
              << " file_bytes=" << FileSize(path) << std::endl;
}

// ---------------------------------------------------------------------------
// Case 4: ALP x five compression algorithms (tree model).
// ---------------------------------------------------------------------------
TEST_F(AlpEncodingTest, CompressionMatrix) {
    const int kRows = 10000;
    const std::string path = NewFile("case04_compression_matrix.tsfile");
    struct CompressionCase {
        common::CompressionType type;
        const char* name;
    };
    const CompressionCase compressions[] = {
        {common::UNCOMPRESSED, "uncompressed"},
        {common::SNAPPY, "snappy"},
        {common::LZ4, "lz4"},
        {common::GZIP, "gzip"},
        {common::ZSTD, "zstd"}};

    {
        TsFileWriter writer;
        ASSERT_EQ(common::E_OK,
                  writer.open(path, O_RDWR | O_CREAT | O_TRUNC, 0666));
        for (const CompressionCase& item : compressions) {
            const std::string device =
                std::string("root.alp.comp_") + item.name;
            ASSERT_EQ(common::E_OK,
                      writer.register_timeseries(
                          device, MeasurementSchema("f", common::FLOAT,
                                                    common::ALP, item.type)));
            ASSERT_EQ(common::E_OK,
                      writer.register_timeseries(
                          device, MeasurementSchema("d", common::DOUBLE,
                                                    common::ALP, item.type)));
        }
        for (const CompressionCase& item : compressions) {
            const std::string device =
                std::string("root.alp.comp_") + item.name;
            for (int r = 0; r < kRows; ++r) {
                TsRecord record(kBaseTime + r, device);
                record.add_point("f", DecimalFloat(r));
                record.add_point("d", DecimalDouble(r));
                ASSERT_EQ(common::E_OK, writer.write_record(record));
            }
        }
        ASSERT_EQ(common::E_OK, writer.close());
    }

    TsFileReader reader;
    ASSERT_EQ(common::E_OK, reader.open(path));
    Mismatch mismatch;
    for (const CompressionCase& item : compressions) {
        const std::string device =
            std::string("root.alp.comp_") + item.name;
        std::vector<std::string> paths = {device + ".f", device + ".d"};
        ResultSet* result = nullptr;
        ASSERT_EQ(common::E_OK,
                  reader.query(paths, kBaseTime, kBaseTime + kRows - 1,
                               result));
        ASSERT_NE(nullptr, result);
        int rows = 0;
        bool has_next = false;
        while (IS_SUCC(result->next(has_next)) && has_next) {
            const int r =
                static_cast<int>(result->get_int64_at(1) - kBaseTime);
            if (r < 0 || r >= kRows) {
                mismatch.Add(std::string(item.name) + " bad timestamp");
                break;
            }
            if (!SameFloatBits(result->get_float_at(2), DecimalFloat(r))) {
                mismatch.Add(std::string(item.name) + " float row " +
                             std::to_string(r));
            }
            if (!SameDoubleBits(result->get_double_at(3), DecimalDouble(r))) {
                mismatch.Add(std::string(item.name) + " double row " +
                             std::to_string(r));
            }
            ++rows;
        }
        EXPECT_EQ(kRows, rows) << item.name;
        reader.destroy_query_data_set(result);
    }
    ASSERT_EQ(common::E_OK, reader.close());
    mismatch.ExpectNone("case04");
    std::cout << "[TSFILE168][case04] compressions=5 rows_each=" << kRows
              << " file_bytes=" << FileSize(path) << std::endl;
}

// ---------------------------------------------------------------------------
// Case 5: block boundary row counts 1023/1024/1025/2048/2049.
// ---------------------------------------------------------------------------
TEST_F(AlpEncodingTest, BlockBoundaryRows) {
    const int kCounts[] = {1023, 1024, 1025, 2048, 2049};
    const std::string path = NewFile("case05_block_boundary.tsfile");

    {
        TsFileWriter writer;
        ASSERT_EQ(common::E_OK,
                  writer.open(path, O_RDWR | O_CREAT | O_TRUNC, 0666));
        for (int count : kCounts) {
            const std::string device =
                "root.alp.boundary" + std::to_string(count);
            ASSERT_EQ(common::E_OK,
                      writer.register_timeseries(
                          device, MeasurementSchema("f", common::FLOAT,
                                                    common::ALP,
                                                    common::UNCOMPRESSED)));
            ASSERT_EQ(common::E_OK,
                      writer.register_timeseries(
                          device, MeasurementSchema("d", common::DOUBLE,
                                                    common::ALP,
                                                    common::UNCOMPRESSED)));
        }
        for (int count : kCounts) {
            const std::string device =
                "root.alp.boundary" + std::to_string(count);
            for (int r = 0; r < count; ++r) {
                TsRecord record(kBaseTime + r, device);
                record.add_point("f", DecimalFloat(r));
                record.add_point("d", DecimalDouble(r));
                ASSERT_EQ(common::E_OK, writer.write_record(record));
            }
        }
        ASSERT_EQ(common::E_OK, writer.close());
    }

    TsFileReader reader;
    ASSERT_EQ(common::E_OK, reader.open(path));
    Mismatch mismatch;
    for (int count : kCounts) {
        const std::string device =
            "root.alp.boundary" + std::to_string(count);
        std::vector<std::string> paths = {device + ".f", device + ".d"};
        ResultSet* result = nullptr;
        ASSERT_EQ(common::E_OK,
                  reader.query(paths, kBaseTime, kBaseTime + count - 1,
                               result));
        ASSERT_NE(nullptr, result);
        int rows = 0;
        bool has_next = false;
        while (IS_SUCC(result->next(has_next)) && has_next) {
            const int r =
                static_cast<int>(result->get_int64_at(1) - kBaseTime);
            if (r < 0 || r >= count) {
                mismatch.Add("count " + std::to_string(count) +
                             " bad timestamp");
                break;
            }
            if (!SameFloatBits(result->get_float_at(2), DecimalFloat(r))) {
                mismatch.Add("count " + std::to_string(count) + " float row " +
                             std::to_string(r));
            }
            if (!SameDoubleBits(result->get_double_at(3), DecimalDouble(r))) {
                mismatch.Add("count " + std::to_string(count) +
                             " double row " + std::to_string(r));
            }
            ++rows;
        }
        EXPECT_EQ(count, rows) << "count " << count;
        reader.destroy_query_data_set(result);
    }
    ASSERT_EQ(common::E_OK, reader.close());
    mismatch.ExpectNone("case05");
    std::cout << "[TSFILE168][case05] counts=5 file_bytes=" << FileSize(path)
              << std::endl;
}

// ---------------------------------------------------------------------------
// Case 6: 50k rows across pages and 5 explicit chunk groups.
// ---------------------------------------------------------------------------
TEST_F(AlpEncodingTest, MultiPageAndChunkGroups) {
    const int kRows = 50000;
    const int kFlushEvery = 10000;
    const std::string path = NewFile("case06_multi_page_chunks.tsfile");

    {
        TsFileWriter writer;
        ASSERT_EQ(common::E_OK,
                  writer.open(path, O_RDWR | O_CREAT | O_TRUNC, 0666));
        ASSERT_EQ(common::E_OK,
                  writer.register_timeseries(
                      "root.alp.pages",
                      MeasurementSchema("d", common::DOUBLE, common::ALP,
                                        common::LZ4)));
        for (int r = 0; r < kRows; ++r) {
            TsRecord record(kBaseTime + r, "root.alp.pages");
            record.add_point("d", static_cast<double>(r) * 0.5 - 12500.0);
            ASSERT_EQ(common::E_OK, writer.write_record(record));
            if ((r + 1) % kFlushEvery == 0) {
                ASSERT_EQ(common::E_OK, writer.flush());
            }
        }
        ASSERT_EQ(common::E_OK, writer.close());
    }

    TsFileReader reader;
    ASSERT_EQ(common::E_OK, reader.open(path));
    std::vector<std::string> paths = {"root.alp.pages.d"};
    ResultSet* result = nullptr;
    ASSERT_EQ(common::E_OK,
              reader.query(paths, kBaseTime, kBaseTime + kRows - 1, result));
    ASSERT_NE(nullptr, result);
    Mismatch mismatch;
    int rows = 0;
    bool has_next = false;
    while (IS_SUCC(result->next(has_next)) && has_next) {
        const int r = static_cast<int>(result->get_int64_at(1) - kBaseTime);
        if (r != rows) {
            mismatch.Add("row " + std::to_string(rows) + " timestamp " +
                         std::to_string(result->get_int64_at(1)));
        }
        const double expected = static_cast<double>(rows) * 0.5 - 12500.0;
        if (!SameDoubleBits(result->get_double_at(2), expected)) {
            mismatch.Add("row " + std::to_string(rows) + " value");
        }
        ++rows;
    }
    EXPECT_EQ(kRows, rows);
    reader.destroy_query_data_set(result);
    ASSERT_EQ(common::E_OK, reader.close());
    mismatch.ExpectNone("case06");
    std::cout << "[TSFILE168][case06] rows=" << rows
              << " file_bytes=" << FileSize(path) << std::endl;
}

// ---------------------------------------------------------------------------
// Case 7: special values bit-exact (file layer and codec layer).
// ---------------------------------------------------------------------------
TEST_F(AlpEncodingTest, SpecialValuesBitExact) {
    std::vector<float> float_values;
    float_values.push_back(std::numeric_limits<float>::quiet_NaN());
    float_values.push_back(-std::numeric_limits<float>::quiet_NaN());
    float_values.push_back(std::numeric_limits<float>::infinity());
    float_values.push_back(-std::numeric_limits<float>::infinity());
    float_values.push_back(0.0f);
    float_values.push_back(-0.0f);
    float_values.push_back(std::numeric_limits<float>::denorm_min());
    float_values.push_back(-std::numeric_limits<float>::denorm_min());
    float_values.push_back(std::numeric_limits<float>::min());
    float_values.push_back(-std::numeric_limits<float>::min());
    float_values.push_back(std::numeric_limits<float>::max());
    float_values.push_back(-std::numeric_limits<float>::max());
    float_values.push_back(FloatBits(0x7FC12345u));
    float_values.push_back(FloatBits(0xFFC56789u));
    for (int i = 0; i < 50; ++i) {
        float_values.push_back(static_cast<float>(i) * 0.25f - 6.25f);
    }

    std::vector<double> double_values;
    double_values.push_back(std::numeric_limits<double>::quiet_NaN());
    double_values.push_back(-std::numeric_limits<double>::quiet_NaN());
    double_values.push_back(std::numeric_limits<double>::infinity());
    double_values.push_back(-std::numeric_limits<double>::infinity());
    double_values.push_back(0.0);
    double_values.push_back(-0.0);
    double_values.push_back(std::numeric_limits<double>::denorm_min());
    double_values.push_back(-std::numeric_limits<double>::denorm_min());
    double_values.push_back(std::numeric_limits<double>::min());
    double_values.push_back(-std::numeric_limits<double>::min());
    double_values.push_back(std::numeric_limits<double>::max());
    double_values.push_back(-std::numeric_limits<double>::max());
    double_values.push_back(DoubleBits(0x7FF8123456789ABCu));
    double_values.push_back(DoubleBits(0xFFF8123456789ABCu));
    for (int i = 0; i < 50; ++i) {
        double_values.push_back(static_cast<double>(i) * 0.5 - 12.5);
    }

    ASSERT_EQ(float_values.size(), double_values.size());
    const std::string path = NewFile("case07_special_values.tsfile");
    {
        TsFileWriter writer;
        ASSERT_EQ(common::E_OK,
                  writer.open(path, O_RDWR | O_CREAT | O_TRUNC, 0666));
        ASSERT_EQ(common::E_OK,
                  writer.register_timeseries(
                      "root.alp.special",
                      MeasurementSchema("f", common::FLOAT, common::ALP,
                                        common::UNCOMPRESSED)));
        ASSERT_EQ(common::E_OK,
                  writer.register_timeseries(
                      "root.alp.special",
                      MeasurementSchema("d", common::DOUBLE, common::ALP,
                                        common::UNCOMPRESSED)));
        for (size_t r = 0; r < float_values.size(); ++r) {
            TsRecord record(kBaseTime + static_cast<int64_t>(r),
                            "root.alp.special");
            record.add_point("f", float_values[r]);
            record.add_point("d", double_values[r]);
            ASSERT_EQ(common::E_OK, writer.write_record(record));
        }
        ASSERT_EQ(common::E_OK, writer.close());
    }

    Mismatch mismatch;
    {
        TsFileReader reader;
        ASSERT_EQ(common::E_OK, reader.open(path));
        std::vector<std::string> paths = {"root.alp.special.f",
                                          "root.alp.special.d"};
        ResultSet* result = nullptr;
        ASSERT_EQ(common::E_OK,
                  reader.query(paths, kBaseTime,
                               kBaseTime + static_cast<int64_t>(
                                               float_values.size()) - 1,
                               result));
        ASSERT_NE(nullptr, result);
        int rows = 0;
        bool has_next = false;
        while (IS_SUCC(result->next(has_next)) && has_next) {
            const int r =
                static_cast<int>(result->get_int64_at(1) - kBaseTime);
            if (r < 0 || r >= static_cast<int>(float_values.size())) {
                mismatch.Add("file layer bad timestamp");
                break;
            }
            const float actual_f = result->get_float_at(2);
            const double actual_d = result->get_double_at(3);
            if (!SameFloatBits(actual_f, float_values[r])) {
                mismatch.Add("file layer float row " + std::to_string(r) +
                             " expected " +
                             FloatBitsText(float_values[r]) + " actual " +
                             FloatBitsText(actual_f));
            }
            if (!SameDoubleBits(actual_d, double_values[r])) {
                mismatch.Add("file layer double row " + std::to_string(r) +
                             " expected " +
                             DoubleBitsText(double_values[r]) + " actual " +
                             DoubleBitsText(actual_d));
            }
            ++rows;
        }
        EXPECT_EQ(static_cast<int>(float_values.size()), rows);
        reader.destroy_query_data_set(result);
        ASSERT_EQ(common::E_OK, reader.close());
    }

    // Codec layer: a page must round-trip the same bit patterns.
    {
        std::vector<uint8_t> encoded;
        ASSERT_EQ(alp::ALP_OK,
                  alp::AlpEncodePage(float_values.data(),
                                     static_cast<uint32_t>(
                                         float_values.size()),
                                     encoded));
        std::vector<float> decoded;
        ASSERT_EQ(alp::ALP_OK,
                  alp::AlpDecodePage(encoded.data(),
                                     static_cast<uint32_t>(encoded.size()),
                                     decoded));
        ASSERT_EQ(float_values.size(), decoded.size());
        for (size_t i = 0; i < float_values.size(); ++i) {
            if (!SameFloatBits(decoded[i], float_values[i])) {
                mismatch.Add("codec layer float index " + std::to_string(i) +
                             " expected " + FloatBitsText(float_values[i]) +
                             " actual " + FloatBitsText(decoded[i]));
            }
        }
    }
    {
        std::vector<uint8_t> encoded;
        ASSERT_EQ(alp::ALP_OK,
                  alp::AlpEncodePage(double_values.data(),
                                     static_cast<uint32_t>(
                                         double_values.size()),
                                     encoded));
        std::vector<double> decoded;
        ASSERT_EQ(alp::ALP_OK,
                  alp::AlpDecodePage(encoded.data(),
                                     static_cast<uint32_t>(encoded.size()),
                                     decoded));
        ASSERT_EQ(double_values.size(), decoded.size());
        for (size_t i = 0; i < double_values.size(); ++i) {
            if (!SameDoubleBits(decoded[i], double_values[i])) {
                mismatch.Add("codec layer double index " + std::to_string(i) +
                             " expected " +
                             DoubleBitsText(double_values[i]) + " actual " +
                             DoubleBitsText(decoded[i]));
            }
        }
    }
    mismatch.ExpectNone("case07");
    std::cout << "[TSFILE168][case07] special_values="
              << float_values.size() << " file_bytes=" << FileSize(path)
              << std::endl;
}

// ---------------------------------------------------------------------------
// Case 8: high entropy falls back to the PLAIN scheme.
// ---------------------------------------------------------------------------
TEST_F(AlpEncodingTest, HighEntropyFallsBackToPlain) {
    Mismatch mismatch;

    auto check_page = [&mismatch](const char* label,
                                  const std::vector<double>& values,
                                  bool require_plain) {
        std::vector<uint8_t> encoded;
        const int status = alp::AlpEncodePage(
            values.data(), static_cast<uint32_t>(values.size()), encoded);
        ASSERT_EQ(alp::ALP_OK, status) << label;
        uint32_t pos = 0;
        alp::AlpPageHeader page_header;
        ASSERT_TRUE(alp::AlpParsePageHeader(encoded.data(),
                                            static_cast<uint32_t>(
                                                encoded.size()),
                                            pos, page_header))
            << label;
        alp::AlpBlockHeader block_header;
        ASSERT_TRUE(alp::AlpParseBlockHeader(encoded.data(),
                                             static_cast<uint32_t>(
                                                 encoded.size()),
                                             pos, block_header))
            << label;
        if (require_plain) {
            EXPECT_EQ(alp::ALP_SCHEME_PLAIN, block_header.scheme)
                << label << " scheme=" << static_cast<int>(block_header.scheme);
        }
        std::vector<double> decoded;
        ASSERT_EQ(alp::ALP_OK,
                  alp::AlpDecodePage(encoded.data(),
                                     static_cast<uint32_t>(encoded.size()),
                                     decoded))
            << label;
        ASSERT_EQ(values.size(), decoded.size()) << label;
        for (size_t i = 0; i < values.size(); ++i) {
            if (!SameDoubleBits(values[i], decoded[i])) {
                mismatch.Add(std::string(label) + " index " +
                             std::to_string(i));
            }
        }
        std::cout << "[TSFILE168][case08] " << label
                  << " scheme=" << static_cast<int>(block_header.scheme)
                  << " bytes=" << encoded.size()
                  << " exception_count=" << block_header.exception_count
                  << std::endl;
    };

    {
        std::vector<double> values(1024);
        uint64_t state = 0x9e3779b97f4a7c15ULL;
        for (size_t i = 0; i < values.size(); ++i) {
            state = state * 6364136223846793005ULL + 1442695040888963407ULL;
            const uint64_t bits = state ^ (state >> 29);
            std::memcpy(&values[i], &bits, sizeof(double));
            if (!std::isfinite(values[i])) {
                values[i] = static_cast<double>(i);
            }
        }
        check_page("double_finite", values, true);
    }
    {
        std::vector<double> values(1024);
        uint64_t state = 0x123456789abcdef0ULL;
        for (size_t i = 0; i < values.size(); ++i) {
            state = state * 6364136223846793005ULL + 1442695040888963407ULL;
            const uint64_t bits = state ^ (state >> 29);
            std::memcpy(&values[i], &bits, sizeof(double));
        }
        check_page("double_raw_bits", values, true);
    }
    {
        std::vector<float> float_values(1000);
        uint64_t state = 0x0f1e2d3c4b5a6978ULL;
        for (size_t i = 0; i < float_values.size(); ++i) {
            state = state * 6364136223846793005ULL + 1442695040888963407ULL;
            const uint32_t bits = static_cast<uint32_t>(state ^ (state >> 33));
            std::memcpy(&float_values[i], &bits, sizeof(float));
            if (!std::isfinite(float_values[i])) {
                float_values[i] = static_cast<float>(i);
            }
        }
        std::vector<uint8_t> encoded;
        ASSERT_EQ(alp::ALP_OK,
                  alp::AlpEncodePage(float_values.data(),
                                     static_cast<uint32_t>(
                                         float_values.size()),
                                     encoded));
        uint32_t pos = 0;
        alp::AlpPageHeader page_header;
        ASSERT_TRUE(alp::AlpParsePageHeader(encoded.data(),
                                            static_cast<uint32_t>(
                                                encoded.size()),
                                            pos, page_header));
        alp::AlpBlockHeader block_header;
        ASSERT_TRUE(alp::AlpParseBlockHeader(encoded.data(),
                                             static_cast<uint32_t>(
                                                 encoded.size()),
                                             pos, block_header));
        EXPECT_EQ(alp::ALP_SCHEME_PLAIN, block_header.scheme);
        std::vector<float> decoded;
        ASSERT_EQ(alp::ALP_OK,
                  alp::AlpDecodePage(encoded.data(),
                                     static_cast<uint32_t>(encoded.size()),
                                     decoded));
        ASSERT_EQ(float_values.size(), decoded.size());
        for (size_t i = 0; i < float_values.size(); ++i) {
            if (!SameFloatBits(float_values[i], decoded[i])) {
                mismatch.Add("float_finite index " + std::to_string(i));
            }
        }
        std::cout << "[TSFILE168][case08] float_finite scheme="
                  << static_cast<int>(block_header.scheme)
                  << " bytes=" << encoded.size()
                  << " exception_count=" << block_header.exception_count
                  << std::endl;
    }
    mismatch.ExpectNone("case08");
}

// ---------------------------------------------------------------------------
// Case 9: table model, 2 tables x (10 tags + 1000 fields) x 10k rows.
// ---------------------------------------------------------------------------
TEST_F(AlpEncodingTest, TableWideLargeScale) {
    const int kTables = 2;
    const int kTags = 10;
    const int kFloatFields = 500;
    const int kDoubleFields = 500;
    const int kFields = kFloatFields + kDoubleFields;
    const int kRows = 10000;
    const int kBatch = 500;
    const std::string path = NewFile("case09_table_wide.tsfile");

    auto table_name = [](int t) { return "t168_wide_" + std::string(1, 'a' + t); };
    auto tag_name = [](int t) { return "tag_" + Pad2(t); };
    auto field_name = [](int f) {
        return (f < 500 ? std::string("f_") + Pad3(f)
                        : std::string("d_") + Pad3(f - 500));
    };
    auto field_type = [](int f) {
        return f < 500 ? common::FLOAT : common::DOUBLE;
    };
    auto float_field = [](int r, int f) {
        return static_cast<float>((r + f) % 2000) * 0.25f - 250.0f;
    };
    auto double_field = [](int r, int f) {
        return static_cast<double>((r * 3 + f) % 4000) * 0.5 - 1000.0;
    };

    std::vector<std::shared_ptr<TableSchema>> schemas;
    for (int t = 0; t < kTables; ++t) {
        std::vector<common::ColumnSchema> columns;
        for (int c = 0; c < kTags; ++c) {
            columns.emplace_back(tag_name(c), common::STRING, common::LZ4,
                                 common::PLAIN, common::ColumnCategory::TAG);
        }
        for (int f = 0; f < kFields; ++f) {
            columns.emplace_back(field_name(f), field_type(f), common::LZ4,
                                 common::ALP, common::ColumnCategory::FIELD);
        }
        schemas.push_back(std::make_shared<TableSchema>(table_name(t),
                                                        columns));
    }

    auto started = std::chrono::steady_clock::now();
    {
        WriteFile write_file;
        ASSERT_EQ(common::E_OK,
                  write_file.create(path, O_WRONLY | O_CREAT | O_TRUNC, 0666));
        TsFileTableWriter writer(&write_file, schemas[0].get());
        ASSERT_EQ(common::E_OK, writer.get_error());
        for (int t = 1; t < kTables; ++t) {
            ASSERT_EQ(common::E_OK, writer.register_table(schemas[t]));
        }
        for (int t = 0; t < kTables; ++t) {
            TableSchema& schema = *schemas[t];
            for (int start = 0; start < kRows; start += kBatch) {
                const int end = std::min(start + kBatch, kRows);
                Tablet tablet(schema.get_table_name(),
                              schema.get_measurement_names(),
                              schema.get_data_types(),
                              schema.get_column_categories(), end - start);
                for (int r = start; r < end; ++r) {
                    const uint32_t row = static_cast<uint32_t>(r - start);
                    ASSERT_EQ(common::E_OK,
                              tablet.add_timestamp(row, kBaseTime + r));
                    for (int c = 0; c < kTags; ++c) {
                        ASSERT_EQ(common::E_OK,
                                  tablet.add_value(
                                      row, static_cast<uint32_t>(c),
                                      common::String("tagv_" +
                                                     std::to_string(c))));
                    }
                    if (r % 7 == 0) {
                        continue;  // both field groups are NULL
                    }
                    for (int f = 0; f < kFields; ++f) {
                        const uint32_t column =
                            static_cast<uint32_t>(kTags + f);
                        if (f < kFloatFields) {
                            ASSERT_EQ(common::E_OK,
                                      tablet.add_value(row, column,
                                                       float_field(r, f)));
                        } else {
                            ASSERT_EQ(common::E_OK,
                                      tablet.add_value(row, column,
                                                       double_field(r, f)));
                        }
                    }
                }
                ASSERT_EQ(common::E_OK, writer.write_table(tablet));
            }
        }
        ASSERT_EQ(common::E_OK, writer.close());
    }

    TsFileReader reader;
    ASSERT_EQ(common::E_OK, reader.open(path));
    Mismatch mismatch;
    int64_t compared_values = 0;
    for (int t = 0; t < kTables; ++t) {
        std::vector<std::string> tag_columns;
        std::vector<std::string> field_columns;
        for (int c = 0; c < kTags; ++c) tag_columns.push_back(tag_name(c));
        for (int f = 0; f < kFields; ++f) field_columns.push_back(field_name(f));

        ResultSet* tag_result = nullptr;
        ASSERT_EQ(common::E_OK,
                  reader.query(table_name(t), tag_columns, kBaseTime,
                               kBaseTime + kRows - 1, tag_result));
        ASSERT_NE(nullptr, tag_result);
        ResultSet* field_result = nullptr;
        ASSERT_EQ(common::E_OK,
                  reader.query(table_name(t), field_columns, kBaseTime,
                               kBaseTime + kRows - 1, field_result));
        ASSERT_NE(nullptr, field_result);

        int rows = 0;
        bool tag_has_next = false;
        bool field_has_next = false;
        while (true) {
            const int tag_ret = tag_result->next(tag_has_next);
            const int field_ret = field_result->next(field_has_next);
            if (!IS_SUCC(tag_ret) || !IS_SUCC(field_ret)) {
                mismatch.Add("table query failed");
                break;
            }
            if (!tag_has_next || !field_has_next) {
                EXPECT_EQ(tag_has_next, field_has_next)
                    << "table " << table_name(t) << " row count differs";
                break;
            }
            const int r =
                static_cast<int>(field_result->get_int64_at(1) - kBaseTime);
            if (r < 0 || r >= kRows) {
                mismatch.Add("table " + table_name(t) + " bad timestamp");
                break;
            }
            for (int c = 0; c < kTags; ++c) {
                common::String* actual =
                    tag_result->get_value<common::String*>(c + 2);
                const std::string expected = "tagv_" + std::to_string(c);
                if (actual == nullptr ||
                    actual->to_std_string() != expected) {
                    mismatch.Add("table " + table_name(t) + " row " +
                                 std::to_string(r) + " tag " +
                                 tag_name(c));
                }
            }
            const bool expect_null = (r % 7 == 0);
            for (int f = 0; f < kFields; ++f) {
                const uint32_t column = static_cast<uint32_t>(f + 2);
                if (field_result->is_null(column) != expect_null) {
                    mismatch.Add("table " + table_name(t) + " row " +
                                 std::to_string(r) + " field " +
                                 field_name(f) + " null mismatch");
                    continue;
                }
                if (expect_null) {
                    continue;
                }
                ++compared_values;
                if (f < kFloatFields) {
                    const float actual = field_result->get_float_at(column);
                    const float expected = float_field(r, f);
                    if (!SameFloatBits(actual, expected)) {
                        mismatch.Add("table " + table_name(t) + " row " +
                                     std::to_string(r) + " field " +
                                     field_name(f) + " expected " +
                                     FloatBitsText(expected) + " actual " +
                                     FloatBitsText(actual));
                    }
                } else {
                    const double actual = field_result->get_double_at(column);
                    const double expected = double_field(r, f);
                    if (!SameDoubleBits(actual, expected)) {
                        mismatch.Add("table " + table_name(t) + " row " +
                                     std::to_string(r) + " field " +
                                     field_name(f) + " expected " +
                                     DoubleBitsText(expected) + " actual " +
                                     DoubleBitsText(actual));
                    }
                }
            }
            ++rows;
        }
        EXPECT_EQ(kRows, rows) << "table " << table_name(t);
        reader.destroy_query_data_set(tag_result);
        reader.destroy_query_data_set(field_result);
    }
    ASSERT_EQ(common::E_OK, reader.close());
    mismatch.ExpectNone("case09");
    const auto elapsed_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - started)
            .count();
    std::cout << "[TSFILE168][case09] tables=" << kTables
              << " rows_per_table=" << kRows
              << " compared_values=" << compared_values
              << " file_bytes=" << FileSize(path)
              << " elapsed_ms=" << elapsed_ms << std::endl;
}

// ---------------------------------------------------------------------------
// Case 10: table model ALP x five compression algorithms.
// ---------------------------------------------------------------------------
TEST_F(AlpEncodingTest, TableCompressionMatrix) {
    const int kRows = 5000;
    const int kBatch = 500;
    const std::string path = NewFile("case10_table_compression.tsfile");
    struct CompressionCase {
        common::CompressionType type;
        const char* name;
    };
    const CompressionCase compressions[] = {
        {common::UNCOMPRESSED, "uncompressed"},
        {common::SNAPPY, "snappy"},
        {common::LZ4, "lz4"},
        {common::GZIP, "gzip"},
        {common::ZSTD, "zstd"}};
    const std::vector<std::string> columns = {"dev", "f1", "f2", "d1", "d2"};

    std::vector<std::shared_ptr<TableSchema>> schemas;
    for (const CompressionCase& item : compressions) {
        std::vector<common::ColumnSchema> column_schemas;
        column_schemas.emplace_back("dev", common::STRING,
                                    common::UNCOMPRESSED, common::PLAIN,
                                    common::ColumnCategory::TAG);
        for (int i = 0; i < 2; ++i) {
            column_schemas.emplace_back("f" + std::to_string(i + 1),
                                        common::FLOAT, item.type, common::ALP,
                                        common::ColumnCategory::FIELD);
        }
        for (int i = 0; i < 2; ++i) {
            column_schemas.emplace_back("d" + std::to_string(i + 1),
                                        common::DOUBLE, item.type, common::ALP,
                                        common::ColumnCategory::FIELD);
        }
        schemas.push_back(std::make_shared<TableSchema>(
            "t168_comp_" + std::string(item.name), column_schemas));
    }

    {
        WriteFile write_file;
        ASSERT_EQ(common::E_OK,
                  write_file.create(path, O_WRONLY | O_CREAT | O_TRUNC, 0666));
        TsFileTableWriter writer(&write_file, schemas[0].get());
        ASSERT_EQ(common::E_OK, writer.get_error());
        for (size_t i = 1; i < schemas.size(); ++i) {
            ASSERT_EQ(common::E_OK, writer.register_table(schemas[i]));
        }
        for (size_t i = 0; i < schemas.size(); ++i) {
            TableSchema& schema = *schemas[i];
            for (int start = 0; start < kRows; start += kBatch) {
                const int end = std::min(start + kBatch, kRows);
                Tablet tablet(schema.get_table_name(),
                              schema.get_measurement_names(),
                              schema.get_data_types(),
                              schema.get_column_categories(), end - start);
                for (int r = start; r < end; ++r) {
                    const uint32_t row = static_cast<uint32_t>(r - start);
                    ASSERT_EQ(common::E_OK,
                              tablet.add_timestamp(row, kBaseTime + r));
                    ASSERT_EQ(common::E_OK,
                              tablet.add_value(row, 0,
                                               common::String("devA")));
                    if (r % 7 == 0) {
                        continue;
                    }
                    ASSERT_EQ(common::E_OK,
                              tablet.add_value(row, 1,
                                               static_cast<float>(r % 997) *
                                                       0.5f -
                                                   250.0f));
                    ASSERT_EQ(common::E_OK,
                              tablet.add_value(row, 2,
                                               static_cast<float>(r % 991) *
                                                       0.25f -
                                                   125.0f));
                    ASSERT_EQ(common::E_OK,
                              tablet.add_value(row, 3,
                                               static_cast<double>(r % 2000) *
                                                       0.5 -
                                                   500.0));
                    ASSERT_EQ(common::E_OK,
                              tablet.add_value(row, 4,
                                               static_cast<double>(r % 1999) *
                                                       0.25 -
                                                   250.0));
                }
                ASSERT_EQ(common::E_OK, writer.write_table(tablet));
            }
        }
        ASSERT_EQ(common::E_OK, writer.close());
    }

    auto expected_f1 = [](int r) {
        return static_cast<float>(r % 997) * 0.5f - 250.0f;
    };
    auto expected_f2 = [](int r) {
        return static_cast<float>(r % 991) * 0.25f - 125.0f;
    };
    auto expected_d1 = [](int r) {
        return static_cast<double>(r % 2000) * 0.5 - 500.0;
    };
    auto expected_d2 = [](int r) {
        return static_cast<double>(r % 1999) * 0.25 - 250.0;
    };

    TsFileReader reader;
    ASSERT_EQ(common::E_OK, reader.open(path));
    Mismatch mismatch;
    for (const CompressionCase& item : compressions) {
        const std::string table = "t168_comp_" + std::string(item.name);
        ResultSet* result = nullptr;
        ASSERT_EQ(common::E_OK,
                  reader.query(table, columns, kBaseTime,
                               kBaseTime + kRows - 1, result));
        ASSERT_NE(nullptr, result);
        int rows = 0;
        bool has_next = false;
        while (IS_SUCC(result->next(has_next)) && has_next) {
            const int r =
                static_cast<int>(result->get_int64_at(1) - kBaseTime);
            if (r < 0 || r >= kRows) {
                mismatch.Add(table + " bad timestamp");
                break;
            }
            common::String* tag = result->get_value<common::String*>(2);
            if (tag == nullptr || tag->to_std_string() != "devA") {
                mismatch.Add(table + " row " + std::to_string(r) + " tag");
            }
            const bool expect_null = (r % 7 == 0);
            for (int c = 3; c <= 6; ++c) {
                if (result->is_null(static_cast<uint32_t>(c)) !=
                    expect_null) {
                    mismatch.Add(table + " row " + std::to_string(r) +
                                 " column " + std::to_string(c) +
                                 " null mismatch");
                }
            }
            if (!expect_null) {
                if (!SameFloatBits(result->get_float_at(3), expected_f1(r))) {
                    mismatch.Add(table + " row " + std::to_string(r) + " f1");
                }
                if (!SameFloatBits(result->get_float_at(4), expected_f2(r))) {
                    mismatch.Add(table + " row " + std::to_string(r) + " f2");
                }
                if (!SameDoubleBits(result->get_double_at(5),
                                    expected_d1(r))) {
                    mismatch.Add(table + " row " + std::to_string(r) + " d1");
                }
                if (!SameDoubleBits(result->get_double_at(6),
                                    expected_d2(r))) {
                    mismatch.Add(table + " row " + std::to_string(r) + " d2");
                }
            }
            ++rows;
        }
        EXPECT_EQ(kRows, rows) << table;
        reader.destroy_query_data_set(result);
    }
    ASSERT_EQ(common::E_OK, reader.close());
    mismatch.ExpectNone("case10");
    std::cout << "[TSFILE168][case10] tables=5 rows_each=" << kRows
              << " file_bytes=" << FileSize(path) << std::endl;
}

// ---------------------------------------------------------------------------
// Case 11: table model, 20 devices x 20 fields x 2000 rows, flush per 500.
// ---------------------------------------------------------------------------
TEST_F(AlpEncodingTest, TableMultiDeviceMultiChunk) {
    const int kDevices = 20;
    const int kFields = 20;
    const int kRows = 2000;
    const int kBatch = 500;
    const std::string table = "t168_multi";
    const std::string path = NewFile("case11_table_multi.tsfile");

    std::vector<common::ColumnSchema> column_schemas;
    column_schemas.emplace_back("device_id", common::STRING, common::LZ4,
                                common::PLAIN,
                                common::ColumnCategory::TAG);
    for (int f = 0; f < kFields; ++f) {
        column_schemas.emplace_back("m" + Pad2(f), common::DOUBLE,
                                    common::LZ4, common::ALP,
                                    common::ColumnCategory::FIELD);
    }
    TableSchema schema(table, column_schemas);

    auto device_tag = [](int d) { return "dev" + Pad2(d); };
    auto field_value = [](int d, int r, int f) {
        return static_cast<double>((d * 131 + r * 7 + f) % 3000) * 0.25 - 375.0;
    };

    {
        WriteFile write_file;
        ASSERT_EQ(common::E_OK,
                  write_file.create(path, O_WRONLY | O_CREAT | O_TRUNC, 0666));
        TsFileTableWriter writer(&write_file, &schema);
        ASSERT_EQ(common::E_OK, writer.get_error());
        for (int d = 0; d < kDevices; ++d) {
            for (int start = 0; start < kRows; start += kBatch) {
                const int end = std::min(start + kBatch, kRows);
                Tablet tablet(schema.get_table_name(),
                              schema.get_measurement_names(),
                              schema.get_data_types(),
                              schema.get_column_categories(), end - start);
                for (int r = start; r < end; ++r) {
                    const uint32_t row = static_cast<uint32_t>(r - start);
                    ASSERT_EQ(common::E_OK,
                              tablet.add_timestamp(row, kBaseTime + r));
                    ASSERT_EQ(common::E_OK,
                              tablet.add_value(row, 0,
                                               common::String(device_tag(d))));
                    for (int f = 0; f < kFields; ++f) {
                        ASSERT_EQ(common::E_OK,
                                  tablet.add_value(
                                      row, static_cast<uint32_t>(f + 1),
                                      field_value(d, r, f)));
                    }
                }
                ASSERT_EQ(common::E_OK, writer.write_table(tablet));
                ASSERT_EQ(common::E_OK, writer.flush());
            }
        }
        ASSERT_EQ(common::E_OK, writer.close());
    }

    std::vector<std::string> columns;
    columns.push_back("device_id");
    for (int f = 0; f < kFields; ++f) columns.push_back("m" + Pad2(f));

    TsFileReader reader;
    ASSERT_EQ(common::E_OK, reader.open(path));
    ResultSet* result = nullptr;
    ASSERT_EQ(common::E_OK,
              reader.query(table, columns, kBaseTime, kBaseTime + kRows - 1,
                           result));
    ASSERT_NE(nullptr, result);
    Mismatch mismatch;
    int rows = 0;
    bool has_next = false;
    while (IS_SUCC(result->next(has_next)) && has_next) {
        const int r = static_cast<int>(result->get_int64_at(1) - kBaseTime);
        common::String* tag = result->get_value<common::String*>(2);
        if (r < 0 || r >= kRows || tag == nullptr) {
            mismatch.Add("bad row/tag");
            continue;
        }
        const std::string tag_value = tag->to_std_string();
        if (tag_value.size() != 5 || tag_value.compare(0, 3, "dev") != 0) {
            mismatch.Add("bad tag " + tag_value);
            continue;
        }
        const int d = std::stoi(tag_value.substr(3));
        if (d < 0 || d >= kDevices) {
            mismatch.Add("bad device index " + tag_value);
            continue;
        }
        for (int f = 0; f < kFields; ++f) {
            const uint32_t column = static_cast<uint32_t>(f + 3);
            const double actual = result->get_double_at(column);
            const double expected = field_value(d, r, f);
            if (!SameDoubleBits(actual, expected)) {
                mismatch.Add("device " + tag_value + " row " +
                             std::to_string(r) + " field " + Pad2(f));
            }
        }
        ++rows;
    }
    EXPECT_EQ(kDevices * kRows, rows);
    reader.destroy_query_data_set(result);
    ASSERT_EQ(common::E_OK, reader.close());
    mismatch.ExpectNone("case11");
    std::cout << "[TSFILE168][case11] devices=" << kDevices
              << " rows=" << rows << " file_bytes=" << FileSize(path)
              << std::endl;
}

// ---------------------------------------------------------------------------
// Case 12: table ALP data read through query() and queryByRow().
// ---------------------------------------------------------------------------
TEST_F(AlpEncodingTest, TableQueryByRowConsistency) {
    const int kRows = 2000;
    const std::string table = "t168_row";
    const std::string path = NewFile("case12_row_query.tsfile");

    std::vector<common::ColumnSchema> column_schemas;
    column_schemas.emplace_back("dev", common::STRING, common::LZ4,
                                common::PLAIN, common::ColumnCategory::TAG);
    column_schemas.emplace_back("f", common::FLOAT, common::LZ4, common::ALP,
                                common::ColumnCategory::FIELD);
    column_schemas.emplace_back("d", common::DOUBLE, common::LZ4, common::ALP,
                                common::ColumnCategory::FIELD);
    TableSchema schema(table, column_schemas);
    const std::vector<std::string> columns = {"dev", "f", "d"};

    {
        WriteFile write_file;
        ASSERT_EQ(common::E_OK,
                  write_file.create(path, O_WRONLY | O_CREAT | O_TRUNC, 0666));
        TsFileTableWriter writer(&write_file, &schema);
        ASSERT_EQ(common::E_OK, writer.get_error());
        Tablet tablet(schema.get_table_name(), schema.get_measurement_names(),
                      schema.get_data_types(), schema.get_column_categories(),
                      kRows);
        for (int r = 0; r < kRows; ++r) {
            const uint32_t row = static_cast<uint32_t>(r);
            ASSERT_EQ(common::E_OK,
                      tablet.add_timestamp(row, kBaseTime + r));
            ASSERT_EQ(common::E_OK,
                      tablet.add_value(row, 0, common::String("devA")));
            ASSERT_EQ(common::E_OK,
                      tablet.add_value(row, 1, DecimalFloat(r)));
            ASSERT_EQ(common::E_OK,
                      tablet.add_value(row, 2, DecimalDouble(r)));
        }
        ASSERT_EQ(common::E_OK, writer.write_table(tablet));
        ASSERT_EQ(common::E_OK, writer.close());
    }

    auto collect = [&](ResultSet* result, std::vector<int64_t>& times,
                       std::vector<float>& floats,
                       std::vector<double>& doubles) {
        bool has_next = false;
        while (IS_SUCC(result->next(has_next)) && has_next) {
            times.push_back(result->get_int64_at(1));
            floats.push_back(result->get_float_at(3));
            doubles.push_back(result->get_double_at(4));
        }
    };

    TsFileReader reader;
    ASSERT_EQ(common::E_OK, reader.open(path));
    std::vector<int64_t> range_times;
    std::vector<float> range_floats;
    std::vector<double> range_doubles;
    {
        ResultSet* result = nullptr;
        ASSERT_EQ(common::E_OK,
                  reader.query(table, columns, kBaseTime,
                               kBaseTime + kRows - 1, result));
        ASSERT_NE(nullptr, result);
        collect(result, range_times, range_floats, range_doubles);
        reader.destroy_query_data_set(result);
    }
    std::vector<int64_t> row_times;
    std::vector<float> row_floats;
    std::vector<double> row_doubles;
    {
        ResultSet* result = nullptr;
        ASSERT_EQ(common::E_OK,
                  reader.queryByRow(table, columns, 0, -1, result));
        ASSERT_NE(nullptr, result);
        collect(result, row_times, row_floats, row_doubles);
        reader.destroy_query_data_set(result);
    }

    Mismatch mismatch;
    if (range_times.size() != static_cast<size_t>(kRows)) {
        mismatch.Add("query() rows=" + std::to_string(range_times.size()));
    }
    if (row_times.size() != static_cast<size_t>(kRows)) {
        mismatch.Add("queryByRow() rows=" + std::to_string(row_times.size()));
    }
    const size_t compared = std::min(range_times.size(), row_times.size());
    for (size_t i = 0; i < compared; ++i) {
        if (range_times[i] != row_times[i]) {
            mismatch.Add("row " + std::to_string(i) + " timestamp");
        }
        if (!SameFloatBits(range_floats[i], row_floats[i])) {
            mismatch.Add("row " + std::to_string(i) + " float");
        }
        if (!SameDoubleBits(range_doubles[i], row_doubles[i])) {
            mismatch.Add("row " + std::to_string(i) + " double");
        }
        if (!SameFloatBits(range_floats[i],
                           DecimalFloat(range_times[i] - kBaseTime))) {
            mismatch.Add("row " + std::to_string(i) + " float vs expected");
        }
        if (!SameDoubleBits(range_doubles[i],
                            DecimalDouble(range_times[i] - kBaseTime))) {
            mismatch.Add("row " + std::to_string(i) + " double vs expected");
        }
    }

    // Offset/limit pushdown must return the matching slice.
    {
        ResultSet* result = nullptr;
        ASSERT_EQ(common::E_OK,
                  reader.queryByRow(table, columns, 100, 500, result));
        ASSERT_NE(nullptr, result);
        int index = 0;
        bool has_next = false;
        while (IS_SUCC(result->next(has_next)) && has_next) {
            const int64_t expected_time = kBaseTime + 100 + index;
            if (result->get_int64_at(1) != expected_time) {
                mismatch.Add("offset slice row " + std::to_string(index) +
                             " timestamp");
            }
            if (!SameFloatBits(result->get_float_at(3),
                               DecimalFloat(100 + index))) {
                mismatch.Add("offset slice row " + std::to_string(index) +
                             " float");
            }
            ++index;
        }
        EXPECT_EQ(500, index);
        reader.destroy_query_data_set(result);
    }
    ASSERT_EQ(common::E_OK, reader.close());
    mismatch.ExpectNone("case12");
    std::cout << "[TSFILE168][case12] rows=" << compared
              << " file_bytes=" << FileSize(path) << std::endl;
}

// ---------------------------------------------------------------------------
// Case 13: encoding recorded for explicit ALP, default and GORILLA columns.
// ---------------------------------------------------------------------------
TEST_F(AlpEncodingTest, EncodingRecorded) {
    const int kRows = 100;
    const std::string device = "root.alp.enc_record";
    const std::string path = NewFile("case13_encoding_record.tsfile");

    {
        TsFileWriter writer;
        ASSERT_EQ(common::E_OK,
                  writer.open(path, O_RDWR | O_CREAT | O_TRUNC, 0666));
        ASSERT_EQ(common::E_OK,
                  writer.register_timeseries(
                      device, MeasurementSchema("f_alp", common::FLOAT,
                                                common::ALP,
                                                common::UNCOMPRESSED)));
        ASSERT_EQ(common::E_OK,
                  writer.register_timeseries(
                      device, MeasurementSchema("f_default",
                                                common::FLOAT)));
        ASSERT_EQ(common::E_OK,
                  writer.register_timeseries(
                      device, MeasurementSchema("f_gorilla", common::FLOAT,
                                                common::GORILLA,
                                                common::UNCOMPRESSED)));
        for (int r = 0; r < kRows; ++r) {
            TsRecord record(kBaseTime + r, device);
            record.add_point("f_alp", DecimalFloat(r));
            record.add_point("f_default", DecimalFloat(r));
            record.add_point("f_gorilla", DecimalFloat(r));
            ASSERT_EQ(common::E_OK, writer.write_record(record));
        }
        ASSERT_EQ(common::E_OK, writer.close());
    }

    TsFileReader reader;
    ASSERT_EQ(common::E_OK, reader.open(path));
    std::vector<MeasurementSchema> schemas;
    ASSERT_EQ(common::E_OK,
              reader.get_timeseries_schema(
                  std::make_shared<StringArrayDeviceID>(device), schemas));
    ASSERT_EQ(3u, schemas.size());
    bool saw_alp = false;
    bool saw_default = false;
    bool saw_gorilla = false;
    for (const MeasurementSchema& schema : schemas) {
        if (schema.measurement_name_ == "f_alp") {
            saw_alp = true;
            EXPECT_EQ(common::ALP, schema.encoding_);
            EXPECT_EQ(common::FLOAT, schema.data_type_);
        } else if (schema.measurement_name_ == "f_default") {
            saw_default = true;
            EXPECT_EQ(common::GORILLA, schema.encoding_);
        } else if (schema.measurement_name_ == "f_gorilla") {
            saw_gorilla = true;
            EXPECT_EQ(common::GORILLA, schema.encoding_);
        }
    }
    EXPECT_TRUE(saw_alp);
    EXPECT_TRUE(saw_default);
    EXPECT_TRUE(saw_gorilla);
    EXPECT_EQ(15, static_cast<int>(common::ALP));
    EXPECT_EQ(8, static_cast<int>(common::GORILLA));

    std::vector<std::string> paths = {device + ".f_alp",
                                      device + ".f_default",
                                      device + ".f_gorilla"};
    ResultSet* result = nullptr;
    ASSERT_EQ(common::E_OK,
              reader.query(paths, kBaseTime, kBaseTime + kRows - 1, result));
    ASSERT_NE(nullptr, result);
    Mismatch mismatch;
    int rows = 0;
    bool has_next = false;
    while (IS_SUCC(result->next(has_next)) && has_next) {
        const int r = static_cast<int>(result->get_int64_at(1) - kBaseTime);
        for (int c = 0; c < 3; ++c) {
            if (!SameFloatBits(result->get_float_at(
                                   static_cast<uint32_t>(c + 2)),
                               DecimalFloat(r))) {
                mismatch.Add("row " + std::to_string(r) + " column " +
                             std::to_string(c));
            }
        }
        ++rows;
    }
    EXPECT_EQ(kRows, rows);
    reader.destroy_query_data_set(result);
    ASSERT_EQ(common::E_OK, reader.close());
    mismatch.ExpectNone("case13");
    std::cout << "[TSFILE168][case13] schemas=3 rows=" << rows
              << " file_bytes=" << FileSize(path) << std::endl;
}

// ---------------------------------------------------------------------------
// Case 26: a Python-written ALP fixture is read twice without side effects.
// ---------------------------------------------------------------------------
TEST_F(AlpEncodingTest, ReadsPythonFixtureTwice) {
    const char* env = std::getenv("TSFILE168_ALP_FIXTURE");
    const std::string fixture =
        (env != nullptr && env[0] != '\0')
            ? std::string(env)
            : std::string("fixtures/tsfile168/alp_python_written.tsfile");
    ASSERT_TRUE(FileExists(fixture)) << "fixture missing: " << fixture;

    const int kRows = 1000;
    const std::string device = "root.alp_py";

    auto read_once = [&](std::vector<float>& floats,
                         std::vector<double>& doubles) {
        TsFileReader reader;
        ASSERT_EQ(common::E_OK, reader.open(fixture));
        std::vector<std::string> paths = {device + ".f", device + ".d"};
        ResultSet* result = nullptr;
        ASSERT_EQ(common::E_OK,
                  reader.query(paths, 0, kRows - 1, result));
        ASSERT_NE(nullptr, result);
        bool has_next = false;
        while (IS_SUCC(result->next(has_next)) && has_next) {
            floats.push_back(result->get_float_at(2));
            doubles.push_back(result->get_double_at(3));
        }
        reader.destroy_query_data_set(result);
        ASSERT_EQ(common::E_OK, reader.close());
    };

    std::vector<float> first_floats;
    std::vector<double> first_doubles;
    std::vector<float> second_floats;
    std::vector<double> second_doubles;
    read_once(first_floats, first_doubles);
    read_once(second_floats, second_doubles);

    Mismatch mismatch;
    ASSERT_EQ(static_cast<size_t>(kRows), first_floats.size());
    ASSERT_EQ(static_cast<size_t>(kRows), second_floats.size());
    for (int r = 0; r < kRows; ++r) {
        const float expected_f = static_cast<float>(r % 1000) * 0.25f - 125.0f;
        const double expected_d = static_cast<double>(r % 2000) * 0.5 - 500.0;
        if (!SameFloatBits(first_floats[r], expected_f)) {
            mismatch.Add("first read float row " + std::to_string(r));
        }
        if (!SameDoubleBits(first_doubles[r], expected_d)) {
            mismatch.Add("first read double row " + std::to_string(r));
        }
        if (!SameFloatBits(first_floats[r], second_floats[r])) {
            mismatch.Add("two reads float row " + std::to_string(r));
        }
        if (!SameDoubleBits(first_doubles[r], second_doubles[r])) {
            mismatch.Add("two reads double row " + std::to_string(r));
        }
    }
    mismatch.ExpectNone("case26");
    std::cout << "[TSFILE168][case26] fixture=" << fixture
              << " rows=" << first_floats.size() << std::endl;
}

// ---------------------------------------------------------------------------
// Case 29: non-ASCII device path and measurement names (tree model).
// ---------------------------------------------------------------------------
TEST_F(AlpEncodingTest, NonAsciiDeviceAndMeasurement) {
    const int kRows = 1000;
    const std::string device = "root.设备_α_温度";
    const std::string float_name = "温度_01";
    const std::string double_name = "湿度_β";
    const std::string path = NewFile("case29_non_ascii_tree.tsfile");

    {
        TsFileWriter writer;
        ASSERT_EQ(common::E_OK,
                  writer.open(path, O_RDWR | O_CREAT | O_TRUNC, 0666));
        ASSERT_EQ(common::E_OK,
                  writer.register_timeseries(
                      device, MeasurementSchema(float_name, common::FLOAT,
                                                common::ALP,
                                                common::UNCOMPRESSED)));
        ASSERT_EQ(common::E_OK,
                  writer.register_timeseries(
                      device, MeasurementSchema(double_name, common::DOUBLE,
                                                common::ALP,
                                                common::UNCOMPRESSED)));
        for (int r = 0; r < kRows; ++r) {
            TsRecord record(kBaseTime + r, device);
            record.add_point(float_name, DecimalFloat(r));
            record.add_point(double_name, DecimalDouble(r));
            ASSERT_EQ(common::E_OK, writer.write_record(record));
        }
        ASSERT_EQ(common::E_OK, writer.close());
    }

    TsFileReader reader;
    ASSERT_EQ(common::E_OK, reader.open(path));
    std::vector<MeasurementSchema> schemas;
    ASSERT_EQ(common::E_OK,
              reader.get_timeseries_schema(
                  std::make_shared<StringArrayDeviceID>(device), schemas));
    ASSERT_EQ(2u, schemas.size());
    bool saw_float = false;
    bool saw_double = false;
    for (const MeasurementSchema& schema : schemas) {
        if (schema.measurement_name_ == float_name) {
            saw_float = true;
        }
        if (schema.measurement_name_ == double_name) {
            saw_double = true;
        }
    }
    EXPECT_TRUE(saw_float);
    EXPECT_TRUE(saw_double);

    std::vector<std::string> paths = {device + "." + float_name,
                                      device + "." + double_name};
    ResultSet* result = nullptr;
    ASSERT_EQ(common::E_OK,
              reader.query(paths, kBaseTime, kBaseTime + kRows - 1, result));
    ASSERT_NE(nullptr, result);
    auto metadata = result->get_metadata();
    ASSERT_EQ(3u, metadata->get_column_count());
    EXPECT_EQ("time", metadata->get_column_name(1));
    EXPECT_EQ(paths[0], metadata->get_column_name(2));
    EXPECT_EQ(paths[1], metadata->get_column_name(3));

    Mismatch mismatch;
    int rows = 0;
    bool has_next = false;
    while (IS_SUCC(result->next(has_next)) && has_next) {
        const int r = static_cast<int>(result->get_int64_at(1) - kBaseTime);
        if (!SameFloatBits(result->get_float_at(2), DecimalFloat(r))) {
            mismatch.Add("float row " + std::to_string(r));
        }
        if (!SameDoubleBits(result->get_double_at(3), DecimalDouble(r))) {
            mismatch.Add("double row " + std::to_string(r));
        }
        ++rows;
    }
    EXPECT_EQ(kRows, rows);
    reader.destroy_query_data_set(result);
    ASSERT_EQ(common::E_OK, reader.close());
    mismatch.ExpectNone("case29");
    std::cout << "[TSFILE168][case29] rows=" << rows
              << " file_bytes=" << FileSize(path) << std::endl;
}

// ---------------------------------------------------------------------------
// Case 30: non-ASCII table name, tag column and field columns (table model).
// ---------------------------------------------------------------------------
TEST_F(AlpEncodingTest, NonAsciiTableAndColumns) {
    const int kRows = 1000;
    const std::string table = "表_α_传感器";
    const std::string tag_name = "设备编号";
    const std::string float_name = "测量值_温度";
    const std::string double_name = "测量值_湿度";
    const std::string path = NewFile("case30_non_ascii_table.tsfile");

    std::vector<common::ColumnSchema> column_schemas;
    column_schemas.emplace_back(tag_name, common::STRING, common::UNCOMPRESSED,
                                common::PLAIN, common::ColumnCategory::TAG);
    column_schemas.emplace_back(float_name, common::FLOAT, common::UNCOMPRESSED,
                                common::ALP, common::ColumnCategory::FIELD);
    column_schemas.emplace_back(double_name, common::DOUBLE,
                                common::UNCOMPRESSED, common::ALP,
                                common::ColumnCategory::FIELD);
    TableSchema schema(table, column_schemas);
    const std::vector<std::string> columns = {tag_name, float_name,
                                              double_name};

    {
        WriteFile write_file;
        ASSERT_EQ(common::E_OK,
                  write_file.create(path, O_WRONLY | O_CREAT | O_TRUNC, 0666));
        TsFileTableWriter writer(&write_file, &schema);
        ASSERT_EQ(common::E_OK, writer.get_error());
        Tablet tablet(schema.get_table_name(), schema.get_measurement_names(),
                      schema.get_data_types(), schema.get_column_categories(),
                      kRows);
        for (int r = 0; r < kRows; ++r) {
            const uint32_t row = static_cast<uint32_t>(r);
            ASSERT_EQ(common::E_OK,
                      tablet.add_timestamp(row, kBaseTime + r));
            ASSERT_EQ(common::E_OK,
                      tablet.add_value(row, 0,
                                       common::String("设备A")));
            if (r % 11 == 0) {
                continue;  // both fields NULL
            }
            ASSERT_EQ(common::E_OK,
                      tablet.add_value(row, 1, DecimalFloat(r)));
            ASSERT_EQ(common::E_OK,
                      tablet.add_value(row, 2, DecimalDouble(r)));
        }
        ASSERT_EQ(common::E_OK, writer.write_table(tablet));
        ASSERT_EQ(common::E_OK, writer.close());
    }

    TsFileReader reader;
    ASSERT_EQ(common::E_OK, reader.open(path));
    ResultSet* result = nullptr;
    ASSERT_EQ(common::E_OK,
              reader.query(table, columns, kBaseTime, kBaseTime + kRows - 1,
                           result));
    ASSERT_NE(nullptr, result);
    auto metadata = result->get_metadata();
    ASSERT_EQ(4u, metadata->get_column_count());
    EXPECT_EQ("time", metadata->get_column_name(1));
    EXPECT_EQ(tag_name, metadata->get_column_name(2));
    EXPECT_EQ(float_name, metadata->get_column_name(3));
    EXPECT_EQ(double_name, metadata->get_column_name(4));

    Mismatch mismatch;
    int rows = 0;
    bool has_next = false;
    while (IS_SUCC(result->next(has_next)) && has_next) {
        const int r = static_cast<int>(result->get_int64_at(1) - kBaseTime);
        common::String* tag = result->get_value<common::String*>(2);
        if (tag == nullptr || tag->to_std_string() != "设备A") {
            mismatch.Add("row " + std::to_string(r) + " tag");
        }
        const bool expect_null = (r % 11 == 0);
        if (result->is_null(3) != expect_null ||
            result->is_null(4) != expect_null) {
            mismatch.Add("row " + std::to_string(r) + " null mismatch");
        } else if (!expect_null) {
            if (!SameFloatBits(result->get_float_at(3), DecimalFloat(r))) {
                mismatch.Add("row " + std::to_string(r) + " float");
            }
            if (!SameDoubleBits(result->get_double_at(4), DecimalDouble(r))) {
                mismatch.Add("row " + std::to_string(r) + " double");
            }
        }
        ++rows;
    }
    EXPECT_EQ(kRows, rows);
    reader.destroy_query_data_set(result);
    ASSERT_EQ(common::E_OK, reader.close());
    mismatch.ExpectNone("case30");
    std::cout << "[TSFILE168][case30] rows=" << rows
              << " file_bytes=" << FileSize(path) << std::endl;
}

// ---------------------------------------------------------------------------
// Case 18: read a Python-written ALP file from C++.
// ---------------------------------------------------------------------------
TEST_F(AlpEncodingTest, ReadsPythonWrittenAlp) {
    const char* env = std::getenv("TSFILE168_ALP_FIXTURE");
    const std::string fixture =
        (env != nullptr && env[0] != '\0')
            ? std::string(env)
            : std::string("fixtures/tsfile168/alp_python_written.tsfile");
    ASSERT_TRUE(FileExists(fixture)) << "fixture missing: " << fixture;

    const int kRows = 1000;
    const std::string device = "root.alp_py";
    TsFileReader reader;
    ASSERT_EQ(common::E_OK, reader.open(fixture));
    std::vector<std::string> paths = {device + ".f", device + ".d"};
    ResultSet* result = nullptr;
    ASSERT_EQ(common::E_OK, reader.query(paths, 0, kRows - 1, result));
    ASSERT_NE(nullptr, result);
    Mismatch mismatch;
    int rows = 0;
    bool has_next = false;
    while (IS_SUCC(result->next(has_next)) && has_next) {
        const int r = static_cast<int>(result->get_int64_at(1));
        if (r < 0 || r >= kRows) {
            mismatch.Add("bad timestamp row " + std::to_string(rows));
            break;
        }
        const float expected_f = static_cast<float>(r % 1000) * 0.25f - 125.0f;
        const double expected_d = static_cast<double>(r % 2000) * 0.5 - 500.0;
        if (!SameFloatBits(result->get_float_at(2), expected_f)) {
            mismatch.Add("float row " + std::to_string(r));
        }
        if (!SameDoubleBits(result->get_double_at(3), expected_d)) {
            mismatch.Add("double row " + std::to_string(r));
        }
        ++rows;
    }
    EXPECT_EQ(kRows, rows);
    reader.destroy_query_data_set(result);
    ASSERT_EQ(common::E_OK, reader.close());
    mismatch.ExpectNone("case18");
    std::cout << "[TSFILE168][case18] fixture=" << fixture << " rows=" << rows
              << std::endl;
}

// ---------------------------------------------------------------------------
// Java compatibility fixture (case 17): same device carries ALP and GORILLA
// columns so the Java side can prove "file opens, ALP series query fails".
// ---------------------------------------------------------------------------
TEST(AlpJavaFixtureTest, GenerateJavaCompatFixture) {
    libtsfile_init();
    const char* env = std::getenv("TSFILE168_JAVA_FIXTURE");
    const std::string path =
        (env != nullptr && env[0] != '\0')
            ? std::string(env)
            : std::string(
                  "/data/test/test-program-B/java-tsfile-api-test/data/"
                  "tsfile/tsfile168_alp_java_compat.tsfile");
    std::remove(path.c_str());
    const std::string device = "root.alp_java.dev";
    {
        TsFileWriter writer;
        ASSERT_EQ(common::E_OK,
                  writer.open(path, O_RDWR | O_CREAT | O_TRUNC, 0666));
        ASSERT_EQ(common::E_OK,
                  writer.register_timeseries(
                      device, MeasurementSchema("f_alp", common::FLOAT,
                                                common::ALP,
                                                common::UNCOMPRESSED)));
        ASSERT_EQ(common::E_OK,
                  writer.register_timeseries(
                      device, MeasurementSchema("f_gorilla", common::FLOAT,
                                                common::GORILLA,
                                                common::UNCOMPRESSED)));
        ASSERT_EQ(common::E_OK,
                  writer.register_timeseries(
                      device, MeasurementSchema("d_alp", common::DOUBLE,
                                                common::ALP,
                                                common::UNCOMPRESSED)));
        ASSERT_EQ(common::E_OK,
                  writer.register_timeseries(
                      device, MeasurementSchema("d_gorilla", common::DOUBLE,
                                                common::GORILLA,
                                                common::UNCOMPRESSED)));
        for (int r = 0; r < 100; ++r) {
            TsRecord record(kBaseTime + r, device);
            const float f = static_cast<float>(r % 100) * 0.25f - 12.5f;
            const double d = static_cast<double>(r) * 0.5 - 25.0;
            record.add_point("f_alp", f);
            record.add_point("f_gorilla", f);
            record.add_point("d_alp", d);
            record.add_point("d_gorilla", d);
            ASSERT_EQ(common::E_OK, writer.write_record(record));
        }
        ASSERT_EQ(common::E_OK, writer.close());
    }
    std::cout << "[TSFILE168][java-fixture] path=" << path
              << " size=" << FileSize(path) << std::endl;
    libtsfile_destroy();
}

}  // namespace
