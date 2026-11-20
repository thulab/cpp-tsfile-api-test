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

// TsFile-168 batch 4: C++ misuse paths (cases 19, 20, 28).

#include <gtest/gtest.h>

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

#include <fcntl.h>
#include <unistd.h>

#include "common/db_common.h"
#include "common/global.h"
#include "common/record.h"
#include "common/schema.h"
#include "common/tsfile_common.h"
#include "encoding/encoder_factory.h"
#include "reader/tsfile_reader.h"
#include "writer/tsfile_writer.h"

namespace {

using namespace storage;  // NOLINT

std::string WorkDir() {
    static const std::string dir = []() {
        char tmpl[] = "/tmp/tsfile168_err_XXXXXX";
        char* made = ::mkdtemp(tmpl);
        return std::string(made == nullptr ? "/tmp" : made);
    }();
    return dir;
}

const char* TypeName(common::TSDataType type) {
    switch (type) {
        case common::BOOLEAN:
            return "BOOLEAN";
        case common::INT32:
            return "INT32";
        case common::INT64:
            return "INT64";
        case common::STRING:
            return "STRING";
        case common::FLOAT:
            return "FLOAT";
        case common::DOUBLE:
            return "DOUBLE";
        default:
            return "OTHER";
    }
}

void AddTypedPoint(TsRecord& record, const std::string& name,
                   common::TSDataType type) {
    switch (type) {
        case common::INT32:
            record.add_point(name, static_cast<int32_t>(1));
            break;
        case common::INT64:
            record.add_point(name, static_cast<int64_t>(1));
            break;
        case common::BOOLEAN:
            record.add_point(name, true);
            break;
        case common::STRING:
            record.add_point(name, common::String("x"));
            break;
        case common::FLOAT:
            record.add_point(name, 1.0f);
            break;
        case common::DOUBLE:
            record.add_point(name, 1.0);
            break;
        default:
            break;
    }
}

class AlpErrorTest : public ::testing::Test {
   protected:
    void SetUp() override { libtsfile_init(); }
    void TearDown() override { libtsfile_destroy(); }
};

// Case 19: ALP registered on a non-float type.  Registration is accepted (the
// baseline contract question is still open); writing must fail visibly instead
// of corrupting data or crashing, and the encoder factory must return nullptr.
TEST_F(AlpErrorTest, NonFloatTypeWithAlpReportsError) {
    const common::TSDataType types[] = {common::INT32, common::INT64,
                                        common::BOOLEAN, common::STRING};
    for (common::TSDataType type : types) {
        const std::string path = WorkDir() + "/case19_" + TypeName(type) +
                                 ".tsfile";
        std::remove(path.c_str());
        TsFileWriter writer;
        ASSERT_EQ(common::E_OK,
                  writer.open(path, O_RDWR | O_CREAT | O_TRUNC, 0666));
        const int register_rc = writer.register_timeseries(
            "root.misuse",
            MeasurementSchema("m", type, common::ALP, common::UNCOMPRESSED));
        TsRecord record(0, "root.misuse");
        AddTypedPoint(record, "m", type);
        const int write_rc = writer.write_record(record);
        const int close_rc = writer.close();
        std::remove(path.c_str());

        EXPECT_EQ(common::E_OK, register_rc)
            << TypeName(type) << " registration unexpectedly failed";
        EXPECT_NE(common::E_OK, write_rc)
            << TypeName(type) << " ALP write silently succeeded";
        EXPECT_EQ(common::E_OK, close_rc);
        std::cout << "[TSFILE168][case19] type=" << TypeName(type)
                  << " register=" << register_rc << " write=" << write_rc
                  << " close=" << close_rc << std::endl;
    }

    for (common::TSDataType type : types) {
        Encoder* encoder =
            EncoderFactory::alloc_value_encoder(common::ALP, type);
        EXPECT_EQ(nullptr, encoder)
            << TypeName(type) << " unexpectedly has an ALP encoder";
        EncoderFactory::free(encoder);
    }
    Encoder* float_encoder =
        EncoderFactory::alloc_value_encoder(common::ALP, common::FLOAT);
    EXPECT_NE(nullptr, float_encoder);
    EncoderFactory::free(float_encoder);
    Encoder* double_encoder = EncoderFactory::alloc_value_encoder(
        common::ALP, common::DOUBLE);
    EXPECT_NE(nullptr, double_encoder);
    EncoderFactory::free(double_encoder);
}

// Case 28: record the registration-stage behaviour for non-float types.
TEST_F(AlpErrorTest, RegisterNonFloatAlp) {
    const common::TSDataType types[] = {common::INT32, common::INT64,
                                        common::BOOLEAN, common::STRING};
    for (common::TSDataType type : types) {
        const std::string path = WorkDir() + "/case28_" + TypeName(type) +
                                 ".tsfile";
        std::remove(path.c_str());
        TsFileWriter writer;
        ASSERT_EQ(common::E_OK,
                  writer.open(path, O_RDWR | O_CREAT | O_TRUNC, 0666));
        const int rc = writer.register_timeseries(
            "root.misuse",
            MeasurementSchema("m", type, common::ALP, common::UNCOMPRESSED));
        ASSERT_EQ(common::E_OK, writer.close());
        std::remove(path.c_str());
        EXPECT_EQ(common::E_OK, rc)
            << TypeName(type) << " registration returned " << rc;
        std::cout << "[TSFILE168][case28] type=" << TypeName(type)
                  << " register=" << rc << std::endl;
    }
}

// Case 20: invalid global encodings are rejected and do not pollute config.
TEST_F(AlpErrorTest, InvalidGlobalEncodingRejected) {
    const uint8_t before = common::get_datatype_encoding(common::FLOAT);
    const int bitmap_rc =
        common::set_datatype_encoding(common::FLOAT, common::BITMAP);
    const uint8_t after_bitmap =
        common::get_datatype_encoding(common::FLOAT);
    const int dictionary_rc =
        common::set_datatype_encoding(common::FLOAT, common::DICTIONARY);
    const uint8_t after_dictionary =
        common::get_datatype_encoding(common::FLOAT);
    const int alp_rc =
        common::set_datatype_encoding(common::FLOAT, common::ALP);
    const uint8_t after_alp = common::get_datatype_encoding(common::FLOAT);
    const int restore_rc = common::set_datatype_encoding(
        common::FLOAT, static_cast<common::TSEncoding>(before));
    const uint8_t after_restore =
        common::get_datatype_encoding(common::FLOAT);

    EXPECT_EQ(common::E_NOT_SUPPORT, bitmap_rc);
    EXPECT_EQ(before, after_bitmap);
    EXPECT_EQ(common::E_NOT_SUPPORT, dictionary_rc);
    EXPECT_EQ(before, after_dictionary);
    EXPECT_EQ(common::E_OK, alp_rc);
    EXPECT_EQ(static_cast<uint8_t>(common::ALP), after_alp);
    EXPECT_EQ(common::E_OK, restore_rc);
    EXPECT_EQ(before, after_restore);

    std::cout << "[TSFILE168][case20] bitmap=" << bitmap_rc
              << " dictionary=" << dictionary_rc << " alp=" << alp_rc
              << " before=" << static_cast<int>(before)
              << " after_bitmap=" << static_cast<int>(after_bitmap)
              << " after_alp=" << static_cast<int>(after_alp)
              << " after_restore=" << static_cast<int>(after_restore)
              << std::endl;
    // Note: an out-of-range encoding id (e.g. 99) trips the debug ASSERT in
    // common::set_datatype_encoding (compiled out with NDEBUG); it is not
    // exercised in-process because this test binary keeps assertions enabled.
}

}  // namespace
