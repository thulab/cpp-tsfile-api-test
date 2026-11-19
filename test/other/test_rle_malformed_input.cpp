// TsFile-196 CVE-2026-115639 regression cases for cpp-tsfile-api-test.
#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <vector>

#include "encoding/float_rle_decoder.h"
#include "encoding/int32_rle_decoder.h"
#include "encoding/int32_rle_encoder.h"
#include "encoding/int64_rle_decoder.h"
#include "encoding/int64_rle_encoder.h"

namespace {

void append_var_uint(std::vector<uint8_t>& out, uint32_t value) {
    while (value & 0xFFFFFF80u) {
        out.push_back(static_cast<uint8_t>((value & 0x7F) | 0x80));
        value >>= 7;
    }
    out.push_back(static_cast<uint8_t>(value));
}

std::vector<char> make_rle_pattern(uint8_t bit_width,
                                   const std::vector<uint8_t>& body) {
    std::vector<uint8_t> pattern;
    append_var_uint(pattern, static_cast<uint32_t>(body.size() + 1));
    pattern.push_back(bit_width);
    pattern.insert(pattern.end(), body.begin(), body.end());
    return std::vector<char>(pattern.begin(), pattern.end());
}

struct Case {
    const char* name;
    std::vector<char> pattern;
    bool partial_read;  // expected E_PARTIAL_READ instead of E_TSFILE_CORRUPTED
};

std::vector<Case> malformed_cases() {
    std::vector<Case> cases;
    {
        std::vector<uint8_t> body;
        append_var_uint(body, (1u << 1) | 1u);
        body.push_back(0);
        body.insert(body.end(), 8, 0);
        cases.push_back({"last0", make_rle_pattern(8, body), false});
    }
    {
        std::vector<uint8_t> body;
        append_var_uint(body, (1u << 1) | 1u);
        body.push_back(9);
        body.insert(body.end(), 8, 0);
        cases.push_back({"last9", make_rle_pattern(8, body), false});
    }
    {
        std::vector<uint8_t> body;
        append_var_uint(body, (1u << 1) | 1u);
        body.push_back(255);
        body.insert(body.end(), 8, 0);
        cases.push_back({"last255", make_rle_pattern(8, body), false});
    }
    {
        std::vector<uint8_t> body;
        append_var_uint(body, (100u << 1) | 1u);
        body.push_back(8);
        body.insert(body.end(), 8, 0);
        cases.push_back({"group_over", make_rle_pattern(8, body), true});
    }
    {
        std::vector<uint8_t> body;
        append_var_uint(body, (0x40000000u << 1) | 1u);
        body.push_back(8);
        body.insert(body.end(), 8, 0);
        cases.push_back({"value_overflow", make_rle_pattern(8, body), false});
    }
    {
        std::vector<uint8_t> body;
        append_var_uint(body, (0x01010101u << 1) | 1u);
        body.push_back(8);
        body.insert(body.end(), 16, 0);
        cases.push_back({"width_wrap", make_rle_pattern(255, body), false});
    }
    {
        std::vector<uint8_t> body;
        append_var_uint(body, (1u << 1) | 1u);
        body.push_back(8);
        body.insert(body.end(), 8, 0);
        cases.push_back({"width_zero", make_rle_pattern(0, body), false});
    }
    {
        std::vector<char> truncated;
        truncated.push_back(32);
        truncated.push_back(8);
        truncated.push_back(3);
        truncated.push_back(8);
        truncated.push_back(0);
        cases.push_back({"truncated", truncated, true});
    }
    return cases;
}

int decode_int32_scalar(const std::vector<char>& p) {
    common::ByteStream in;
    in.wrap_from(const_cast<char*>(p.data()), static_cast<int32_t>(p.size()));
    storage::Int32RleDecoder d;
    d.reset();
    for (int i = 0; i < 64; ++i) {
        if (!d.has_remaining(in)) break;
        int32_t v = 0;
        int ret = d.read_int32(v, in);
        if (ret != common::E_OK) return ret;
    }
    return common::E_OK;
}

int decode_int32_batch(const std::vector<char>& p) {
    common::ByteStream in;
    in.wrap_from(const_cast<char*>(p.data()), static_cast<int32_t>(p.size()));
    storage::Int32RleDecoder d;
    d.reset();
    int32_t buf[8];
    for (int i = 0; i < 16; ++i) {
        if (!d.has_remaining(in)) break;
        int actual = 0;
        int ret = d.read_batch_int32(buf, 8, actual, in);
        if (ret != common::E_OK) return ret;
        if (actual == 0) break;
    }
    return common::E_OK;
}

int decode_int64_scalar(const std::vector<char>& p) {
    common::ByteStream in;
    in.wrap_from(const_cast<char*>(p.data()), static_cast<int32_t>(p.size()));
    storage::Int64RleDecoder d;
    d.reset();
    for (int i = 0; i < 64; ++i) {
        if (!d.has_remaining(in)) break;
        int64_t v = 0;
        int ret = d.read_int64(v, in);
        if (ret != common::E_OK) return ret;
    }
    return common::E_OK;
}

int decode_int64_batch(const std::vector<char>& p) {
    common::ByteStream in;
    in.wrap_from(const_cast<char*>(p.data()), static_cast<int32_t>(p.size()));
    storage::Int64RleDecoder d;
    d.reset();
    int64_t buf[8];
    for (int i = 0; i < 16; ++i) {
        if (!d.has_remaining(in)) break;
        int actual = 0;
        int ret = d.read_batch_int64(buf, 8, actual, in);
        if (ret != common::E_OK) return ret;
        if (actual == 0) break;
    }
    return common::E_OK;
}

int decode_float_scalar(const std::vector<char>& p) {
    common::ByteStream in;
    in.wrap_from(const_cast<char*>(p.data()), static_cast<int32_t>(p.size()));
    storage::FloatRLEDecoder d;
    d.reset();
    for (int i = 0; i < 64; ++i) {
        if (!d.has_remaining(in)) break;
        float v = 0;
        int ret = d.read_float(v, in);
        if (ret != common::E_OK) return ret;
    }
    return common::E_OK;
}

int decode_double_scalar(const std::vector<char>& p) {
    common::ByteStream in;
    in.wrap_from(const_cast<char*>(p.data()), static_cast<int32_t>(p.size()));
    storage::DoubleRLEDecoder d;
    d.reset();
    for (int i = 0; i < 64; ++i) {
        if (!d.has_remaining(in)) break;
        double v = 0;
        int ret = d.read_double(v, in);
        if (ret != common::E_OK) return ret;
    }
    return common::E_OK;
}

bool roundtrip_int32(int n) {
    common::ByteStream out(1024, common::MOD_ENCODER_OBJ);
    storage::Int32RleEncoder enc;
    for (int i = 0; i < n; ++i) enc.encode(i * 7 + 1, out);
    enc.flush(out);
    char* bytes = common::get_bytes_from_bytestream(out);
    const int32_t size = static_cast<int32_t>(out.total_size());
    common::ByteStream in;
    in.wrap_from(bytes, size);
    storage::Int32RleDecoder dec;
    int32_t v = 0;
    std::vector<int32_t> got;
    while (dec.has_remaining(in)) {
        if (dec.read_int32(v, in) != common::E_OK) {
            common::mem_free(bytes);
            return false;
        }
        got.push_back(v);
    }
    common::mem_free(bytes);
    if (static_cast<int>(got.size()) != n) return false;
    for (int i = 0; i < n; ++i) {
        if (got[static_cast<size_t>(i)] != i * 7 + 1) return false;
    }
    return true;
}

bool roundtrip_int64(int n) {
    common::ByteStream out(1024, common::MOD_ENCODER_OBJ);
    storage::Int64RleEncoder enc;
    for (int i = 0; i < n; ++i) {
        int64_t value = static_cast<int64_t>(i) * 1000000007LL + 3;
        enc.encode(value, out);
    }
    enc.flush(out);
    char* bytes = common::get_bytes_from_bytestream(out);
    const int32_t size = static_cast<int32_t>(out.total_size());
    common::ByteStream in;
    in.wrap_from(bytes, size);
    storage::Int64RleDecoder dec;
    int64_t v = 0;
    std::vector<int64_t> got;
    while (dec.has_remaining(in)) {
        if (dec.read_int64(v, in) != common::E_OK) {
            common::mem_free(bytes);
            return false;
        }
        got.push_back(v);
    }
    common::mem_free(bytes);
    if (static_cast<int>(got.size()) != n) return false;
    for (int i = 0; i < n; ++i) {
        if (got[static_cast<size_t>(i)] !=
            static_cast<int64_t>(i) * 1000000007LL + 3) {
            return false;
        }
    }
    return true;
}

}  // namespace

TEST(RleMalformedInputTest, Int32MalformedScalarRejected) {
    for (const auto& c : malformed_cases()) {
        const int ret = decode_int32_scalar(c.pattern);
        const int expected =
            c.partial_read ? common::E_PARTIAL_READ : common::E_TSFILE_CORRUPTED;
        EXPECT_EQ(ret, expected) << "case=" << c.name;
    }
}

TEST(RleMalformedInputTest, Int32MalformedBatchRejected) {
    for (const auto& c : malformed_cases()) {
        const int ret = decode_int32_batch(c.pattern);
        const int expected =
            c.partial_read ? common::E_PARTIAL_READ : common::E_TSFILE_CORRUPTED;
        EXPECT_EQ(ret, expected) << "case=" << c.name;
    }
}

TEST(RleMalformedInputTest, Int64MalformedScalarRejected) {
    for (const auto& c : malformed_cases()) {
        const int ret = decode_int64_scalar(c.pattern);
        const int expected =
            c.partial_read ? common::E_PARTIAL_READ : common::E_TSFILE_CORRUPTED;
        EXPECT_EQ(ret, expected) << "case=" << c.name;
    }
}

TEST(RleMalformedInputTest, Int64MalformedBatchRejected) {
    for (const auto& c : malformed_cases()) {
        const int ret = decode_int64_batch(c.pattern);
        const int expected =
            c.partial_read ? common::E_PARTIAL_READ : common::E_TSFILE_CORRUPTED;
        EXPECT_EQ(ret, expected) << "case=" << c.name;
    }
}

TEST(RleMalformedInputTest, FloatDoubleMalformedRejected) {
    for (const auto& c : malformed_cases()) {
        const int rf = decode_float_scalar(c.pattern);
        const int rd = decode_double_scalar(c.pattern);
        // float/double reuse the int decoders; assert the observed per-case
        // error code instead of a generic non-OK check.
        int expected = common::E_PARTIAL_READ;
        if (std::strcmp(c.name, "last9") == 0 ||
            std::strcmp(c.name, "last255") == 0 ||
            std::strcmp(c.name, "group_over") == 0 ||
            std::strcmp(c.name, "value_overflow") == 0) {
            expected = common::E_TSFILE_CORRUPTED;
        } else if (std::strcmp(c.name, "last0") == 0) {
            expected = common::E_DECODE_ERR;
        }
        EXPECT_EQ(rf, expected) << "float case=" << c.name;
        EXPECT_EQ(rd, expected) << "double case=" << c.name;
    }
}

TEST(RleMalformedInputTest, ValidRleRoundTripStillWorks) {
    for (int n = 1; n <= 8; ++n) {
        EXPECT_TRUE(roundtrip_int32(n)) << "n=" << n;
        EXPECT_TRUE(roundtrip_int64(n)) << "n=" << n;
    }
}
