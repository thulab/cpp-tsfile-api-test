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

// RN-146 测试支撑：自建小夹具，消除对外部夹具目录 / /tmp 批次目录的依赖。
//
// 夹具在系统临时目录现写（每进程一个 mkdtemp 目录，幂等复用），内容与验证期基线同构：
//   - table：表 test，s0(STRING,TAG) / s2(DOUBLE,FIELD)，60 行（s0 交替 a/b）
//   - tree ：device root.ln.wf01.wt01，temperature(DOUBLE) / status(INT32)，8 行
// 显式设置对应环境变量时按环境变量路径读取（排障逃生口，不参与默认路径）。

#pragma once

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

#include "common/schema.h"
#include "common/tablet.h"
#include "common/tsfile_common.h"
#include "file/write_file.h"
#include "writer/tsfile_table_writer.h"
#include "writer/tsfile_tree_writer.h"

namespace rn146_fixture {

inline std::string ReadFileBytes(const std::string &path) {
    std::ifstream in(path.c_str(), std::ios::binary);
    std::ostringstream bytes;
    bytes << in.rdbuf();
    return bytes.str();
}

inline bool FileNonEmpty(const std::string &path) {
    struct stat st;
    return ::stat(path.c_str(), &st) == 0 && st.st_size > 0;
}

inline const std::string &FixtureDir() {
    static const std::string dir = [] {
        char tmpl[] = "/tmp/rn146_fixture_XXXXXX";
        char *made = ::mkdtemp(tmpl);
        return std::string(made == nullptr ? "/tmp" : made);
    }();
    return dir;
}

// 表模型夹具：表 test(s0/s1 STRING/TAG, s2 INT64/FIELD)，60 行（前 30 行 s0=a，后 30 行 s0=b）。
inline std::string BuildTableFixture() {
    const std::string path = FixtureDir() + "/simple_table_t1.tsfile";
    if (FileNonEmpty(path)) {
        return path;
    }
    storage::libtsfile_init();
    storage::WriteFile file;
    const int flags = O_WRONLY | O_CREAT | O_TRUNC;
    if (file.create(path, flags, 0666) != common::E_OK) {
        std::fprintf(stderr, "[rn146-fixture] create failed: %s\n", path.c_str());
        return std::string();
    }
    auto *schema = new storage::TableSchema(
        "test",
        std::vector<common::ColumnSchema>{
            common::ColumnSchema("s0", common::STRING,
                                 common::ColumnCategory::TAG),
            common::ColumnSchema("s1", common::STRING,
                                 common::ColumnCategory::TAG),
            common::ColumnSchema("s2", common::INT64,
                                 common::ColumnCategory::FIELD),
        });
    auto *writer = new storage::TsFileTableWriter(&file, schema);
    storage::Tablet tablet("test", {"s0", "s1", "s2"},
                           {common::STRING, common::STRING, common::INT64},
                           {common::ColumnCategory::TAG,
                            common::ColumnCategory::TAG,
                            common::ColumnCategory::FIELD},
                           60);
    for (int row = 0; row < 60; ++row) {
        tablet.add_timestamp(row, 1760106020000LL + row * 1000);
        tablet.add_value(row, "s0", row < 30 ? "a" : "b");
        tablet.add_value(row, "s1", row < 30 ? "b" : "d");
        tablet.add_value(row, "s2", static_cast<int64_t>(1010 + row));
    }
    int rc = writer->write_table(tablet);
    if (rc == common::E_OK) {
        rc = writer->flush();
    }
    if (rc == common::E_OK) {
        rc = writer->close();
    }
    delete writer;
    delete schema;
    if (rc != common::E_OK) {
        std::fprintf(stderr, "[rn146-fixture] write failed rc=%d: %s\n", rc,
                     path.c_str());
        return std::string();
    }
    return path;
}

// 树模型夹具：root.ln.wf01.wt01（temperature DOUBLE / status INT32），8 行。
inline std::string BuildTreeFixture() {
    const std::string path = FixtureDir() + "/simple_tree.tsfile";
    if (FileNonEmpty(path)) {
        return path;
    }
    storage::libtsfile_init();
    storage::WriteFile file;
    const int flags = O_WRONLY | O_CREAT | O_TRUNC;
    if (file.create(path, flags, 0666) != common::E_OK) {
        std::fprintf(stderr, "[rn146-fixture] create failed: %s\n", path.c_str());
        return std::string();
    }
    storage::TsFileTreeWriter writer(&file);
    std::string device = "root.ln.wf01.wt01";
    auto *temperature = new storage::MeasurementSchema("temperature", common::DOUBLE);
    auto *status = new storage::MeasurementSchema("status", common::INT32);
    writer.register_timeseries(device, temperature);
    writer.register_timeseries(device, status);
    for (int row = 0; row < 8; ++row) {
        storage::TsRecord record(device, static_cast<int64_t>(row));
        record.add_point("temperature", static_cast<double>(row) + 0.5);
        record.add_point("status", static_cast<int32_t>(row % 2));
        writer.write(record);
    }
    writer.flush();
    writer.close();
    delete temperature;
    delete status;
    return path;
}

// 环境变量（排障逃生口）优先；未设置时现写自建夹具。
inline const std::string &TableFixturePath(const char *env_name) {
    static std::string cached;
    if (!cached.empty()) {
        return cached;
    }
    const char *env = env_name != nullptr ? std::getenv(env_name) : nullptr;
    cached = (env != nullptr && env[0] != '\0') ? std::string(env)
                                                : BuildTableFixture();
    return cached;
}

inline const std::string &TreeFixturePath(const char *env_name) {
    static std::string cached;
    if (!cached.empty()) {
        return cached;
    }
    const char *env = env_name != nullptr ? std::getenv(env_name) : nullptr;
    cached = (env != nullptr && env[0] != '\0') ? std::string(env)
                                                : BuildTreeFixture();
    return cached;
}

}  // namespace rn146_fixture
