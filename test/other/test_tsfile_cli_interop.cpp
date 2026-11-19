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

// RN-143 用例 58：CLI 产物由 C++ SDK 读回（需求非功能「兼容性」）——black-box + SDK 读侧

#include <gtest/gtest.h>

#include <fstream>
#include <string>
#include <vector>

#include "cli_test_support.h"

namespace {

using cli_test::MakeTempDir;
using cli_test::RunCli;

std::string Path(const std::string &dir, const std::string &name) {
    return dir + "/" + name;
}

// 用例 58：CLI 写出的文件由 C++ SDK 读回（同源互通）
TEST(CliInteropTest, CliOutputReadByCppSdk) {
    // SDK 读路径的列解码依赖 libtsfile_init() 创建的进程级 worker pool；
    // 缺此调用时干净进程里 queryByRow 会静默返回 0 行（2026-09-29 分进程对照发现，
    // 此前「通过」依赖同进程内其他夹具先做过初始化）。
    ASSERT_EQ(storage::libtsfile_init(), common::E_OK) << "libtsfile_init 失败";
    std::string dir = MakeTempDir();
    std::string csv = Path(dir, "in.csv");
    {
        std::ofstream out(csv.c_str(), std::ios::binary | std::ios::trunc);
        out << "time,site,temp,note\n"
               "1000,beijing,20.5,alpha\n"
               "2000,beijing,21.0,beta\n"
               "1000,shanghai,22.5,gamma\n";
    }
    std::string tsfile = Path(dir, "cli-output.tsfile");
    cli_test::CliResult w = RunCli({"write", "--table", "sensors", "--tag", "site",
                                    "STRING", "--field", "temp", "FLOAT", "--field",
                                    "note", "TEXT", "-i", csv, "-o", tsfile});
    ASSERT_EQ(w.code, 0) << w.err;

    // —— C++ SDK 读侧 ——
    storage::TsFileReader reader;
    ASSERT_EQ(reader.open(tsfile), common::E_OK) << "SDK 打不开 CLI 产物";

    auto schema = reader.get_table_schema("sensors");
    ASSERT_TRUE(schema != nullptr) << "SDK 读不到表 sensors";
    std::vector<std::string> names = schema->get_measurement_names();
    ASSERT_EQ(names.size(), 3u) << "列数不符";
    EXPECT_EQ(names[0], "site");
    EXPECT_EQ(names[1], "temp");
    EXPECT_EQ(names[2], "note");

    storage::ResultSet *rs = nullptr;
    ASSERT_EQ(reader.queryByRow("sensors", names, 0, -1, rs), common::E_OK)
        << "SDK 行读取失败";
    int row_count = 0;
    bool has_next = false;
    while (rs->next(has_next) == common::E_OK && has_next) {
        ++row_count;
    }
    EXPECT_EQ(row_count, 3) << "SDK 读回的逻辑行数应与写入一致";
    reader.destroy_query_data_set(rs);
    EXPECT_EQ(reader.close(), common::E_OK);

    unlink(csv.c_str());
    unlink(tsfile.c_str());
}

}  // namespace
