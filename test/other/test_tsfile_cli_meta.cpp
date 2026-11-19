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

// RN-143 用例 14–20：文件查看（meta / ls / schema，需求 3.2.1–3.2.2）——black-box

#include <gtest/gtest.h>

#include <sys/stat.h>

#include <fstream>
#include <string>
#include <vector>

#include "cli_test_support.h"

namespace {

using cli_test::Contains;
using cli_test::MakeTempDir;
using cli_test::RunCli;
using cli_test::SplitCsvRow;
using cli_test::SplitLines;
using cli_test::WriteMultiTableFixture;
using cli_test::WriteTableFixture;
using cli_test::WriteTreeFixture;

std::string Path(const std::string &dir, const std::string &name) {
    return dir + "/" + name;
}

// 用例 14：meta 三字段与真实文件属性一致（需求 3.2.3）
TEST(CliMetaTest, MetaFieldsMatchFileTruth) {
    std::string dir = MakeTempDir();
    std::string tree = WriteTreeFixture(Path(dir, "tree.tsfile"));
    std::string table = WriteTableFixture(Path(dir, "table.tsfile"));
    ASSERT_TRUE(cli_test::FileExists(tree));
    ASSERT_TRUE(cli_test::FileExists(table));

    for (const std::string &file : {tree, table}) {
        struct stat st;
        ASSERT_EQ(stat(file.c_str(), &st), 0);
        cli_test::CliResult r = RunCli({"meta", "-f", "csv", file});
        ASSERT_EQ(r.code, 0) << r.err;
        std::vector<std::string> lines = SplitLines(r.out);
        ASSERT_EQ(lines.size(), 2u) << r.out;
        EXPECT_EQ(lines[0], "size_bytes,format_version,model");
        std::vector<std::string> cells = SplitCsvRow(lines[1]);
        ASSERT_EQ(cells.size(), 3u) << lines[1];
        EXPECT_EQ(cells[0], std::to_string(static_cast<long long>(st.st_size)));
        EXPECT_FALSE(cells[1].empty()) << "format_version must not be empty";
        EXPECT_TRUE(cells[2] == "tree" || cells[2] == "table") << lines[1];
    }
    unlink(tree.c_str());
    unlink(table.c_str());
}

// 用例 15：meta 选项拒绝与输入异常（需求 3.2.3 异常表）
TEST(CliMetaTest, MetaRejectsScopeAndFailsOnBadInput) {
    std::string dir = MakeTempDir();
    std::string tree = WriteTreeFixture(Path(dir, "tree.tsfile"));
    std::string damaged = Path(dir, "damaged.tsfile");
    {
        std::ofstream out(damaged.c_str(), std::ios::binary);
        out << "not a tsfile at all";
    }
    ASSERT_TRUE(cli_test::FileExists(tree));

    cli_test::CliResult scope_t = RunCli({"meta", "-t", "sensors", tree});
    EXPECT_EQ(scope_t.code, 1) << scope_t.err;
    EXPECT_TRUE(scope_t.out.empty());

    cli_test::CliResult scope_m = RunCli({"meta", "-m", "m1", tree});
    EXPECT_EQ(scope_m.code, 1) << scope_m.err;

    cli_test::CliResult bad = RunCli({"meta", damaged});
    EXPECT_EQ(bad.code, 2) << bad.err;
    EXPECT_TRUE(bad.out.empty());

    cli_test::CliResult missing = RunCli({"meta", Path(dir, "nope.tsfile")});
    EXPECT_EQ(missing.code, 2) << missing.err;
    unlink(tree.c_str());
    unlink(damaged.c_str());
}

// 用例 16：ls 对象列表与原生顺序（需求 3.2.1）
TEST(CliMetaTest, LsListsObjectsInNativeOrder) {
    std::string dir = MakeTempDir();
    std::string multi = WriteMultiTableFixture(Path(dir, "multi.tsfile"));
    std::string tree = WriteTreeFixture(Path(dir, "tree.tsfile"));
    std::string empty = cli_test::WriteEmptyTableFixture(Path(dir, "empty.tsfile"));
    ASSERT_TRUE(cli_test::FileExists(multi));

    cli_test::CliResult r = RunCli({"ls", "-f", "csv", multi});
    ASSERT_EQ(r.code, 0) << r.err;
    std::vector<std::string> lines = SplitLines(r.out);
    ASSERT_EQ(lines.size(), 3u) << r.out;
    EXPECT_EQ(lines[0], "model,object");
    // 原生（写入）顺序：sensors_a 先于 sensors_b，不按字典序重排
    std::vector<std::string> first = SplitCsvRow(lines[1]);
    std::vector<std::string> second = SplitCsvRow(lines[2]);
    ASSERT_EQ(first.size(), 2u);
    EXPECT_EQ(first[0], "table");
    EXPECT_EQ(first[1], "sensors_a");
    EXPECT_EQ(second[1], "sensors_b");

    // 树模型与空表
    cli_test::CliResult t = RunCli({"ls", "-f", "csv", tree});
    ASSERT_EQ(t.code, 0) << t.err;
    EXPECT_EQ(SplitCsvRow(SplitLines(t.out)[1])[1], "root.test.d1");

    cli_test::CliResult e = RunCli({"ls", "-f", "csv", empty});
    ASSERT_EQ(e.code, 0) << e.err;
    std::vector<std::string> elines = SplitLines(e.out);
    ASSERT_EQ(elines.size(), 2u) << e.out;
    EXPECT_EQ(SplitCsvRow(elines[1])[1], "sensors");
    unlink(multi.c_str());
    unlink(tree.c_str());
    unlink(empty.c_str());
}

// 用例 17：ls 选项与输入异常（需求 3.2.1 异常表）
TEST(CliMetaTest, LsRejectsScopeAndBadInput) {
    std::string dir = MakeTempDir();
    std::string tree = WriteTreeFixture(Path(dir, "tree.tsfile"));
    std::string tree2 = WriteTreeFixture(Path(dir, "tree2.tsfile"));
    ASSERT_TRUE(cli_test::FileExists(tree));

    cli_test::CliResult scope = RunCli({"ls", "-d", "root.test.d1", tree});
    EXPECT_EQ(scope.code, 1) << scope.err;

    cli_test::CliResult two = RunCli({"ls", tree, tree2});
    EXPECT_EQ(two.code, 1) << two.err;

    cli_test::CliResult dir_in = RunCli({"ls", dir});
    EXPECT_EQ(dir_in.code, 2) << dir_in.err;

    cli_test::CliResult missing = RunCli({"ls", Path(dir, "nope.tsfile")});
    EXPECT_EQ(missing.code, 2) << missing.err;
    unlink(tree.c_str());
    unlink(tree2.c_str());
}

// 用例 18：schema 七字段与物理事实（需求 3.2.2）
TEST(CliSchemaTest, SevenFieldContractMatchesWriter) {
    std::string dir = MakeTempDir();
    std::string table = WriteTableFixture(Path(dir, "table.tsfile"));
    std::string tree = WriteTreeFixture(Path(dir, "tree.tsfile"));
    ASSERT_TRUE(cli_test::FileExists(table));

    cli_test::CliResult r = RunCli({"schema", "-t", "sensors", "-f", "csv", table});
    ASSERT_EQ(r.code, 0) << r.err;
    std::vector<std::string> lines = SplitLines(r.out);
    ASSERT_EQ(lines.size(), 4u) << r.out;  // 表头 + site/temp/note
    EXPECT_EQ(lines[0], "model,object,column,category,data_type,encoding,compression");
    std::vector<std::string> tag = SplitCsvRow(lines[1]);
    std::vector<std::string> f1 = SplitCsvRow(lines[2]);
    std::vector<std::string> f2 = SplitCsvRow(lines[3]);
    ASSERT_EQ(tag.size(), 7u);
    EXPECT_EQ(tag[2], "site");
    EXPECT_EQ(tag[3], "TAG");
    EXPECT_EQ(tag[4], "STRING");
    EXPECT_EQ(f1[2], "temp");
    EXPECT_EQ(f1[3], "FIELD");
    EXPECT_EQ(f1[4], "FLOAT");
    EXPECT_EQ(f2[2], "note");
    EXPECT_EQ(f2[4], "TEXT");

    cli_test::CliResult t = RunCli({"schema", "-f", "csv", tree});
    ASSERT_EQ(t.code, 0) << t.err;
    std::vector<std::string> tlines = SplitLines(t.out);
    ASSERT_EQ(tlines.size(), 3u) << t.out;
    EXPECT_EQ(SplitCsvRow(tlines[1])[3], "FIELD");
    unlink(table.c_str());
    unlink(tree.c_str());
}

// 用例 19：表模型名称 ASCII 大小写不敏感并使用规范名（需求 3.1.4.2）
TEST(CliSchemaTest, NameMatchingAndCanonicalNames) {
    std::string dir = MakeTempDir();
    std::string table = WriteTableFixture(Path(dir, "table.tsfile"));
    std::string tree = WriteTreeFixture(Path(dir, "tree.tsfile"));
    ASSERT_TRUE(cli_test::FileExists(table));

    cli_test::CliResult r = RunCli({"schema", "-t", "SENSORS", "-m", "TEMP", "-f",
                                    "csv", table});
    ASSERT_EQ(r.code, 0) << r.err;
    std::vector<std::string> lines = SplitLines(r.out);
    ASSERT_EQ(lines.size(), 2u) << r.out;
    std::vector<std::string> cells = SplitCsvRow(lines[1]);
    EXPECT_EQ(cells[1], "sensors") << "canonical table name expected";
    EXPECT_EQ(cells[2], "temp") << "canonical column name expected";

    // 树模型区分大小写
    cli_test::CliResult wrong_case = RunCli({"schema", "-d", "ROOT.TEST.D1", tree});
    EXPECT_EQ(wrong_case.code, 1) << wrong_case.err;
    unlink(table.c_str());
    unlink(tree.c_str());
}

// 用例 20：schema 列选择与顺序/Category 约束（需求 3.2.2 异常表）
TEST(CliSchemaTest, ColumnSelectionAndCategoryRules) {
    std::string dir = MakeTempDir();
    std::string table = WriteTableFixture(Path(dir, "table.tsfile"));
    ASSERT_TRUE(cli_test::FileExists(table));

    cli_test::CliResult missing = RunCli({"schema", "-t", "sensors", "-m", "missing", table});
    EXPECT_EQ(missing.code, 1) << missing.err;

    cli_test::CliResult dup = RunCli({"schema", "-t", "sensors", "-m", "temp", "-m",
                                      "temp", table});
    EXPECT_EQ(dup.code, 1) << dup.err;

    // -m 只筛选不重排：按 schema 顺序输出（site 在 temp 之前）
    cli_test::CliResult reorder = RunCli({"schema", "-t", "sensors", "-m", "temp", "-m",
                                          "site", "-f", "csv", table});
    ASSERT_EQ(reorder.code, 0) << reorder.err;
    std::vector<std::string> lines = SplitLines(reorder.out);
    ASSERT_EQ(lines.size(), 3u) << reorder.out;
    EXPECT_EQ(SplitCsvRow(lines[1])[2], "site");
    EXPECT_EQ(SplitCsvRow(lines[2])[2], "temp");
    unlink(table.c_str());
}

}  // namespace
