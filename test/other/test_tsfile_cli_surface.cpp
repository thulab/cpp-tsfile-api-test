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

// RN-143 用例 1–8：命令入口与状态码（需求 3.1）——black-box，子进程调用真实 tsfile-cli

#include <gtest/gtest.h>

#include <fstream>
#include <string>
#include <vector>

#include "cli_test_support.h"

namespace {

using cli_test::CliResult;
using cli_test::Contains;
using cli_test::MakeTempDir;
using cli_test::ReadFileBytes;
using cli_test::RunCli;
using cli_test::WriteNonPureFixture;
using cli_test::WriteTableFixture;
using cli_test::WriteTreeFixture;

std::string WriteTextFile(const std::string &path, const std::string &text) {
    std::ofstream out(path.c_str(), std::ios::binary | std::ios::trunc);
    out << text;
    out.close();
    return path;
}

std::string MakeCorruptFixture(const std::string &dir) {
    return WriteTextFile(dir + "/corrupt.tsfile", "this is not a tsfile");
}

// 用例 1：--version 独占输出四元信息（需求 3.1.1）
TEST(CliSurfaceTest, VersionLineContract) {
    CliResult version = RunCli({"--version"});
    ASSERT_EQ(version.code, 0) << "stderr=" << version.err;
    ASSERT_TRUE(version.err.empty()) << "stderr=" << version.err;

    // 固定一行、以 LF 结束
    ASSERT_EQ(version.out.find('\n') != std::string::npos, true);
    ASSERT_EQ(version.out.find('\n'), version.out.size() - 1)
        << "expect single line ending with LF, got: " << version.out;
    std::string line = version.out.substr(0, version.out.size() - 1);

    // tsfile-cli <cli-version> tsfile=<tsfile-version> commit=<full-sha> built=<utc-time>
    std::istringstream ss(line);
    std::string bin, cli_ver, tsfile_kv, commit_kv, built_kv, extra;
    ss >> bin >> cli_ver >> tsfile_kv >> commit_kv >> built_kv >> extra;
    EXPECT_EQ(bin, "tsfile-cli");
    EXPECT_FALSE(cli_ver.empty());
    EXPECT_EQ(tsfile_kv.rfind("tsfile=", 0), 0u);
    EXPECT_EQ(commit_kv.rfind("commit=", 0), 0u);
    EXPECT_EQ(built_kv.rfind("built=", 0), 0u);
    EXPECT_TRUE(extra.empty()) << "unexpected extra token: " << extra;

    // commit 为完整 sha（40 hex）
    std::string sha = commit_kv.substr(std::string("commit=").size());
    ASSERT_EQ(sha.size(), 40u) << "commit should be full sha, got: " << sha;
    for (char c : sha) {
        EXPECT_TRUE((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))
            << "non-hex char in commit: " << sha;
    }

    // --version 必须独立出现
    std::string dir = MakeTempDir();
    std::string tree = WriteTreeFixture(dir + "/tree.tsfile");
    ASSERT_TRUE(cli_test::FileExists(tree));
    CliResult mixed = RunCli({"--version", tree});
    EXPECT_EQ(mixed.code, 1);
    EXPECT_TRUE(mixed.out.empty());
    EXPECT_FALSE(mixed.err.empty());
    unlink(tree.c_str());
}

// 用例 2：顶层与命令级 help 独占输出（需求 3.1.1）
TEST(CliSurfaceTest, HelpIsStandaloneAndScoped) {
    const std::vector<std::string> commands = {
        "ls", "schema", "meta", "stats", "count",
        "sketch", "head", "cat", "export", "write"};

    for (const std::string &flag : {"--help", "-h", "help"}) {
        CliResult help = RunCli({flag});
        ASSERT_EQ(help.code, 0) << flag << " stderr=" << help.err;
        for (const std::string &cmd : commands) {
            EXPECT_TRUE(Contains(help.out, cmd)) << flag << " misses " << cmd;
        }
    }

    CliResult cat_help = RunCli({"cat", "--help"});
    ASSERT_EQ(cat_help.code, 0);
    EXPECT_TRUE(Contains(cat_help.out, "Usage: tsfile-cli cat"))
        << cat_help.out;
    EXPECT_FALSE(Contains(cat_help.out, "Usage: tsfile-cli export"))
        << "command help must be scoped: " << cat_help.out;

    std::string dir = MakeTempDir();
    std::string tree = WriteTreeFixture(dir + "/tree.tsfile");
    CliResult mixed = RunCli({"cat", "--help", tree});
    EXPECT_EQ(mixed.code, 1);
    EXPECT_TRUE(mixed.out.empty());

    CliResult unknown = RunCli({"frobnicate", "--help"});
    EXPECT_EQ(unknown.code, 1);
    EXPECT_TRUE(unknown.out.empty());
    unlink(tree.c_str());
}

// 用例 3：无参数与未知命令的用法报错（需求 3.1.1）
TEST(CliSurfaceTest, NoArgsAndUnknownCommandUsageError) {
    CliResult no_args = RunCli({});
    EXPECT_EQ(no_args.code, 1);
    EXPECT_TRUE(no_args.out.empty()) << "stdout must stay empty: " << no_args.out;
    EXPECT_TRUE(Contains(no_args.err, "Usage")) << no_args.err;

    CliResult unknown = RunCli({"frobnicate", "x.tsfile"});
    EXPECT_EQ(unknown.code, 1);
    EXPECT_TRUE(unknown.out.empty());
    EXPECT_FALSE(unknown.err.empty());
}

// 用例 4：文件路径必须是最后一个位置参数（需求 3.1.2）
TEST(CliSurfaceTest, FilePathMustBeLastPositional) {
    std::string dir = MakeTempDir();
    std::string tree = WriteTreeFixture(dir + "/tree.tsfile");
    std::string tree2 = WriteTreeFixture(dir + "/tree2.tsfile");
    ASSERT_TRUE(cli_test::FileExists(tree));

    CliResult path_not_last = RunCli({"cat", tree, "--start", "1"});
    EXPECT_EQ(path_not_last.code, 1) << "stderr=" << path_not_last.err;
    EXPECT_FALSE(path_not_last.err.empty());

    CliResult options_first = RunCli({"cat", "-f", "csv", "-m", "m1", tree});
    EXPECT_EQ(options_first.code, 0) << "stderr=" << options_first.err;

    CliResult two_files = RunCli({"ls", tree, tree2});
    EXPECT_EQ(two_files.code, 1);
    unlink(tree.c_str());
    unlink(tree2.c_str());
}

// 用例 5：单值选项重复与非法取值为用法错误（需求 3.1.2）
TEST(CliSurfaceTest, SingletonOptionRepeatsAreErrors) {
    std::string dir = MakeTempDir();
    std::string tree = WriteTreeFixture(dir + "/tree.tsfile");
    std::string table = WriteTableFixture(dir + "/table.tsfile");

    struct Case {
        std::vector<std::string> args;
        const char *why;
    };
    std::vector<Case> cases = {
        {{"cat", "-f", "csv", "-f", "ndjson", tree}, "repeat -f"},
        {{"cat", "-t", "sensors", "-t", "sensors2", table}, "repeat -t"},
        {{"cat", "--offset", "0", "--offset", "1", tree}, "repeat --offset"},
        {{"count", "-f", "json", tree}, "bad format value"},
        {{"head", "--offset", "01", tree}, "non-decimal offset"},
    };
    for (const Case &c : cases) {
        CliResult r = RunCli(c.args);
        EXPECT_EQ(r.code, 1) << c.why << " stderr=" << r.err;
        EXPECT_TRUE(r.out.empty()) << c.why << " stdout=" << r.out;
        EXPECT_FALSE(r.err.empty()) << c.why;
    }
    unlink(tree.c_str());
    unlink(table.c_str());
}

// 用例 6：状态码 0/1/2/3 分层矩阵（需求 3.1.1）
TEST(CliSurfaceTest, ExitCodeMatrix) {
    std::string dir = MakeTempDir();
    std::string tree = WriteTreeFixture(dir + "/tree.tsfile");
    std::string table = WriteTableFixture(dir + "/table.tsfile");
    std::string corrupt = MakeCorruptFixture(dir);
    std::string nonpure = WriteNonPureFixture(dir + "/nonpure.tsfile");
    ASSERT_TRUE(cli_test::FileExists(table));
    ASSERT_TRUE(cli_test::FileExists(nonpure));

    // 0：成功
    EXPECT_EQ(RunCli({"meta", tree}).code, 0);

    // 2：输入问题
    CliResult missing = RunCli({"meta", dir + "/missing.tsfile"});
    EXPECT_EQ(missing.code, 2) << missing.err;
    CliResult directory = RunCli({"ls", dir});
    EXPECT_EQ(directory.code, 2) << directory.err;
    CliResult damaged = RunCli({"meta", corrupt});
    EXPECT_EQ(damaged.code, 2) << damaged.err;
    EXPECT_TRUE(damaged.out.empty());

    // 1：参数/对象错误
    CliResult unknown_col = RunCli({"cat", "-d", "root.test.d1", "-m", "missing", tree});
    EXPECT_EQ(unknown_col.code, 1) << unknown_col.err;
    EXPECT_TRUE(unknown_col.out.empty());

    // 3：目标冲突 / 同源
    std::string existing = dir + "/existing.csv";
    WriteTextFile(existing, "old");
    CliResult conflict = RunCli({"export", "-t", "sensors", "--type", "csv", "-o",
                                 existing, table});
    EXPECT_EQ(conflict.code, 3) << conflict.err;
    CliResult same_file = RunCli({"export", "-t", "sensors", "--type", "csv", "-o",
                                  table, "--force", table});
    EXPECT_EQ(same_file.code, 3) << same_file.err;

    unlink(tree.c_str());
    unlink(table.c_str());
    unlink(corrupt.c_str());
    unlink(nonpure.c_str());
    unlink(existing.c_str());
}

// 用例 7：模型自动识别与作用域匹配/互斥（需求 3.1.4.1）
TEST(CliSurfaceTest, ModelDetectionAndScopeRules) {
    std::string dir = MakeTempDir();
    std::string tree = WriteTreeFixture(dir + "/tree.tsfile");
    std::string table = WriteTableFixture(dir + "/table.tsfile");

    CliResult meta_tree = RunCli({"meta", "-f", "csv", tree});
    ASSERT_EQ(meta_tree.code, 0);
    EXPECT_TRUE(Contains(meta_tree.out, "tree")) << meta_tree.out;

    CliResult meta_table = RunCli({"meta", "-f", "csv", table});
    ASSERT_EQ(meta_table.code, 0);
    EXPECT_TRUE(Contains(meta_table.out, "table")) << meta_table.out;

    CliResult both_scope = RunCli({"schema", "-d", "root.test.d1", "-t", "sensors", table});
    EXPECT_EQ(both_scope.code, 1) << both_scope.err;

    CliResult table_on_tree = RunCli({"schema", "-t", "sensors", tree});
    EXPECT_EQ(table_on_tree.code, 1) << table_on_tree.err;

    CliResult device_on_table = RunCli({"schema", "-d", "root.test.d1", table});
    EXPECT_EQ(device_on_table.code, 1) << device_on_table.err;

    unlink(tree.c_str());
    unlink(table.c_str());
}

// 用例 8：非纯模型文件按输入问题拒绝（需求 3.1.4.1 + 验收范围「文件查看」）
TEST(CliSurfaceTest, NonPureModelRejected) {
    std::string dir = MakeTempDir();
    std::string nonpure = WriteNonPureFixture(dir + "/nonpure.tsfile");
    ASSERT_TRUE(cli_test::FileExists(nonpure));
    ASSERT_GT(ReadFileBytes(nonpure).size(), 0u);

    std::vector<std::vector<std::string>> commands = {
        {"ls", nonpure},
        {"schema", nonpure},
        {"meta", nonpure},
        {"head", nonpure},
        {"cat", nonpure},
        {"export", "-t", "hybrid_table", "--type", "csv", "-o",
         dir + "/out.csv", nonpure},
    };
    for (const auto &args : commands) {
        CliResult r = RunCli(args);
        EXPECT_EQ(r.code, 2) << args[0] << " stderr=" << r.err;
        EXPECT_TRUE(r.out.empty()) << args[0] << " stdout=" << r.out;
    }
    EXPECT_FALSE(cli_test::FileExists(dir + "/out.csv"));
    unlink(nonpure.c_str());
}

}  // namespace
