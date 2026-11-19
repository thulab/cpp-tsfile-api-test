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

// RN-143 用例 49–57：创建新文件 write（需求 3.5）——black-box

#include <gtest/gtest.h>

#include <fstream>
#include <map>
#include <sstream>
#include <string>
#include <vector>

#include "cli_test_support.h"

namespace {

using cli_test::CliResult;
using cli_test::Contains;
using cli_test::MakeTempDir;
using cli_test::ReadFileBytes;
using cli_test::RunCli;
using cli_test::RunCliWithStdin;
using cli_test::SplitCsvRow;
using cli_test::SplitCsvRows;

std::string Path(const std::string &dir, const std::string &name) {
    return dir + "/" + name;
}

std::string WriteTextFile(const std::string &path, const std::string &text) {
    std::ofstream out(path.c_str(), std::ios::binary | std::ios::trunc);
    out << text;
    out.close();
    return path;
}

std::vector<std::string> ReadLines(const std::string &dir, const std::string &file) {
    return cli_test::SplitLines(ReadFileBytes(dir + "/" + file));
}

// 用例 49：CSV 导入基本往返（含回读）（需求 3.5）
TEST(CliWriteTest, ImportRoundTripWithReadBack) {
    std::string dir = MakeTempDir();
    std::string csv = WriteTextFile(Path(dir, "in.csv"),
                                    "time,site,temp,note\n"
                                    "1000,beijing,20.5,alpha\n"
                                    "2000,beijing,21.0,beta\n"
                                    "1000,shanghai,22.5,gamma\n");
    std::string out = Path(dir, "out.tsfile");
    CliResult w = RunCli({"write", "--table", "sensors", "--tag", "site", "STRING",
                          "--field", "temp", "FLOAT", "--field", "note", "TEXT",
                          "-i", csv, "-o", out});
    ASSERT_EQ(w.code, 0) << w.err;
    EXPECT_TRUE(w.out.empty());
    EXPECT_TRUE(w.err.empty()) << "无 -v 时成功应静默：" << w.err;

    CliResult r = RunCli({"cat", "-t", "sensors", "-f", "csv", out});
    ASSERT_EQ(r.code, 0) << r.err;
    std::vector<std::string> rows = SplitCsvRows(r.out);
    ASSERT_EQ(rows.size(), 4u) << r.out;  // 表头 + 3 行
    EXPECT_EQ(rows[0], "time,site,temp,note");
    std::map<std::string, std::vector<std::string>> by_key;
    for (size_t i = 1; i < rows.size(); ++i) {
        std::vector<std::string> cells = SplitCsvRow(rows[i]);
        ASSERT_EQ(cells.size(), 4u);
        by_key[cells[0] + "|" + cells[1]] = cells;
    }
    EXPECT_EQ(by_key["1000|beijing"][2], "20.5");
    EXPECT_EQ(by_key["1000|beijing"][3], "alpha");
    EXPECT_EQ(by_key["2000|beijing"][3], "beta");
    EXPECT_EQ(by_key["1000|shanghai"][3], "gamma");
    unlink(csv.c_str());
    unlink(out.c_str());
}

// 用例 50：header 形状错误在读取数据前失败（需求 3.5）
TEST(CliWriteTest, HeaderShapeErrorsBeforeData) {
    std::string dir = MakeTempDir();
    struct Case {
        const char *name;
        const char *content;
        const char *why;
    };
    std::vector<Case> cases = {
        {"missing.csv", "time,site\n1000,beijing\n", "缺列（声明了 temp 但表头没有）"},
        {"undeclared.csv", "time,site,temp,extra\n1000,beijing,20,x\n", "含未声明列 extra"},
        {"dup.csv", "time,site,temp,temp\n1000,beijing,20,21\n", "重复列 temp"},
        {"no_time.csv", "site,temp\nbeijing,20\n", "缺 time 列"},
        {"conflict.csv", "time,TIME,site,temp\n1000,1000,beijing,20\n", "time/TIME 大小写冲突"},
    };
    for (const Case &c : cases) {
        std::string csv = WriteTextFile(Path(dir, c.name), c.content);
        std::string out = Path(dir, std::string("out-") + c.name + ".tsfile");
        CliResult r = RunCli({"write", "--table", "sensors", "--tag", "site", "STRING",
                              "--field", "temp", "FLOAT", "-i", csv, "-o", out});
        EXPECT_EQ(r.code, 2) << c.why << " stderr=" << r.err;
        EXPECT_FALSE(cli_test::FileExists(out)) << c.why << " 不得产生目标文件";
        unlink(csv.c_str());
    }
}

// 用例 51：严格 CSV 语法与 BOM（需求 3.5）
TEST(CliWriteTest, StrictCsvSyntax) {
    std::string dir = MakeTempDir();
    struct Case {
        const char *name;
        std::string content;
        int expect;
        const char *why;
    };
    std::vector<Case> cases = {
        {"lf.csv", "time,site,temp\n1000,bj,20\n2000,bj,21\n", 0, "LF 合法"},
        {"crlf.csv", "time,site,temp\r\n1000,bj,20\r\n2000,bj,21\r\n", 0, "CRLF 合法"},
        {"mixed.csv", "time,site,temp\r\n1000,bj,20\n2000,bj,21\r\n", 0, "LF/CRLF 混用合法"},
        {"comment.csv", "time,site,temp\n#comment\n1000,bj,20\n", 2, "不支持注释"},
        {"unclosed.csv", "time,site,temp\n1000,bj,\"unclosed\n", 2, "未闭合引号"},
        {"cr_only.csv", "time,site,temp\r1000,bj,20\r2000,bj,21\r", 2, "单独 CR 不接受"},
        {"bom.csv", "\xEF\xBB\xBF" "time,site,temp\n1000,bj,20\n", 0, "单个开头 BOM 可接受"},
        {"header_only.csv", "time,site,temp\n", 0, "仅表头（空表）合法"},
    };
    for (const Case &c : cases) {
        std::string csv = WriteTextFile(Path(dir, c.name), c.content);
        std::string out = Path(dir, std::string("out-") + c.name + ".tsfile");
        CliResult r = RunCli({"write", "--table", "sensors", "--tag", "site", "STRING",
                              "--field", "temp", "FLOAT", "-i", csv, "-o", out});
        EXPECT_EQ(r.code, c.expect) << c.why << " stderr=" << r.err;
        if (c.expect == 0) {
            EXPECT_TRUE(cli_test::FileExists(out)) << c.why;
        } else {
            EXPECT_FALSE(cli_test::FileExists(out)) << c.why << " 不得产生目标文件";
        }
        unlink(csv.c_str());
        if (cli_test::FileExists(out)) {
            unlink(out.c_str());
        }
    }
}

// 用例 52：时间严格递增与实体交错（需求 3.5）
TEST(CliWriteTest, TimestampMonotonicPerEntity) {
    std::string dir = MakeTempDir();
    std::string bad = WriteTextFile(Path(dir, "out-of-order.csv"),
                                    "time,site,temp\n2000,bj,20\n1000,bj,21\n");
    std::string bad_out = Path(dir, "bad.tsfile");
    CliResult r1 = RunCli({"write", "--table", "sensors", "--tag", "site", "STRING",
                           "--field", "temp", "FLOAT", "-i", bad, "-o", bad_out});
    EXPECT_EQ(r1.code, 2) << r1.err;
    EXPECT_FALSE(cli_test::FileExists(bad_out)) << "乱序不得产生目标文件";
    EXPECT_TRUE(Contains(r1.err, "logical_record") || Contains(r1.err, "physical_line") ||
                Contains(r1.err, "column"))
        << "需求 §3.5 要求报告「逻辑记录号、物理行号或范围、列名及列序号」；实测仅给出物理行号，"
           "缺列名/列序号/逻辑记录号（按用户判据属 P2 类「报错信息不明确」候选）：" << r1.err;

    std::string inter = WriteTextFile(Path(dir, "interleaved.csv"),
                                      "time,site,temp\n"
                                      "1000,bj,20\n1000,sh,30\n2000,bj,21\n2000,sh,31\n");
    std::string inter_out = Path(dir, "inter.tsfile");
    CliResult r2 = RunCli({"write", "--table", "sensors", "--tag", "site", "STRING",
                           "--field", "temp", "FLOAT", "-i", inter, "-o", inter_out});
    ASSERT_EQ(r2.code, 0) << "不同实体可交错：" << r2.err;
    CliResult read = RunCli({"count", "-t", "sensors", "-f", "csv", inter_out});
    ASSERT_EQ(read.code, 0);
    EXPECT_TRUE(Contains(read.out, ",2,")) << "应识别为 2 个实体：" << read.out;
    unlink(bad.c_str());
    unlink(inter.c_str());
    unlink(inter_out.c_str());
}

// 用例 53：类型与物理参数 override（需求 3.5）
TEST(CliWriteTest, TypePhysicalOverrides) {
    std::string dir = MakeTempDir();
    std::string csv = WriteTextFile(Path(dir, "float.csv"),
                                    "time,site,temp,humidity\n1000,bj,20,40\n");
    std::string out = Path(dir, "float.tsfile");
    CliResult ok = RunCli({"write", "--table", "sensors", "--tag", "site", "STRING",
                           "--field", "temp", "FLOAT", "--field", "humidity", "FLOAT",
                           "--encoding", "FLOAT", "GORILLA", "--compression", "FLOAT", "LZ4",
                           "-i", csv, "-o", out});
    ASSERT_EQ(ok.code, 0) << ok.err;
    CliResult schema = RunCli({"schema", "-t", "sensors", "-f", "csv", out});
    ASSERT_EQ(schema.code, 0) << schema.err;
    int gorilla = 0, lz4 = 0;
    for (const std::string &line : SplitCsvRows(schema.out)) {
        std::vector<std::string> cells = SplitCsvRow(line);
        if (cells.size() == 7 && cells[3] == "FIELD" && cells[4] == "FLOAT") {
            if (cells[5] == "GORILLA") ++gorilla;
            if (cells[6] == "LZ4") ++lz4;
        }
    }
    EXPECT_EQ(gorilla, 2) << "按数据类型应用到全部 FLOAT 列：" << schema.out;
    EXPECT_EQ(lz4, 2) << schema.out;

    struct Case {
        std::vector<std::string> extra;
        const char *why;
    };
    std::vector<Case> cases = {
        {{"--encoding", "DOUBLE", "GORILLA"}, "未使用的类型"},
        {{"--encoding", "FLOAT", "GORILLA", "--encoding", "FLOAT", "RLE"}, "同类型重复指定"},
    };
    for (const Case &c : cases) {
        std::string bad_out = Path(dir, "bad.tsfile");
        std::vector<std::string> args = {"write", "--table", "sensors", "--tag", "site",
                                         "STRING", "--field", "temp", "FLOAT"};
        args.insert(args.end(), c.extra.begin(), c.extra.end());
        args.insert(args.end(), {"-i", csv, "-o", bad_out});
        CliResult r = RunCli(args);
        EXPECT_EQ(r.code, 1) << c.why << " stderr=" << r.err;
        EXPECT_FALSE(cli_test::FileExists(bad_out)) << c.why;
    }
    unlink(csv.c_str());
    unlink(out.c_str());
}

// 用例 54：结构声明约束（需求 3.5）
TEST(CliWriteTest, StructureDeclarationRules) {
    std::string dir = MakeTempDir();
    std::string csv = WriteTextFile(Path(dir, "in.csv"), "time,site,temp\n1000,bj,20\n");
    struct Case {
        std::vector<std::string> args;
        const char *why;
    };
    std::string out = Path(dir, "x.tsfile");
    std::vector<Case> cases = {
        {{"write", "--table", "t", "--tag", "site", "INT32", "--field", "temp", "FLOAT",
          "-i", csv, "-o", out}, "TAG 类型固定 STRING"},
        {{"write", "--table", "t", "--tag", "site", "STRING", "-i", csv, "-o", out},
         "至少一个 FIELD"},
        {{"write", "--device", "root.d1", "--field", "temp", "FLOAT", "-i", csv, "-o", out},
         "write 只创建表模型"},
        {{"write", "--table", "t", "--field", "temp", "FLOAT", "--field", "temp", "INT32",
          "-i", csv, "-o", out}, "FIELD 重复声明"},
        {{"write", "--table", "t", "--tag", "site", "STRING", "--field", "temp", "float",
          "-i", csv, "-o", out}, "类型须规范大写"},
    };
    for (const Case &c : cases) {
        CliResult r = RunCli(c.args);
        EXPECT_EQ(r.code, 1) << c.why << " stderr=" << r.err;
        EXPECT_FALSE(cli_test::FileExists(out)) << c.why;
    }
    unlink(csv.c_str());
}

// 用例 55：零 TAG 表与 null/空串实体（需求 3.5）
TEST(CliWriteTest, ZeroTagAndNullEntities) {
    std::string dir = MakeTempDir();
    std::string zero = WriteTextFile(Path(dir, "zero.csv"), "time,temp\n1000,20\n2000,21\n");
    std::string zero_out = Path(dir, "zero.tsfile");
    CliResult r1 = RunCli({"write", "--table", "global_metrics", "--field", "temp", "FLOAT",
                           "-i", zero, "-o", zero_out});
    ASSERT_EQ(r1.code, 0) << r1.err;
    CliResult c1 = RunCli({"count", "-t", "global_metrics", "-f", "csv", zero_out});
    ASSERT_EQ(c1.code, 0);
    EXPECT_TRUE(Contains(c1.out, ",2,1,") || Contains(c1.out, "1,"))
        << "零 TAG 表 entity_count 应为 1：" << c1.out;

    std::string ents = WriteTextFile(Path(dir, "entities.csv"),
                                     "time,site,temp\n1000,\\N,20\n1000,\"\",22\n");
    std::string ents_out = Path(dir, "entities.tsfile");
    CliResult r2 = RunCli({"write", "--table", "entity_demo", "--tag", "site", "STRING",
                           "--field", "temp", "FLOAT", "-i", ents, "-o", ents_out});
    ASSERT_EQ(r2.code, 0) << r2.err;
    CliResult c2 = RunCli({"count", "-t", "entity_demo", "-m", "site", "-f", "csv", ents_out});
    ASSERT_EQ(c2.code, 0) << c2.err;
    std::vector<std::string> rows = SplitCsvRows(c2.out);
    ASSERT_EQ(rows.size(), 2u) << c2.out;
    std::vector<std::string> cells = SplitCsvRow(rows[1]);
    EXPECT_EQ(cells[4], "2") << "null 与空串应形成 2 个实体：" << c2.out;
    EXPECT_EQ(cells[6], "1") << "non_null_count：" << c2.out;
    EXPECT_EQ(cells[7], "1") << "null_count：" << c2.out;
    unlink(zero.c_str());
    unlink(zero_out.c_str());
    unlink(ents.c_str());
    unlink(ents_out.c_str());
}

// 用例 56：输入来源规则（需求 3.5）
TEST(CliWriteTest, InputSourceRules) {
    std::string dir = MakeTempDir();
    std::string csv = WriteTextFile(Path(dir, "in.csv"), "time,site,temp\n1000,bj,20\n");

    std::string stdin_out = Path(dir, "stdin.tsfile");
    CliResult s = RunCliWithStdin({"write", "--table", "t", "--tag", "site", "STRING",
                                   "--field", "temp", "FLOAT", "--stdin", "-o", stdin_out},
                                  "time,site,temp\n1000,bj,20\n");
    ASSERT_EQ(s.code, 0) << s.err;
    EXPECT_TRUE(cli_test::FileExists(stdin_out));

    CliResult both = RunCli({"write", "--table", "t", "--tag", "site", "STRING",
                             "--field", "temp", "FLOAT", "-i", csv, "--stdin",
                             "-o", Path(dir, "both.tsfile")});
    EXPECT_EQ(both.code, 1) << "--input 与 --stdin 互斥：" << both.err;

    CliResult devnull = RunCli({"write", "--table", "t", "--tag", "site", "STRING",
                                "--field", "temp", "FLOAT", "-i", "/dev/null",
                                "-o", Path(dir, "devnull.tsfile")});
    EXPECT_EQ(devnull.code, 2) << "特殊输入源应报输入问题：" << devnull.err;

    CliResult none = RunCli({"write", "--table", "t", "--tag", "site", "STRING",
                             "--field", "temp", "FLOAT", "-o", Path(dir, "none.tsfile")});
    EXPECT_EQ(none.code, 1) << "必须二选一：" << none.err;
    unlink(csv.c_str());
    unlink(stdin_out.c_str());
}

// 用例 57：目标保护、原子性与 -v 摘要（需求 3.5）
TEST(CliWriteTest, TargetProtectionAndVerbose) {
    std::string dir = MakeTempDir();
    std::string csv = WriteTextFile(Path(dir, "in.csv"), "time,site,temp\n1000,bj,20\n");

    std::string existing = Path(dir, "existing.tsfile");
    WriteTextFile(existing, "sentinel-content");
    std::string before = ReadFileBytes(existing);
    CliResult conflict = RunCli({"write", "--table", "t", "--tag", "site", "STRING",
                                 "--field", "temp", "FLOAT", "-i", csv, "-o", existing});
    EXPECT_EQ(conflict.code, 3) << conflict.err;
    EXPECT_EQ(ReadFileBytes(existing), before) << "既有目标不得被改动";

    CliResult nodir = RunCli({"write", "--table", "t", "--tag", "site", "STRING",
                              "--field", "temp", "FLOAT", "-i", csv,
                              "-o", Path(dir, "no-such-dir/out.tsfile")});
    EXPECT_EQ(nodir.code, 3) << "父目录不自动创建：" << nodir.err;

    std::string ok = Path(dir, "ok.tsfile");
    CliResult silent = RunCli({"write", "--table", "t", "--tag", "site", "STRING",
                               "--field", "temp", "FLOAT", "-i", csv, "-o", ok});
    ASSERT_EQ(silent.code, 0) << silent.err;
    EXPECT_TRUE(silent.out.empty());
    EXPECT_TRUE(silent.err.empty()) << "成功且无 -v 时应完全静默：" << silent.err;

    std::string ok2 = Path(dir, "ok2.tsfile");
    CliResult verbose = RunCli({"write", "--table", "t", "--tag", "site", "STRING",
                                "--field", "temp", "FLOAT", "-i", csv, "-o", ok2, "-v"});
    ASSERT_EQ(verbose.code, 0) << verbose.err;
    EXPECT_TRUE(verbose.out.empty()) << "-v 摘要写 stderr，stdout 应空";
    EXPECT_TRUE(Contains(verbose.err, "created model=table")) << verbose.err;
    EXPECT_TRUE(Contains(verbose.err, "rows=1")) << verbose.err;
    EXPECT_TRUE(Contains(verbose.err, "output=")) << verbose.err;
    EXPECT_TRUE(Contains(verbose.err, "column=")) << "应含列物理设置行：" << verbose.err;
    unlink(csv.c_str());
    unlink(existing.c_str());
    unlink(ok.c_str());
    unlink(ok2.c_str());
}

}  // namespace
