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

// RN-143 用例 9–13：结果格式与序列化契约（需求 3.1.3）——black-box

#include <gtest/gtest.h>

#include <cmath>
#include <fstream>
#include <string>
#include <vector>

#include "cli_test_support.h"

namespace {

using cli_test::CliResult;
using cli_test::Contains;
using cli_test::MakeTempDir;
using cli_test::ParseFlatJson;
using cli_test::RunCli;
using cli_test::SplitCsvRows;
using cli_test::SplitLines;
using cli_test::SplitWhitespace;
using cli_test::WriteTableFixture;
using cli_test::WriteTreeFixture;

std::string Path(const std::string &dir, const std::string &name) {
    return dir + "/" + name;
}

// 含特殊字符的 TEXT 夹具：反斜杠 / 换行 / 制表符 / 长文本 / 空串 / 逗号引号
std::string WriteEscapesFixture(const std::string &path) {
    storage::libtsfile_init();
    storage::WriteFile file;
    const int flags = O_WRONLY | O_CREAT | O_TRUNC;
    if (file.create(path, flags, 0666) != common::E_OK) {
        return std::string();
    }
    auto *schema = new storage::TableSchema(
        "escapes",
        std::vector<common::ColumnSchema>{
            common::ColumnSchema("site", common::STRING,
                                 common::ColumnCategory::TAG),
            common::ColumnSchema("note", common::TEXT,
                                 common::ColumnCategory::FIELD),
            common::ColumnSchema("big", common::INT64,
                                 common::ColumnCategory::FIELD),
        });
    auto *writer = new storage::TsFileTableWriter(&file, schema);
    struct Row {
        int64_t t;
        const char *note;
        int64_t big;
    };
    std::string long_text(200, 'x');
    std::vector<Row> rows = {
        {1000, "back\\slash", 9007199254740993LL},
        {2000, "line1\nline2", 42},
        {3000, "tab\there", 43},
        {4000, "", 44},
        {5000, "comma,quote\"end", 45},
        {7000, "\\leading", 47},
    };
    for (const Row &r : rows) {
        storage::Tablet tablet("escapes", {"site", "note", "big"},
                               {common::STRING, common::TEXT, common::INT64},
                               {common::ColumnCategory::TAG,
                                common::ColumnCategory::FIELD,
                                common::ColumnCategory::FIELD},
                               1);
        tablet.add_timestamp(0, r.t);
        tablet.add_value(0, "site", "s1");
        tablet.add_value(0, "note", r.note);
        tablet.add_value(0, "big", r.big);
        if (writer->write_table(tablet) != common::E_OK) {
            delete writer;
            delete schema;
            return std::string();  // 夹具写入失败：显式暴露给用例断言
        }
    }
    {
        storage::Tablet tablet("escapes", {"site", "note", "big"},
                               {common::STRING, common::TEXT, common::INT64},
                               {common::ColumnCategory::TAG,
                                common::ColumnCategory::FIELD,
                                common::ColumnCategory::FIELD},
                               1);
        tablet.add_timestamp(0, 8000);
        tablet.add_value(0, "site", "s1");
        tablet.add_value(0, "note", long_text.c_str());
        tablet.add_value(0, "big", static_cast<int64_t>(46));
        if (writer->write_table(tablet) != common::E_OK) {
            delete writer;
            delete schema;
            return std::string();
        }
    }
    writer->flush();
    writer->close();
    delete writer;
    delete schema;
    return path;
}

// 用例 9：table 表头/零行/长值不截断（需求 3.1.3）
TEST(CliFormatTest, TableHeaderZeroRowAndLongValue) {
    std::string dir = MakeTempDir();
    std::string escapes = WriteEscapesFixture(Path(dir, "escapes.tsfile"));
    std::string tree = WriteTreeFixture(Path(dir, "tree.tsfile"));
    ASSERT_FALSE(escapes.empty()) << "fixture write failed";
    ASSERT_TRUE(cli_test::FileExists(tree));

    // 表头恒显示；零行只显示表头
    cli_test::CliResult full = RunCli({"cat", "-t", "escapes", "-f", "table", escapes});
    ASSERT_EQ(full.code, 0) << full.err;
    std::vector<std::string> full_lines = SplitLines(full.out);
    ASSERT_FALSE(full_lines.empty());
    std::vector<std::string> header = SplitWhitespace(full_lines[0]);
    ASSERT_EQ(header.size(), 4u) << full.out;  // time, site, note, big
    EXPECT_EQ(header[0], "time");
    EXPECT_EQ(header[1], "site");
    EXPECT_EQ(header[2], "note");
    EXPECT_EQ(header[3], "big");

    cli_test::CliResult zero =
        RunCli({"head", "-t", "escapes", "-n", "0", "-f", "table", escapes});
    ASSERT_EQ(zero.code, 0) << zero.err;
    std::vector<std::string> zero_lines = SplitLines(zero.out);
    EXPECT_EQ(zero_lines.size(), 1u) << "zero rows must keep header only: " << zero.out;

    // 长值不截断（需求 D-055：单元格不截断）
    EXPECT_TRUE(Contains(full.out, std::string(200, 'x'))) << "long value truncated";
    unlink(escapes.c_str());
    unlink(tree.c_str());
}

// 用例 9b：table 格式控制字符可见转义（需求 3.1.3 与设计决策 D-056）
//  EXPECT：反斜杠/换行/回车/制表符分别显示为 \\、\n、\r、\t，一个逻辑记录一个物理行
//  2026-09-29 实测（缺陷态）：实现按原字符输出（未转义），换行会把一条记录拆成两行 → 已提报 TsFile-154
//  2026-10-09 复测（#974 修复后）：三项转义断言全部通过；行数修正 9→8——夹具为 7 条数据 + 1 行表头，
//  原期望 9 把缺陷态「换行拆断」多出的 1 行误当基准，属测试代码笔误，与需求 D-056「一条逻辑记录对应一条物理表格行」相悖
TEST(CliFormatTest, TableVisibleEscapingContract) {
    std::string dir = MakeTempDir();
    std::string escapes = WriteEscapesFixture(Path(dir, "escapes.tsfile"));
    ASSERT_FALSE(escapes.empty()) << "fixture write failed";

    cli_test::CliResult full = RunCli({"cat", "-t", "escapes", "-f", "table", escapes});
    ASSERT_EQ(full.code, 0) << full.err;

    EXPECT_TRUE(Contains(full.out, "back\\\\slash"))
        << "table 格式应以 \\\\ 显示反斜杠（D-056）";
    EXPECT_TRUE(Contains(full.out, "line1\\nline2"))
        << "table 格式应以 \\n 显示换行（D-056）";
    EXPECT_TRUE(Contains(full.out, "tab\\there"))
        << "table 格式应以 \\t 显示制表符（D-056）";

    // 一个逻辑记录对应一个物理行（控制字符不得造成物理换行）
    std::vector<std::string> lines = SplitLines(full.out);
    EXPECT_EQ(lines.size(), 8u)
        << "7 条数据 + 1 行表头；实际 " << lines.size() << " 行 → 存在物理换行";
    unlink(escapes.c_str());
}

// 用例 10：NDJSON 逐行对象与类型/null 契约（需求 3.1.3）
TEST(CliFormatTest, NdjsonObjectAndTypeContract) {
    std::string dir = MakeTempDir();
    std::string table = WriteTableFixture(Path(dir, "table.tsfile"));
    std::string tree = WriteTreeFixture(Path(dir, "tree.tsfile"));
    ASSERT_TRUE(cli_test::FileExists(table));

    cli_test::CliResult r = RunCli({"cat", "-t", "sensors", "-f", "ndjson", table});
    ASSERT_EQ(r.code, 0) << r.err;
    std::vector<std::string> lines = SplitLines(r.out);
    ASSERT_EQ(lines.size(), 12u) << r.out;
    for (const std::string &line : lines) {
        ASSERT_FALSE(line.empty());
        EXPECT_EQ(line[0], '{') << line;
        EXPECT_EQ(line[line.size() - 1], '}') << line;
        std::map<std::string, std::string> obj = ParseFlatJson(line);
        EXPECT_EQ(obj.size(), 4u) << line;  // time, site, temp, note
        ASSERT_TRUE(obj.count("time") == 1) << line;
        // time / INT64 使用十进制字符串（带引号）
        EXPECT_EQ(obj["time"][0], '"') << line;
    }
    // 行序 = 底层读取顺序（需求设计决策 D-023：不做全局排序）：按内容断言而非按位置
    int null_notes = 0;
    for (const std::string &line : lines) {
        std::map<std::string, std::string> obj = ParseFlatJson(line);
        if (obj["note"] == "null") {
            ++null_notes;
            EXPECT_EQ(obj["time"], "\"3000\"") << line;
        }
    }
    EXPECT_EQ(null_notes, 1) << "exactly one NULL note row expected: " << r.out;
    // FLOAT 为裸数字（不加引号）
    std::map<std::string, std::string> any_row = ParseFlatJson(lines[0]);
    EXPECT_FALSE(any_row["temp"].empty());
    EXPECT_NE(any_row["temp"][0], '"') << lines[0];

    // 零行 → 空字节流
    cli_test::CliResult zero = RunCli({"head", "-t", "sensors", "-n", "0", "-f", "ndjson", table});
    ASSERT_EQ(zero.code, 0);
    EXPECT_TRUE(zero.out.empty()) << "ndjson zero-row must be empty stream: " << zero.out;

    // 树模型同样逐行对象
    cli_test::CliResult tr = RunCli({"cat", "-f", "ndjson", tree});
    ASSERT_EQ(tr.code, 0) << tr.err;
    std::vector<std::string> tlines = SplitLines(tr.out);
    ASSERT_EQ(tlines.size(), 12u) << tr.out;
    std::map<std::string, std::string> tobj = ParseFlatJson(tlines[0]);
    EXPECT_EQ(tobj.size(), 3u) << tlines[0];  // time, m1, m2
    unlink(table.c_str());
    unlink(tree.c_str());
}

// 用例 11：CSV 表头/RFC4180/空值与类型词法契约（需求 3.1.3）
TEST(CliFormatTest, CsvRfc4180NullAndTypeContract) {
    std::string dir = MakeTempDir();
    std::string escapes = WriteEscapesFixture(Path(dir, "escapes.tsfile"));
    std::string table = WriteTableFixture(Path(dir, "table.tsfile"));
    ASSERT_TRUE(cli_test::FileExists(escapes));

    cli_test::CliResult r = RunCli({"cat", "-t", "escapes", "-f", "csv", escapes});
    ASSERT_EQ(r.code, 0) << r.err;

    // 无 BOM、LF 行结束
    ASSERT_GE(r.out.size(), 3u);
    EXPECT_NE(static_cast<unsigned char>(r.out[0]), 0xEF) << "unexpected BOM";
    EXPECT_EQ(r.out.find("\r\n"), std::string::npos) << "CSV must use LF";

    std::vector<std::string> rows = cli_test::SplitCsvRows(r.out);
    ASSERT_EQ(rows.size(), 8u) << r.out;  // 表头 + 7 数据行（含多行字段仍为一行）
    EXPECT_EQ(rows[0], "time,site,note,big");

    std::vector<std::string> row1 = cli_test::SplitCsvRow(rows[1]);
    ASSERT_EQ(row1.size(), 4u) << rows[1];
    EXPECT_EQ(row1[0], "1000");
    EXPECT_EQ(row1[2], "back\\slash") << "mid-string backslash must stay as-is";
    EXPECT_EQ(row1[3], "9007199254740993") << "INT64 must keep exact decimal";

    // 换行按 RFC4180 引用（多行字段），制表符无需引用
    EXPECT_EQ(cli_test::SplitCsvRow(rows[2])[2], "line1\nline2") << rows[2];
    EXPECT_EQ(cli_test::SplitCsvRow(rows[3])[2], "tab\there") << rows[3];
    // 空串 → ""（引号包裹）
    EXPECT_EQ(cli_test::SplitCsvRow(rows[4])[2], "") << rows[4];
    // 逗号与引号按 RFC4180 引用
    EXPECT_EQ(cli_test::SplitCsvRow(rows[5])[2], "comma,quote\"end") << rows[5];

    // null → 未加引号的 \N（表夹具 time=3000 的 note 为 NULL）
    cli_test::CliResult t = RunCli({"cat", "-t", "sensors", "-f", "csv", table});
    ASSERT_EQ(t.code, 0) << t.err;
    std::vector<std::string> trows = cli_test::SplitCsvRows(t.out);
    ASSERT_EQ(trows.size(), 13u) << t.out;
    bool found_null = false;
    for (size_t i = 1; i < trows.size(); ++i) {
        std::vector<std::string> cells = cli_test::SplitCsvRow(trows[i]);
        if (cells[0] == "3000") {
            EXPECT_EQ(cells[3], "\\N") << trows[i];
            found_null = true;
        }
    }
    EXPECT_TRUE(found_null) << t.out;
    unlink(escapes.c_str());
    unlink(table.c_str());
}

// 用例 11b：CSV 行首反斜杠加倍规则（需求 3.1.3 与设计决策 D-047）
//  2026-09-29 实测：实现未加倍（原样输出 \leading）→ 待用户裁决
TEST(CliFormatTest, CsvLeadingBackslashRule) {
    std::string dir = MakeTempDir();
    std::string escapes = WriteEscapesFixture(Path(dir, "escapes.tsfile"));
    ASSERT_FALSE(escapes.empty()) << "fixture write failed";

    cli_test::CliResult r = RunCli({"cat", "-t", "escapes", "-f", "csv", escapes});
    ASSERT_EQ(r.code, 0) << r.err;
    std::vector<std::string> rows = cli_test::SplitCsvRows(r.out);
    ASSERT_EQ(rows.size(), 8u) << r.out;
    // 中段反斜杠不加倍
    EXPECT_EQ(cli_test::SplitCsvRow(rows[1])[2], "back\\slash") << rows[1];
    // time=7000 行 note 原值为 \leading：契约要求输出 \\leading
    EXPECT_EQ(cli_test::SplitCsvRow(rows[6])[2], "\\\\leading")
        << "CSV 行首反斜杠应额外前置一个反斜杠（D-047）；实际：" << rows[6];
    unlink(escapes.c_str());
}

// 用例 11c：NULL 与字面文本 \N 的 CSV 往返可逆性（需求 3.1.3 + D-047）
//  2026-09-29 实测：CSV 输出把 NULL 与字面 \N 打成同形，再导入后字面 \N 被吞成 NULL（静默数据损坏）
//  → 已出独立缺陷报告，保持失败可见。
TEST(CliFormatTest, CsvNullLiteralRoundTrip) {
    std::string dir = MakeTempDir();
    std::string csv_path = Path(dir, "in.csv");
    {
        std::ofstream out(csv_path.c_str(), std::ios::binary | std::ios::trunc);
        out << "time,site,note\n";
        out << "1000,s1,\\N\n";      // 未加引号的 \N = NULL
        out << "2000,s1,\"\\N\"\n";  // 带引号 = 字面文本 \N
    }
    std::string t1 = Path(dir, "t1.tsfile");
    CliResult w1 = RunCli({"write", "--table", "rt", "--tag", "site", "STRING",
                           "--field", "note", "TEXT", "-i", csv_path, "-o", t1});
    ASSERT_EQ(w1.code, 0) << w1.err;

    CliResult n1 = RunCli({"cat", "-t", "rt", "-f", "ndjson", t1});
    ASSERT_EQ(n1.code, 0) << n1.err;
    EXPECT_TRUE(Contains(n1.out, "\"note\":null")) << n1.out;
    EXPECT_TRUE(Contains(n1.out, "\"note\":\"\\\\N\"")) << "原始数据应可区分：" << n1.out;

    // 第一步往返：CLI 自己的 CSV 输出
    CliResult c1 = RunCli({"cat", "-t", "rt", "-f", "csv", t1});
    ASSERT_EQ(c1.code, 0) << c1.err;
    std::vector<std::string> rows = cli_test::SplitCsvRows(c1.out);
    ASSERT_EQ(rows.size(), 3u) << c1.out;
    std::string round = Path(dir, "round.csv");
    {
        std::ofstream out(round.c_str(), std::ios::binary | std::ios::trunc);
        out << c1.out;
    }
    std::string t2 = Path(dir, "t2.tsfile");
    CliResult w2 = RunCli({"write", "--table", "rt", "--tag", "site", "STRING",
                           "--field", "note", "TEXT", "-i", round, "-o", t2});
    ASSERT_EQ(w2.code, 0) << w2.err;
    CliResult n2 = RunCli({"cat", "-t", "rt", "-f", "ndjson", t2});
    ASSERT_EQ(n2.code, 0) << n2.err;
    EXPECT_TRUE(Contains(n2.out, "\"note\":\"\\\\N\""))
        << "往返后字面文本 \\N 必须仍存在（需求：创建 CSV 与读取输出共用同一区分规则）；"
           "实测被吞成 null → 静默数据损坏。CSV 输出=" << c1.out << " 再导入后=" << n2.out;

    unlink(csv_path.c_str());
    unlink(t1.c_str());
    unlink(round.c_str());
    unlink(t2.c_str());
}

// 用例 12：三种格式同范围数据等价（需求 3.1.3）
TEST(CliFormatTest, ThreeFormatsAreEquivalent) {
    std::string dir = MakeTempDir();
    std::string table = WriteTableFixture(Path(dir, "table.tsfile"));
    ASSERT_TRUE(cli_test::FileExists(table));

    cli_test::CliResult csv = RunCli({"cat", "-t", "sensors", "-f", "csv", table});
    cli_test::CliResult ndjson = RunCli({"cat", "-t", "sensors", "-f", "ndjson", table});
    cli_test::CliResult tbl = RunCli({"cat", "-t", "sensors", "-f", "table", table});
    ASSERT_EQ(csv.code, 0) << csv.err;
    ASSERT_EQ(ndjson.code, 0) << ndjson.err;
    ASSERT_EQ(tbl.code, 0) << tbl.err;

    std::vector<std::string> csv_lines = SplitLines(csv.out);
    std::vector<std::string> json_lines = SplitLines(ndjson.out);
    std::vector<std::string> tbl_lines = SplitLines(tbl.out);
    ASSERT_EQ(csv_lines.size(), 13u);
    ASSERT_EQ(json_lines.size(), 12u);
    ASSERT_EQ(tbl_lines.size(), 13u);

    // 逐行比对 time / site / temp 的逻辑值
    for (int i = 0; i < 12; ++i) {
        std::vector<std::string> row = cli_test::SplitCsvRow(csv_lines[i + 1]);
        std::map<std::string, std::string> obj = ParseFlatJson(json_lines[i]);
        ASSERT_EQ(row.size(), 4u);
        EXPECT_EQ("\"" + row[0] + "\"", obj["time"]);
        EXPECT_EQ("\"" + row[1] + "\"", obj["site"]);
        std::vector<std::string> cols = SplitWhitespace(tbl_lines[i + 1]);
        EXPECT_EQ(cols[0], row[0]) << tbl_lines[i + 1];
    }
    unlink(table.c_str());
}

// 用例 13：默认格式与不自动推断（需求 3.1.3）
TEST(CliFormatTest, DefaultFormatAndNoInference) {
    std::string dir = MakeTempDir();
    std::string table = WriteTableFixture(Path(dir, "table.tsfile"));
    std::string out_path = Path(dir, "redirected.txt");
    ASSERT_TRUE(cli_test::FileExists(table));

    // shell 重定向到文件（非 TTY）后仍应是 table 格式
    std::string cmd = cli_test::CliPath() + " cat -t sensors " + table +
                      " > " + out_path + " 2>/dev/null";
    ASSERT_EQ(system(cmd.c_str()), 0);
    std::string redirected = cli_test::ReadFileBytes(out_path);
    EXPECT_TRUE(Contains(redirected, "time")) << redirected;
    EXPECT_TRUE(Contains(redirected, "site")) << redirected;
    EXPECT_FALSE(Contains(redirected, "{\"time\"")) << "must not switch to ndjson";
    EXPECT_FALSE(Contains(redirected, "time,site")) << "must not switch to csv";

    // export 的扩展名不参与推断
    std::string typed = Path(dir, "typed.txt");
    cli_test::CliResult r = RunCli({"export", "-t", "sensors", "--type", "csv", "-o",
                                    typed, table});
    ASSERT_EQ(r.code, 0) << r.err;
    std::string exported = cli_test::ReadFileBytes(typed);
    EXPECT_EQ(exported.rfind("time,site,temp,note", 0), 0u) << exported.substr(0, 80);
    unlink(table.c_str());
    unlink(out_path.c_str());
    unlink(typed.c_str());
}

}  // namespace
