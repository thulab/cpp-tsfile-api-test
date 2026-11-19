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

// RN-143 用例 21–27：stats / count / sketch（需求 3.2.4–3.2.6）——black-box

#include <gtest/gtest.h>

#include <cmath>
#include <cstdio>
#include <map>
#include <string>
#include <vector>

#include "cli_test_support.h"

namespace {

using cli_test::CliResult;
using cli_test::Contains;
using cli_test::MakeTempDir;
using cli_test::ReadFileBytes;
using cli_test::RunCli;
using cli_test::SplitCsvRow;
using cli_test::SplitCsvRows;
using cli_test::WriteEmptyTableFixture;
using cli_test::WriteTableFixture;
using cli_test::WriteTreeFixture;

std::string Path(const std::string &dir, const std::string &name) {
    return dir + "/" + name;
}

// 多类型夹具（用例 22 用）：INT32/INT64/FLOAT/DOUBLE/BOOLEAN/STRING/TEXT/BLOB
std::string WriteTypeMatrixFixture(const std::string &path) {
    storage::libtsfile_init();
    storage::WriteFile file;
    const int flags = O_WRONLY | O_CREAT | O_TRUNC;
    if (file.create(path, flags, 0666) != common::E_OK) {
        return std::string();
    }
    auto *schema = new storage::TableSchema(
        "types",
        std::vector<common::ColumnSchema>{
            common::ColumnSchema("site", common::STRING,
                                 common::ColumnCategory::TAG),
            common::ColumnSchema("i32", common::INT32,
                                 common::ColumnCategory::FIELD),
            common::ColumnSchema("i64", common::INT64,
                                 common::ColumnCategory::FIELD),
            common::ColumnSchema("f32", common::FLOAT,
                                 common::ColumnCategory::FIELD),
            common::ColumnSchema("f64", common::DOUBLE,
                                 common::ColumnCategory::FIELD),
            common::ColumnSchema("flag", common::BOOLEAN,
                                 common::ColumnCategory::FIELD),
            common::ColumnSchema("label", common::STRING,
                                 common::ColumnCategory::FIELD),
            common::ColumnSchema("note", common::TEXT,
                                 common::ColumnCategory::FIELD),
            common::ColumnSchema("payload", common::BLOB,
                                 common::ColumnCategory::FIELD),
        });
    auto *writer = new storage::TsFileTableWriter(&file, schema);
    std::vector<std::string> names = {"site",  "i32",   "i64", "f32", "f64",
                                      "flag",  "label", "note", "payload"};
    std::vector<common::TSDataType> types = {
        common::STRING, common::INT32,  common::INT64,  common::FLOAT,
        common::DOUBLE, common::BOOLEAN, common::STRING, common::TEXT,
        common::BLOB};
    std::vector<common::ColumnCategory> cats = {
        common::ColumnCategory::TAG,   common::ColumnCategory::FIELD,
        common::ColumnCategory::FIELD, common::ColumnCategory::FIELD,
        common::ColumnCategory::FIELD, common::ColumnCategory::FIELD,
        common::ColumnCategory::FIELD, common::ColumnCategory::FIELD,
        common::ColumnCategory::FIELD};
    for (int row = 0; row < 3; ++row) {
        storage::Tablet tablet("types", names, types, cats, 1);
        tablet.add_timestamp(0, 1000 + row * 1000);
        tablet.add_value(0, "site", "s1");
        tablet.add_value(0, "i32", static_cast<int32_t>(row + 1));
        tablet.add_value(0, "i64", static_cast<int64_t>(row + 10));
        tablet.add_value(0, "f32", static_cast<float>(row) + 0.5f);
        tablet.add_value(0, "f64", static_cast<double>(row) + 1.25);
        tablet.add_value(0, "flag", row % 2 == 0);
        tablet.add_value(0, "label", row == 0 ? "aa" : (row == 1 ? "bb" : "cc"));
        tablet.add_value(0, "note", std::string("note-") + std::to_string(row));
        tablet.add_value(0, "payload", std::string("\x01\x02\x03"));
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

// 把 stats/count 的 csv 输出转成 {列名: 值} 列表
std::vector<std::map<std::string, std::string>> RowsAsMaps(
    const std::string &csv_out) {
    std::vector<std::string> rows = SplitCsvRows(csv_out);
    std::vector<std::map<std::string, std::string>> out;
    if (rows.empty()) {
        return out;
    }
    std::vector<std::string> header = SplitCsvRow(rows[0]);
    for (size_t i = 1; i < rows.size(); ++i) {
        std::vector<std::string> cells = SplitCsvRow(rows[i]);
        std::map<std::string, std::string> m;
        for (size_t j = 0; j < header.size() && j < cells.size(); ++j) {
            m[header[j]] = cells[j];
        }
        out.push_back(m);
    }
    return out;
}

double Num(const std::string &s) { return std::stod(s); }

// 用例 21：stats 字段真值与按实体拆分（需求 3.2.4）
TEST(CliStatsTest, StatsFieldsAndEntitySplit) {
    std::string dir = MakeTempDir();
    std::string table = WriteTableFixture(Path(dir, "table.tsfile"), 12);
    ASSERT_TRUE(cli_test::FileExists(table));

    CliResult r = RunCli({"stats", "-t", "sensors", "-m", "temp", "-f", "csv", table});
    ASSERT_EQ(r.code, 0) << r.err;
    std::vector<std::map<std::string, std::string>> rows = RowsAsMaps(r.out);
    ASSERT_EQ(rows.size(), 2u) << r.out;  // 两个 TAG 实体各一行

    // 定位 beijing / shanghai 两实体
    std::map<std::string, std::string> beijing, shanghai;
    for (auto &m : rows) {
        if (m.count("tag.site") && m["tag.site"] == "beijing") {
            beijing = m;
        } else if (m.count("tag.site") && m["tag.site"] == "shanghai") {
            shanghai = m;
        }
    }
    ASSERT_FALSE(beijing.empty()) << r.out;
    ASSERT_FALSE(shanghai.empty()) << r.out;

    for (const auto &kv : std::vector<std::pair<std::map<std::string, std::string>, double>>{
             {beijing, 0.0}, {shanghai, 1.0}}) {
        std::map<std::string, std::string> m = kv.first;
        double offset = kv.second;
        ASSERT_TRUE(m.count("field") && m["field"] == "temp") << r.out;
        EXPECT_EQ(m["data_type"], "FLOAT");
        EXPECT_EQ(m["non_null_count"], "6");
        EXPECT_NEAR(Num(m["min"]), 20.0 + offset, 1e-6);
        EXPECT_NEAR(Num(m["max"]), 30.0 + offset, 1e-6);
        EXPECT_NEAR(Num(m["first"]), 20.0 + offset, 1e-6);
        EXPECT_NEAR(Num(m["last"]), 30.0 + offset, 1e-6);
        EXPECT_NEAR(Num(m["sum"]), 150.0 + 6.0 * offset, 1e-6);
        EXPECT_NEAR(Num(m["min_time"]), 1000.0 + offset * 1000.0, 1e-6);
        EXPECT_NEAR(Num(m["max_time"]), 11000.0 + offset * 1000.0, 1e-6);
        EXPECT_TRUE(m.count("stats_source")) << r.out;
        EXPECT_TRUE(m["stats_source"] == "statistics" || m["stats_source"] == "scan")
            << m["stats_source"];
    }
    unlink(table.c_str());
}

// 用例 22：stats 类型适用范围矩阵（需求 3.2.4 类型表）
TEST(CliStatsTest, TypeSupportMatrix) {
    std::string dir = MakeTempDir();
    std::string types = WriteTypeMatrixFixture(Path(dir, "types.tsfile"));
    ASSERT_FALSE(types.empty()) << "fixture write failed";

    CliResult r = RunCli({"stats", "-t", "types", "-f", "csv", types});
    ASSERT_EQ(r.code, 0) << r.err;
    std::vector<std::map<std::string, std::string>> rows = RowsAsMaps(r.out);
    std::map<std::string, std::map<std::string, std::string>> by_field;
    for (auto &m : rows) {
        by_field[m["field"]] = m;
    }
    auto get = [&](const std::string &f, const std::string &k) {
        return by_field[f][k];
    };

    // INT32/FLOAT/DOUBLE：min/max/first/last/sum 均有值
    for (const char *f : {"i32", "f32", "f64"}) {
        EXPECT_FALSE(get(f, "sum").empty()) << f;
        EXPECT_FALSE(get(f, "min").empty()) << f;
        EXPECT_FALSE(get(f, "max").empty()) << f;
    }
    // INT64：sum 为 null
    EXPECT_EQ(get("i64", "sum"), "\\N") << r.out;
    // BOOLEAN：只支持 first/last（min/max 为 null），sum = true 计数
    EXPECT_EQ(get("flag", "min"), "\\N") << r.out;
    EXPECT_EQ(get("flag", "max"), "\\N") << r.out;
    EXPECT_FALSE(get("flag", "first").empty());
    EXPECT_FALSE(get("flag", "last").empty());
    EXPECT_EQ(get("flag", "sum"), "2") << "true 值计数：" << r.out;
    // STRING：min/max/first/last，sum null
    EXPECT_FALSE(get("label", "min").empty());
    EXPECT_FALSE(get("label", "max").empty());
    EXPECT_EQ(get("label", "sum"), "\\N") << r.out;
    // TEXT：只支持 first/last
    EXPECT_EQ(get("note", "min"), "\\N") << r.out;
    EXPECT_EQ(get("note", "max"), "\\N") << r.out;
    EXPECT_EQ(get("note", "sum"), "\\N") << r.out;
    EXPECT_FALSE(get("note", "first").empty());
    // BLOB：五项值统计均为 null
    for (const char *k : {"min", "max", "first", "last", "sum"}) {
        EXPECT_EQ(get("payload", k), "\\N") << "payload." << k << " " << r.out;
    }
    // 计数与时间范围对所有类型都适用
    for (auto &kv : by_field) {
        EXPECT_EQ(kv.second.at("non_null_count"), "3") << kv.first;
    }
    unlink(types.c_str());
}

// 用例 23：stats_source 来源标注与补算（需求 3.2.4 + 非功能性能边界）
TEST(CliStatsTest, StatsSourceAndValueCorrectness) {
    std::string dir = MakeTempDir();
    std::string table = WriteTableFixture(Path(dir, "table.tsfile"), 12);
    std::string tree = WriteTreeFixture(Path(dir, "tree.tsfile"), 12);
    ASSERT_TRUE(cli_test::FileExists(table));

    CliResult r = RunCli({"stats", "-t", "sensors", "-m", "temp", "-f", "csv", table});
    ASSERT_EQ(r.code, 0) << r.err;
    std::vector<std::map<std::string, std::string>> rows = RowsAsMaps(r.out);
    ASSERT_EQ(rows.size(), 2u);
    for (auto &m : rows) {
        EXPECT_TRUE(m["stats_source"] == "statistics" || m["stats_source"] == "scan")
            << m["stats_source"];
        // 计数真值（与夹具一致，无论来源是 statistics 还是 scan 都必须正确）
        EXPECT_EQ(m["non_null_count"], "6");
    }

    // 树模型：空统计来源不掩盖真值
    CliResult tr = RunCli({"stats", "-d", "root.test.d1", "-f", "csv", tree});
    ASSERT_EQ(tr.code, 0) << tr.err;
    std::vector<std::map<std::string, std::string>> trows = RowsAsMaps(tr.out);
    ASSERT_EQ(trows.size(), 2u) << tr.out;  // m1, m2
    for (auto &m : trows) {
        EXPECT_EQ(m["non_null_count"], "12") << tr.out;
        EXPECT_EQ(m["min_time"], "1000") << tr.out;
        EXPECT_EQ(m["max_time"], "12000") << tr.out;
    }
    unlink(table.c_str());
    unlink(tree.c_str());
}

// 用例 23b：stats 的 null_count 必须给出数值（需求示例全部为数字，且 §3.2.4「统计缺失或不可靠时扫描补算，不能用默认值代替」）
//  2026-09-29 实测：statistics 路径下 null_count 恒为 \N → 待用户裁决
TEST(CliStatsTest, NullCountIsAlwaysNumeric) {
    std::string dir = MakeTempDir();
    std::string table = WriteTableFixture(Path(dir, "table.tsfile"), 12);
    ASSERT_TRUE(cli_test::FileExists(table));

    CliResult temp = RunCli({"stats", "-t", "sensors", "-m", "temp", "-f", "csv", table});
    ASSERT_EQ(temp.code, 0) << temp.err;
    std::vector<std::map<std::string, std::string>> rows = RowsAsMaps(temp.out);
    ASSERT_EQ(rows.size(), 2u) << temp.out;
    for (auto &m : rows) {
        EXPECT_NE(m["null_count"], "\\N")
            << "temp 无空值，null_count 应为 0（需求示例：「non_null_count 2 / null_count 0」）；实际："
            << m["null_count"] << " stats_source=" << m["stats_source"];
        EXPECT_EQ(m["null_count"], "0") << temp.out;
    }

    // note 列在 time=3000 为空值 → beijing 实体应有 1 个空值
    CliResult note = RunCli({"stats", "-t", "sensors", "-m", "note", "-f", "csv", table});
    ASSERT_EQ(note.code, 0) << note.err;
    std::vector<std::map<std::string, std::string>> nrows = RowsAsMaps(note.out);
    ASSERT_EQ(nrows.size(), 2u) << note.out;
    for (auto &m : nrows) {
        if (m["tag.site"] == "beijing") {
            EXPECT_EQ(m["null_count"], "1")
                << "beijing 的 note 有 1 个空值（time=3000）；实际：" << m["null_count"];
        } else {
            EXPECT_EQ(m["null_count"], "0") << note.out;
        }
    }
    unlink(table.c_str());
}

// 用例 24：count 计数语义与 entity_count（需求 3.2.5）
TEST(CliCountTest, CountFieldsAndEntitySemantics) {
    std::string dir = MakeTempDir();
    std::string table = WriteTableFixture(Path(dir, "table.tsfile"), 12);
    std::string tree = WriteTreeFixture(Path(dir, "tree.tsfile"), 12);
    ASSERT_TRUE(cli_test::FileExists(table));

    CliResult r = RunCli({"count", "-t", "sensors", "-f", "csv", table});
    ASSERT_EQ(r.code, 0) << r.err;
    std::vector<std::map<std::string, std::string>> rows = RowsAsMaps(r.out);
    ASSERT_EQ(rows.size(), 3u) << r.out;  // site / temp / note 三列，无 summary 行

    std::map<std::string, std::map<std::string, std::string>> by_col;
    for (auto &m : rows) {
        by_col[m["column"]] = m;
        EXPECT_EQ(m["row_count"], "12") << r.out;
        EXPECT_EQ(m["entity_count"], "2") << "TAG 组合去重后 2 个实体：" << r.out;
        // non_null + null == row_count
        EXPECT_EQ(std::stoll(m["non_null_count"]) + std::stoll(m["null_count"]), 12)
            << r.out;
        EXPECT_TRUE(m.count("time_source")) << r.out;
    }
    EXPECT_EQ(by_col["site"]["category"], "TAG");
    EXPECT_EQ(by_col["temp"]["category"], "FIELD");
    EXPECT_EQ(by_col["note"]["null_count"], "1") << "time=3000 的 note 为 NULL";

    // 树模型：entity_count 为 null
    CliResult tr = RunCli({"count", "-d", "root.test.d1", "-f", "csv", tree});
    ASSERT_EQ(tr.code, 0) << tr.err;
    std::vector<std::map<std::string, std::string>> trows = RowsAsMaps(tr.out);
    ASSERT_EQ(trows.size(), 2u) << tr.out;
    for (auto &m : trows) {
        EXPECT_EQ(m["entity_count"], "\\N") << "树模型 entity_count 应为 null：" << tr.out;
        EXPECT_EQ(m["row_count"], "12");
    }
    unlink(table.c_str());
    unlink(tree.c_str());
}

// 用例 25：count 空表、time_source 与选项拒绝（需求 3.2.5）
TEST(CliCountTest, EmptyTableAndOptionRejection) {
    std::string dir = MakeTempDir();
    std::string empty = WriteEmptyTableFixture(Path(dir, "empty.tsfile"));
    std::string table = WriteTableFixture(Path(dir, "table.tsfile"));
    ASSERT_TRUE(cli_test::FileExists(empty));

    CliResult e = RunCli({"count", "-t", "sensors", "-f", "csv", empty});
    ASSERT_EQ(e.code, 0) << e.err;
    std::vector<std::map<std::string, std::string>> rows = RowsAsMaps(e.out);
    ASSERT_EQ(rows.size(), 2u) << e.out;
    for (auto &m : rows) {
        EXPECT_EQ(m["row_count"], "0");
        EXPECT_EQ(m["entity_count"], "0");
        EXPECT_EQ(m["min_time"], "\\N") << "空表 min_time 应为 null（机器格式）";
        EXPECT_EQ(m["max_time"], "\\N");
        EXPECT_EQ(m["time_source"], "\\N");
    }

    CliResult start = RunCli({"count", "-t", "sensors", "-m", "temp", "--start", "1000", table});
    EXPECT_EQ(start.code, 1) << start.err;

    CliResult tag = RunCli({"count", "-t", "sensors", "--tag-filter", "site", "eq", "beijing", table});
    EXPECT_EQ(tag.code, 1) << tag.err;

    CliResult dup = RunCli({"count", "-t", "sensors", "-m", "temp", "-m", "temp", table});
    EXPECT_EQ(dup.code, 1) << dup.err;
    unlink(empty.c_str());
    unlink(table.c_str());
}

// 用例 26：sketch 完整物理结构输出与 -o 一致性（需求 3.2.6）
// 树模型给出完整物理布局（marker/Chunk/页/尾部）；表模型的输出形态见 26b
TEST(CliSketchTest, PrintsCompletePhysicalLayout) {
    std::string dir = MakeTempDir();
    std::string tree = WriteTreeFixture(Path(dir, "tree.tsfile"));
    ASSERT_TRUE(cli_test::FileExists(tree));

    CliResult r = RunCli({"sketch", tree});
    ASSERT_EQ(r.code, 0) << r.err;
    EXPECT_TRUE(Contains(r.out, "TsFile Sketch")) << r.out.substr(0, 300);
    EXPECT_TRUE(Contains(r.out, "[magic head] TsFile")) << r.out.substr(0, 400);
    EXPECT_TRUE(Contains(r.out, "[magic tail] TsFile")) << "缺尾部标记：" << r.out.substr(0, 400);
    EXPECT_TRUE(Contains(r.out, "END of TsFile")) << "缺 END of TsFile";
    EXPECT_TRUE(Contains(r.out, "[Chunk]") || Contains(r.out, "[ChunkGroup]"))
        << "缺 Chunk/ChunkGroup 层级" << r.out.substr(0, 600);
    // 不是四列表格替代品
    EXPECT_FALSE(Contains(r.out, "model,object,column")) << "sketch 不得改造成结果表";

    // -o 模式：stdout 为空、文件内容与 stdout 模式逐字节一致
    std::string out_path = Path(dir, "sketch.txt");
    CliResult o = RunCli({"sketch", "-o", out_path, tree});
    ASSERT_EQ(o.code, 0) << o.err;
    EXPECT_TRUE(o.out.empty()) << "-o 模式下 stdout 应为空";
    EXPECT_EQ(ReadFileBytes(out_path), r.out) << "-o 文件内容须与 stdout 模式逐字节一致";
    unlink(tree.c_str());
    unlink(out_path.c_str());
}

// 用例 26b：表模型 sketch 的输出形态（需求 3.2.6 / D-191）
//  2026-09-29 实测（缺陷态）：表模型仅输出 `file path` 与 `model: table` 两行，不展开 Chunk/Page/尾部 → 已提报 TsFile-156
//  2026-10-09 复测（#838+#978 修复后）：sketch 输出真实物理布局，表模型与 tree 一致（magic head/ChunkGroup/Page/
//  metadata/magic tail/END of TsFile）；旧桩独有行 `model: table` 随修复移除，需求无此要求，断言更新为布局要素。
TEST(CliSketchTest, TableModelSketchOutputShape) {
    std::string dir = MakeTempDir();
    std::string table = WriteTableFixture(Path(dir, "table.tsfile"));
    ASSERT_TRUE(cli_test::FileExists(table));

    CliResult r = RunCli({"sketch", table});
    ASSERT_EQ(r.code, 0) << r.err;
    EXPECT_TRUE(Contains(r.out, "TsFile Sketch")) << r.out;
    EXPECT_TRUE(Contains(r.out, "file path:")) << r.out;
    EXPECT_TRUE(Contains(r.out, "[magic tail] TsFile")) << r.out;
    EXPECT_TRUE(Contains(r.out, "[ChunkGroup]")) << r.out;
    EXPECT_FALSE(Contains(r.out, "model,object,column")) << r.out;
    unlink(table.c_str());
}

// 用例 27：sketch 选项约束与输出原子性（需求 3.2.6 异常表）
TEST(CliSketchTest, OutputOptionAtomicity) {
    std::string dir = MakeTempDir();
    std::string table = WriteTableFixture(Path(dir, "table.tsfile"));
    ASSERT_TRUE(cli_test::FileExists(table));

    CliResult fmt = RunCli({"sketch", "-f", "csv", table});
    EXPECT_EQ(fmt.code, 1) << fmt.err;

    CliResult force_alone = RunCli({"sketch", "--force", table});
    EXPECT_EQ(force_alone.code, 1) << force_alone.err;

    std::string existing = Path(dir, "existing.txt");
    {
        std::ofstream out(existing.c_str());
        out << "old\n";
    }
    std::string before = ReadFileBytes(existing);
    CliResult conflict = RunCli({"sketch", "-o", existing, table});
    EXPECT_EQ(conflict.code, 3) << conflict.err;
    EXPECT_EQ(ReadFileBytes(existing), before) << "失败时既有目标不得被改动";

    CliResult same = RunCli({"sketch", "-o", table, "--force", table});
    EXPECT_EQ(same.code, 3) << "同源输出应拒绝：" << same.err;

    std::string link = Path(dir, "link.txt");
    ASSERT_EQ(symlink(existing.c_str(), link.c_str()), 0);
    CliResult sym = RunCli({"sketch", "-o", link, table});
    EXPECT_EQ(sym.code, 3) << "符号链接不得被替换：" << sym.err;

    std::string fresh = Path(dir, "fresh.txt");
    CliResult ok = RunCli({"sketch", "-o", fresh, table});
    EXPECT_EQ(ok.code, 0) << ok.err;
    EXPECT_TRUE(cli_test::FileExists(fresh));
    unlink(table.c_str());
    unlink(existing.c_str());
    unlink(link.c_str());
    unlink(fresh.c_str());
}

}  // namespace
