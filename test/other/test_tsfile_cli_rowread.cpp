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

// RN-143 用例 28–40：数据获取 head/cat（需求 3.3）——black-box
// 注意（需求设计决策 D-023）：行序 = 底层读取顺序，不做全局排序；
// 因此涉及"前 N 行/第 k 行"的断言一律与"同请求全量输出的前缀"比较，不假设时间升序。

#include <gtest/gtest.h>

#include <map>
#include <string>
#include <vector>

#include "cli_test_support.h"

namespace {

using cli_test::CliResult;
using cli_test::Contains;
using cli_test::MakeTempDir;
using cli_test::ParseFlatJson;
using cli_test::RunCli;
using cli_test::RunCliCloseStdoutAfter;
using cli_test::SplitCsvRow;
using cli_test::SplitCsvRows;
using cli_test::WriteEmptyTableFixture;
using cli_test::WriteLargeTableFixture;
using cli_test::WriteNullableTagFixture;
using cli_test::WriteTableFixture;
using cli_test::WriteTreeFixture;

std::string Path(const std::string &dir, const std::string &name) {
    return dir + "/" + name;
}

std::vector<std::vector<std::string>> DataRows(const std::string &csv_out) {
    std::vector<std::string> rows = SplitCsvRows(csv_out);
    std::vector<std::vector<std::string>> cells;
    for (size_t i = 1; i < rows.size(); ++i) {  // 跳过表头
        cells.push_back(SplitCsvRow(rows[i]));
    }
    return cells;
}

// 用例 28：head 默认 10 行与 -n 生效（需求 3.3.1；D-023 行序）
TEST(CliRowReadTest, HeadDefaultLimitAndPrefix) {
    std::string dir = MakeTempDir();
    std::string table = WriteTableFixture(Path(dir, "table.tsfile"), 15);
    ASSERT_TRUE(cli_test::FileExists(table));

    CliResult full = RunCli({"cat", "-t", "sensors", "-f", "csv", table});
    ASSERT_EQ(full.code, 0) << full.err;
    std::vector<std::vector<std::string>> all_rows = DataRows(full.out);
    ASSERT_EQ(all_rows.size(), 15u);

    CliResult def = RunCli({"head", "-t", "sensors", "-f", "csv", table});
    ASSERT_EQ(def.code, 0) << def.err;
    std::vector<std::vector<std::string>> head_rows = DataRows(def.out);
    ASSERT_EQ(head_rows.size(), 10u) << "head default limit is 10";
    for (size_t i = 0; i < head_rows.size(); ++i) {
        EXPECT_EQ(head_rows[i], all_rows[i]) << "head(N) must equal full prefix";
    }

    CliResult n3 = RunCli({"head", "-t", "sensors", "-n", "3", "-f", "csv", table});
    ASSERT_EQ(n3.code, 0) << n3.err;
    std::vector<std::vector<std::string>> n3_rows = DataRows(n3.out);
    ASSERT_EQ(n3_rows.size(), 3u);
    for (size_t i = 0; i < 3; ++i) {
        EXPECT_EQ(n3_rows[i], all_rows[i]);
    }
    unlink(table.c_str());
}

// 用例 29：cat 读取全部匹配行（需求 3.3.2）
TEST(CliRowReadTest, CatReadsAllMatchingRows) {
    std::string dir = MakeTempDir();
    std::string table = WriteTableFixture(Path(dir, "table.tsfile"), 15);
    std::string tree = WriteTreeFixture(Path(dir, "tree.tsfile"), 7);
    ASSERT_TRUE(cli_test::FileExists(table));

    CliResult t = RunCli({"cat", "-t", "sensors", "-f", "csv", table});
    ASSERT_EQ(t.code, 0) << t.err;
    std::vector<std::vector<std::string>> rows = DataRows(t.out);
    ASSERT_EQ(rows.size(), 15u) << t.out;
    // 逐行校验 TAG 与 temp 真值（row i: site = i%2==0?beijing:shanghai, temp = 20+i）
    for (const auto &cells : rows) {
        ASSERT_EQ(cells.size(), 4u);
        int64_t idx = (std::stoll(cells[0]) - 1000) / 1000;
        EXPECT_EQ(cells[1], idx % 2 == 0 ? "beijing" : "shanghai");
        EXPECT_EQ(std::stod(cells[2]), 20.0 + idx);
    }

    CliResult tr = RunCli({"cat", "-f", "csv", tree});
    ASSERT_EQ(tr.code, 0) << tr.err;
    EXPECT_EQ(DataRows(tr.out).size(), 7u);
    unlink(table.c_str());
    unlink(tree.c_str());
}

// 用例 30：结果列规则与 TAG 恒在前（需求 3.1.4.2）
TEST(CliRowReadTest, ResultColumnRules) {
    std::string dir = MakeTempDir();
    std::string table = WriteTableFixture(Path(dir, "table.tsfile"));
    std::string tree = WriteTreeFixture(Path(dir, "tree.tsfile"));
    ASSERT_TRUE(cli_test::FileExists(table));

    CliResult tr = RunCli({"cat", "-m", "m1", "-f", "csv", tree});
    ASSERT_EQ(tr.code, 0) << tr.err;
    EXPECT_EQ(SplitCsvRows(tr.out)[0], "time,m1");

    // 带 -d 作用域（需求 3.3.1 示例的写法：head -d <device> -m <field>）
    CliResult tr_scoped =
        RunCli({"cat", "-d", "root.test.d1", "-m", "m1", "-f", "csv", tree});
    ASSERT_EQ(tr_scoped.code, 0) << tr_scoped.err;
    EXPECT_EQ(SplitCsvRows(tr_scoped.out)[0], "time,m1")
        << "需求示例（带 -d）期望短列名；实际：" << SplitCsvRows(tr_scoped.out)[0];

    CliResult ta = RunCli({"cat", "-t", "sensors", "-m", "temp", "-f", "csv", table});
    ASSERT_EQ(ta.code, 0) << ta.err;
    EXPECT_EQ(SplitCsvRows(ta.out)[0], "time,site,temp")
        << "TAG 不因投影移除且恒在前";

    CliResult all = RunCli({"cat", "-t", "sensors", "-f", "csv", table});
    ASSERT_EQ(all.code, 0) << all.err;
    EXPECT_EQ(SplitCsvRows(all.out)[0], "time,site,temp,note");
    unlink(table.c_str());
    unlink(tree.c_str());
}

// 用例 31：-m 投影语义与非法形式（需求 3.1.4.2）
TEST(CliRowReadTest, MeasurementProjectionRules) {
    std::string dir = MakeTempDir();
    std::string table = WriteTableFixture(Path(dir, "table.tsfile"));
    ASSERT_TRUE(cli_test::FileExists(table));

    CliResult comma = RunCli({"cat", "-t", "sensors", "-m", "temp,note", table});
    EXPECT_EQ(comma.code, 1) << comma.err;

    CliResult dup = RunCli({"cat", "-t", "sensors", "-m", "temp", "-m", "temp", table});
    EXPECT_EQ(dup.code, 1) << dup.err;

    CliResult tag = RunCli({"cat", "-t", "sensors", "-m", "site", table});
    EXPECT_EQ(tag.code, 1) << "head/cat 的 -m 只接受 FIELD：" << tag.err;

    // 输出列按参数出现顺序
    CliResult order = RunCli({"cat", "-t", "sensors", "-m", "note", "-m", "temp", "-f",
                              "csv", table});
    ASSERT_EQ(order.code, 0) << order.err;
    EXPECT_EQ(SplitCsvRows(order.out)[0], "time,site,note,temp");
    unlink(table.c_str());
}

// 用例 32：时间范围闭区间与词法校验（需求 3.1.4.3）
TEST(CliRowReadTest, TimeRangeInclusiveAndLexical) {
    std::string dir = MakeTempDir();
    std::string tree = WriteTreeFixture(Path(dir, "tree.tsfile"), 12);
    ASSERT_TRUE(cli_test::FileExists(tree));

    CliResult point = RunCli({"cat", "-d", "root.test.d1", "--start", "3000",
                              "--end", "3000", "-f", "csv", tree});
    ASSERT_EQ(point.code, 0) << point.err;
    std::vector<std::vector<std::string>> rows = DataRows(point.out);
    ASSERT_EQ(rows.size(), 1u) << point.out;
    EXPECT_EQ(rows[0][0], "3000");

    CliResult start_only = RunCli({"cat", "-d", "root.test.d1", "--start", "9000",
                                   "-f", "csv", tree});
    ASSERT_EQ(start_only.code, 0) << start_only.err;
    EXPECT_EQ(DataRows(start_only.out).size(), 4u) << start_only.out;  // 9000..12000

    CliResult end_only = RunCli({"cat", "-d", "root.test.d1", "--end", "2000",
                                 "-f", "csv", tree});
    ASSERT_EQ(end_only.code, 0) << end_only.err;
    EXPECT_EQ(DataRows(end_only.out).size(), 2u) << end_only.out;  // 1000,2000

    CliResult inverted = RunCli({"cat", "-d", "root.test.d1", "--start", "3000",
                                 "--end", "2000", tree});
    EXPECT_EQ(inverted.code, 1) << inverted.err;

    CliResult plus = RunCli({"cat", "-d", "root.test.d1", "--start", "+1", tree});
    EXPECT_EQ(plus.code, 1) << plus.err;

    CliResult leading_zero = RunCli({"cat", "-d", "root.test.d1", "--start", "01", tree});
    EXPECT_EQ(leading_zero.code, 1) << leading_zero.err;
    unlink(tree.c_str());
}

// 用例 33：offset/limit 临界语义（需求 3.1.4.3）
TEST(CliRowReadTest, OffsetLimitBoundaries) {
    std::string dir = MakeTempDir();
    std::string table = WriteTableFixture(Path(dir, "table.tsfile"), 12);
    ASSERT_TRUE(cli_test::FileExists(table));

    CliResult full = RunCli({"cat", "-t", "sensors", "-f", "csv", table});
    ASSERT_EQ(full.code, 0) << full.err;
    std::vector<std::vector<std::string>> all_rows = DataRows(full.out);
    ASSERT_EQ(all_rows.size(), 12u);

    // offset 先跳后取：第 6–8 行
    CliResult window = RunCli({"cat", "-t", "sensors", "--offset", "5", "-n", "3",
                               "-f", "csv", table});
    ASSERT_EQ(window.code, 0) << window.err;
    std::vector<std::vector<std::string>> w = DataRows(window.out);
    ASSERT_EQ(w.size(), 3u) << window.out;
    for (size_t i = 0; i < 3; ++i) {
        EXPECT_EQ(w[i], all_rows[5 + i]);
    }

    // 恰好等于匹配数：成功零行
    CliResult exact = RunCli({"cat", "-t", "sensors", "--offset", "12", "-f", "csv", table});
    ASSERT_EQ(exact.code, 0) << exact.err;
    EXPECT_TRUE(DataRows(exact.out).empty()) << exact.out;

    // 越界：用法错误
    CliResult over = RunCli({"cat", "-t", "sensors", "--offset", "13", table});
    EXPECT_EQ(over.code, 1) << over.err;
    EXPECT_TRUE(over.out.empty());

    // limit=0 只允许 offset=0
    CliResult zero_offset = RunCli({"cat", "-t", "sensors", "-n", "0", "--offset", "1", table});
    EXPECT_EQ(zero_offset.code, 1) << zero_offset.err;
    unlink(table.c_str());
}

// 用例 34：TAG 五种谓词语义（需求 3.1.4.4）
TEST(CliRowReadTest, TagPredicateSemantics) {
    std::string dir = MakeTempDir();
    std::string table = WriteTableFixture(Path(dir, "table.tsfile"), 12);
    ASSERT_TRUE(cli_test::FileExists(table));

    struct Case {
        std::vector<std::string> filter;
        const char *site;
        bool expect_some;
    };
    std::vector<Case> cases = {
        {{"eq", "beijing"}, "beijing", true},
        {{"eq", "BEIJING"}, "beijing", false},   // 区分大小写
        {{"neq", "beijing"}, "shanghai", true},
        {{"regexp", "bei.*"}, "beijing", true},
        {{"regexp", "bei"}, "beijing", false},   // 完整匹配，无隐式子串
    };
    for (const Case &c : cases) {
        std::vector<std::string> args = {"cat", "-t", "sensors", "-f", "csv",
                                         "--tag-filter", "site"};
        args.insert(args.end(), c.filter.begin(), c.filter.end());
        args.push_back(table);
        CliResult r = RunCli(args);
        ASSERT_EQ(r.code, 0) << r.err;
        std::vector<std::vector<std::string>> rows = DataRows(r.out);
        if (c.expect_some) {
            EXPECT_FALSE(rows.empty()) << c.filter[0] << " " << c.filter[1];
            for (const auto &cells : rows) {
                EXPECT_EQ(cells[1], c.site) << r.out;
            }
        } else {
            EXPECT_TRUE(rows.empty()) << r.out;
        }
    }

    // 非法形态：带值谓词给两个值 / 无值谓词带值
    CliResult two_values = RunCli({"cat", "-t", "sensors", "--tag-filter", "site", "eq",
                                   "a", "b", table});
    EXPECT_EQ(two_values.code, 1) << two_values.err;
    CliResult value_on_isnull = RunCli({"cat", "-t", "sensors", "--tag-filter", "site",
                                        "is-null", "x", table});
    EXPECT_EQ(value_on_isnull.code, 1) << value_on_isnull.err;
    unlink(table.c_str());
}

// 用例 35：TAG 的 null / 空串 / 字符串 "null" 可区分（需求 3.1.4.5）
TEST(CliRowReadTest, TagNullEmptyAndLiteralNull) {
    std::string dir = MakeTempDir();
    std::string nullable = WriteNullableTagFixture(Path(dir, "nullable.tsfile"));
    ASSERT_FALSE(nullable.empty()) << "fixture write failed";

    CliResult is_null = RunCli({"cat", "-t", "sensors", "--tag-filter", "site",
                                "is-null", "-f", "csv", nullable});
    ASSERT_EQ(is_null.code, 0) << is_null.err;
    std::vector<std::vector<std::string>> null_rows = DataRows(is_null.out);
    ASSERT_EQ(null_rows.size(), 1u) << is_null.out;
    EXPECT_EQ(null_rows[0][1], "\\N") << "null TAG 以 \\N 呈现";

    CliResult empty = RunCli({"cat", "-t", "sensors", "--tag-filter", "site", "eq", "",
                              "-f", "csv", nullable});
    ASSERT_EQ(empty.code, 0) << empty.err;
    std::vector<std::vector<std::string>> empty_rows = DataRows(empty.out);
    ASSERT_EQ(empty_rows.size(), 1u) << empty.out;
    EXPECT_EQ(empty_rows[0][1], "") << "空串实体应命中且输出空串";

    CliResult literal = RunCli({"cat", "-t", "sensors", "--tag-filter", "site", "eq",
                                "null", "-f", "csv", nullable});
    ASSERT_EQ(literal.code, 0) << literal.err;
    std::vector<std::vector<std::string>> literal_rows = DataRows(literal.out);
    ASSERT_EQ(literal_rows.size(), 1u) << literal.out;
    EXPECT_EQ(literal_rows[0][1], "null") << "字面量 \"null\" 独立于 SQL null";

    CliResult not_null = RunCli({"cat", "-t", "sensors", "--tag-filter", "site",
                                 "not-null", "-f", "csv", nullable});
    ASSERT_EQ(not_null.code, 0) << not_null.err;
    EXPECT_EQ(DataRows(not_null.out).size(), 2u) << not_null.out;

    // TAG 身份列不省略
    CliResult all = RunCli({"cat", "-t", "sensors", "-f", "csv", nullable});
    ASSERT_EQ(all.code, 0) << all.err;
    EXPECT_EQ(DataRows(all.out).size(), 3u) << all.out;
    unlink(nullable.c_str());
}

// 用例 36：regexp 完整匹配与非法正则（需求 3.1.4.4）
TEST(CliRowReadTest, TagRegexpValidation) {
    std::string dir = MakeTempDir();
    std::string table = WriteTableFixture(Path(dir, "table.tsfile"));
    ASSERT_TRUE(cli_test::FileExists(table));

    CliResult bad = RunCli({"cat", "-t", "sensors", "--tag-filter", "site", "regexp",
                            "[", table});
    EXPECT_EQ(bad.code, 1) << bad.err;
    EXPECT_TRUE(bad.out.empty());
    unlink(table.c_str());
}

// 用例 37：多条件 tag-match 必填与 all/any（需求 3.1.4.4）
TEST(CliRowReadTest, TagMatchRequiredAndCombine) {
    std::string dir = MakeTempDir();
    std::string table = WriteTableFixture(Path(dir, "table.tsfile"), 12);
    ASSERT_TRUE(cli_test::FileExists(table));

    CliResult missing = RunCli({"cat", "-t", "sensors", "--tag-filter", "site", "eq",
                                "beijing", "--tag-filter", "note", "not-null", table});
    EXPECT_EQ(missing.code, 1) << "两个条件必须显式 --tag-match";
    EXPECT_TRUE(missing.out.empty());

    CliResult single = RunCli({"cat", "-t", "sensors", "--tag-filter", "site", "eq",
                               "beijing", "--tag-match", "all", table});
    EXPECT_EQ(single.code, 1) << "单条件不得指定 --tag-match";

    CliResult any = RunCli({"cat", "-t", "sensors", "--tag-filter", "site", "eq",
                            "beijing", "--tag-filter", "site", "eq", "shanghai",
                            "--tag-match", "any", "-f", "csv", table});
    ASSERT_EQ(any.code, 0) << any.err;
    EXPECT_EQ(DataRows(any.out).size(), 12u) << "any = 并集";

    CliResult all = RunCli({"cat", "-t", "sensors", "--tag-filter", "site", "eq",
                            "beijing", "--tag-filter", "site", "eq", "shanghai",
                            "--tag-match", "all", "-f", "csv", table});
    ASSERT_EQ(all.code, 0) << all.err;
    EXPECT_TRUE(DataRows(all.out).empty()) << "all = 交集（互斥条件应为空）";

    CliResult zero_cond = RunCli({"cat", "-t", "sensors", "--tag-match", "all", table});
    EXPECT_EQ(zero_cond.code, 1) << "零条件不得指定 --tag-match";
    unlink(table.c_str());
}

// 用例 38：单对象可省作用域、多对象必显（需求 3.1.4.1）
TEST(CliRowReadTest, ScopeOptionalOnlyForSingleObject) {
    std::string dir = MakeTempDir();
    std::string single = WriteTableFixture(Path(dir, "single.tsfile"));
    std::string multi = cli_test::WriteMultiTableFixture(Path(dir, "multi.tsfile"));
    ASSERT_TRUE(cli_test::FileExists(single));
    ASSERT_TRUE(cli_test::FileExists(multi));

    CliResult implicit = RunCli({"cat", "-f", "csv", single});
    ASSERT_EQ(implicit.code, 0) << implicit.err;
    CliResult explicit_scope = RunCli({"cat", "-t", "sensors", "-f", "csv", single});
    ASSERT_EQ(explicit_scope.code, 0) << explicit_scope.err;
    EXPECT_EQ(implicit.out, explicit_scope.out) << "单对象省略作用域结果须一致";

    CliResult multi_no_scope = RunCli({"cat", "-f", "csv", multi});
    EXPECT_EQ(multi_no_scope.code, 1) << multi_no_scope.err;
    EXPECT_TRUE(multi_no_scope.out.empty());
    unlink(single.c_str());
    unlink(multi.c_str());
}

// 用例 39：空文件与零行三格式契约（需求 3.1.4.3 + 3.1.3）
TEST(CliRowReadTest, EmptyFileZeroRowContracts) {
    std::string dir = MakeTempDir();
    std::string empty = WriteEmptyTableFixture(Path(dir, "empty.tsfile"));
    std::string table = WriteTableFixture(Path(dir, "table.tsfile"));
    ASSERT_TRUE(cli_test::FileExists(empty));

    CliResult csv = RunCli({"cat", "-t", "sensors", "-f", "csv", empty});
    ASSERT_EQ(csv.code, 0) << csv.err;
    EXPECT_EQ(csv.out, "time,site,temp\n") << "空表 CSV 只输出表头";

    CliResult ndjson = RunCli({"cat", "-t", "sensors", "-f", "ndjson", empty});
    ASSERT_EQ(ndjson.code, 0) << ndjson.err;
    EXPECT_TRUE(ndjson.out.empty()) << "空表 NDJSON 为空字节流";

    CliResult tbl = RunCli({"cat", "-t", "sensors", "-f", "table", empty});
    ASSERT_EQ(tbl.code, 0) << tbl.err;
    EXPECT_EQ(tbl.out, "time  site  temp\n") << tbl.out;

    // -n 0 三格式
    for (const std::string &fmt : {"csv", "ndjson", "table"}) {
        CliResult z = RunCli({"head", "-t", "sensors", "-n", "0", "-f", fmt, table});
        EXPECT_EQ(z.code, 0) << fmt << " " << z.err;
        if (fmt == "ndjson") {
            EXPECT_TRUE(z.out.empty()) << fmt;
        } else if (fmt == "csv") {
            EXPECT_EQ(z.out, "time,site,temp,note\n") << fmt;
        }
    }
    unlink(empty.c_str());
    unlink(table.c_str());
}

// 用例 40：下游提前关闭管道 → 退出码 3（需求 3.1.3 输出通道与完整性）
TEST(CliRowReadTest, SigpipeReturnsExecutionError) {
    std::string dir = MakeTempDir();
    std::string big = WriteLargeTableFixture(Path(dir, "big.tsfile"), 20000);
    ASSERT_FALSE(big.empty()) << "fixture write failed";

    // 读满 1 字节即关闭读端，制造 EPIPE
    CliResult r = RunCliCloseStdoutAfter({"cat", "-t", "big", "-f", "ndjson", big}, 1);
    EXPECT_EQ(r.code, 3) << "SIGPIPE/EPIPE 应返回 3；实际 code=" << r.code
                         << " stderr=" << r.err;
    unlink(big.c_str());
}

}  // namespace
