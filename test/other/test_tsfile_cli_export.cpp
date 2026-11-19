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

// RN-143 用例 41–48：数据导出 export（需求 3.4）——black-box

#include <gtest/gtest.h>

#include <cstdio>
#include <fstream>
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
using cli_test::SplitCsvRows;
using cli_test::SplitLines;
using cli_test::WriteMultiTableFixture;
using cli_test::WriteTableFixture;
using cli_test::WriteTreeFixture;

std::string Path(const std::string &dir, const std::string &name) {
    return dir + "/" + name;
}

// 多表夹具的「第二张表损坏」版本：保持 footer 与 sensors_a 可读，
// 仅把 sensors_b 的首个 chunk header 前若干字节改写为 0xFF，使读取 sensors_b 失败。
std::string WriteMultiTableCorruptFixture(const std::string &path) {
    std::string base = WriteMultiTableFixture(path);
    if (base.empty()) {
        return base;
    }
    int64_t chunk_offset = -1;
    {
        storage::TsFileReader reader;
        if (reader.open(path) != common::E_OK) {
            return std::string();
        }
        const auto devices = reader.get_all_devices("sensors_b");
        const auto metadata = reader.get_timeseries_metadata(devices);
        for (const auto &entry : metadata) {
            for (const auto &index : entry.second) {
                if (index == nullptr) {
                    continue;
                }
                auto *chunks = index->get_value_chunk_meta_list();
                if (chunks == nullptr) {
                    chunks = index->get_chunk_meta_list();
                }
                if (chunks == nullptr) {
                    continue;
                }
                for (auto it = chunks->begin(); it != chunks->end(); it++) {
                    if (it.get() != nullptr) {
                        chunk_offset = it.get()->offset_of_chunk_header_;
                        break;
                    }
                }
                if (chunk_offset >= 0) {
                    break;
                }
            }
            if (chunk_offset >= 0) {
                break;
            }
        }
        reader.close();
    }
    if (chunk_offset < 0) {
        return std::string();
    }
    std::fstream file(path.c_str(), std::ios::in | std::ios::out | std::ios::binary);
    if (!file.good()) {
        return std::string();
    }
    file.seekp(static_cast<std::streamoff>(chunk_offset));
    const char garbage[8] = {(char)0xFF, (char)0xFF, (char)0xFF, (char)0xFF,
                             (char)0xFF, (char)0xFF, (char)0xFF, (char)0xFF};
    file.write(garbage, sizeof(garbage));
    file.close();
    return path;
}

// 用例 41：单对象导出与 cat 逐字节一致（需求 3.4）
TEST(CliExportTest, SingleObjectMatchesCatBytewise) {
    std::string dir = MakeTempDir();
    std::string table = WriteTableFixture(Path(dir, "table.tsfile"), 12);
    ASSERT_TRUE(cli_test::FileExists(table));

    for (const std::string &fmt : {"csv", "ndjson"}) {
        std::string out = Path(dir, "out-" + fmt);
        CliResult r = RunCli({"export", "-t", "sensors", "--type", fmt, "-o", out, table});
        ASSERT_EQ(r.code, 0) << fmt << " " << r.err;
        EXPECT_TRUE(r.out.empty()) << "成功导出 stdout 应为空";

        std::string cat_out = Path(dir, "cat-" + fmt);
        std::string cmd = cli_test::CliPath() + " cat -t sensors -f " + fmt + " " +
                          table + " > " + cat_out + " 2>/dev/null";
        ASSERT_EQ(system(cmd.c_str()), 0);
        EXPECT_EQ(ReadFileBytes(out), ReadFileBytes(cat_out))
            << fmt << " 导出与 cat 必须逐字节一致";
        unlink(out.c_str());
        unlink(cat_out.c_str());
    }
    unlink(table.c_str());
}

// 用例 42：export 必填项与不支持选项（需求 3.4 异常表）
TEST(CliExportTest, RequiredAndUnsupportedOptions) {
    std::string dir = MakeTempDir();
    std::string table = WriteTableFixture(Path(dir, "table.tsfile"));
    std::string tree = WriteTreeFixture(Path(dir, "tree.tsfile"));
    ASSERT_TRUE(cli_test::FileExists(table));

    struct Case {
        std::vector<std::string> args;
        const char *why;
    };
    std::string out = Path(dir, "x.csv");
    std::vector<Case> cases = {
        {{"export", "-t", "sensors", "-o", out, table}, "缺 --type"},
        {{"export", "-t", "sensors", "--type", "json", "-o", out, table}, "非法 --type"},
        {{"export", "-t", "sensors", "--type", "csv", "--no-header", "-o", out, table}, "--no-header 不支持"},
        {{"export", "--type", "csv", "-o", out, table}, "缺作用域"},
        {{"export", "-d", "root.test.d1", "-t", "sensors", "--type", "csv", "-o", out, tree},
         "-d 与 -t 互斥"},
    };
    for (const Case &c : cases) {
        CliResult r = RunCli(c.args);
        EXPECT_EQ(r.code, 1) << c.why << " stderr=" << r.err;
        EXPECT_TRUE(r.out.empty()) << c.why;
        EXPECT_FALSE(cli_test::FileExists(out)) << c.why << " 不应产生目标文件";
    }
    unlink(table.c_str());
    unlink(tree.c_str());
}

// 用例 43：导出零行与 offset 越界（需求 3.4 异常表）
TEST(CliExportTest, ZeroRowCommitAndOffsetOverflow) {
    std::string dir = MakeTempDir();
    std::string table = WriteTableFixture(Path(dir, "table.tsfile"), 12);
    ASSERT_TRUE(cli_test::FileExists(table));

    std::string zero = Path(dir, "zero.csv");
    CliResult z = RunCli({"export", "-t", "sensors", "--offset", "12", "--type", "csv",
                          "-o", zero, table});
    ASSERT_EQ(z.code, 0) << z.err;
    ASSERT_TRUE(cli_test::FileExists(zero));
    EXPECT_EQ(ReadFileBytes(zero), "time,site,temp,note\n") << "零行 CSV 只含表头";

    std::string over = Path(dir, "over.csv");
    CliResult o = RunCli({"export", "-t", "sensors", "--offset", "13", "--type", "csv",
                          "-o", over, table});
    EXPECT_EQ(o.code, 1) << o.err;
    EXPECT_FALSE(cli_test::FileExists(over)) << "越界不得提交目标";
    unlink(table.c_str());
    unlink(zero.c_str());
}

// 用例 44：--force 语义与同源/符号链接保护（需求 3.4 + 非功能可靠性）
TEST(CliExportTest, ForceReplaceAndSameFileGuard) {
    std::string dir = MakeTempDir();
    std::string table = WriteTableFixture(Path(dir, "table.tsfile"));
    ASSERT_TRUE(cli_test::FileExists(table));

    std::string target = Path(dir, "existing.csv");
    {
        std::ofstream out(target.c_str());
        out << "old-content\n";
    }
    std::string before = ReadFileBytes(target);

    CliResult conflict = RunCli({"export", "-t", "sensors", "--type", "csv", "-o", target, table});
    EXPECT_EQ(conflict.code, 3) << conflict.err;
    EXPECT_EQ(ReadFileBytes(target), before) << "无 --force 时既有目标不得被修改";

    CliResult forced = RunCli({"export", "-t", "sensors", "--type", "csv", "-o", target,
                               "--force", table});
    ASSERT_EQ(forced.code, 0) << forced.err;
    EXPECT_TRUE(Contains(ReadFileBytes(target), "time,site,temp,note")) << "--force 应替换普通文件";

    CliResult same = RunCli({"export", "-t", "sensors", "--type", "csv", "-o", table,
                             "--force", table});
    EXPECT_EQ(same.code, 3) << "同源输出应拒绝：" << same.err;

    std::string link = Path(dir, "link.csv");
    ASSERT_EQ(symlink(target.c_str(), link.c_str()), 0);
    CliResult sym = RunCli({"export", "-t", "sensors", "--type", "csv", "-o", link,
                            "--force", table});
    EXPECT_EQ(sym.code, 3) << "符号链接不得被替换：" << sym.err;
    unlink(table.c_str());
    unlink(target.c_str());
    unlink(link.c_str());
}

// 用例 45：多对象 manifest 与编号文件（需求 3.4）
TEST(CliExportTest, MultiObjectManifestComplete) {
    std::string dir = MakeTempDir();
    std::string multi = WriteMultiTableFixture(Path(dir, "multi.tsfile"));
    ASSERT_TRUE(cli_test::FileExists(multi));

    std::string out_dir = Path(dir, "exported");
    CliResult r = RunCli({"export", "-t", "sensors_a", "-t", "sensors_b", "--type", "csv",
                          "--output-dir", out_dir, multi});
    ASSERT_EQ(r.code, 0) << r.err;

    std::string f1 = out_dir + "/0001.csv";
    std::string f2 = out_dir + "/0002.csv";
    ASSERT_TRUE(cli_test::FileExists(f1)) << "缺 0001.csv";
    ASSERT_TRUE(cli_test::FileExists(f2)) << "缺 0002.csv";

    std::string manifest = ReadFileBytes(out_dir + "/_manifest.json");
    EXPECT_TRUE(Contains(manifest, "\"complete\": true")) << manifest;
    EXPECT_TRUE(Contains(manifest, "\"0001.csv\"")) << manifest;
    EXPECT_TRUE(Contains(manifest, "\"0002.csv\"")) << manifest;
    EXPECT_TRUE(Contains(manifest, "\"sensors_a\"")) << manifest;
    EXPECT_TRUE(Contains(manifest, "\"sensors_b\"")) << manifest;
    EXPECT_TRUE(Contains(manifest, "\"type\": \"csv\"") || Contains(manifest, "\"type\":\"csv\""))
        << manifest;
    // rows 为十进制字符串，且与文件实际行数（不含表头）一致
    EXPECT_TRUE(Contains(manifest, "\"rows\": \"1\"") || Contains(manifest, "\"rows\":\"1\""))
        << manifest;
    EXPECT_EQ(SplitCsvRows(ReadFileBytes(f1)).size() - 1, 1u);
    EXPECT_EQ(SplitCsvRows(ReadFileBytes(f2)).size() - 1, 1u);
    unlink(multi.c_str());
}

// 用例 46：多对象首错即停与部分成功保留（需求 3.4 + 非功能可靠性）
//  2026-09-29 实测：读取损坏的第二张表时 CLI 段错误（exit 139 = SIGSEGV），
//  而需求 §3.1.1 明确「TsFile 损坏」应返回输入问题 2 → 保持失败可见，待用户裁决。
TEST(CliExportTest, MultiObjectFirstErrorStops) {
    std::string dir = MakeTempDir();
    std::string corrupt = WriteMultiTableCorruptFixture(Path(dir, "multi-corrupt.tsfile"));
    ASSERT_FALSE(corrupt.empty()) << "损坏夹具构造失败";

    // 落一份到固定路径，便于脱离测试程序手工复现（详见报告）
    std::string stable = "/tmp/rn143-multi-corrupt.tsfile";
    {
        std::string bytes = ReadFileBytes(corrupt);
        std::ofstream out(stable.c_str(), std::ios::binary | std::ios::trunc);
        out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    }

    std::string out_dir = Path(dir, "exported-failure");
    CliResult r = RunCli({"export", "-t", "sensors_a", "-t", "sensors_b", "--type", "csv",
                          "--output-dir", out_dir, corrupt});
    EXPECT_EQ(r.code, 2)
        << "第二张表读取失败应为输入问题 2；实测退出码=" << r.code
        << "（139=SIGSEGV 崩溃） stderr=" << r.err
        << " 损坏夹具已存 " << stable;

    std::string f1 = out_dir + "/0001.csv";
    std::string f2 = out_dir + "/0002.csv";
    EXPECT_TRUE(cli_test::FileExists(f1)) << "已成功对象应保留";
    EXPECT_FALSE(cli_test::FileExists(f2)) << "失败对象不得产生正式文件";
    if (cli_test::FileExists(out_dir + "/_manifest.json")) {
        std::string manifest = ReadFileBytes(out_dir + "/_manifest.json");
        EXPECT_TRUE(Contains(manifest, "\"complete\": false")) << manifest;
        EXPECT_TRUE(Contains(manifest, "\"0001.csv\"")) << manifest;
        EXPECT_FALSE(Contains(manifest, "\"0002.csv\"")) << "未记录文件不得算成功：" << manifest;
    }
    unlink(corrupt.c_str());
}

// 用例 47：多对象预校验失败不创建目录（需求 3.4）
TEST(CliExportTest, MultiObjectPreflightNoSideEffects) {
    std::string dir = MakeTempDir();
    std::string multi = WriteMultiTableFixture(Path(dir, "multi.tsfile"));
    std::string table = WriteTableFixture(Path(dir, "table.tsfile"));
    ASSERT_TRUE(cli_test::FileExists(multi));

    struct Case {
        std::vector<std::string> args;
        const char *why;
    };
    std::string d1 = Path(dir, "dup-dir");
    std::string d2 = Path(dir, "mixed-dir");
    std::string d3 = Path(dir, "missing-dir");
    std::vector<Case> cases = {
        {{"export", "-t", "sensors_a", "-t", "sensors_a", "--type", "csv",
          "--output-dir", d1, multi}, "重复对象"},
        {{"export", "-d", "x", "-t", "sensors_b", "--type", "csv",
          "--output-dir", d2, table}, "混用 -d/-t"},
        {{"export", "-t", "sensors_a", "-t", "missing_table", "--type", "csv",
          "--output-dir", d3, multi}, "对象不存在"},
    };
    const char *dirs[] = {d1.c_str(), d2.c_str(), d3.c_str()};
    for (size_t i = 0; i < cases.size(); ++i) {
        CliResult r = RunCli(cases[i].args);
        EXPECT_EQ(r.code, 1) << cases[i].why << " stderr=" << r.err;
        EXPECT_FALSE(cli_test::FileExists(dirs[i]))
            << cases[i].why << " 不得创建输出目录或 0001 文件";
    }
    unlink(multi.c_str());
    unlink(table.c_str());
}

// 用例 48：多对象目录冲突与选项约束（需求 3.4）
TEST(CliExportTest, MultiObjectDirAndForceRejection) {
    std::string dir = MakeTempDir();
    std::string multi = WriteMultiTableFixture(Path(dir, "multi.tsfile"));
    ASSERT_TRUE(cli_test::FileExists(multi));

    // 目标目录必须预先不存在
    std::string existing = Path(dir, "existing-dir");
    ASSERT_EQ(mkdir(existing.c_str(), 0700), 0);
    std::string sentinel = existing + "/keep.txt";
    {
        std::ofstream out(sentinel.c_str());
        out << "keep\n";
    }
    CliResult r1 = RunCli({"export", "-t", "sensors_a", "-t", "sensors_b", "--type", "csv",
                           "--output-dir", existing, multi});
    EXPECT_NE(r1.code, 0) << "目录已存在应失败（实测退出码记录如下）；stderr=" << r1.err;
    std::string note = "（实测退出码=" + std::to_string(r1.code) + "，需求未写死 1 或 3）";
    EXPECT_TRUE(cli_test::FileExists(sentinel)) << "既有目录内容不得被改动 " << note;
    EXPECT_EQ(ReadFileBytes(sentinel), "keep\n") << note;

    // 多对象不接受 --force / -o
    std::string d2 = Path(dir, "force-dir");
    CliResult r2 = RunCli({"export", "-t", "sensors_a", "-t", "sensors_b", "--type", "csv",
                           "--force", "--output-dir", d2, multi});
    EXPECT_EQ(r2.code, 1) << r2.err;
    EXPECT_FALSE(cli_test::FileExists(d2));

    CliResult r3 = RunCli({"export", "-t", "sensors_a", "-t", "sensors_b", "--type", "csv",
                           "-o", Path(dir, "single.csv"), multi});
    EXPECT_EQ(r3.code, 1) << r3.err;
    unlink(multi.c_str());
}

}  // namespace
