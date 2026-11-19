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

// RN-143 用例 63–67：兼容与安全 / 交付验收（需求非功能「可靠性与数据完整性」「本地执行与安全」）——black-box

#include <gtest/gtest.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <cstdio>
#include <cstdlib>
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
using cli_test::SplitCsvRows;
using cli_test::WriteLargeTableFixture;
using cli_test::WriteTableFixture;

std::string Path(const std::string &dir, const std::string &name) {
    return dir + "/" + name;
}

// 用例 63：特殊文件与符号链接/硬链接身份检查（需求非功能「可靠性与数据完整性」）
TEST(CliSafetyTest, SpecialFilesAndIdentityChecks) {
    std::string dir = MakeTempDir();
    std::string table = WriteTableFixture(Path(dir, "table.tsfile"));
    ASSERT_TRUE(cli_test::FileExists(table));

    // 输入侧：FIFO / 目录 / 悬空链接 → 输入问题 2
    //  2026-09-29 实测：FIFO 会让 CLI **永久阻塞**（未先判文件类型即 open），
    //  harness 30s 超时后杀死并标记 code=-2；需求 §3.1.2 要求特殊文件返回 2 → 保持失败可见，待裁决。
    std::string fifo = Path(dir, "pipe.tsfile");
    ASSERT_EQ(mkfifo(fifo.c_str(), 0600), 0);
    CliResult r_fifo = RunCli({"meta", fifo});
    EXPECT_NE(r_fifo.code, -2)
        << "FIFO 作为输入导致 CLI 挂起（30s 无返回，已被 harness 杀死）；需求要求返回输入问题 2";
    EXPECT_EQ(r_fifo.code, 2) << "FIFO 作为输入应为输入问题：" << r_fifo.err;

    CliResult r_dir = RunCli({"meta", dir});
    EXPECT_EQ(r_dir.code, 2) << r_dir.err;

    std::string dangling = Path(dir, "dangling.tsfile");
    ASSERT_EQ(symlink("/tmp/rn143-nonexistent-target.tsfile", dangling.c_str()), 0);
    CliResult r_dangle = RunCli({"meta", dangling});
    EXPECT_EQ(r_dangle.code, 2) << "悬空符号链接应为输入问题：" << r_dangle.err;

    // 输出侧：普通符号链接 / 目录 / 同源硬链接 → 3，且不得破坏目标
    std::string real = Path(dir, "real.csv");
    {
        std::ofstream out(real.c_str());
        out << "keep\n";
    }
    std::string link_path = Path(dir, "link.csv");
    ASSERT_EQ(symlink(real.c_str(), link_path.c_str()), 0);
    CliResult r_link = RunCli({"export", "-t", "sensors", "--type", "csv", "-o", link_path,
                               "--force", table});
    EXPECT_EQ(r_link.code, 3) << "符号链接不得被替换：" << r_link.err;
    EXPECT_EQ(ReadFileBytes(real), "keep\n") << "链接目标不得被改写";

    CliResult r_dirout = RunCli({"export", "-t", "sensors", "--type", "csv", "-o", dir,
                                 "--force", table});
    EXPECT_EQ(r_dirout.code, 3) << "目录不得作为输出目标：" << r_dirout.err;

    std::string hard = Path(dir, "hard.tsfile");
    ASSERT_EQ(link(table.c_str(), hard.c_str()), 0);
    CliResult r_hard = RunCli({"export", "-t", "sensors", "--type", "csv", "-o", hard,
                               "--force", table});
    EXPECT_EQ(r_hard.code, 3) << "硬链接指向同一实际文件，应拒绝：" << r_hard.err;
    EXPECT_TRUE(cli_test::FileExists(table)) << "源文件不得被破坏";

    unlink(fifo.c_str());
    unlink(dangling.c_str());
    unlink(link_path.c_str());
    unlink(hard.c_str());
    unlink(real.c_str());
    unlink(table.c_str());
}

// 用例 64：非法 UTF-8 标识符替换（需求 3.1.2）
//  说明：需先能用 SDK 写出含非法 UTF-8 名称的文件；写不出时按夹具失败暴露（不静默跳过）。
TEST(CliSafetyTest, InvalidUtf8IdentifierHandling) {
    std::string dir = MakeTempDir();
    std::string path = Path(dir, "badutf8.tsfile");
    storage::libtsfile_init();
    storage::WriteFile file;
    const int flags = O_WRONLY | O_CREAT | O_TRUNC;
    ASSERT_EQ(file.create(path, flags, 0666), common::E_OK);

    std::string bad_table = std::string("t") + "\xFF\xFE";
    std::string bad_col = std::string("c") + "\xFF";
    auto *schema = new storage::TableSchema(
        bad_table, std::vector<common::ColumnSchema>{
                       common::ColumnSchema(bad_col, common::STRING,
                                            common::ColumnCategory::TAG),
                       common::ColumnSchema("v", common::INT64,
                                            common::ColumnCategory::FIELD)});
    auto *writer = new storage::TsFileTableWriter(&file, schema);
    storage::Tablet tablet(bad_table, {bad_col, "v"},
                           {common::STRING, common::INT64},
                           {common::ColumnCategory::TAG,
                            common::ColumnCategory::FIELD},
                           1);
    tablet.add_timestamp(0, 1000);
    tablet.add_value(0, bad_col, "x");
    tablet.add_value(0, "v", static_cast<int64_t>(1));
    const int wret = writer->write_table(tablet);
    writer->flush();
    writer->close();
    delete writer;
    delete schema;
    ASSERT_EQ(wret, common::E_OK) << "SDK 无法写出含非法 UTF-8 名称的夹具（用例如需改一次性执行）";

    // 落一份到固定路径，便于脱离测试程序手工复现（详见报告）
    std::string stable = "/tmp/rn143-badutf8.tsfile";
    {
        std::string bytes = ReadFileBytes(path);
        std::ofstream out(stable.c_str(), std::ios::binary | std::ios::trunc);
        out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    }

    CliResult r = RunCli({"ls", "-f", "csv", path});
    EXPECT_NE(r.code, 139) << "不得崩溃";
    EXPECT_TRUE(r.code == 0 || r.code == 2) << "期望 0（替换后可读）或 2（替换后冲突）：" << r.err;
    if (r.code == 0) {
        // 非法字节应替换为 U+FFFD（EF BF BD）
        EXPECT_TRUE(Contains(r.out, "\xEF\xBF\xBD"))
            << "未按 U+FFFD 替换（需求 §3.1.2）；夹具已存 " << stable << " 实测输出：" << r.out;
    }
    unlink(path.c_str());
}

// 用例 65：字段值中的提示注入文本原样处理、不被执行（需求非功能「本地执行与安全」）
TEST(CliSafetyTest, DataIsNeverExecuted) {
    std::string dir = MakeTempDir();
    std::string marker = "/tmp/rn143-pwn-marker";
    unlink(marker.c_str());

    std::string payload = "; rm -rf /tmp/rn143-pwn-marker #";
    std::string payload2 = "$(touch /tmp/rn143-pwn-marker)";
    std::string payload3 = "`touch /tmp/rn143-pwn-marker`";
    std::string csv = Path(dir, "in.csv");
    {
        std::ofstream out(csv.c_str(), std::ios::binary | std::ios::trunc);
        out << "time,site,note\n";
        out << "1000,beijing,\"" << payload << "\"\n";
        out << "2000,beijing,\"" << payload2 << "\"\n";
        out << "3000,beijing,\"" << payload3 << "\"\n";
    }
    std::string out_file = Path(dir, "out.tsfile");
    CliResult w = RunCli({"write", "--table", "sensors", "--tag", "site", "STRING",
                          "--field", "note", "TEXT", "-i", csv, "-o", out_file});
    ASSERT_EQ(w.code, 0) << w.err;

    CliResult r = RunCli({"cat", "-t", "sensors", "-f", "csv", out_file});
    ASSERT_EQ(r.code, 0) << r.err;
    EXPECT_TRUE(Contains(r.out, payload)) << "值应原样输出：" << r.out;
    EXPECT_TRUE(Contains(r.out, payload2)) << r.out;
    EXPECT_TRUE(Contains(r.out, payload3)) << r.out;
    EXPECT_FALSE(cli_test::FileExists(marker))
        << "字段值中的命令文本不得被执行（不应生成 " << marker << "）";

    unlink(csv.c_str());
    unlink(out_file.c_str());
}

// 用例 66：源文件只读性与失败后无暂存物残留（需求非功能「可靠性 / 异常中止与暂存物」）
TEST(CliSafetyTest, SourceReadOnlyAndNoTempResidue) {
    std::string dir = MakeTempDir();
    std::string table = WriteTableFixture(Path(dir, "table.tsfile"), 12);
    std::string tree = cli_test::WriteTreeFixture(Path(dir, "tree.tsfile"), 6);
    ASSERT_TRUE(cli_test::FileExists(table));
    const std::string before_table = ReadFileBytes(table);
    const std::string before_tree = ReadFileBytes(tree);

    // 全部读取命令
    std::vector<std::vector<std::string>> reads = {
        {"ls", "-f", "csv", table},   {"schema", "-t", "sensors", "-f", "csv", table},
        {"meta", "-f", "csv", table}, {"stats", "-t", "sensors", "-f", "csv", table},
        {"count", "-t", "sensors", "-f", "csv", table},
        {"head", "-t", "sensors", "-f", "csv", table},
        {"cat", "-t", "sensors", "-f", "csv", table},
        {"sketch", table},            {"ls", "-f", "csv", tree},
    };
    for (const auto &args : reads) {
        CliResult r = RunCli(args);
        EXPECT_TRUE(r.code == 0 || r.code == 2) << args[0] << " rc=" << r.code << " " << r.err;
    }
    EXPECT_EQ(ReadFileBytes(table), before_table) << "读取命令不得修改源文件";
    EXPECT_EQ(ReadFileBytes(tree), before_tree) << "读取命令不得修改源文件";

    // 失败写入不得留下 *.tmp 暂存物
    std::string fail_dir = Path(dir, "faildir");
    ASSERT_EQ(mkdir(fail_dir.c_str(), 0700), 0);
    std::string existing = fail_dir + "/existing.tsfile";
    {
        std::ofstream out(existing.c_str());
        out << "sentinel";
    }
    std::string csv = Path(dir, "in.csv");
    {
        std::ofstream out(csv.c_str());
        out << "time,site,temp\n1000,bj,20\n";
    }
    CliResult f = RunCli({"write", "--table", "t", "--tag", "site", "STRING", "--field",
                          "temp", "FLOAT", "-i", csv, "-o", existing});
    EXPECT_EQ(f.code, 3) << f.err;
    std::string listing = Path(dir, "listing.txt");
    std::string cmd = "ls -a " + fail_dir + " > " + listing + " 2>&1";
    ASSERT_EQ(system(cmd.c_str()), 0);
    std::string names = ReadFileBytes(listing);
    EXPECT_FALSE(Contains(names, ".tmp")) << "失败后不得残留暂存物：" << names;
    EXPECT_EQ(ReadFileBytes(existing), "sentinel") << "既有目标不得被改动";

    unlink(table.c_str());
    unlink(tree.c_str());
    unlink(existing.c_str());
    unlink(csv.c_str());
    unlink(listing.c_str());
}

// 用例 67：大文件流式读取的内存有界性（需求非功能「性能与资源边界」）
TEST(CliSafetyTest, CatStreamsWithBoundedMemory) {
    std::string dir = MakeTempDir();
    std::string big = WriteLargeTableFixture(Path(dir, "big.tsfile"), 20000);
    ASSERT_FALSE(big.empty()) << "夹具写入失败";
    struct stat st;
    ASSERT_EQ(stat(big.c_str(), &st), 0);

    std::string rss_file = Path(dir, "time.out");
    std::string cmd = "/usr/bin/time -v " + cli_test::CliPath() +
                      " cat -t big -f ndjson " + big + " > /dev/null 2> " + rss_file;
    ASSERT_EQ(system(cmd.c_str()), 0);
    std::string out = ReadFileBytes(rss_file);
    const std::string key = "Maximum resident set size (kbytes):";
    size_t pos = out.find(key);
    ASSERT_NE(pos, std::string::npos) << "/usr/bin/time 输出异常：" << out;
    long rss_kb = std::strtol(out.c_str() + pos + key.size(), nullptr, 10);
    // 记录实测值并断言有界（文件 ~200KB，20k 行；上限取 256MB 作为宽松上界）
    RecordProperty("max_rss_kb", static_cast<int>(rss_kb));
    RecordProperty("file_bytes", static_cast<int>(st.st_size));
    EXPECT_GT(rss_kb, 0);
    EXPECT_LT(rss_kb, 262144) << "峰值 RSS 过高（可能整载）：" << rss_kb << " KB";
    unlink(big.c_str());
}

}  // namespace
