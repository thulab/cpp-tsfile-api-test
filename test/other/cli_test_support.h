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

// RN-143「TsFile Cli」黑盒测试支撑（用户 2026-09-29 确认落在 cpp-tsfile-api-test/test/other/）
//
// 设计要点：
//  - 子进程 fork/exec 调用**真实** tsfile-cli 二进制（不用进程内 run_cli），
//    以便验证退出码、stdout/stderr 分流、原子提交与 SIGPIPE 等只有真实进程才有的契约；
//  - CLI 路径解析：环境变量 TSFILE_CLI > 编译期 TSFILE_CLI_DEFAULT（由 CMake 注入）> "bin/tsfile-cli"；
//  - 夹具全部现场生成（自包含），落在 mkdtemp 出来的私有目录，不依赖外部夹具目录；
//  - 仅依赖已发布 SDK 头文件与 libtsfile.so。

#ifndef TSFILE_CLI_TEST_SUPPORT_H
#define TSFILE_CLI_TEST_SUPPORT_H

#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cstdlib>
#include <cstring>
#include <fstream>
#include <map>
#include <sstream>
#include <string>
#include <vector>

#include "common/record.h"
#include "common/schema.h"
#include "common/tablet.h"
#include "common/tsfile_common.h"
#include "file/write_file.h"
#include "reader/tsfile_reader.h"
#include "writer/tsfile_table_writer.h"
#include "writer/tsfile_tree_writer.h"
#include "writer/tsfile_writer.h"

namespace cli_test {

struct CliResult {
    int code = -1;
    std::string out;
    std::string err;
};

inline std::string CliPath() {
    const char *env = std::getenv("TSFILE_CLI");
    if (env != nullptr && env[0] != '\0') {
        return std::string(env);
    }
#ifdef TSFILE_CLI_DEFAULT
    return std::string(TSFILE_CLI_DEFAULT);
#else
    return std::string("bin/tsfile-cli");
#endif
}

// 私有临时目录（每个测试一个，避免用例间互相影响）
inline std::string MakeTempDir() {
    char tmpl[] = "/tmp/tsfile_cli_case_XXXXXX";
    char *dir = mkdtemp(tmpl);
    return std::string(dir == nullptr ? "/tmp" : dir);
}

inline std::string ReadFileBytes(const std::string &path) {
    std::ifstream in(path.c_str(), std::ios::binary);
    std::ostringstream bytes;
    bytes << in.rdbuf();
    return bytes.str();
}

inline bool FileExists(const std::string &path) {
    struct stat st;
    return ::stat(path.c_str(), &st) == 0;
}

inline bool IsRegularFile(const std::string &path) {
    struct stat st;
    if (::stat(path.c_str(), &st) != 0) {
        return false;
    }
    return S_ISREG(st.st_mode);
}

// ---------------------------------------------------------------------------
// 子进程执行
//   close_stdout_after < 0：正常读完 stdout/stderr（poll 驱动，避免管道死锁）
//   close_stdout_after >= 0：读满该字节数后主动关闭读端（制造 SIGPIPE/EPIPE）
// ---------------------------------------------------------------------------
inline CliResult RunCliEx(const std::vector<std::string> &args,
                          const std::string &stdin_data, bool use_stdin,
                          long close_stdout_after) {
    CliResult result;
    int out_pipe[2];
    int err_pipe[2];
    int in_pipe[2] = {-1, -1};
    if (pipe(out_pipe) != 0 || pipe(err_pipe) != 0) {
        return result;
    }
    if (use_stdin && pipe(in_pipe) != 0) {
        return result;
    }

    const std::string cli = CliPath();
    std::vector<char *> argv;
    argv.push_back(const_cast<char *>(cli.c_str()));
    for (size_t i = 0; i < args.size(); ++i) {
        argv.push_back(const_cast<char *>(args[i].c_str()));
    }
    argv.push_back(nullptr);

    pid_t pid = fork();
    if (pid < 0) {
        return result;
    }
    if (pid == 0) {
        dup2(out_pipe[1], STDOUT_FILENO);
        dup2(err_pipe[1], STDERR_FILENO);
        if (use_stdin) {
            dup2(in_pipe[0], STDIN_FILENO);
        } else {
            int devnull = ::open("/dev/null", O_RDONLY);
            if (devnull >= 0) {
                dup2(devnull, STDIN_FILENO);
            }
        }
        ::close(out_pipe[0]);
        ::close(out_pipe[1]);
        ::close(err_pipe[0]);
        ::close(err_pipe[1]);
        if (use_stdin) {
            ::close(in_pipe[0]);
            ::close(in_pipe[1]);
        }
        execv(cli.c_str(), argv.data());
        _exit(127);  // exec 失败：127 供父进程识别
    }

    ::close(out_pipe[1]);
    ::close(err_pipe[1]);
    if (use_stdin) {
        ::close(in_pipe[0]);
    }

    bool stdout_open = true;
    bool stderr_open = true;
    bool stdin_open = use_stdin;
    size_t stdin_sent = 0;
    bool timed_out = false;

    while (stdout_open || stderr_open || stdin_open) {
        struct pollfd fds[3];
        int nfds = 0;
        int idx_out = -1, idx_err = -1, idx_in = -1;
        if (stdout_open) {
            idx_out = nfds;
            fds[nfds].fd = out_pipe[0];
            fds[nfds].events = POLLIN;
            fds[nfds].revents = 0;
            ++nfds;
        }
        if (stderr_open) {
            idx_err = nfds;
            fds[nfds].fd = err_pipe[0];
            fds[nfds].events = POLLIN;
            fds[nfds].revents = 0;
            ++nfds;
        }
        if (stdin_open) {
            idx_in = nfds;
            fds[nfds].fd = in_pipe[1];
            fds[nfds].events = POLLOUT;
            fds[nfds].revents = 0;
            ++nfds;
        }
        int ret = poll(fds, static_cast<nfds_t>(nfds), 30000);
        if (ret < 0) {
            break;
        }
        if (ret == 0) {
            timed_out = true;  // 30s 无活动：视为被测进程挂起
            break;
        }
        if (stdout_open && (fds[idx_out].revents & (POLLIN | POLLHUP))) {
            char buf[4096];
            ssize_t n = ::read(out_pipe[0], buf, sizeof(buf));
            if (n > 0) {
                result.out.append(buf, static_cast<size_t>(n));
                if (close_stdout_after >= 0 &&
                    static_cast<long>(result.out.size()) >= close_stdout_after) {
                    ::close(out_pipe[0]);
                    stdout_open = false;
                }
            } else {
                ::close(out_pipe[0]);
                stdout_open = false;
            }
        }
        if (stderr_open && (fds[idx_err].revents & (POLLIN | POLLHUP))) {
            char buf[4096];
            ssize_t n = ::read(err_pipe[0], buf, sizeof(buf));
            if (n > 0) {
                result.err.append(buf, static_cast<size_t>(n));
            } else {
                ::close(err_pipe[0]);
                stderr_open = false;
            }
        }
        if (stdin_open && (fds[idx_in].revents & (POLLOUT | POLLHUP))) {
            if (stdin_sent < stdin_data.size()) {
                ssize_t n = ::write(in_pipe[1], stdin_data.data() + stdin_sent,
                                    stdin_data.size() - stdin_sent);
                if (n > 0) {
                    stdin_sent += static_cast<size_t>(n);
                } else {
                    ::close(in_pipe[1]);
                    stdin_open = false;
                }
            } else {
                ::close(in_pipe[1]);
                stdin_open = false;
            }
        }
    }
    if (stdout_open) {
        ::close(out_pipe[0]);
    }
    if (stderr_open) {
        ::close(err_pipe[0]);
    }
    if (stdin_open) {
        ::close(in_pipe[1]);
    }

    int status = 0;
    if (timed_out) {
        ::kill(pid, SIGKILL);  // 挂起时杀掉子进程，避免 waitpid 永久阻塞
    }
    if (waitpid(pid, &status, 0) < 0) {
        return result;
    }
    if (timed_out) {
        result.code = -2;  // 与退出码空间区分：-2 = harness 判定被测进程挂起
        result.err += "\n[HARNESS_TIMEOUT] 30s 无输出，子进程已被杀死";
        return result;
    }
    if (WIFEXITED(status)) {
        result.code = WEXITSTATUS(status);
    } else if (WIFSIGNALED(status)) {
        result.code = 128 + WTERMSIG(status);
    }
    return result;
}

inline CliResult RunCli(const std::vector<std::string> &args) {
    return RunCliEx(args, std::string(), false, -1);
}

inline CliResult RunCliWithStdin(const std::vector<std::string> &args,
                                 const std::string &stdin_data) {
    return RunCliEx(args, stdin_data, true, -1);
}

// 读满 close_after 字节后关闭读端，制造下游提前退出（SIGPIPE/EPIPE 场景）
inline CliResult RunCliCloseStdoutAfter(const std::vector<std::string> &args,
                                        long close_after) {
    return RunCliEx(args, std::string(), false, close_after);
}

// ---------------------------------------------------------------------------
// 夹具生成（SDK 现场写；只依赖已发布头文件）
// ---------------------------------------------------------------------------

// 表模型夹具：sensors 表，TAG(site STRING)，FIELD(temp FLOAT, note TEXT)
// rows 行，时间戳 1000,2000,...；site 交替 beijing/shanghai；note 第 2 行为 NULL
inline std::string WriteTableFixture(const std::string &path,
                                     int rows = 12) {
    storage::libtsfile_init();
    storage::WriteFile file;
    const int flags = O_WRONLY | O_CREAT | O_TRUNC;
    if (file.create(path, flags, 0666) != common::E_OK) {
        return std::string();
    }
    auto *schema = new storage::TableSchema(
        "sensors",
        std::vector<common::ColumnSchema>{
            common::ColumnSchema("site", common::STRING,
                                 common::ColumnCategory::TAG),
            common::ColumnSchema("temp", common::FLOAT,
                                 common::ColumnCategory::FIELD),
            common::ColumnSchema("note", common::TEXT,
                                 common::ColumnCategory::FIELD),
        });
    auto *writer = new storage::TsFileTableWriter(&file, schema);
    for (int row = 0; row < rows; ++row) {
        storage::Tablet tablet("sensors", {"site", "temp", "note"},
                               {common::STRING, common::FLOAT, common::TEXT},
                               {common::ColumnCategory::TAG,
                                common::ColumnCategory::FIELD,
                                common::ColumnCategory::FIELD},
                               1);
        tablet.add_timestamp(0, 1000 + row * 1000);
        tablet.add_value(0, "site", row % 2 == 0 ? "beijing" : "shanghai");
        tablet.add_value(0, "temp", static_cast<float>(20.0 + row));
        if (row != 2) {
            tablet.add_value(0, "note",
                             std::string("note-") + std::to_string(row));
        }
        writer->write_table(tablet);
    }
    writer->flush();
    writer->close();
    delete writer;
    delete schema;
    return path;
}

// 树模型夹具：root.test.d1（m1 INT32 / m2 DOUBLE），时间 0..rows-1
inline std::string WriteTreeFixture(const std::string &path,
                                    int rows = 12) {
    storage::libtsfile_init();
    storage::WriteFile file;
    const int flags = O_WRONLY | O_CREAT | O_TRUNC;
    if (file.create(path, flags, 0666) != common::E_OK) {
        return std::string();
    }
    storage::TsFileTreeWriter writer(&file);
    std::string device = "root.test.d1";
    auto *s1 = new storage::MeasurementSchema("m1", common::INT32);
    auto *s2 = new storage::MeasurementSchema("m2", common::DOUBLE);
    writer.register_timeseries(device, s1);
    writer.register_timeseries(device, s2);
    for (int row = 0; row < rows; ++row) {
        storage::TsRecord record(device, static_cast<int64_t>(1000 + row * 1000));
        record.add_point("m1", static_cast<int32_t>(row));
        record.add_point("m2", static_cast<double>(row) + 0.5);
        writer.write(record);
    }
    writer.flush();
    writer.close();
    delete s1;
    delete s2;
    return path;
}

// 多表夹具：sensors_a 与 sensors_b 各一行，用于多对象导出/作用域规则
inline std::string WriteMultiTableFixture(const std::string &path) {
    storage::libtsfile_init();
    storage::WriteFile file;
    const int flags = O_WRONLY | O_CREAT | O_TRUNC;
    if (file.create(path, flags, 0666) != common::E_OK) {
        return std::string();
    }
    auto *schema_a = new storage::TableSchema(
        "sensors_a",
        std::vector<common::ColumnSchema>{
            common::ColumnSchema("site", common::STRING,
                                 common::ColumnCategory::TAG),
            common::ColumnSchema("va", common::INT64,
                                 common::ColumnCategory::FIELD),
        });
    auto *writer = new storage::TsFileTableWriter(&file, schema_a);
    auto schema_b = std::make_shared<storage::TableSchema>(
        "sensors_b",
        std::vector<common::ColumnSchema>{
            common::ColumnSchema("site", common::STRING,
                                 common::ColumnCategory::TAG),
            common::ColumnSchema("vb", common::INT64,
                                 common::ColumnCategory::FIELD),
        });
    writer->register_table(schema_b);
    {
        storage::Tablet t("sensors_a", {"site", "va"},
                          {common::STRING, common::INT64},
                          {common::ColumnCategory::TAG,
                           common::ColumnCategory::FIELD},
                          1);
        t.add_timestamp(0, 1000);
        t.add_value(0, "site", "beijing");
        t.add_value(0, "va", static_cast<int64_t>(11));
        writer->write_table(t);
    }
    {
        storage::Tablet t("sensors_b", {"site", "vb"},
                          {common::STRING, common::INT64},
                          {common::ColumnCategory::TAG,
                           common::ColumnCategory::FIELD},
                          1);
        t.add_timestamp(0, 1000);
        t.add_value(0, "site", "shanghai");
        t.add_value(0, "vb", static_cast<int64_t>(22));
        writer->write_table(t);
    }
    writer->flush();
    writer->close();
    delete writer;
    delete schema_a;
    return path;
}

// 空表模型夹具（只有结构、无数据行）
inline std::string WriteEmptyTableFixture(const std::string &path) {
    storage::libtsfile_init();
    storage::WriteFile file;
    const int flags = O_WRONLY | O_CREAT | O_TRUNC;
    if (file.create(path, flags, 0666) != common::E_OK) {
        return std::string();
    }
    auto *schema = new storage::TableSchema(
        "sensors",
        std::vector<common::ColumnSchema>{
            common::ColumnSchema("site", common::STRING,
                                 common::ColumnCategory::TAG),
            common::ColumnSchema("temp", common::FLOAT,
                                 common::ColumnCategory::FIELD),
        });
    auto *writer = new storage::TsFileTableWriter(&file, schema);
    writer->flush();
    writer->close();
    delete writer;
    delete schema;
    return path;
}

// 可空 TAG 夹具：实体 site=NULL / site="" / site="null"（三类值必须可区分）
inline std::string WriteNullableTagFixture(const std::string &path) {
    storage::libtsfile_init();
    storage::WriteFile file;
    const int flags = O_WRONLY | O_CREAT | O_TRUNC;
    if (file.create(path, flags, 0666) != common::E_OK) {
        return std::string();
    }
    auto *schema = new storage::TableSchema(
        "sensors",
        std::vector<common::ColumnSchema>{
            common::ColumnSchema("site", common::STRING,
                                 common::ColumnCategory::TAG),
            common::ColumnSchema("temp", common::DOUBLE,
                                 common::ColumnCategory::FIELD),
        });
    auto *writer = new storage::TsFileTableWriter(&file, schema);
    struct Entity {
        bool is_null;
        const char *site;
        double temp;
    };
    std::vector<Entity> entities = {
        {true, nullptr, 20.0},
        {false, "", 21.0},
        {false, "null", 22.0},
    };
    for (const Entity &e : entities) {
        storage::Tablet tablet("sensors", {"site", "temp"},
                               {common::STRING, common::DOUBLE},
                               {common::ColumnCategory::TAG,
                                common::ColumnCategory::FIELD},
                               1);
        tablet.add_timestamp(0, 1000);
        if (!e.is_null) {
            tablet.add_value(0, "site", e.site);
        }
        tablet.add_value(0, "temp", e.temp);
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

// 大表夹具（用于 SIGPIPE/流式场景，输出量需超过管道缓冲）
inline std::string WriteLargeTableFixture(const std::string &path, int rows) {
    storage::libtsfile_init();
    storage::WriteFile file;
    const int flags = O_WRONLY | O_CREAT | O_TRUNC;
    if (file.create(path, flags, 0666) != common::E_OK) {
        return std::string();
    }
    auto *schema = new storage::TableSchema(
        "big",
        std::vector<common::ColumnSchema>{
            common::ColumnSchema("site", common::STRING,
                                 common::ColumnCategory::TAG),
            common::ColumnSchema("v", common::INT64,
                                 common::ColumnCategory::FIELD),
        });
    auto *writer = new storage::TsFileTableWriter(&file, schema);
    for (int row = 0; row < rows; ++row) {
        storage::Tablet tablet("big", {"site", "v"},
                               {common::STRING, common::INT64},
                               {common::ColumnCategory::TAG,
                                common::ColumnCategory::FIELD},
                               1);
        tablet.add_timestamp(0, 1000 + row);
        tablet.add_value(0, "site", "s1");
        tablet.add_value(0, "v", static_cast<int64_t>(row));
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

// 非纯模型夹具：同一文件内同时注册 tree 与 table（用于"非纯模型必须拒绝"的验证）
inline std::string WriteNonPureFixture(const std::string &path) {
    storage::libtsfile_init();
    storage::WriteFile file;
    const int flags = O_WRONLY | O_CREAT | O_TRUNC;
    if (file.create(path, flags, 0666) != common::E_OK) {
        return std::string();
    }
    storage::TsFileWriter writer;
    writer.init(&file);
    auto table_schema = std::make_shared<storage::TableSchema>(
        "hybrid_table",
        std::vector<common::ColumnSchema>{
            common::ColumnSchema("value", common::INT64, common::UNCOMPRESSED,
                                 common::PLAIN,
                                 common::ColumnCategory::FIELD)});
    if (writer.register_table(table_schema) != common::E_OK) {
        writer.close();
        return std::string();
    }
    storage::MeasurementSchema tree_schema("value", common::INT32);
    if (writer.register_timeseries("root.hybrid.d1", tree_schema) !=
        common::E_OK) {
        writer.close();
        return std::string();
    }
    storage::TsRecord tree_record("root.hybrid.d1", 0);
    tree_record.add_point("value", static_cast<int32_t>(1));
    if (writer.write_tree(tree_record) != common::E_OK) {
        writer.close();
        return std::string();
    }
    storage::Tablet table_tablet("hybrid_table", {"value"}, {common::INT64},
                                 {common::ColumnCategory::FIELD}, 1);
    table_tablet.add_timestamp(0, 0);
    table_tablet.add_value(0, "value", static_cast<int64_t>(2));
    if (writer.write_table(table_tablet) != common::E_OK ||
        writer.flush() != common::E_OK || writer.close() != common::E_OK) {
        return std::string();
    }
    return path;
}

// CSV 工具：按行/逗号切分（仅用于测试自身解析，不做 RFC4180 完整实现）
inline std::vector<std::string> SplitLines(const std::string &text) {
    std::vector<std::string> lines;
    std::string current;
    for (size_t i = 0; i < text.size(); ++i) {
        if (text[i] == '\n') {
            lines.push_back(current);
            current.clear();
        } else if (text[i] != '\r') {
            current.push_back(text[i]);
        }
    }
    if (!current.empty()) {
        lines.push_back(current);
    }
    return lines;
}

inline std::vector<std::string> SplitCsvRow(const std::string &row) {
    std::vector<std::string> cells;
    std::string cell;
    bool in_quotes = false;
    for (size_t i = 0; i < row.size(); ++i) {
        char c = row[i];
        if (in_quotes) {
            if (c == '"') {
                if (i + 1 < row.size() && row[i + 1] == '"') {
                    cell.push_back('"');
                    ++i;
                } else {
                    in_quotes = false;
                }
            } else {
                cell.push_back(c);
            }
        } else if (c == '"') {
            in_quotes = true;
        } else if (c == ',') {
            cells.push_back(cell);
            cell.clear();
        } else {
            cell.push_back(c);
        }
    }
    cells.push_back(cell);
    return cells;
}

inline bool Contains(const std::string &haystack, const std::string &needle) {
    return haystack.find(needle) != std::string::npos;
}

// NDJSON 行解析（扁平对象；值保留原始 token 文本：字符串含引号、数字/布尔/null 原样）
inline std::map<std::string, std::string> ParseFlatJson(const std::string &line) {
    std::map<std::string, std::string> fields;
    size_t i = 0;
    while (i < line.size() && line[i] != '{') {
        ++i;
    }
    if (i >= line.size()) {
        return fields;
    }
    ++i;
    while (i < line.size()) {
        while (i < line.size() && (line[i] == ' ' || line[i] == ',')) {
            ++i;
        }
        if (i >= line.size() || line[i] == '}') {
            break;
        }
        if (line[i] != '"') {
            break;
        }
        std::string key;
        ++i;
        while (i < line.size() && line[i] != '"') {
            key.push_back(line[i++]);
        }
        ++i;  // closing quote
        while (i < line.size() && (line[i] == ' ' || line[i] == ':')) {
            ++i;
        }
        std::string token;
        if (i < line.size() && line[i] == '"') {
            token.push_back(line[i++]);
            while (i < line.size()) {
                token.push_back(line[i]);
                if (line[i] == '\\' && i + 1 < line.size()) {
                    token.push_back(line[i + 1]);
                    i += 2;
                    continue;
                }
                if (line[i] == '"') {
                    ++i;
                    break;
                }
                ++i;
            }
        } else {
            while (i < line.size() && line[i] != ',' && line[i] != '}') {
                token.push_back(line[i++]);
            }
            while (!token.empty() && token[token.size() - 1] == ' ') {
                token.erase(token.size() - 1);
            }
        }
        fields[key] = token;
    }
    return fields;
}

// CSV 整体切分为逻辑行（引号内的换行不切分，供多行字段用例使用）
inline std::vector<std::string> SplitCsvRows(const std::string &text) {
    std::vector<std::string> rows;
    std::string current;
    bool in_quotes = false;
    for (size_t i = 0; i < text.size(); ++i) {
        char c = text[i];
        if (c == '"') {
            current.push_back(c);
            if (in_quotes && i + 1 < text.size() && text[i + 1] == '"') {
                current.push_back(text[++i]);
            } else {
                in_quotes = !in_quotes;
            }
        } else if (c == '\n' && !in_quotes) {
            rows.push_back(current);
            current.clear();
        } else if (c != '\r') {
            current.push_back(c);
        }
    }
    if (!current.empty()) {
        rows.push_back(current);
    }
    return rows;
}

// table 输出按空白切分（用于断言列名/顺序；值比较请优先用 csv/ndjson）
inline std::vector<std::string> SplitWhitespace(const std::string &line) {
    std::vector<std::string> cells;
    std::string cell;
    for (size_t i = 0; i < line.size(); ++i) {
        if (line[i] == ' ' || line[i] == '\t') {
            if (!cell.empty()) {
                cells.push_back(cell);
                cell.clear();
            }
        } else {
            cell.push_back(line[i]);
        }
    }
    if (!cell.empty()) {
        cells.push_back(cell);
    }
    return cells;
}

}  // namespace cli_test

#endif  // TSFILE_CLI_TEST_SUPPORT_H
