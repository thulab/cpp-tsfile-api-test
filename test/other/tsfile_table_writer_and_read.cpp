      
#include "gtest/gtest.h"

#include "common/db_common.h"
#include "common/path.h"
#include "common/record.h"
#include "common/row_record.h"
#include "common/schema.h"
#include "common/tablet.h"
#include "file/write_file.h"
#include "reader/expression.h"
#include "reader/filter/filter.h"
#include "reader/qds_with_timegenerator.h"
#include "reader/qds_without_timegenerator.h"
#include "reader/tsfile_reader.h"
#include "writer/tsfile_table_writer.h"
#include "writer/tsfile_writer.h"
#include "cwrapper/tsfile_cwrapper.h"
#include "cwrapper/errno_define_c.h"
#include <iostream>
#include <writer/tsfile_table_writer.h>
#include <string>
// 添加必要的头文件以支持路径操作
#include <filesystem>
#include <unistd.h>

// 表名
std::string table_name = "t1";
// 文件名
std::string file_path = "test_table.tsfile";

// 初始化文件路径
void init_file_path() {
    // 定义字符数组存储可执行文件路径
    char result[ PATH_MAX ];
    // 使用 readlink 函数读取 /proc/self/exe 符号链接获取当前进程可执行文件的实际路径
    ssize_t count = readlink( "/proc/self/exe", result, PATH_MAX );
    // 将获取到的路径字符数组转换为 std::string 对象
    std::string executable_path = std::string( result, (count > 0) ? count : 0 );
    // 使用 std::filesystem::path 对象解析可执行文件路径并返回目录部分
    std::filesystem::path path_obj(executable_path);
    // 获取可执行文件所在目录
    std::string exec_path = path_obj.parent_path().string();
    // 获取项目根目录（假设可执行文件在项目根目录或其子目录中）
    std::filesystem::path root_path(exec_path);
    // 向上查找直到找到包含"data"目录的根目录
    while (!root_path.empty() && !std::filesystem::exists(root_path / "data")) {
        root_path = root_path.parent_path();
    }
    
    if (!root_path.empty()) {
        // 构建完整的文件路径
        std::filesystem::path full_path = root_path / "data" / "tsfile" / file_path;
        file_path = full_path.string();
        std::cout << "file_path: " << file_path << std::endl;
        
        // 确保目录存在
        std::filesystem::path dir_path = full_path.parent_path();
        if (!std::filesystem::exists(dir_path)) {
            std::filesystem::create_directories(dir_path);
        }
    }
}

// 查询数据
void query_data() {
    // 初始化文件路径
    init_file_path();
    
    // 初始化 TsFile 存储模块
    storage::libtsfile_init();
    
    // 创建 TsFile 读取器对象，用于读取 TsFile 文件
    storage::TsFileReader reader;
    reader.open(file_path);

    // 查询
    std::vector<std::string> columns;
    columns.emplace_back("tag1");
    columns.emplace_back("tag2");
    columns.emplace_back("field1");
    columns.emplace_back("field2");
    columns.emplace_back("field3");
    columns.emplace_back("field4");
    columns.emplace_back("field5");
    std::int64_t start_time = INT64_MIN;
    std::int64_t end_time = INT64_MAX;
    storage::ResultSet* temp_ret = nullptr;
    reader.query(table_name, columns, start_time, end_time, temp_ret);

    // 强制转换为 TableResultSet 类型
    auto ret = dynamic_cast<storage::TableResultSet*>(temp_ret);
    // 获取查询结果的元数据信息
    auto metadata = ret->get_metadata();
    // 获取查询结果的列数
    int column_num = metadata->get_column_count();
    // // 获取列名和列类型
    // for (int i = 1; i <= column_num; i++) {
    //     std::cout << metadata->get_column_name(i) << " ";
    // }
    // std::cout << std::endl;
    // for (int i = 1; i <= column_num; i++) {
    //     auto type = metadata->get_column_type(i);
    //     switch (type) {
    //         case common::TSDataType::INT32: 
    //             std::cout << "INT32 ";
    //             break;
    //         case common::TSDataType::INT64: 
    //             std::cout << "INT64 ";
    //             break;
    //         case common::TSDataType::FLOAT: 
    //             std::cout << "FLOAT ";
    //             break;
    //         case common::TSDataType::DOUBLE: 
    //             std::cout << "DOUBLE ";
    //             break;
    //         case common::TSDataType::STRING: 
    //             std::cout << "STRING ";
    //             break;
    //         default: std::cout << "UNKNOWN: " << metadata->get_column_type(i) << " ";break;
    //     }
    // }
    // std::cout << std::endl;

    int num = 0;
    // 定义一个布尔变量，用于标记是否有更多数据
    bool has_next = false;
    // 循环读取查询结果中的数据
    while ((ret->next(has_next)) == common::E_OK && has_next) {
        // 获取当前行的时间戳
        std::cout << ret->get_value<Timestamp>(1) << " ";
        // 获取值
        for (int i = 1; i <= column_num; i++) {
            if (ret->is_null(i)) {
                    std::cout << "null" << " ";
                } else {
                    switch (metadata->get_column_type(i)) {
                        case common::DATE:
                        case common::INT32:
                            std::cout << ret->get_value<int32_t>(i) << " ";
                            break;
                        case common::TIMESTAMP:
                        case common::INT64:
                            std::cout << ret->get_value<int64_t>(i) << " ";
                            break;
                        case common::FLOAT:
                            std::cout << ret->get_value<float>(i) << " ";
                            break;
                        case common::DOUBLE:
                            std::cout << ret->get_value<double>(i) << " ";
                            break;
                        case common::BLOB:
                        case common::TEXT:
                        case common::STRING:
                            std::cout << ret->get_value<common::String*>(i)
                                                 ->to_std_string()
                                      << " ";
                            break;
                        case common::BOOLEAN:
                            std::cout << (ret->get_value<bool>(i) == 0 ? "false" : "true") << " ";
                            break;
                        default:
                            ASSERT_TRUE(false) << "Unsupported data type: " << metadata->get_column_type(i);
                    }
                }
        }
        num++;
        std::cout << std::endl;
    }
    std::cout << "实际获取到的数据行数：" << num << std::endl;

    // 关闭查询结果集，释放资源
    ret->close();

    // 关闭查询接口，释放资源.
    reader.close();
}

// 写入接口
int main() {
    // 初始化文件路径
    init_file_path();
    
    // 初始化 TsFile 存储模块
    storage::libtsfile_init();

    // 创建一个具有指定路径的文件来写入tsfile
    storage::WriteFile file;
    int flags = O_WRONLY | O_CREAT | O_TRUNC;
    mode_t mode = 0666;
    #ifdef _WIN32
        flags |= O_BINARY;
    #endif
    file.create(file_path, flags, mode);

    // 创建表模式来描述ts文件中的表
    auto* schema = new storage::TableSchema(
        table_name,
        {
            common::ColumnSchema("tag1", common::TSDataType::STRING, common::ColumnCategory::TAG),
            common::ColumnSchema("tag2", common::TSDataType::STRING, common::ColumnCategory::TAG),
            common::ColumnSchema("field1", common::TSDataType::INT64, common::ColumnCategory::FIELD),
            common::ColumnSchema("field2", common::TSDataType::INT32, common::ColumnCategory::FIELD),
            common::ColumnSchema("field3", common::TSDataType::FLOAT),
            common::ColumnSchema("field4", common::TSDataType::DOUBLE),
            common::ColumnSchema("field5", common::TSDataType::TEXT),
        });

    // 写入的缓存大小
    uint64_t memory_threshold = 128 * 1024 * 1024;

    // 创建一个具有指定路径的文件来写入tsfile
    auto* writer = new storage::TsFileTableWriter(&file, schema, memory_threshold);

    // 创建 tablet 以插入数据。
    storage::Tablet tablet(
        {
            "tag1", 
            "tag2", 
            "field1", 
            "field2", 
            "field3", 
            "field4", 
            "field5", 
        },
        {
            common::TSDataType::STRING, 
            common::TSDataType::STRING, 
            common::TSDataType::INT64, 
            common::TSDataType::INT32, 
            common::TSDataType::FLOAT, 
            common::TSDataType::DOUBLE, 
            common::TSDataType::TEXT, 
        },
        10);
    // std::cout << "预期输出效果" << std::endl;
    // for (int row = 0; row < 10; row++) {
    //     std::cout << row << " ";
    //     std::cout << row << " ";
    //     if (row % 2 == 0) {
    //         std::cout << "tag1" << " ";
    //         std::cout << "tag2" << " ";
    //         std::cout << row << " ";
    //         std::cout << "1" << " ";
    //         std::cout << "1.1" << " ";
    //         std::cout << "1.2" << " ";
    //         std::cout << "text" << " ";
    //     } else {
    //         std::cout << "null" << " ";
    //         std::cout << "null" << " ";
    //         std::cout << "null" << " ";
    //         std::cout << "null" << " ";
    //         std::cout << "null" << " ";
    //         std::cout << "null" << " ";
    //     }
    //     std::cout << std::endl;
    // }
    // std::cout << "预期获取到的数据行数：10" << std::endl;
    // std::cout << "-----------------------------------------" << std::endl;
    // std::cout << "实际输出效果" << std::endl;
    for (int row = 0; row < 10; row++) {
        long timestamp = row;
        tablet.add_timestamp(row, timestamp);
        tablet.add_value(row, "tag1", "tag1");
        tablet.add_value(row, "tag2", "tag2");
        tablet.add_value(row, "field1", static_cast<int64_t>(row));
        tablet.add_value(row, "field2", static_cast<int32_t>(row));
        tablet.add_value(row, "field3", static_cast<float>(1.1));
        tablet.add_value(row, "field4", static_cast<double>(1.2));
        if (row % 2 == 0) {
            tablet.add_value(row, "field5", "text");
        }
    }
    // 写入数据
    writer->write_table(tablet);
    // 刷新数据
    writer->flush();

    // 关闭写入
    writer->close();

    // 释放动态分配的内存
    delete writer;
    delete schema;

    // 查询数据
    query_data();

}


    