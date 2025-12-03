// #include <iterator>
// #include <time.h>

// #include <iostream>
// #include <random>
// #include <string>
// #include <filesystem>

// #include "common/db_common.h"
// #include "cpp_test.h"


// using namespace storage;

// // 用于演示写入操作
// int main() {

//     // 创建一个TsFileWriter对象
//     TsFileWriter* tsfile_writer_ = new TsFileWriter();
//     // 初始化libtsfile库
//     libtsfile_init();
//     // 生成一个文件名
//     std::string file_name_ = std::string("tree_tsfile.tsfile");

//     // 设置文件打开的标志:只写模式打开|文件不存在会创建一个新的文件|文件已经存在且能够成功打开文件那么内容会被清空
//     int flags = O_WRONLY | O_CREAT | O_TRUNC;
//     // 设置文件权限模式
//     mode_t mode = 0666;
//     // 使用TsFileWriter打开文件
//     tsfile_writer_->open(file_name_, flags, mode);
//     // 定义设备和物理量的数量
//     const int device_num = 10;
//     const int measurement_num = 10;
//     // 定义用于存放设备和物理量名的数组
//     std::string device_names[device_num];
//     std::string measurement_name[measurement_num];
//     // 定义一个用于存储MeasurementSchema的容器
//     std::vector<MeasurementSchema> schema_vec[10];
//     // 遍历设备
//     for (int i = 0; i < device_num; i++) {
//         // 添加设备名
//         device_names[i]= "root.db1.d" + std::to_string(i);
//         int num = 0;
//         // 遍历物理量
//         for (int j = 0; j < measurement_num; j++) {
//             // 根据数据类型写入对应的值
//             switch (num) {
//                 case 0:
//                     // 添加物理量名
//                     measurement_name[j] = "m" + std::to_string(i) + std::to_string(j);
//                     // 添加schema
//                     schema_vec[i].push_back(MeasurementSchema(measurement_name[j], common::TSDataType::BOOLEAN, common::TSEncoding::PLAIN, common::CompressionType::UNCOMPRESSED));
//                     // 注册时间序列
//                     tsfile_writer_->register_timeseries(device_names[i], measurement_name[j], common::TSDataType::BOOLEAN, common::TSEncoding::PLAIN, common::CompressionType::UNCOMPRESSED);
//                     break;
//                 case 1:
//                     // 添加物理量名
//                     measurement_name[j] = "m" + std::to_string(i) + std::to_string(j);
//                     // 添加schema
//                     schema_vec[i].push_back(MeasurementSchema(measurement_name[j], common::TSDataType::INT32, common::TSEncoding::PLAIN, common::CompressionType::UNCOMPRESSED));
//                     // 注册时间序列
//                     tsfile_writer_->register_timeseries(device_names[i], measurement_name[j], common::TSDataType::INT32, common::TSEncoding::PLAIN, common::CompressionType::UNCOMPRESSED);
//                     break;
//                 case 2:
//                     // 添加物理量名
//                     measurement_name[j] = "m" + std::to_string(i) + std::to_string(j);
//                     // 添加schema
//                     schema_vec[i].push_back(MeasurementSchema(measurement_name[j], common::TSDataType::INT64, common::TSEncoding::PLAIN, common::CompressionType::UNCOMPRESSED));
//                     // 注册时间序列
//                     tsfile_writer_->register_timeseries(device_names[i], measurement_name[j], common::TSDataType::INT64, common::TSEncoding::PLAIN, common::CompressionType::UNCOMPRESSED);
//                     break;
//                 case 3:
//                     // 添加物理量名
//                     measurement_name[j] = "m" + std::to_string(i) + std::to_string(j);
//                     // 添加schema
//                     schema_vec[i].push_back(MeasurementSchema(measurement_name[j], common::TSDataType::FLOAT, common::TSEncoding::PLAIN, common::CompressionType::UNCOMPRESSED));
//                     // 注册时间序列
//                     tsfile_writer_->register_timeseries(device_names[i], measurement_name[j], common::TSDataType::FLOAT, common::TSEncoding::PLAIN, common::CompressionType::UNCOMPRESSED);
//                     break;
//                 case 4:
//                     // 添加物理量名
//                     measurement_name[j] = "m" + std::to_string(i) + std::to_string(j);
//                     // 添加schema
//                     schema_vec[i].push_back(MeasurementSchema(measurement_name[j], common::TSDataType::DOUBLE, common::TSEncoding::PLAIN, common::CompressionType::UNCOMPRESSED));
//                     // 注册时间序列
//                     tsfile_writer_->register_timeseries(device_names[i], measurement_name[j], common::TSDataType::DOUBLE, common::TSEncoding::PLAIN, common::CompressionType::UNCOMPRESSED);
//                     break;
//                 // TODO：现在写入暂不支持text类型
//                 // case 5:
//                 //     // 添加物理量名
//                 //     measurement_name[j] = "m" + std::to_string(j);
//                 //     // 添加schema
//                 //     schema_vec[i].push_back(MeasurementSchema(measurement_name[j], common::TSDataType::TEXT, common::TSEncoding::PLAIN, common::CompressionType::UNCOMPRESSED));
//                 //     // 注册时间序列
//                 //     tsfile_writer_->register_timeseries(device_names[i], measurement_name[j], common::TSDataType::TEXT, common::TSEncoding::PLAIN, common::CompressionType::UNCOMPRESSED);
//                 //     break;
//                 default:
//                      // 添加物理量名
//                     measurement_name[j] = "m" + std::to_string(i) + std::to_string(j);
//                     // 添加schema
//                     schema_vec[i].push_back(MeasurementSchema(measurement_name[j], common::TSDataType::BOOLEAN, common::TSEncoding::PLAIN, common::CompressionType::UNCOMPRESSED));
//                     // 注册时间序列
//                     tsfile_writer_->register_timeseries(device_names[i], measurement_name[j], common::TSDataType::BOOLEAN, common::TSEncoding::PLAIN, common::CompressionType::UNCOMPRESSED);
//                     num=0;
//                     break;
//             }
//             num++;
//         }
//     }
    
//     // 提示用户输入tablet大小
//     std::cout << "请输入 tablet 大小：";
//     int tablet_size;
//     std::cin >> tablet_size;
//     // 定义最大行数和当前行数
//     int max_rows = 100;
//     int cur_row = 0;
//     // 遍历每个设备
//     for (int i = 0; i < device_num; i++) {
//         // 获取设备名
//         std::string device_name = device_names[i];
//         // 声明tablet对象
//         Tablet tablet(device_name, &schema_vec[i], tablet_size);
//         // 初始化
//         tablet.init();
//         // 设置时间戳
//         for (int row = 0; row < tablet_size; row++) {
//             tablet.set_timestamp(row, 1 + row);
//         }
//         // 设置值
//         for (int j = 0; j < measurement_num; j++) { // 遍历每个物理量
//             for (int row = 0; row < tablet_size; row++) { // 遍历每行
//                 // 根据数据类型写入对应的值
//                 switch (schema_vec[i][j].data_type_) {
//                     case common::BOOLEAN:
//                         tablet.set_value(row, j, ((row % 2) != 0? true : false));
//                         break;
//                     case common::INT32:
//                         tablet.set_value(row, j, row);
//                         break;
//                     case common::INT64:
//                         tablet.set_value(row, j, row);
//                         break;
//                     case common::FLOAT:
//                         tablet.set_value(row, j, static_cast<float>(row));
//                         break;
//                     case common::DOUBLE:
//                         tablet.set_value(row, j, static_cast<double>(row));
//                         break;
//                     // TODO：set_value方法暂不支持添加text数据
//                     // case common::TEXT:
//                     //     tablet.set_value(row, j, std::string(std::to_string(row )));
//                     //     break;
//                     default:
//                         ASSERT(false); // 断言失败，表示代码中出现了未处理的类型
//                         break;
//                 }
//             }
//         }
//         // 写入tablet并刷新
//         tsfile_writer_->write_tablet(tablet);
//     }
//     tsfile_writer_->flush();
//     // 关闭TsFileWriter
//     tsfile_writer_->close();
//     // 返回0表示成功
//     return file_name_;
// }