# 测试多个文件添加测试文件和源文件文件
add_executable(main ${CMAKE_SOURCE_DIR}/test/main.cpp
# 表模型测试用例
${CMAKE_SOURCE_DIR}/test/table/test_table_writer.cpp
${CMAKE_SOURCE_DIR}/test/table/test_table_query_by_row.cpp
${CMAKE_SOURCE_DIR}/test/table/test_table_get_timeseries_metadata.cpp
${CMAKE_SOURCE_DIR}/test/table/test_table_restorable_writer.cpp
# 树模型测试用例
${CMAKE_SOURCE_DIR}/test/tree/test_tree_writer.cpp
${CMAKE_SOURCE_DIR}/test/tree/test_tree_query_by_row.cpp
${CMAKE_SOURCE_DIR}/test/tree/test_tree_get_timeseries_metadata.cpp
${CMAKE_SOURCE_DIR}/test/tree/test_tree_restorable_writer.cpp
# 文件后端 / mmap 读后端用例（RN-144）
${CMAKE_SOURCE_DIR}/test/other/test_file_read_backend.cpp
${CMAKE_SOURCE_DIR}/test/other/test_random_access_contract.cpp
# CLI 黑盒用例（RN-143，子进程调用真实 tsfile-cli）
${CMAKE_SOURCE_DIR}/test/other/test_tsfile_cli_surface.cpp
${CMAKE_SOURCE_DIR}/test/other/test_tsfile_cli_format.cpp
${CMAKE_SOURCE_DIR}/test/other/test_tsfile_cli_meta.cpp
${CMAKE_SOURCE_DIR}/test/other/test_tsfile_cli_stats.cpp
${CMAKE_SOURCE_DIR}/test/other/test_tsfile_cli_rowread.cpp
${CMAKE_SOURCE_DIR}/test/other/test_tsfile_cli_export.cpp
${CMAKE_SOURCE_DIR}/test/other/test_tsfile_cli_write.cpp
${CMAKE_SOURCE_DIR}/test/other/test_tsfile_cli_safety.cpp
${CMAKE_SOURCE_DIR}/test/other/test_tsfile_cli_interop.cpp
${CMAKE_SOURCE_DIR}/test/other/test_rle_malformed_input.cpp
${CMAKE_SOURCE_DIR}/test/other/test_alp_encoding.cpp
${CMAKE_SOURCE_DIR}/test/other/test_alp_security.cpp
${CMAKE_SOURCE_DIR}/test/other/test_alp_errors.cpp
# 测试覆盖率的源文件
${CMAKE_SOURCE_DIR}/include
)
# CLI 默认路径（运行时可用环境变量 TSFILE_CLI 覆盖）
target_compile_definitions(main PRIVATE TSFILE_CLI_DEFAULT="${CMAKE_SOURCE_DIR}/bin/tsfile-cli")
target_link_libraries(main tsfile pthread antlr4-runtime lzokay LZ4 snappy z "${CMAKE_SOURCE_DIR}/lib/libgtest.a")

# # 测试单个独立文件（自身带main函数，若不带main函数，且符合gtest格式，则可以添加到多文件测试目录中）
# add_executable(main ${CMAKE_SOURCE_DIR}/test/other/tsfile_table_writer_and_read.cpp)
# target_link_libraries(main tsfile pthread antlr4-runtime lzokay LZ4 snappy z "${CMAKE_SOURCE_DIR}/lib/libgtest.a")
