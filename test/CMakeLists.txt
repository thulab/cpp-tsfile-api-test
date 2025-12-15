# # 测试多个文件添加测试文件和源文件文件
# add_executable(main ${CMAKE_SOURCE_DIR}/test/main.cpp 
# # 表模型测试用例
# ${CMAKE_SOURCE_DIR}/test/table/test_table_writer.cpp 
# # 树模型测试用例
# # ${CMAKE_SOURCE_DIR}/test/tree/test_tree_writer.cpp
# # 测试覆盖率的源文件
# )
# target_link_libraries(main tsfile "${CMAKE_SOURCE_DIR}/lib/libgtest.a")

# 测试单个独立文件（自身带main函数，若不带main函数，且符合gtest格式，则可以添加到多文件测试目录中）
add_executable(main ${CMAKE_SOURCE_DIR}/test/other/tsfile_table_writer_and_read.cpp)
target_link_libraries(main tsfile "${CMAKE_SOURCE_DIR}/lib/libgtest.a")
