# # 添加测试文件和源文件文件
# add_executable(main ${CMAKE_SOURCE_DIR}/test/main.cpp 
# # 表模型测试用例
# ${CMAKE_SOURCE_DIR}/test/table/writer_test.cpp 
# # 树模型测试用例
# # 测试覆盖率的源文件
# )

# # 设置链接库
# target_link_libraries(main tsfile "${CMAKE_SOURCE_DIR}/lib/libgtest.a")

# add_executable(demo_read_and_write ${CMAKE_SOURCE_DIR}/test/table/demo_read_and_write.cpp)
# target_link_libraries(demo_read_and_write tsfile "${CMAKE_SOURCE_DIR}/lib/libgtest.a")

add_executable(demo_write ${CMAKE_SOURCE_DIR}/test/table/demo_write.cpp)
target_link_libraries(demo_write tsfile "${CMAKE_SOURCE_DIR}/lib/libgtest.a")

