# 添加文件
add_executable(main ${CMAKE_SOURCE_DIR}/example/main.cpp ${CMAKE_SOURCE_DIR}/example/cpp_examples.h ${CMAKE_SOURCE_DIR}/example/demo_read.cpp ${CMAKE_SOURCE_DIR}/example/demo_write.cpp)

# 设置链接库
target_link_libraries(main tsfile "${CMAKE_SOURCE_DIR}/lib/libgtest.a")