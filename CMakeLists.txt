# 设置CMake的最低版本要求为3.10
cmake_minimum_required(VERSION 3.10)

# 定义项目名
project(cpp-tsfile-api-test-v4)
# 设置C++标准为C++17
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0 -g")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS} -O0 -g")
# 设置SDK的包含头文件目录
set(SDK_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/include/)
# 设置SDK的库目录
set(SDK_LIB_DIR_RELEASE ${PROJECT_SOURCE_DIR}/lib)
# 代码覆盖率选项
# SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fprofile-arcs -ftest-coverage")
# SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fprofile-arcs -ftest-coverage")
# SET(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fprofile-arcs -ftest-coverage")

# 添加头文件路径
INCLUDE_DIRECTORIES(${SDK_INCLUDE_DIR})

# 指定库文件的目录
LINK_DIRECTORIES(${SDK_LIB_DIR_RELEASE})

# 包含子项目
# add_subdirectory(example)

add_subdirectory(test/table)


