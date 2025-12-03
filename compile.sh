#!/bin/bash

# 检查当前目录下是否存在 build 目录
if [ -d "build" ]; then
    # 如果 build 目录存在，则清空目录下的所有文件和子目录
    rm -rf build/*
    # 确保目录是空的
    rmdir build/* 2>/dev/null
else
    # 如果 build 目录不存在，则创建该目录
    mkdir build
fi

# 进入 build 目录
cd build

# 执行 cmake 命令
cmake ..

# 执行 make 命令
make

# # 执行 make 命令并捕获返回状态
# make_status=0
# make || make_status=$?

# # 检查 make 命令是否成功执行
# if [ $make_status -eq 0 ]; then
#     echo "/********* Compilation successful *********/"
# else
#     echo "Compilation failed with status code $make_status"
# fi