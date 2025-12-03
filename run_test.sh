#!/bin/bash

# 提示用户输入文件名
echo "请输入文件名："
read filename

# 检查文件是否存在并且是可执行的
if [ -x "./build/$filename" ]; then
    echo "/******* Start Test ********/"
    cd ./build
    # 执行文件
    ./$filename --gtest_output=xml:test_results.xml
else
    # 文件不存在或不可执行，打印错误信息
    echo "!!!错误：build/test 目录下 $filename 文件不存在或不是可执行文件。"
fi