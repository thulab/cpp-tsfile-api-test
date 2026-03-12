#!/bin/bash


# 检查文件是否存在并且是可执行的
if [ -x "./build/test/$filename" ]; then
   echo "/******* Start test ********/"
   cd ./build/test
   # 执行文件
   ./main --gtest_output="json:cpp_tsfile_test_report.json"
else
   # 文件不存在或不可执行，打印错误信息
   echo "!!!错误:build/test 目录下 $filename 文件不存在或不是可执行文件。"
fi


