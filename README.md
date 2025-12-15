# cpp-tsfile-api-test-v4

----

# 环境

- CMake >= 3.22.1
- Maven >= 3.9.9
- GCC >= 11.4.0
- GNU Make >= 4.3

# 结构

```bash
# 项目结构
|—— example                  # 存放示例代码的目录
|—— include                  # 存放编译后生成的头文件的目录
|—— lib                      # 存放编译后生成的库文件的目录
|—— test                     # 存放测试用例的目录
|   |—— table                # 存放表模型测试用例
|   |—— tree                 # 存放树模型测试用例
|   |—— CMakeLists.txt       # 子目录的 CMakeLists 文件
|—— CMakeLists.txt           # 父 CMakeLists 文件
|—— compile.sh               # 编译测试用例的脚本
|—— README.md                # 说明文档
|—— run_test.sh              # 编译后执行测试的脚本（目前只能指定测试一个测试用例）

```

# 依赖

步骤一：编译源码生成头文件和库文件

```bash
git clone https://github.com/apache/tsfile.git
cd tsfile
mvn clean install -P with-cpp -DskipTests

```

注意：如果 Maven 没有安装，您可以在 Linux 上使用 mvnw。


步骤二：配置头文件和库文件

- 头文件目录: 编译后位于 tsfile/cpp/target/build/include 下，将 include 目录复制到测试程序根目录下的 include 目录（也可以复制 tsfile/cpp/src 目录中内容）。
- 库文件目录: 编译后位于 tsfile/cpp/target/build/lib，将 lib 目录复制到测试程序根目录下的 lib 目录。
- 第三方库：编译后位于 tsfile/cpp/third_party/antlr4-cpp-runtime-4/runtime/src 下，将 src 目录下全部文件和文件夹复制到 include 目录（不包含 src 目录）。
- 其他：由于本程序全部测试用例使用GTest格式进行编写，所以需要安装GTest，详见下方步骤

# 测试

## 自动化测试——GTest

### 安装

以Ubuntu22为例，已配置好C++环境，可略过，测试程序已集成
1. 下载并解压压缩包

```bash
wget https://github.com/google/googletest/archive/refs/tags/v1.14.0.zip
unzip v1.14.0.zip
cd googletest-1.14.0
```

2. 编译

```bash
mkdir build
cd build
cmake ..
make
sudo make install
```

3. 集成到C++测试程序项目中
头文件位于/usr/local/include目录下的gtest文件夹，库文件位于/usr/local/lib目录下名为libgmock和libgtest开头的库文件（4个），或可使用googletest-1.14.0/build/lib中的库
- 步骤一：复制头文件目录gtest到项目的include目录下，复制名为libgmock和libgtest开头的库文件（4个）到项目lib目录下
- 步骤二：往CMackLists.txt文件中target_link_libraries添加${CMAKE_SOURCE_DIR}/lib/libgtest.a链接，编写代码时引入头文件#include "gtest/gtest.h"即可使用
- 步骤三（可选）：若需要使用一个main文件作为全部测试文件的启动入口，则需要在main文件中添加下面代码，这样只需要添加main作为可执行文件，会自动识别当前目录下及其子目录以_test结尾的用例文件

```bash
#include "gtest/gtest.h"

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
```

### 使用

```bash
cd cpp-tsfile-test-v4/
mkdir build
cd build
cmake ..
make
./main
```

## 覆盖率测试——Lcov

### 安装

```bash
# 前往 https://github.com/linux-test-project/lcov/releases 下载
tar xzvf lcov-2.3.1.tar.gz
cd lcov-2.3.1
make install
sudo apt install cpanminus
sudo cpanm Capture::Tiny
sudo cpanm DateTime
hash -r
lcov --version
```

在父CMakeLists中添加下配置

```bash
# coverage option
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fprofile-arcs -ftest-coverage")
SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fprofile-arcs -ftest-coverage")
SET(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fprofile-arcs -ftest-coverage")
```

在子CMakeLists中添加测试文件里面添加需要测试的源码

```bash
add_executable(main xxx)
```

### 使用

```bash
# 1、编译
cd cpp-tsfile-test-v4/
mkdir build
cd build
cmake ..
make
# 2、执行
./main
# 3、收集覆盖率数据
cd ..
lcov --capture --directory build --output-file coverage.info --rc branch_coverage=1 --ignore-errors inconsistent
#忽略执行数据：lcov --remove coverage.info '/boost/*' '*/gtest/*' '/c++/*' '/data/iotdb-test/test/cpp-session-test/test/table/*' --output-file coverage.info --rc branch_coverage=1 --ignore-errors inconsistent --ignore-errors mismatch
# 4、生成html报告
genhtml coverage.info --output-directory coverage_report --branch-coverage --ignore-errors corrupt,inconsistent
```

- 收集覆盖率数据参数
-c 或 --capture ：表示捕获覆盖率数据。
-d 或 --directory ：指定要扫描的目录，收集该目录下的覆盖率数据。
-o 或 --output-file：指定输出文件名。
--ignore-errors inconsistent：显式忽略了不一致的错误。
--rc lcov_branch_coverage=1：开启分支覆盖。
- 生成html报告参数
--output-directory：输出html目录。
--branch-coverage：启用分支覆盖率的显示。


## 性能测试——

暂未实现

