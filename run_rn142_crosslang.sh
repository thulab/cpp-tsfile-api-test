#!/usr/bin/env bash
# RN-142 用例 44/45（crosslang：Python 生成 dataset index → C++ 读取验证）生成+编译+运行。
#
# 落位背景：2026-10-10 审查发现该用例此前只在 /tmp 临时工作区编译运行过
#   （依赖头 crosslang_expected.h 与 fixture 均未随仓库、未编入 CMakeLists，
#   运行依赖易失路径）。本脚本把原批次（2026-09-28）流程固化为可复现三步：
#   ① 用 python 侧 rn142_tests/prepare_crosslang_index.py 生成 fixture 与期望头
#      （文件 mtime 指纹在生成时刻写入索引与头文件——生成即用，请勿再拷贝移动）；
#   ② g++ 独立编译（该用例不进主套件：主套件编译时 fixture/头尚不存在，独立目标
#      可避免默认全量运行对预生成产物的硬依赖）；
#   ③ 以 RN142_CROSSLANG_DIR 指向 fixture 运行（用例经 env 定位 fixture）。
# 用法: bash run_rn142_crosslang.sh [输出目录]   默认: <脚本目录>/build/rn142-crosslang
# 前置: python 测试程序 .venv（软链产品 venv，装有当期 tsfile wheel）；lib/ 为当期产品库。
set -euo pipefail
BASE="$(cd "$(dirname "$0")" && pwd)"
PY=/data/test/test-program/python-tsfile-api-test
OUT="${1:-$BASE/build/rn142-crosslang}"
SRC_DATA="${RN142_SOURCE:-/data/test/rn142-data/D1/table}"
case "$OUT" in /|""|"$BASE"|"$BASE/") echo "FAIL: 非法输出目录: $OUT"; exit 2;; esac

echo "== ① 生成 fixture 与期望头 -> $OUT =="
rm -rf "$OUT"; mkdir -p "$OUT"
"$PY/.venv/bin/python" -B "$PY/rn142_tests/prepare_crosslang_index.py" \
  --source "$SRC_DATA" --output "$OUT/fixture" --header "$OUT/crosslang_expected.h"

echo "== ② 独立编译用例（g++，与 2026-09-28 批次一致）=="
g++ -std=c++17 -O0 -g \
  -I"$BASE/include" -I"$OUT" \
  "$BASE/test/main.cpp" "$BASE/test/other/test_dataset_index_crosslang.cpp" \
  -L"$BASE/lib" -Wl,-rpath,"$BASE/lib" -ltsfile "$BASE/lib/libgtest.a" -pthread \
  -o "$OUT/rn142_dataset_index_tests"
ldd "$OUT/rn142_dataset_index_tests" | grep -i tsfile || true

echo "== ③ 运行 =="
RN142_CROSSLANG_DIR="$OUT/fixture" "$OUT/rn142_dataset_index_tests" \
  --gtest_filter='DatasetIndexCrossLangTest.*'
echo "== 完成: 结果见上方 gtest 输出 =="
