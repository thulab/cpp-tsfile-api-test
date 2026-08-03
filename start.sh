#!/bin/sh
# ============================================================================
# cpp-tsfile-api-test 统一入口
# 测试框架: Google Test 1.14  |  CASE_ID: SuiteName.TestName
# ============================================================================
set -u

CMD="${1:-help}"
TGT="${2:-}"
REPORTS_DIR="${TSFILE_REPORT_DIR:-reports}"
BUILD_DIR="build"
BIN="$BUILD_DIR/test/main"
CMAKE_BIN="/usr/local/cmake-3.29.6-linux-aarch64/bin/cmake"
[ -x "$CMAKE_BIN" ] || CMAKE_BIN="cmake"

# ---- proxy (不硬编码地址，从环境变量 TEST_PROGRAM_PROXY 读取) ----
proxy_apply() {
  if [ -n "${TEST_PROGRAM_PROXY:-}" ]; then
    export HTTPS_PROXY="$TEST_PROGRAM_PROXY"
    export HTTP_PROXY="$TEST_PROGRAM_PROXY"
    # git 子模块克隆也需要代理（TsFile cmake 内部走 git clone）
    git config --global http.proxy "$TEST_PROGRAM_PROXY" 2>/dev/null
    git config --global https.proxy "$TEST_PROGRAM_PROXY" 2>/dev/null
    echo "[proxy] $TEST_PROGRAM_PROXY (git proxy also configured)"
  else
    echo "[proxy] WARNING: TEST_PROGRAM_PROXY 未设置，--proxy 无效" >&2
  fi
}
for a in "$@"; do [ "$a" = "--proxy" ] && proxy_apply; done

usage() { cat >&2 <<'EOF'
Usage: sh start.sh <command> [target] [--proxy]
  prepare            编译 TsFile C++ SDK + gtest + 测试程序
  all                运行全部测试 (gtest)
  page <name>        page table | page tree
  case <case-id>     Suite.TestName (例: TsFileTableQueryByRowTest.TestTableName_Lowercase)
  cases <id1,id2>    批量 (逗号 → gtest : 分隔)
  plm / plm-all      PLM 全量
  list-cases --json  用例清单 (JSON)
  help               帮助

Proxy: 设置环境变量后使用 --proxy (prepare 也需要)
  export TEST_PROGRAM_PROXY=http://your-proxy:port
  sh start.sh <command> --proxy
EOF
}

need_bin() { [ -x "$BIN" ] || { echo "ERROR: 未编译，先 sh start.sh prepare" >&2; exit 1; }; }

prepare() {
  # 1. gtest（从系统包安装或从源码编译）
  if [ ! -f lib/libgtest.a ] && [ ! -f lib/libgtest.so ]; then
    local gz="/root/test-program/v1.14.0.zip"
    if [ -f "$gz" ]; then
      echo "=== 编译 Google Test 1.14 ==="
      (cd /tmp && rm -rf googletest-1.14.0 && unzip -q "$gz" && cd googletest-1.14.0 && mkdir -p b && cd b && "$CMAKE_BIN" .. -DCMAKE_BUILD_TYPE=Release -q 2>&1 | tail -1 && make -j4 -s 2>&1 | tail -1)
      cp /tmp/googletest-1.14.0/b/lib/libgtest.a lib/ 2>/dev/null
      cp /tmp/googletest-1.14.0/b/lib/libgtest_main.a lib/ 2>/dev/null
      rm -rf include/gtest 2>/dev/null
      cp -r /tmp/googletest-1.14.0/googletest/include/gtest include/ 2>/dev/null
      echo "=== gtest 安装完成 ==="
    else
      echo "WARNING: libgtest.a 不存在且 $gz 也找不到，请手动安装"
    fi
  fi

  # 2. TsFile C++ SDK
  local ts="../tsfile"
  [ -f "$ts/pom.xml" ] || { echo "ERROR: $ts 不存在" >&2; return 1; }
  echo "=== 编译 TsFile C++ SDK ==="
  (cd "$ts" && mvn install -P with-cpp -DskipTests -Dspotless.check.skip=true 2>&1) || { echo "ERROR: TsFile 编译失败" >&2; return 1; }

  # 3. 同步产物
  echo "=== 同步 SDK 头文件和库文件 ==="
  local cpp="$ts/cpp"
  [ -d "$cpp/build/include" ] && { rm -rf include/common include/table include/tree 2>/dev/null; cp -rn "$cpp/build/include/"* include/ 2>/dev/null; }
  [ -d "$cpp/build/lib" ]     && { cp -rn "$cpp/build/lib/"* lib/ 2>/dev/null; }

  # 4. CMake + Make
  echo "=== CMake + Make ==="
  rm -rf "$BUILD_DIR" && mkdir -p "$BUILD_DIR"
  (cd "$BUILD_DIR" && "$CMAKE_BIN" .. -DCMAKE_BUILD_TYPE=Release -q 2>&1 | tail -1 && make -j4 2>&1 | tail -3)
  if [ -x "$BIN" ]; then echo "=== prepare 完成: $BIN ==="; ls -lh "$BIN"
  else echo "ERROR: $BIN 未生成" >&2; return 1; fi
}

list_cases() {
  need_bin; mkdir -p "$REPORTS_DIR"
  "$BIN" --gtest_list_tests 2>/dev/null | python3 -c "
import json,sys; cs=[]; s=''
for ln in sys.stdin:
 ln=ln.rstrip('\n')
 if not ln: continue
 if ln.endswith('.') and not ln.startswith(' '): s=ln[:-1]
 elif ln.startswith('  '):
  n=ln.strip()
  if n and 'DISABLED_' not in n: cs.append({'caseId':f'{s}.{n}','automationType':'ts-cpp','nodeId':f'{s}.{n}','sourceFile':'','description':''})
print(json.dumps(cs,ensure_ascii=False,indent=2))"
}

all()  { need_bin; mkdir -p "$REPORTS_DIR"; "$BIN" --gtest_output="json:$REPORTS_DIR/report.json" 2>&1; echo "=== $REPORTS_DIR/report.json ==="; }
page() {
  need_bin; mkdir -p "$REPORTS_DIR"
  case "$1" in table) F="TsFileTable*.*";; tree) F="TsFileTree*.*";; *) echo "Unknown: $1" >&2; return 2;; esac
  "$BIN" --gtest_filter="$F" --gtest_output="json:$REPORTS_DIR/page-$1.json" 2>&1
}
case_one()    { need_bin; [ -z "$1" ] && { usage; return 2; }; mkdir -p "$REPORTS_DIR"; "$BIN" --gtest_filter="$1" --gtest_output="json:$REPORTS_DIR/case.json" 2>&1; }
cases_batch() { need_bin; [ -z "$1" ] && { usage; return 2; }; mkdir -p "$REPORTS_DIR"; "$BIN" --gtest_filter="$(echo "$1" | sed 's/,/:/g')" --gtest_output="json:$REPORTS_DIR/cases.json" 2>&1; }
plm() { echo "[plm] 全量测试"; all; }

case "$CMD" in
  prepare) prepare;;  all) all;;  page) page "$TGT";;
  case|case-id) case_one "$TGT";;  cases) cases_batch "$TGT";;
  plm|plm-all) plm;;  list-cases) list_cases;;  help|-h|--help) usage;;
  *.*) case_one "$CMD";;  *) echo "Unknown: $CMD" >&2; usage; exit 2;;
esac
