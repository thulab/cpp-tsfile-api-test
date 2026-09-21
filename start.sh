#!/bin/sh
# ============================================================================
# cpp-tsfile-api-test 统一入口
# 测试框架: Google Test 1.14  |  CASE_ID: SuiteName.TestName
# ============================================================================
set -u

CMD="${1:-help}"
TGT="${2:-}"
REPORTS_DIR="${REPORT_DIR:-${TSFILE_REPORT_DIR:-reports}}"
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
      (cd /tmp && rm -rf googletest-1.14.0 && unzip -q "$gz" && cd googletest-1.14.0 && mkdir -p b && cd b && "$CMAKE_BIN" .. -DCMAKE_BUILD_TYPE=Release 2>&1 | tail -1 && make -j4 -s 2>&1 | tail -1)
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
  # 兼容两种 SDK 产物布局：老版本 cpp/build/，新版本 cpp/target/build/
  for cpp_out in "$cpp/build" "$cpp/target/build"; do
    [ -d "$cpp_out/include" ] && { rm -rf include/common include/table include/tree 2>/dev/null; cp -rn "$cpp_out/include/"* include/ 2>/dev/null; }
    [ -d "$cpp_out/lib" ]     && { cp -rn "$cpp_out/lib/"* lib/ 2>/dev/null; }
  done

  # 4. CMake + Make
  echo "=== CMake + Make ==="
  rm -rf "$BUILD_DIR" && mkdir -p "$BUILD_DIR"
  (cd "$BUILD_DIR" && "$CMAKE_BIN" .. -DCMAKE_BUILD_TYPE=Release 2>&1 | tail -1 && make -j4 2>&1 | tail -3)
  if [ -x "$BIN" ]; then echo "=== prepare 完成: $BIN ==="; ls -lh "$BIN"
  else echo "ERROR: $BIN 未生成" >&2; return 1; fi
}

list_cases() {
  need_bin; mkdir -p "$REPORTS_DIR"
  "$BIN" --gtest_list_tests 2>/dev/null | python3 -c "
import json,sys,re,glob
cs=[]; s=''
SRC={}
for _fp in glob.glob('test/**/*.cpp', recursive=True):
    try: _t=open(_fp,encoding='utf-8',errors='replace').read()
    except Exception: continue
    for _m in re.finditer(r'\\bTEST(?:_F|_P)?\\s*\\(\\s*([A-Za-z0-9_]+)\\s*,', _t): SRC.setdefault(_m.group(1), _fp)
    for _m in re.finditer(r'\\bINSTANTIATE_TEST_SUITE_P\\s*\\(\\s*([A-Za-z0-9_]+)', _t): SRC.setdefault(_m.group(1), _fp)
for ln in sys.stdin:
 ln=ln.rstrip('\n')
 if not ln: continue
 if ln.endswith('.') and not ln.startswith(' '): s=ln[:-1]
 elif ln.startswith('  '):
  n=ln.strip()
  if n and 'DISABLED_' not in n: cs.append({'caseId':f'{s}.{n}','automationType':'ts-cpp','nodeId':f'{s}.{n}','sourceFile':(SRC.get(s) or SRC.get(s.split('/')[0]) or 'test/'),'description':''})
# Build reverse map gtest name -> Cpp CASE_ID from .plm-case-id-map.json
rev = {}
try:
    with open('.plm-case-id-map.json') as fm: m = json.load(fm)
    nmap = m.get('nmap', m)
    for cid, names in nmap.items():
        if isinstance(names, list):
            for n in names: rev[n] = cid
        elif isinstance(names, str):
            rev[names] = cid
except: pass
cn_map = {}
try:
    with open('.plm-cn-names.json') as f2: cn_map = json.load(f2)
except: pass
for case_item in cs:
    gname = case_item['caseId']
    cid = rev.get(gname, gname)
    case_item['caseId'] = cid
    case_item['nodeId'] = gname
    case_item['name'] = cn_map.get(cid, cid)
print(json.dumps(cs,ensure_ascii=False,indent=2))"
}

all()  { need_bin; mkdir -p "$REPORTS_DIR"; "$BIN" --gtest_output="json:$REPORTS_DIR/report.json" 2>&1; echo "=== $REPORTS_DIR/report.json ==="; }
page() {
  need_bin; mkdir -p "$REPORTS_DIR"
  case "$1" in table) F="TsFileTable*.*";; tree) F="TsFileTree*.*";; *) echo "Unknown: $1" >&2; return 2;; esac
  "$BIN" --gtest_filter="$F" --gtest_output="json:$REPORTS_DIR/page-$1.json" 2>&1
}
case_one() {
  need_bin; [ -z "$1" ] && { usage; return 2; }
  mkdir -p "$REPORTS_DIR"
  local mf=".plm-case-id-map.json"
  if [ ! -f "$mf" ]; then
    echo "[start.sh] .plm-case-id-map.json 缺失，请恢复备份（list_cases 不生成 PLM CASE_ID map）" >&2
    return 5
  fi
  local filter=$(python3 map_lookup.py "$1" 2>/dev/null); [ -z "$filter" ] && filter="$1"
  timeout 240 "$BIN" --gtest_filter="$filter" --gtest_output="json:$REPORTS_DIR/case.json" 2>&1
  local rc=$?
  python3 -c "
import json, sys, os
from xml.etree import ElementTree as ET
jf = '$REPORTS_DIR/case.json'
xf = '$REPORTS_DIR/case-$1.xml'
if not os.path.exists(jf): sys.exit(2)
with open(jf) as f: d = json.load(f)
ET.register_namespace('', '')
root = ET.Element('testsuites', name='gtest')
for ts in d.get('testsuites', []):
    suite = ET.SubElement(root, 'testsuite', name=ts.get('name', ''),
        tests=str(ts.get('tests',0)), failures=str(ts.get('failures',0)),
        errors=str(ts.get('errors',0)))
    for tc in ts.get('testsuite', []):
        cn = tc.get('classname', '')
        nm = tc.get('name', '')
        cid = cn + '.' + nm if cn and nm else nm
        status = tc.get('result', '')
        elapsed = str(float(tc.get('time', '0s').replace('s', '')) * 1000)
        el = ET.SubElement(suite, 'testcase', classname=cn, name=nm, time=elapsed)
        if status != 'COMPLETED':
            fail = tc.get('failures', [{}])[0].get('failure', status) if tc.get('failures') else status
            ET.SubElement(el, 'failure', message=fail)
        props = ET.SubElement(el, 'properties')
        ET.SubElement(props, 'property', {'name': 'case_id', 'value': cid})
ET.ElementTree(root).write(xf, encoding='utf-8', xml_declaration=True)
" 2>/dev/null
  local post_rc=$?
  if [ $post_rc -ne 0 ]; then
    echo "[start.sh] gtest JSON to XML conversion failed (exit=$post_rc)" >&2
    rm -f "$REPORTS_DIR/case-$1.xml"
    return 1
  fi
  # Fix: overwrite gtest name with PLM CASE_ID in JUnit XML case_id property
  sed -i "s|name=\"case_id\" value=\"[^\"]*\"|name=\"case_id\" value=\"$1\"|" "$REPORTS_DIR/case-$1.xml"
  return $rc
}
cases_batch() {
  need_bin; [ -z "$1" ] && { usage; return 2; }
  mkdir -p "$REPORTS_DIR"
  local mf=".plm-case-id-map.json"
  if [ ! -f "$mf" ]; then
    echo "[start.sh] .plm-case-id-map.json missing, restore backup first" >&2
    return 5
  fi
  local filters=""
  local missing_ids=""
  _oldifs="$IFS"; IFS=','
  set -f
  for id in $1; do
    id=$(echo "$id" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
    [ -z "$id" ] && continue
    local m=$(python3 map_lookup.py "$id" 2>/dev/null)
    if [ -n "$m" ]; then
      if [ -n "$filters" ]; then filters="$filters:"; fi
      filters="$filters$m"
    else
      if [ -n "$missing_ids" ]; then missing_ids="$missing_ids "; fi
      missing_ids="$missing_ids$id"
    fi
  done
  set +f
  IFS="$_oldifs"
  if [ -z "$filters" ]; then
    echo "[start.sh] No matching tests" >&2
    return 5
  fi
  # Pre-build reverse map: gtest_name -> PLM_CASE_ID for case_id injection
  local rev_map_json; rev_map_json=$(python3 -c "
import json, sys, os
ids = '$1'.split(',')
script_dir = os.path.dirname(os.path.abspath('start.sh'))
map_path = os.path.join(script_dir, '.plm-case-id-map.json')
rev = {}
try:
    with open(map_path) as f: m = json.load(f)
    nmap = m.get('nmap', m)
    al = m.get('aliases') or {}
    for cid in ids:
        cid = cid.strip()
        # case-insensitive: 先正式编号，再旧编号别名（存量用例 execution_case_id）
        hit = None
        for k, v in nmap.items():
            if k.upper() == cid.upper() and v:
                hit = v
                break
        if hit is None:
            for k, v in al.items():
                if k.upper() == cid.upper() and v:
                    hit = v
                    break
        if hit:
            # hit[0] is the gtest filter (e.g. Suite.Test)
            rev[hit[0]] = cid
except: pass
print(json.dumps(rev))
" 2>/dev/null)
  timeout 240 "$BIN" --gtest_filter="$filters" --gtest_output="json:$REPORTS_DIR/cases.json" 2>&1
  local rc=$?
  # Crash fallback: if batch gtest crashed (signal) and no JSON, run each test individually
  if [ $rc -ge 128 ] && [ ! -f "$REPORTS_DIR/cases.json" ]; then
    echo "[start.sh] batch crashed (rc=$rc), running individually..." >&2
    python3 - "$(pwd)/.plm-case-id-map.json" "$1" "$REPORTS_DIR" "$BIN" << 'FBEOF'
import json, os, subprocess, sys
map_path, ids_arg, rd, bin_path = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]

# Load map and build case-insensitive lookup
with open(map_path) as f:
    m = json.load(f)
nmap = m.get('nmap', m)
# Build upper-case index for case-insensitive lookup
uc_index = {}
for k, v in nmap.items():
    ku = k.upper()
    if ku in uc_index:
        print(f'[start.sh] WARNING: duplicate case-insensitive key: {k} vs {uc_index[ku][0]}', file=sys.stderr)
    else:
        uc_index[ku] = (k, v)
for k, v in (m.get('aliases') or {}).items():
    ku = k.upper()
    if ku not in uc_index:
        uc_index[ku] = (k, v)

merged = {'tests': 0, 'failures': 0, 'disabled': 0, 'errors': 0, 'name': 'AllTests', 'testsuites': []}
for raw in ids_arg.split(','):
    cid = raw.strip()
    if not cid:
        continue
    entry = uc_index.get(cid.upper())
    if not entry:
        continue
    gtest_filter = entry[1][0] if isinstance(entry[1], list) else str(entry[1])
    jf = os.path.join(rd, f'fb-{cid}.json')
    subprocess.run(['timeout', '60', bin_path,
                    f'--gtest_filter={gtest_filter}',
                    f'--gtest_output=json:{jf}'],
                   capture_output=True)
    if os.path.exists(jf):
        try:
            with open(jf) as fh:
                d = json.load(fh)
            merged['tests'] += d.get('tests', 0)
            merged['failures'] += d.get('failures', 0)
            merged['disabled'] += d.get('disabled', 0)
            merged['errors'] += d.get('errors', 0)
            merged['testsuites'].extend(d.get('testsuites', []))
        except Exception:
            pass
if merged['testsuites']:
    with open(os.path.join(rd, 'cases.json'), 'w') as fh:
        json.dump(merged, fh)
FBEOF
    rc=0
  fi
  python3 -c "
import json, sys, os
from xml.etree import ElementTree as ET
jf = '$REPORTS_DIR/cases.json'
xf = '$REPORTS_DIR/cases.xml'
rev_map = json.loads('$rev_map_json' or '{}')
if not os.path.exists(jf): sys.exit(2)
with open(jf) as f: d = json.load(f)
ET.register_namespace('', '')
root = ET.Element('testsuites', name='gtest')
for ts in d.get('testsuites', []):
    suite = ET.SubElement(root, 'testsuite', name=ts.get('name', ''),
        tests=str(ts.get('tests',0)), failures=str(ts.get('failures',0)),
        errors=str(ts.get('errors',0)))
    for tc in ts.get('testsuite', []):
        cn = tc.get('classname', '')
        nm = tc.get('name', '')
        gtest_name = cn + '.' + nm if cn and nm else nm
        status = tc.get('result', '')
        elapsed = str(float(tc.get('time', '0s').replace('s', '')) * 1000)
        el = ET.SubElement(suite, 'testcase', classname=cn, name=nm, time=elapsed)
        if status != 'COMPLETED':
            fail = tc.get('failures', [{}])[0].get('failure', status) if tc.get('failures') else status
            ET.SubElement(el, 'failure', message=fail)
        props = ET.SubElement(el, 'properties')
        # Use PLM CASE_ID from reverse map, fallback to gtest name
        plm_cid = rev_map.get(gtest_name, gtest_name)
        ET.SubElement(props, 'property', {'name': 'case_id', 'value': plm_cid})
ET.ElementTree(root).write(xf, encoding='utf-8', xml_declaration=True)
" 2>/dev/null
  local post_rc=$?
  if [ $post_rc -ne 0 ]; then
    echo "[start.sh] gtest JSON to XML conversion failed (exit=$post_rc)" >&2
    rm -f "$REPORTS_DIR/cases.xml"
    return 1
  fi
  # Override case_id properties with requested PLM CASE_IDs (Python one-pass, preserves order)
  python3 - "$1" "$REPORTS_DIR/cases.xml" << 'SEDPY'
import re, sys
ids = sys.argv[1].split(',')
with open(sys.argv[2], 'r') as f: content = f.read()
def repl(m, ids=[x.strip() for x in ids], i=[0]):
    result = 'name="case_id" value="' + ids[i[0]] + '"'
    i[0] = min(i[0] + 1, len(ids) - 1)
    return result
content = re.sub(r'name="case_id" value="[^"]*"', repl, content)
with open(sys.argv[2], 'w') as f: f.write(content)
SEDPY
  if [ -n "$missing_ids" ]; then
    echo "[start.sh] CASE_ID(s) not found: $missing_ids" >&2
    exit 5
  fi
  return $rc
}
plm() { echo "[plm] 全量测试"; all; }

case "$CMD" in
  prepare) prepare;;  all) all;;  page) page "$TGT";;
  case|case-id) case_one "$TGT" || exit;;  cases) cases_batch "$TGT" || exit;;
  plm|plm-all) plm;;  list-cases) list_cases;;  help|-h|--help) usage;;
  *.*) case_one "$CMD";;  *) echo "Unknown: $CMD" >&2; usage; exit 2;;
esac
