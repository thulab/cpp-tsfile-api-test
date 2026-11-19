#!/bin/sh
# ============================================================================
# RN-143（TsFile CLI）全量回归 11 条失败用例 —— 手动复现命令集
#   用法：sh /data/test/test-program/cpp-tsfile-api-test/bin/rn143-fail-manual.sh
#   节点：192.168.99.74（Ubuntu 24.04.4）
#   被测：tsfile-cli 2.4.1.dev commit=e6d09dd7（rc/2.5.0 现编）
#   说明：全部真机绝对路径；夹具现场生成于临时目录；两条损坏夹具取自持久目录 fixtures/rn143/
#   （由用例构造函数生成后归档，勿删）
# ============================================================================
set -u
CLI=/data/test/test-program/cpp-tsfile-api-test/bin/tsfile-cli
FX=/data/test/test-program/cpp-tsfile-api-test/fixtures/rn143
D=$(mktemp -d /tmp/rn143-manual.XXXXXX)

echo "工作目录: $D"
echo "夹具目录: $FX"
printf '版本: '; $CLI --version
echo

# ---------------------------------------------------------------------------
echo "########## 1/11【PLM TsFile-154】table 格式控制字符未做可见转义（换行拆行） ##########"
echo "用例 CliFormatTest.TableVisibleEscapingContract（台账用例 9b）"
printf 'time,site,note\n1000,s1,"l1\nl2"\n' > "$D/esc.csv"
$CLI write --table esc --tag site STRING --field note TEXT -i "$D/esc.csv" -o "$D/esc.tsfile"
echo "--- 命令：$CLI cat -t esc -f table \$D/esc.tsfile ---"
$CLI cat -t esc -f table "$D/esc.tsfile"
echo "输出行数=$($CLI cat -t esc -f table "$D/esc.tsfile" | wc -l)（期望 2 = 表头 + 1 条记录；实测 3，换行把记录拆成两行）"
echo "--- 对照 csv / ndjson（同一值的转义是正确的） ---"
$CLI cat -t esc -f csv "$D/esc.tsfile"
$CLI cat -t esc -f ndjson "$D/esc.tsfile"
echo "【期望】table 中控制字符以可见形式转义，1 条记录 1 行；【实际】换行原样输出致拆行"
echo

# ---------------------------------------------------------------------------
echo "########## 2/11【PLM TsFile-155】CSV 输出未对行首反斜杠加倍 ##########"
echo "用例 CliFormatTest.CsvLeadingBackslashRule（台账用例 11b；需求 §3.1.3 + D-047）"
printf 'time,site,note\n1000,s1,\\leading\n2000,s1,back\\slash\n' > "$D/bs.csv"
$CLI write --table bs --tag site STRING --field note TEXT -i "$D/bs.csv" -o "$D/bs.tsfile"
echo "--- 命令：$CLI cat -t bs -f csv \$D/bs.tsfile ---"
$CLI cat -t bs -f csv "$D/bs.tsfile"
echo '【期望】行首反斜杠加倍：\leading -> \\leading（中段 back\slash 保持原样）；【实际】行首原样 \leading'
echo

# ---------------------------------------------------------------------------
echo "########## 3/11【PLM TsFile-159】CSV 往返把字面 \\N 吞成 NULL ##########"
echo "用例 CliFormatTest.CsvNullLiteralRoundTrip（台账用例 11c）"
printf 'time,site,note\n1000,s1,\\N\n2000,s1,"\\N"\n' > "$D/rt.csv"
$CLI write --table rt --tag site STRING --field note TEXT -i "$D/rt.csv" -o "$D/rt.tsfile"
echo "--- 原始数据（第 1 行 NULL、第 2 行字面 \\N）：$CLI cat -t rt -f ndjson ---"
$CLI cat -t rt -f ndjson "$D/rt.tsfile"
echo "--- CLI 的 CSV 输出（两行同形，无区分） ---"
$CLI cat -t rt -f csv "$D/rt.tsfile" | tee "$D/rt-round.csv"
$CLI write --table rt --tag site STRING --field note TEXT -i "$D/rt-round.csv" -o "$D/rt2.tsfile"
echo "--- 再导入后（第 2 行应仍为字面 \\N） ---"
$CLI cat -t rt -f ndjson "$D/rt2.tsfile"
echo "【期望】NULL 与字面 \\N 可区分、往返不丢；【实际】字面 \\N 被吞成 NULL（静默数据损坏）"
echo

# ---------------------------------------------------------------------------
echo "########## 4/11【PLM TsFile-157】stats 的 null_count 恒为空（不扫描补算） ##########"
echo "用例 CliStatsTest.NullCountIsAlwaysNumeric（台账用例 27）"
cat > "$D/st.csv" <<'CSV'
time,site,temp,note
1000,beijing,20,n0
2000,shanghai,21,n1
3000,beijing,22,\N
4000,shanghai,23,n3
CSV
$CLI write --table sensors --tag site STRING --field temp FLOAT --field note TEXT -i "$D/st.csv" -o "$D/st.tsfile"
echo "--- stats -m temp（该列无空值 → null_count 期望 0） ---"
$CLI stats -t sensors -m temp -f csv "$D/st.tsfile"
echo "--- stats -m note（beijing 有 1 个空值 → 期望 1 / shanghai 期望 0） ---"
$CLI stats -t sensors -m note -f csv "$D/st.tsfile"
echo "--- 对照 count（同名字段正常给数） ---"
$CLI count -t sensors -f csv "$D/st.tsfile"
echo "【期望】null_count 为数值（0/1）；【实际】恒为 \\N"
echo

# ---------------------------------------------------------------------------
echo "########## 5/11【PLM TsFile-156】sketch 未实现：固定 4 行桩、rc=0 ##########"
echo "用例 CliSketchTest.PrintsCompletePhysicalLayout（台账用例 31）"
TREE_SAMPLE=/data/test/src/tsfile/python/tests/resources/simple_tree.tsfile
echo "--- 命令：$CLI sketch \$TREE_SAMPLE（树模型） ---"
$CLI sketch "$TREE_SAMPLE"; echo "rc=$?"
echo "行数=$($CLI sketch "$TREE_SAMPLE" | wc -l)  字节=$($CLI sketch "$TREE_SAMPLE" | wc -c)"
echo "--- 表模型（CLI 自己刚写的文件） ---"
$CLI sketch "$D/st.tsfile"; echo "rc=$?"
echo "【期望】含 [magic head] TsFile / [version number] / [ChunkGroup] / [Chunk] / [magic tail] / END of TsFile 且随文件变化；【实际】固定 4 行桩、任意文件输出相同"
echo

# ---------------------------------------------------------------------------
echo "########## 6/11【PLM TsFile-158】树模型结果列名用全限定路径（需求示例为短名） ##########"
echo "用例 CliRowReadTest.ResultColumnRules（台账用例 28）"
DEV=$($CLI ls -f csv "$TREE_SAMPLE" | sed -n '2p' | cut -d, -f2)
echo "device = $DEV"
echo "--- head -d \$DEV -m status -f table（看表头） ---"
$CLI head -d "$DEV" -m status -f table "$TREE_SAMPLE" | head -2
echo "--- cat -d \$DEV -m status -f csv（表头） ---"
$CLI cat -d "$DEV" -m status -f csv "$TREE_SAMPLE" | head -1
echo "--- head -d \$DEV -m status -f ndjson（键名） ---"
$CLI head -d "$DEV" -m status -f ndjson "$TREE_SAMPLE" | head -1
echo "【期望】time  status / time,status / \"status\"（需求 §3.1.4.2 示例不带设备前缀）；【实际】全限定 $DEV.status"
echo

# ---------------------------------------------------------------------------
echo "########## 7/11 读取损坏 TsFile 段错误（应 exit 2） ##########"
echo "用例 CliExportTest.MultiObjectFirstErrorStops（台账用例 46；报告已出待提 PLM）"
if [ -f "$FX/rn143-multi-corrupt.tsfile" ]; then
  rm -rf "$D/exp"
  echo "--- 命令：$CLI export -t sensors_a -t sensors_b --type csv --output-dir \$D/exp \$FX/rn143-multi-corrupt.tsfile ---"
  $CLI export -t sensors_a -t sensors_b --type csv --output-dir "$D/exp" "$FX/rn143-multi-corrupt.tsfile"
  echo "exit=$?（实测 139 = 128+SIGSEGV（Segmentation fault）；期望 2）"
  echo "--- 产物（0001.csv 与 complete=false Manifest 应保留；不应有暂存物） ---"
  ls -la "$D/exp/" 2>/dev/null; cat "$D/exp/_manifest.json" 2>/dev/null
  echo "--- 对照：只导出完好的 sensors_a ---"
  $CLI export -t sensors_a --type csv -o "$D/exp-ok.csv" "$FX/rn143-multi-corrupt.tsfile"; echo "exit=$?"; cat "$D/exp-ok.csv"
else
  echo "[夹具缺失] $FX/rn143-multi-corrupt.tsfile（先跑一次 sh start.sh case CliExportTest.MultiObjectFirstErrorStops 生成）"
fi
echo

# ---------------------------------------------------------------------------
echo "########## 8/11 FIFO 作为输入永久阻塞（应 exit 2） ##########"
echo "用例 CliSafetyTest.SpecialFilesAndIdentityChecks（台账用例 63；报告已出待提 PLM）"
FIFO="$D/fifo.tsfile"; rm -f "$FIFO"; mkfifo "$FIFO"
echo "--- 命令：timeout 5 $CLI meta \$FIFO ---"
timeout 5 $CLI meta "$FIFO"; echo "exit=$?（124 = 被 timeout 杀死，即挂起；期望 2）"
echo "--- 对照：目录 / 悬空链接 / 普通文件 ---"
$CLI meta /tmp >/dev/null 2>&1; echo "目录 exit=$?（期望 2）"
ln -sf "$D/nope.tsfile" "$D/dangling.tsfile"
$CLI meta "$D/dangling.tsfile" >/dev/null 2>&1; echo "悬空链接 exit=$?（期望 2）"
$CLI meta -f csv "$TREE_SAMPLE" >/dev/null 2>&1; echo "普通文件 exit=$?（期望 0）"
echo

# ---------------------------------------------------------------------------
echo "########## 9/11 非法 UTF-8 标识符原样透传（未替换 U+FFFD） ##########"
echo "用例 CliSafetyTest.InvalidUtf8IdentifierHandling（台账用例 64；报告已出待提 PLM）"
if [ -f "$FX/rn143-badutf8.tsfile" ]; then
  echo "--- 夹具（表名 t+FF FE、列名 c+FF）：$FX/rn143-badutf8.tsfile ---"
  echo "--- 命令：$CLI ls -f csv \$FX/rn143-badutf8.tsfile | cat -v ---"
  $CLI ls -f csv "$FX/rn143-badutf8.tsfile" | cat -v
  echo "--- 第二行原始字节（od；期望出现 EF BF BD = U+FFFD） ---"
  $CLI ls -f csv "$FX/rn143-badutf8.tsfile" | sed -n '2p' | od -c | head -3
  echo "--- schema / count 同夹具 ---"
  $CLI schema -f csv "$FX/rn143-badutf8.tsfile" | cat -v | head -4
  $CLI count -f csv "$FX/rn143-badutf8.tsfile" | cat -v | head -3
else
  echo "[夹具缺失] $FX/rn143-badutf8.tsfile"
fi
echo "【期望】非法 UTF-8 替换为 U+FFFD（EF BF BD）；【实际】原样透传 FF FE / FF"
echo

# ---------------------------------------------------------------------------
echo "########## 10/11 时间戳乱序报错缺列名/列序号/逻辑记录号（P2 候选，待裁决） ##########"
echo "用例 CliWriteTest.TimestampMonotonicPerEntity（台账用例 52）"
cat > "$D/ooo.csv" <<'CSV'
time,site,temp
2000,s1,1
1000,s1,2
CSV
echo "--- 命令：$CLI write --table ooo --tag site STRING --field temp FLOAT -i \$D/ooo.csv -o \$D/ooo.tsfile ---"
$CLI write --table ooo --tag site STRING --field temp FLOAT -i "$D/ooo.csv" -o "$D/ooo.tsfile"
echo "exit=$?"
echo "【期望】（需求 §3.5）报告逻辑记录号、物理行号或范围、列名及列序号；【实际】仅物理行号"
echo

# ---------------------------------------------------------------------------
echo "########## 11/11 产品自带测试失败（非 CLI、非本 RN 引入，供产品侧参考） ##########"
echo "用例 RestorableTsFileWriterTest.AlignedTimeseriesRecoverAndWriteNullValue"
echo "源码：test/tree/test_tree_restorable_writer.cpp:612（产品自带测试；第 2 步损坏/恢复段大段注释）"
C=/data/test/test-program/cpp-tsfile-api-test
echo "--- suite 内跑（实测 FAILED） ---"
(cd "$C" && ./build/test/main --gtest_filter='RestorableTsFileWriterTest.*' 2>&1 | tail -6)
echo "--- 单独跑同一条（同一二进制结果不稳定：曾 FAILED 也曾 PASSED；本次见下行） ---"
(cd "$C" && ./build/test/main --gtest_filter='RestorableTsFileWriterTest.AlignedTimeseriesRecoverAndWriteNullValue' 2>&1 | grep -E "Failure|Which is|FAILED|PASSED" | head -6)
echo "失败点：reader.query(device_ids, measurement_names, 0, 100, result_set) 实际返回 24（期望 E_OK=0）"
echo "【判定】与 RN-143 无关：①CLI 用例在其之后运行；②临时移除本 RN 的测试改动并重建后仍失败；"
echo "        ③属产品自带半成品测试，建议产品侧定位（不作为本 RN 结论）"
echo

echo "============================================================"
echo "现场保留在 $D（如需清理：rm -rf $D）"
echo "夹具目录 $FX 为持久归档，勿删（用例 46/64 复现依赖）。"
