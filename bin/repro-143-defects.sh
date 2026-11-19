#!/bin/sh
# RN-143 新发现缺陷复现（③树模型列名 / ④stats null_count / ⑤sketch 桩；附 ② 往返丢数据）
# 用法：sh /data/test/test-program/cpp-tsfile-api-test/bin/repro-143-defects.sh
# 说明：全部使用真机绝对路径；只读产品仓库样本 + 在 /tmp 现场生成夹具；不修改任何源码或既有文件。
set -u

TREE_SAMPLE=/data/test/src/tsfile/python/tests/resources/simple_tree.tsfile
D=$(mktemp -d /tmp/rn143-repro.XXXXXX)

echo "工作目录: $D"
printf '版本: '; /data/test/test-program/cpp-tsfile-api-test/bin/tsfile-cli --version

echo
echo "########## ③ 树模型结果列名应为测量名（需求 §3.1.4.2 示例为 time  temp  status，不带设备前缀） ##########"
/data/test/test-program/cpp-tsfile-api-test/bin/tsfile-cli ls -f csv "$TREE_SAMPLE"
DEV=$(/data/test/test-program/cpp-tsfile-api-test/bin/tsfile-cli ls -f csv "$TREE_SAMPLE" | sed -n '2p' | cut -d, -f2)
echo "device = $DEV"
echo "--- 实际输出 1：head -d \$DEV -m status -f table（看表头） ---"
/data/test/test-program/cpp-tsfile-api-test/bin/tsfile-cli head -d "$DEV" -m status -f table "$TREE_SAMPLE" | head -2
echo "--- 实际输出 2：cat -d \$DEV -m status -f csv（表头） ---"
/data/test/test-program/cpp-tsfile-api-test/bin/tsfile-cli cat -d "$DEV" -m status -f csv "$TREE_SAMPLE" | head -1
echo "--- 实际输出 3：-f ndjson（键名） ---"
/data/test/test-program/cpp-tsfile-api-test/bin/tsfile-cli head -d "$DEV" -m status -f ndjson "$TREE_SAMPLE" | head -1
echo "【期望】表头/键名为 time  status / time,status / \"status\"（无 device. 前缀）"

echo
echo "########## ④ stats 的 null_count 恒为空（应为数值：无空值=0、有 1 个空值=1） ##########"
cat > "$D/in.csv" <<'CSV'
time,site,temp,note
1000,beijing,20,n0
2000,shanghai,21,n1
3000,beijing,22,\N
4000,shanghai,23,n3
CSV
/data/test/test-program/cpp-tsfile-api-test/bin/tsfile-cli write --table sensors --tag site STRING --field temp FLOAT --field note TEXT -i "$D/in.csv" -o "$D/t.tsfile"
echo "--- stats -m temp（该列无空值 → null_count 期望 0） ---"
/data/test/test-program/cpp-tsfile-api-test/bin/tsfile-cli stats -t sensors -m temp -f csv "$D/t.tsfile"
echo "--- stats -m note（beijing 有 1 个空值 → 期望 1、shanghai 期望 0） ---"
/data/test/test-program/cpp-tsfile-api-test/bin/tsfile-cli stats -t sensors -m note -f csv "$D/t.tsfile"
echo "--- 对照 count（同名字段正常给数） ---"
/data/test/test-program/cpp-tsfile-api-test/bin/tsfile-cli count -t sensors -f csv "$D/t.tsfile"

echo
echo "########## ⑤ sketch 未实现：只回固定 4 行，rc=0 无任何物理布局 ##########"
echo "--- 树模型 ---"
/data/test/test-program/cpp-tsfile-api-test/bin/tsfile-cli sketch "$TREE_SAMPLE"; echo "rc=$?"
echo "行数=$(/data/test/test-program/cpp-tsfile-api-test/bin/tsfile-cli sketch "$TREE_SAMPLE" | wc -l)  字节=$(/data/test/test-program/cpp-tsfile-api-test/bin/tsfile-cli sketch "$TREE_SAMPLE" | wc -c)"
echo "--- 表模型（CLI 自己刚写的文件） ---"
/data/test/test-program/cpp-tsfile-api-test/bin/tsfile-cli sketch "$D/t.tsfile"; echo "rc=$?"
echo "【期望】含 POSITION| CONTENT 表、[magic head] TsFile、[version number]、[ChunkGroup]、[Chunk]、"
echo "        [magic tail] TsFile、END of TsFile；且换文件内容随之变化"

echo
echo "########## 附：② 往返丢数据（字面 \\N 被吞成 NULL） ##########"
printf 'time,site,note\n1000,s1,\\N\n2000,s1,"\\N"\n' > "$D/rt.csv"
/data/test/test-program/cpp-tsfile-api-test/bin/tsfile-cli write --table rt --tag site STRING --field note TEXT -i "$D/rt.csv" -o "$D/rt.tsfile"
echo "--- 原始数据（第一行 NULL、第二行字面 \\N） ---"
/data/test/test-program/cpp-tsfile-api-test/bin/tsfile-cli cat -t rt -f ndjson "$D/rt.tsfile"
echo "--- CLI 自己的 CSV 输出（两行变成一样） ---"
/data/test/test-program/cpp-tsfile-api-test/bin/tsfile-cli cat -t rt -f csv "$D/rt.tsfile" | tee "$D/rt-round.csv"
/data/test/test-program/cpp-tsfile-api-test/bin/tsfile-cli write --table rt --tag site STRING --field note TEXT -i "$D/rt-round.csv" -o "$D/rt2.tsfile"
echo "--- 再导入后（第二行应仍为字面 \\N，实测被吞成 null） ---"
/data/test/test-program/cpp-tsfile-api-test/bin/tsfile-cli cat -t rt -f ndjson "$D/rt2.tsfile"

echo
echo "现场保留在 $D（如需清理：rm -rf $D）"
