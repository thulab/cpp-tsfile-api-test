#!/usr/bin/env python3
"""Lookup original test identifier from CASE_ID (case-insensitive, per PLM spec all CASE_IDs are uppercase)."""
import json, sys, os
cid = sys.argv[1]
script_dir = os.path.dirname(os.path.abspath(__file__))
map_path = os.path.join(script_dir, ".plm-case-id-map.json")
try:
    with open(map_path) as f:
        m = json.load(f)
    nmap = m.get("nmap", m)
    # 1. 精确匹配
    if cid in nmap:
        v = nmap[cid]
        print(v[0] if v else cid)
        sys.exit(0)
    # 2. 大小写不敏感回退（PLM normalizeCaseId 统一大写后传入）
    upper = cid.upper()
    for k, v in nmap.items():
        if k.upper() == upper:
            print(v[0] if v else cid)
            sys.exit(0)
    # 3. 旧编号别名兼容（存量用例的 execution_case_id）
    al = m.get("aliases") or {}
    if cid in al:
        v = al[cid]
        print(v[0] if v else "")
        sys.exit(0)
    for k, v in al.items():
        if k.upper() == upper:
            print(v[0] if v else "")
            sys.exit(0)
    # 4. 未匹配，回退原始值
    print("")
except Exception as e:
    print("")
