# coding=utf-8
import os
import sys
import json
import pprint
import re
import subprocess

# if len(sys.argv) < 2:
#     raise ValueError("Usage: kudu_rebalance.py <Source TS>, <Target TS>, <Table name>, <Range Partition("
#                      "ex: 20190801)>")
# for arg in sys.argv:
#     print('arg value = ', arg)

table_name = "cbs.corebanking_log"
tablet_id = "49353607a4ca4c8997189f6f407e9f99"
trgt_ts = "sp-dat-hdp03-slv04.kbin.io"


# 1) Source TS Web UI 에서 Tablet 리스트 추출
def extraction_tablets():
    tablets = subprocess.Popen(['cat /Users/dongjin/Downloads/kudu_rebalancing_Test/view-source_sp-dat-hdp03-slv22.kbin'
                                '.io_8050_tablets.html | grep -w "cbs.corebanking_log" -A2 | grep "PARTITION '
                                '&quot;20190801&quot;" -B1 | grep -o id=.*'], stdout=subprocess.PIPE, shell=True).stdout
    tlist = tablets.read().strip()
    tablets.close()
    tlist = re.sub('(id=)', '', string=tlist)
    tlist = re.sub('".*', '', string=tlist)
    tlist = tlist.split("\n")
    print(tlist)
    print(len(tlist))
    return tlist


ex_tablet_list = extraction_tablets()


# 2) 추출된 Tablet 리스트에서 Target TS 가 포함된 replica 제거
def sort_tablet_list(tablet_list):
    for t in ex_tablet_list:
        # kudu cluster ksck sp-dat-hdp03-mst01,sp-dat-hdp03-mst02,sp-dat-hdp03-mst03 -ksck_format=json_pretty
        # -tables=<table_name> -tablets=<tablet_id>
        cmd_ksck = "kudu cluster ksck sp-dat-hdp03-mst01,sp-dat-hdp03-mst02,sp-dat-hdp03-mst03 " \
                   "-ksck_format=json_pretty -tables=" + table_name + " -tablets=" + t
        output_ksck = json.loads(subprocess.Popen(cmd_ksck, stdout=subprocess.PIPE, shell=True))
        for i in range(0, 3):
            ts_address = output_ksck['tablet_summaries'][0]['replicas'][i]['ts_address']
            print(ts_address)
            # Target TS 가 포함된 replica 체크하여 포함되지 않는 tablet 들로 list 구성
            if ts_address == trgt_ts:
                print("대상아님")
                break
            else:
                print("대상")


json_file = open("/Users/dongjin/Downloads/kudu_rebalancing_Test/ksck_core.json").read()
json_data = json.loads(json_file)
# json_string = json_data['tablet_summaries'][0]['replicas']
for i in range(0, 3):
    ts_address = json_data['tablet_summaries'][0]['replicas'][i]['ts_address']
    print(ts_address)

pp = pprint.PrettyPrinter(indent=4)
# pp.pprint(json_string)
