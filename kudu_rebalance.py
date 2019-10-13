# coding=utf-8
import os
import sys
import json
import pprint
import re
import subprocess

if len(sys.argv) < 2:
    raise ValueError("Usage: kudu_rebalance.py <Source TS>, <Target TS>, <Table name>, <Range Partition("
                     "ex: 20190801)>, pre-execution")

# TODO: add parameter validation check
# for arg in sys.argv:
#     print('arg value = ', arg)

# src_ts = "sp-dat-hdp03-slv22.kbin.io"
# trgt_ts = "sp-dat-hdp03-slv04.kbin.io"
# trgt_tbl = "cbs.corebanking_log"
# partition = "20190801"
# tablet_id = "49353607a4ca4c8997189f6f407e9f99"

# 1) Source TS Web UI 에서 Tablet 리스트 추출
def extract_tablets(src_ts, trgt_tbl, partition):
    # 추출 예제: curl -s http://sp-dat-hdp03-slv22.kbin.io:8050/tablets  | grep -w "cbs.corebanking_log" -A2
    #  | grep "PARTITION &quot;20190801&quot;" -B1 | grep -o id=.*\" | sed 's/id=//g' | sed 's/"//g'
    tablets = subprocess.Popen(['cat /Users/dongjin/Downloads/kudu_rebalancing_Test/view-source_'
                                + src_ts + '_8050_tablets.html | grep -w "' + trgt_tbl + '" -A2 '
                                '| grep "PARTITION &quot;' + partition + '&quot;" -B1 | grep -o id=.*']
                               , stdout=subprocess.PIPE, shell=True).stdout
    extr_tlist = tablets.read().strip()
    tablets.close()
    extr_tlist = re.sub('(id=)', '', string=extr_tlist)
    extr_tlist = re.sub('".*', '', string=extr_tlist)
    extr_tlist = extr_tlist.split("\n")
    # print(extr_tlist)
    # print(len(extr_tlist))
    return extr_tlist


# 2) 추출된 Tablet 리스트에서 Target TS 가 포함된 replica 제외
def sort_tablets(trgt_tbl, tablet_list):
    sorted_tlist = None
    for t in tablet_list:
        # kudu cluster ksck sp-dat-hdp03-mst01,sp-dat-hdp03-mst02,sp-dat-hdp03-mst03 -ksck_format=json_pretty
        # -tables=<table_name> -tablets=<tablet_id>
        cmd_ksck = "kudu cluster ksck sp-dat-hdp03-mst01,sp-dat-hdp03-mst02,sp-dat-hdp03-mst03 " \
                   "-ksck_format=json_pretty -tables=" + trgt_tbl + " -tablets=" + t
        output_ksck = json.loads(subprocess.Popen(cmd_ksck, stdout=subprocess.PIPE, shell=True))
        for i in range(0, 3):
            ts_address = output_ksck['tablet_summaries'][0]['replicas'][i]['ts_address']
            print(ts_address)
            # Target TS 가 포함된 replica 체크하여 포함되지 않는 tablet 들로 list 구성
            tgrt_yn = False
            if ts_address == trgt_ts:
                print("move_replica 대상아님")
                tgrt_yn = False
                break
            else:
                print("move_replica 대상")
                tgrt_yn = True
        if tgrt_yn:
            sorted_tlist.append(t)
    return sorted_tlist


def move_replica(tablet_list, num_ts, src_ts, trgt_ts):
    # cat /Users/dongjin/Downloads/kudu_rebalancing_Test/view-source_sp-dat-hdp03-slv22.kbin.io_8050_tablets.html |
    # grep -w "server uuid"  | sed "s/server uuid //g" | sed "s/<\/pre>//g"
    src_ts_uuid = subprocess.Popen('cat /Users/dongjin/Downloads/kudu_rebalancing_Test/view-source_'
                              + src_ts + '_8050_tablets.html | grep -w "server uuid" | sed "s/server uuid //g" '
                              '| sed "s/<\/pre>//g"', stdout=subprocess.PIPE, shell=True)
    trgt_ts_uuid = subprocess.Popen('cat /Users/dongjin/Downloads/kudu_rebalancing_Test/view-source_'
                              + trgt_ts + '_8050_tablets.html | grep -w "server uuid" | sed "s/server uuid //g" '
                              '| sed "s/<\/pre>//g"', stdout=subprocess.PIPE, shell=True)
    for i in num_ts:
        subprocess.Popen("nohup kudu tablet change_config move_replica "
                         "sp-dat-hdp03-mst01,sp-dat-hdp03-mst02,sp-dat-hdp03-mst03 "
                         "" + tablet_list[i] + " " + src_ts_uuid + " " + trgt_ts_uuid + " &"
                         , stdout=subprocess.PIPE, shell=True)


src_extracted_tablets = extract_tablets(sys.argv[1], sys.argv[3], sys.argv[4])
trgt_extracted_tablets = extract_tablets(sys.argv[2], sys.argv[3], sys.argv[4])
# src_extracted_tablets = extract_tablets("sp-dat-hdp03-slv22.kbin.io", "cbs.corebanking_log", "20190801")
# trgt_extracted_tablets = extract_tablets("sp-dat-hdp03-slv04.kbin.io", "cbs.corebanking_log", "20190801")

# sorted_tablets = sort_tablets("cbs.corebanking_log", src_extracted_tablets)

# 3) 이동 대상 TS 개수 산정
# print((10 - 3) / 2)
# print((len(src_extracted_tablets) - len(trgt_extracted_tablets)) / 2)
move_num_ts = (len(src_extracted_tablets) - len(trgt_extracted_tablets)) / 2

# 4) pre-execution 인 경우 실행 계획 출력하고 실제 실행은 하지 않음
if sys.argv[5] is not None and sys.argv[5] == "true":
    print("Source Tablet Server: %s, Tablets: %s" % (sys.argv[1], len(src_extracted_tablets)))
    print("Target Tablet Server: %s, Tablets: %s" % (sys.argv[2], len(trgt_extracted_tablets)))
    print("Number of target tablets: %s" % move_num_ts)
else:
    print("Source Tablet Server: %s, Tablets: %s" % (sys.argv[1], len(src_extracted_tablets)))
    print("Target Tablet Server: %s, Tablets: %s" % (sys.argv[2], len(trgt_extracted_tablets)))
    print("Number of target tablets: %s" % move_num_ts)
    print("move_replica 실행")
    move_replica(src_extracted_tablets, move_num_ts, sys.argv[1], sys.argv[2])

# json_file = open("/Users/dongjin/Downloads/kudu_rebalancing_Test/ksck_core.json").read()
# json_data = json.loads(json_file)
# # json_string = json_data['tablet_summaries'][0]['replicas']
# for i in range(0, 3):
#     ts_address = json_data['tablet_summaries'][0]['replicas'][i]['ts_address']
#     print(ts_address)
#
# pp = pprint.PrettyPrinter(indent=4)
# pp.pprint(json_string)