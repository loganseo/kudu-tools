# coding=utf-8
import os
import sys
import json
import pprint
import re
import subprocess
import time

if len(sys.argv) < 2:
    raise ValueError("Usage: kudu_rebalance.py <Source TS>, <Target TS>, <Table name>, <Range Partition("
                     "ex: 20190801)>, pre-execution")


# TODO: add parameter validation check
for arg in sys.argv:
    print('arg value = ', arg)


# 1) Source TS Web UI 에서 Tablet 리스트 추출
def extract_tablets(src_ts, trgt_tbl, partition):
    # 추출 예제: curl -s http://sp-dat-hdp03-slv22.kbin.io:8050/tablets | grep -w "cbs.corebanking_log" -A2
    #  | grep "PARTITION &quot;20190801&quot;" -B1 | grep -o id=.*\" | sed 's/id=//g' | sed 's/"//g'
    cmd_extr = 'curl -s http://' + src_ts + ':8050/tablets | grep -w "' + trgt_tbl + '" -A2 ' \
               '| grep "PARTITION &quot;' + partition + '&quot;" -B1 | grep -o id=.*'
    tablets = subprocess.Popen(cmd_extr
                               , stdout=subprocess.PIPE
                               , shell=True).stdout
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
    sorted_tlist = []
    for t in tablet_list:
        # kudu cluster ksck sp-dat-hdp03-mst01,sp-dat-hdp03-mst02,sp-dat-hdp03-mst03 -ksck_format=json_pretty
        # -tables=<table_name> -tablets=<tablet_id>
        cmd_ksck = "kudu cluster ksck sp-dat-hdp03-mst01,sp-dat-hdp03-mst02,sp-dat-hdp03-mst03 " \
                   "-ksck_format=json_pretty -tables=" + trgt_tbl + " -tablets=" + t
        temp_output = subprocess.Popen(cmd_ksck, stdout=subprocess.PIPE, shell=True).stdout
        output_ksck = temp_output.read().strip()
        temp_output.close()
        output_ksck = json.loads(output_ksck.decode("utf-8","ignore"))
        for i in range(0, 3):
            ts_address = output_ksck['tablet_summaries'][0]['replicas'][i]['ts_address']
            ts_address = re.sub(':7050', '', string=ts_address)
            # print(ts_address)
            # Target TS 가 포함된 replica 체크하여 포함되지 않는 tablet 들로 list 구성
            tgrt_yn = False
            if ts_address == sys.argv[2]:
                # print("move_replica 대상아님")
                tgrt_yn = False
                break
            else:
                # print("move_replica 대상")
                tgrt_yn = True
        if tgrt_yn:
            sorted_tlist.append(t)
    return sorted_tlist


def move_replica(tablet_list, num_ts, src_ts, trgt_ts):
    # cat /Users/dongjin/Downloads/kudu_rebalancing_Test/view-source_sp-dat-hdp03-slv22.kbin.io_8050_tablets.html |
    # grep -w "server uuid"  | sed "s/server uuid //g" | sed "s/<\/pre>//g"
    # curl -s http://sp-dat-hdp03-slv22.kbin.io:8050/tablets | grep -w "server uuid"  | sed "s/server uuid //g" | sed "s/<\/pre>//g"
    ts_uuid = subprocess.Popen('curl -s http://' + src_ts + ':8050/tablets '
                                   '| grep -w "server uuid"'
                                   , stdout=subprocess.PIPE
                                   , shell=True).stdout
    src_ts_uuid = ts_uuid.read().strip()
    ts_uuid.close()
    src_ts_uuid = re.sub('server uuid ', '', string=src_ts_uuid)
    src_ts_uuid = re.sub('</pre>', '', string=src_ts_uuid)
    # print("src_ts_uuid %s" % src_ts_uuid)
    ts_uuid = subprocess.Popen('curl -s http://' + trgt_ts + ':8050/tablets '
                                   '| grep -w "server uuid"'
                                   , stdout=subprocess.PIPE
                                   , shell=True).stdout
    trgt_ts_uuid = ts_uuid.read().strip()
    ts_uuid.close()
    trgt_ts_uuid = re.sub('server uuid ', '', string=trgt_ts_uuid)
    trgt_ts_uuid = re.sub('</pre>', '', string=trgt_ts_uuid)
    # print("trgt_ts_uuid %s" % trgt_ts_uuid)
    print("Start moving tablets...")
    moved_tlist = []
    for i in range(num_ts):
        cmd_move = "nohup kudu tablet change_config move_replica " \
                   "sp-dat-hdp03-mst01,sp-dat-hdp03-mst02,sp-dat-hdp03-mst03 " \
                   + tablet_list[i] + " " + src_ts_uuid + " " + trgt_ts_uuid + " & > /dev/null"
        # print(cmd_move)
        subprocess.call(cmd_move, shell=True)
        time.sleep(5)
        print(tablet_list[i])
        # 삭제된 tablet 리스트에 저장
        moved_tlist.append(tablet_list[i])
    time.sleep(30)
    cmd_check = 'kudu cluster ksck sp-dat-hdp03-mst01,sp-dat-hdp03-mst02,sp-dat-hdp03-mst03 | grep -E "Bootstrap|Copy" | wc -l'
    moving_cnt = int(subprocess.check_output(cmd_check, shell=True))
    while moving_cnt > 0:
        moving_cnt = int(subprocess.check_output(cmd_check, shell=True))
        print("moving tablet count: %s" % moving_cnt)
        time.sleep(30)
    # moved 된 tablet 삭제
    if moving_cnt == 0:
        for t in moved_tlist:
            print("completed to move this tablet: %s" % t)
            cmd_ksck = "kudu cluster ksck sp-dat-hdp03-mst01,sp-dat-hdp03-mst02,sp-dat-hdp03-mst03 " \
                       "-ksck_format=json_pretty -tables=" + sys.argv[3] + " -tablets=" + t
            temp_output = subprocess.Popen(cmd_ksck, stdout=subprocess.PIPE, shell=True).stdout
            output_ksck = temp_output.read().strip()
            temp_output.close()
            output_ksck = json.loads(output_ksck.decode("utf-8","ignore"))
            for i in range(0, 3):
                is_leader = output_ksck['tablet_summaries'][0]['replicas'][i]['is_leader']
                print("is_leader: %s" % is_leader)
                if is_leader == "true":
                    leader_ts = output_ksck['tablet_summaries'][0]['replicas'][i]['ts_uuid']
            time.sleep(5)
            cmd_remove = 'kudu tablet change_config remove_replica sp-dat-hdp03-mst01,sp-dat-hdp03-mst02,sp-dat-hdp03-mst03 ' \
                         + t + ' ' + src_ts_uuid
            print("leader tablet server uuid: %s" % leader_ts)
            print("source tablet server uuid: %s" % src_ts)
            if src_ts == leader_ts:
                print("Doing leader_step_down")
                cmd_leader_step_down = 'kudu tablet leader_step_down sp-dat-hdp03-mst01,sp-dat-hdp03-mst02,sp-dat-hdp03-mst03 ' + t
                subprocess.call(cmd_leader_step_down, shell=True)
                time.sleep(5)
                subprocess.call(cmd_remove, shell=True)
                print("removed tablet")
            else:
                subprocess.call(cmd_remove, shell=True)
                print("removed tablet")


src_extracted_tablets = extract_tablets(sys.argv[1], sys.argv[3], sys.argv[4])
trgt_extracted_tablets = extract_tablets(sys.argv[2], sys.argv[3], sys.argv[4])
# src_extracted_tablets = extract_tablets("sp-dat-hdp03-slv22.kbin.io", "cbs.corebanking_log", "20190801")
# trgt_extracted_tablets = extract_tablets("sp-dat-hdp03-slv04.kbin.io", "cbs.corebanking_log", "20190801")

sorted_tablets = sort_tablets(sys.argv[3], src_extracted_tablets)
# print("remain tablets after excepted: %s" % len(sorted_tablets))

# 3) 이동 대상 TS 개수 산정
move_num_ts = (len(sorted_tablets) - len(trgt_extracted_tablets)) / 2

# 4) pre-execution 인 경우 실행 계획 출력하고 실제 실행은 하지 않음
if sys.argv[5] is not None and sys.argv[5] == "true":
    print("Source Tablet Server: %s, Tablets: %s" % (sys.argv[1], len(sorted_tablets)))
    print("Target Tablet Server: %s, Tablets: %s" % (sys.argv[2], len(trgt_extracted_tablets)))
    print("Number of target tablets: %s" % move_num_ts)
else:
    print("Source Tablet Server: %s, Tablets: %s" % (sys.argv[1], len(sorted_tablets)))
    print("Target Tablet Server: %s, Tablets: %s" % (sys.argv[2], len(trgt_extracted_tablets)))
    print("Number of target tablets: %s" % move_num_ts)
    print("Execute move_replica...")
    move_replica(sorted_tablets, move_num_ts, sys.argv[1], sys.argv[2])