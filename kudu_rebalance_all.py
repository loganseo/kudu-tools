# coding=utf-8
import sys
import json
import re
import subprocess
import time
import unittest
from collections import OrderedDict

if len(sys.argv) < 2:
    raise ValueError("Usage: kudu_rebalance_all.py <Master Address>, <Table name>, <Range Partition("
                     "ex: 20190801)>, pre-execution")

# TODO: add parameter validation check
for arg in sys.argv:
    print('arg value = ', arg)


# TODO: 전체 Tablet Server 의 Tablet 개수를 추출하고, 상위 TS -> 하위 TS 로 move 하는 로직


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
    return extr_tlist


# 2) 추출된 Tablet 리스트에서 Target TS 가 포함 되있거나, 이미 실행된 move_replica 에 포함된 경우 해당 replica 는 제외
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
        output_ksck = json.loads(output_ksck.decode("utf-8", "ignore"))
        tgrt_yn = False
        for i in range(0, len(output_ksck['tablet_summaries'][0]['replicas'])):
            ts_address = output_ksck['tablet_summaries'][0]['replicas'][i]['ts_address']
            ts_address = re.sub(':7050', '', string=ts_address)
            state = output_ksck['tablet_summaries'][0]['replicas'][i]['state']
            # print(ts_address)
            # Target TS 가 포함된 replica 체크하여 포함되지 않는 tablet 들로 list 구성
            if ts_address == sys.argv[2] or state == "INITIALIZED":
                # print("move_replica 대상아님")
                tgrt_yn = False
                break
            else:
                # print("move_replica 대상")
                tgrt_yn = True
        if tgrt_yn:
            sorted_tlist.append(t)
    return sorted_tlist


def ksck_tablets(tlist):
    moved_tlist_str = ','.join(map(str, tlist))
    print("moved_tlist_str: %s" % moved_tlist_str)
    cmd_ksck = "kudu cluster ksck sp-dat-hdp03-mst01,sp-dat-hdp03-mst02,sp-dat-hdp03-mst03 " \
               "-ksck_format=json_pretty -tables=" + sys.argv[3] + " -tablets=" + moved_tlist_str
    temp_output = subprocess.Popen(cmd_ksck, stdout=subprocess.PIPE, shell=True).stdout
    output_ksck = temp_output.read().strip()
    temp_output.close()
    output_ksck = json.loads(output_ksck.decode("utf-8", "ignore"))
    return output_ksck


def ksck_single_tablet(a_tablet):
    cmd_ksck = "kudu cluster ksck sp-dat-hdp03-mst01,sp-dat-hdp03-mst02,sp-dat-hdp03-mst03 " \
               "-ksck_format=json_pretty -tables=" + sys.argv[3] + " -tablets=" + a_tablet
    temp_output = subprocess.Popen(cmd_ksck, stdout=subprocess.PIPE, shell=True).stdout
    output_ksck = temp_output.read().strip()
    temp_output.close()
    output_ksck = json.loads(output_ksck.decode("utf-8", "ignore"))
    return output_ksck


def extract_ts_uuid(ts_id):
    ts_uuid = subprocess.Popen('curl -s http://' + ts_id + ':8050/tablets '
                                                           '| grep -w "server uuid"'
                               , stdout=subprocess.PIPE
                               , shell=True).stdout
    ext_ts_uuid = ts_uuid.read().strip()
    ts_uuid.close()
    ext_ts_uuid = re.sub('server uuid ', '', string=ext_ts_uuid)
    ext_ts_uuid = re.sub('</pre>', '', string=ext_ts_uuid)
    return ext_ts_uuid


def move_replica(tablet_list, num_ts, src_ts, trgt_ts):
    # cat /Users/dongjin/Downloads/kudu_rebalancing_Test/view-source_sp-dat-hdp03-slv22.kbin.io_8050_tablets.html |
    # grep -w "server uuid"  | sed "s/server uuid //g" | sed "s/<\/pre>//g"
    # curl -s http://sp-dat-hdp03-slv22.kbin.io:8050/tablets | grep -w "server uuid"  | sed "s/server uuid //g" | sed "s/<\/pre>//g"
    global src_ts_uuid
    src_ts_uuid = extract_ts_uuid(src_ts)
    # print("src_ts_uuid %s" % src_ts_uuid)
    trgt_ts_uuid = extract_ts_uuid(trgt_ts)
    # print("trgt_ts_uuid %s" % trgt_ts_uuid)
    print("Start moving tablets...")
    moved_tlist = []
    for i in range(num_ts):
        cmd_move = "nohup kudu tablet change_config move_replica " \
                   "sp-dat-hdp03-mst01,sp-dat-hdp03-mst02,sp-dat-hdp03-mst03 " \
                   + tablet_list[i] + " " + src_ts_uuid + " " + trgt_ts_uuid + " & > /dev/null 2>&1"
        # print(cmd_move)
        subprocess.call(cmd_move, shell=True)
        time.sleep(5)
        print(tablet_list[i])
        # moving tablet id 리스트에 저장
        moved_tlist.append(tablet_list[i])
    time.sleep(60)
    # cmd_check = 'kudu cluster ksck sp-dat-hdp03-mst01,sp-dat-hdp03-mst02,sp-dat-hdp03-mst03 | grep -E "Bootstrap|Copy" | wc -l'
    # moving_cnt = int(subprocess.check_output(cmd_check, shell=True))
    # while moving_cnt > 0:
    #     moving_cnt = int(subprocess.check_output(cmd_check, shell=True))
    #     print("moving tablet count: %s" % moving_cnt)
    #     time.sleep(60)
    # checking status of the moving job
    # output_ksck = ksck_tablets(moved_tlist)
    # moving_cnt = len(output_ksck['tablet_summaries'])
    # if moving_cnt == 0:
    #     sys.exit("There is no moving tablet...")
    # else:
    #     print("move_replica::moving_cnt: %s" % moving_cnt)
    return moved_tlist


def checking_move_replica(tablet_list):
    output_ksck = ksck_tablets(tablet_list)
    moving_cnt = len(output_ksck['tablet_summaries'])
    while moving_cnt > 0:
        output_ksck = ksck_tablets(tablet_list)
        print("tablet_summaries: %s" % len(output_ksck['tablet_summaries']))
        check_count = 0
        for i in range(len(output_ksck['tablet_summaries'])):
            for j in range(len(output_ksck['tablet_summaries'][i]['replicas'])):
                state = output_ksck['tablet_summaries'][i]['replicas'][j]['state']
                if state == "INITIALIZED":
                    tablet_id = output_ksck['tablet_summaries'][i]['replicas'][j]['status_pb']['tablet_id']
                    last_status = output_ksck['tablet_summaries'][i]['replicas'][j]['status_pb']['last_status']
                    print("state: %s" % state)
                    print("tablet_id: %s" % tablet_id)
                    print("last_status: %s" % last_status)
                    check_count = check_count + 1
                elif state == "UNKNOWN":
                    print("state: %s" % state)
                    check_count = check_count + 1
        print("moving_cnt: %s, check_count: %s" % (moving_cnt, check_count))
        if moving_cnt > check_count:
            moving_cnt = check_count
        # if check_count == 0:
        #     moving_cnt = moving_cnt - 1
        time.sleep(60)
    print("checking_move_replica::moving_cnt: %s" % moving_cnt)
    return moving_cnt


def remove_replica(tablet_list, moved_count):
    # moved 된 tablet 삭제
    if moved_count == 0:
        for t in tablet_list:
            print("completed to move this tablet: %s" % t)
            output_ksck = ksck_single_tablet(t)
            leader_ts_uuid = output_ksck['tablet_summaries'][0]['master_cstate']['leader_uuid']
            # for i in range(0, 3):
            #     is_leader = output_ksck['tablet_summaries'][0]['replicas'][i]['is_leader']
            #     # print("is_leader: %s" % is_leader)
            #     if is_leader:
            #         leader_ts_uuid = output_ksck['tablet_summaries'][0]['replicas'][i]['ts_uuid']
            time.sleep(5)
            cmd_remove = 'kudu tablet change_config remove_replica sp-dat-hdp03-mst01,sp-dat-hdp03-mst02,sp-dat-hdp03-mst03 ' \
                         + t + ' ' + src_ts_uuid
            # print("leader tablet server uuid: %s" % leader_ts_uuid)
            # print("source tablet server uuid: %s" % src_ts_uuid)
            if src_ts_uuid == leader_ts_uuid:
                print("Doing leader_step_down")
                cmd_leader_step_down = 'kudu tablet leader_step_down sp-dat-hdp03-mst01,sp-dat-hdp03-mst02,sp-dat-hdp03-mst03 ' + t
                subprocess.call(cmd_leader_step_down, shell=True)
                time.sleep(5)
                subprocess.call(cmd_remove, shell=True)
                print("removed tablet")
            else:
                subprocess.call(cmd_remove, shell=True)
                print("removed tablet")


# Tablet Server List 추출
def extract_tserver(masters):
    cmd_extrct = "kudu cluster ksck " + masters + " " \
                                                  "-ksck_format=json_pretty -sections=TSERVER_SUMMARIES"
    temp_output = subprocess.Popen(cmd_extrct, stdout=subprocess.PIPE, shell=True).stdout
    output_tserver = temp_output.read().strip()
    temp_output.close()
    output_tserver = json.loads(output_tserver.decode("utf-8", "ignore"))
    return output_tserver


# Table Range Partition 의 Tablet 분포도 추출(JSON)
def extract_dist_status(master_addrs):
    output_tserver = extract_tserver(master_addrs)
    tserver_list = []
    for i in range(len(output_tserver['tserver_summaries'])):
        address = output_tserver['tserver_summaries'][i]['address']
        ts_address = re.sub(':7050', '', string=address)
        print("ts_address: %s" % ts_address)
        tserver_list.append(ts_address)
    tablet_dist_status = OrderedDict()
    tablet_dist_status["tablet_summaries"] = []
    total_count = 0
    for ts in tserver_list:
        extracted_tablets = extract_tablets(ts, sys.argv[2], sys.argv[3])
        print("%s" % len(extracted_tablets))
        total_count = total_count + len(extracted_tablets)
        tablet_dist_status["tablet_summaries"].append({"ts_address": ts, "tablet_count": len(extracted_tablets)})
    tablet_dist_status["total_count"] = total_count
    # Tablet Server 당 적절한 Tablet 수
    tablet_dist_status["expected_count"] = total_count / len(tserver_list)
    json_tablet_dist_status = json.dumps(tablet_dist_status, ensure_ascii=False, indent=4)
    return json_tablet_dist_status


json_tablet_status = extract_dist_status(sys.argv[1])
json_tablet_status = json.loads(json_tablet_status.decode("utf-8", "ignore"))
print(json_tablet_status)

# 적정수 - 보유수 = +인 TServer 와 -인 TServer 분리해서 리스트 저장
exceeded_ts = OrderedDict()
less_ts = OrderedDict()
exceeded_ts["exceeded_summaries"] = []
less_ts["less_summaries"] = []
expected_count = json_tablet_status["expected_count"]
for i in range(len(json_tablet_status['tablet_summaries'])):
    result = json_tablet_status['tablet_summaries'][i]['tablet_count'] - expected_count
    if result > 0:
        exceeded_ts["exceeded_summaries"].append({"ts_address": json_tablet_status['tablet_summaries'][i]['ts_address'],
                                                  "tablet_count": json_tablet_status['tablet_summaries'][i][
                                                      'tablet_count'], "result_count": result})
    elif result <= 0:
        less_ts["less_summaries"].append({"ts_address": json_tablet_status['tablet_summaries'][i]['ts_address'],
                                              "tablet_count": json_tablet_status['tablet_summaries'][i][
                                                  'tablet_count'], "result_count": result})
# res = sorted(exceeded_ts["exceeded_summaries"].items(), key=lambda kv:kv[1]['result_count'], reverse=True)
# print(json.dumps(res, ensure_ascii=False, indent=4))
# print(json.dumps(exceeded_ts, ensure_ascii=False, indent=4))
# print(json.dumps(less_ts, ensure_ascii=False, indent=4))


# Tablet 이동 (조건: (1) 큰수->작은수, (2) Target TS 에 없는 Tablet)


# src_extracted_tablets = extract_tablets(sys.argv[1], sys.argv[3], sys.argv[4])
# trgt_extracted_tablets = extract_tablets(sys.argv[2], sys.argv[3], sys.argv[4])


# sorted_tablets = sort_tablets(sys.argv[3], src_extracted_tablets)
# print("remain tablets after excepted: %s" % len(sorted_tablets))

# 3) 이동 대상 TS 개수 산정
# move_num_ts = (len(sorted_tablets) - len(trgt_extracted_tablets)) / 2

# 4) pre-execution 인 경우 실행 계획 출력하고 실제 실행은 하지 않음
# if sys.argv[5] is not None and sys.argv[5] == "true":
#     print("Source Tablet Server: %s, Tablets: %s" % (sys.argv[1], len(sorted_tablets)))
#     print("Target Tablet Server: %s, Tablets: %s" % (sys.argv[2], len(trgt_extracted_tablets)))
#     print("Number of target tablets: %s" % move_num_ts)
# else:
#     print("Source Tablet Server: %s, Tablets: %s" % (sys.argv[1], len(sorted_tablets)))
#     print("Target Tablet Server: %s, Tablets: %s" % (sys.argv[2], len(trgt_extracted_tablets)))
#     print("Number of target tablets: %s" % move_num_ts)
#     if move_num_ts <= 0:
#         print("There is nothing to move...")
#     else:
#         print("Execute move_replica...")
#         moving_tblt_list = move_replica(sorted_tablets, move_num_ts, sys.argv[1], sys.argv[2])
#         moved_cnt = checking_move_replica(moving_tblt_list)
#         remove_replica(moving_tblt_list, moved_cnt)
