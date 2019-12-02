# coding=utf-8
import Queue
import sys
import json
import re
import subprocess
import time
import copy
import unittest
from collections import OrderedDict

reload(sys)
sys.setdefaultencoding('utf-8')

if len(sys.argv) < 2:
    raise ValueError("Usage: kudu_rebalance_all.py <Master Address>, <Table name>, <Range Partition("
                     "ex: 20190801)>, <max_moves_tablets(default 3)>, pre-execution")

# TODO: add parameter validation check
global masters
global table_name
global target_partition
global max_moves
global pre_exe

for arg in sys.argv:
    print('arg value = ', arg)
    masters = sys.argv[1]
    table_name = sys.argv[2]
    target_partition = sys.argv[3]
    max_moves = sys.argv[4]
    pre_exe = sys.argv[5]


# Source TS Web UI 에서 Tablet 리스트 추출
def extract_tablets(src_ts):
    cmd_extr = 'curl -s http://' + src_ts + ':8050/tablets | grep -w "' + table_name + '" -A2 ' \
            '| grep "PARTITION &quot;' + target_partition + '&quot;" -B1 | grep -o id=.*'
    tablets = subprocess.Popen(cmd_extr
                               , stdout=subprocess.PIPE
                               , shell=True).stdout
    extr_tlist = tablets.read().strip()
    tablets.close()
    extr_tlist = re.sub('(id=)', '', string=extr_tlist)
    extr_tlist = re.sub('".*', '', string=extr_tlist)
    extr_tlist = extr_tlist.split("\n")
    return extr_tlist


# 추출된 Tablet 리스트에서 Target TS 가 포함 되있거나, 이미 실행된 move_replica 에 포함된 경우 해당 replica 는 제외
def sort_tablets(target_ts, tablet_list):
    sorted_tlist = []
    for t in tablet_list:
        cmd_ksck = "kudu cluster ksck " + masters + " " \
                "-ksck_format=json_pretty -tables=" + table_name + " -tablets=" + t
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
            if ts_address == target_ts or state == "INITIALIZED":
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
    cmd_ksck = "kudu cluster ksck " + masters + " " \
            "-ksck_format=json_pretty -tables=" + table_name + " -tablets=" + moved_tlist_str
    temp_output = subprocess.Popen(cmd_ksck, stdout=subprocess.PIPE, shell=True).stdout
    output_ksck = temp_output.read().strip()
    temp_output.close()
    output_ksck = json.loads(output_ksck.decode("utf-8", "ignore"))
    return output_ksck


def ksck_single_tablet(a_tablet):
    cmd_ksck = "kudu cluster ksck " + masters + " " \
            "-ksck_format=json_pretty -tables=" + table_name + " -tablets=" + a_tablet
    temp_output = subprocess.Popen(cmd_ksck, stdout=subprocess.PIPE, shell=True).stdout
    output_ksck = temp_output.read().strip()
    temp_output.close()
    output_ksck = json.loads(output_ksck.decode("utf-8", "ignore"))
    return output_ksck


def extract_ts_uuid(ts_id):
    ts_uuid = subprocess.Popen('curl -s http://' + ts_id + ':8050/tablets | grep -w "server uuid"'
                               , stdout=subprocess.PIPE
                               , shell=True).stdout
    ext_ts_uuid = ts_uuid.read().strip()
    ts_uuid.close()
    ext_ts_uuid = re.sub('server uuid ', '', string=ext_ts_uuid)
    ext_ts_uuid = re.sub('</pre>', '', string=ext_ts_uuid)
    return ext_ts_uuid


def move_replica(tablet_list, num_ts, src_ts, trgt_ts):
    global src_ts_uuid
    src_ts_uuid = extract_ts_uuid(src_ts)
    # print("src_ts_uuid %s" % src_ts_uuid)
    trgt_ts_uuid = extract_ts_uuid(trgt_ts)
    # print("trgt_ts_uuid %s" % trgt_ts_uuid)
    print("Start moving tablets...")
    moved_tlist = []
    for i in range(num_ts):
        cmd_move = "nohup kudu tablet change_config move_replica " + masters + " " \
                   + tablet_list[i] + " " + src_ts_uuid + " " + trgt_ts_uuid + " & > /dev/null 2>&1"
        # print(cmd_move)
        subprocess.call(cmd_move, shell=True)
        time.sleep(5)
        print(tablet_list[i])
        # moving tablet id 리스트에 저장
        moved_tlist.append(tablet_list[i])
    time.sleep(60)
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
            time.sleep(5)
            cmd_remove = "kudu tablet change_config remove_replica " + masters + " " \
                         + t + " " + src_ts_uuid
            if src_ts_uuid == leader_ts_uuid:
                print("Doing leader_step_down")
                cmd_leader_step_down = "kudu tablet leader_step_down " + masters + " " + t
                subprocess.call(cmd_leader_step_down, shell=True)
                time.sleep(5)
                subprocess.call(cmd_remove, shell=True)
                print("removed tablet")
            else:
                subprocess.call(cmd_remove, shell=True)
                print("removed tablet")


# Tablet Server List 추출
def extract_tserver():
    cmd_extrct = "kudu cluster ksck " + masters + " -ksck_format=json_pretty -sections=TSERVER_SUMMARIES"
    temp_output = subprocess.Popen(cmd_extrct, stdout=subprocess.PIPE, shell=True).stdout
    output_tserver = temp_output.read().strip()
    temp_output.close()
    output_tserver = json.loads(output_tserver.decode("utf-8", "ignore"))
    return output_tserver


# Table Range Partition 의 Tablet 분포도 추출(JSON)
def extract_dist_status():
    output_tserver = extract_tserver()
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
        extracted_tablets = extract_tablets(ts)
        print("%s" % len(extracted_tablets))
        total_count = total_count + len(extracted_tablets)
        tablet_dist_status["tablet_summaries"].append({"ts_address": ts, "tablet_count": len(extracted_tablets)})
    tablet_dist_status["total_count"] = total_count
    # Tablet Server 당 적절한 Tablet 수
    tablet_dist_status["expected_count"] = total_count / len(tserver_list)
    json_tablet_dist_status = json.dumps(tablet_dist_status, ensure_ascii=False, indent=4)
    return json_tablet_dist_status


# 이미 candidate_queue 존재하는 tablet 제거
def check_duplicate_tablet(c_queue, s_tablets):
    temp_queue_list = []
    while c_queue.qsize():
        temp_str = c_queue.get()
        temp_queue_list.append(temp_str['tablet_id'])
    temp_dup_list = []
    for list_num in range(len(s_tablets)):
        for i in range(len(temp_queue_list)):
            # print("%s == %s" % (s_tablets[list_num], temp_queue_list[i]))
            if s_tablets[list_num] == temp_queue_list[i]:
                temp_dup_list.append(list_num)
    print("len(temp_dup_list): %s" % len(temp_dup_list))
    print("len(s_tablets): %s" % len(s_tablets))
    if len(temp_dup_list) > 0:
        for dup_cnt in range(len(temp_dup_list)):
            s_tablets.pop(temp_dup_list[dup_cnt])
    print("len(s_tablets): %s" % len(s_tablets))
    return s_tablets


# Starting Kudu Rebalancing Job
json_tablet_status = extract_dist_status()
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
    elif result < 0:
        less_ts["less_summaries"].append({"ts_address": json_tablet_status['tablet_summaries'][i]['ts_address'],
                                          "tablet_count": json_tablet_status['tablet_summaries'][i][
                                              'tablet_count'], "result_count": result})
# Sorting
exceeded_ts["exceeded_summaries"] = sorted(exceeded_ts["exceeded_summaries"],
                                           key=lambda item: item['result_count'], reverse=True)
less_ts["less_summaries"] = sorted(less_ts["less_summaries"], key=lambda item: item['result_count'])
print(json.dumps(exceeded_ts, ensure_ascii=False, indent=4))
print(json.dumps(less_ts, ensure_ascii=False, indent=4))

# Tablet 이동 실행 계획 만들기 (조건: (1) 큰수->작은수, (2) Target TS 에 없는 Tablet)
excd_queue = Queue.Queue()
less_queue = Queue.Queue()
excd_result_cnt = 0
less_result_cnt = 0
for i in range(len(exceeded_ts['exceeded_summaries'])):
    excd_queue.put(exceeded_ts['exceeded_summaries'][i])
    excd_result_cnt = excd_result_cnt + exceeded_ts['exceeded_summaries'][i]['result_count']
for j in range(len(less_ts['less_summaries'])):
    less_queue.put(less_ts['less_summaries'][j])
    less_result_cnt = less_result_cnt + less_ts['less_summaries'][j]['result_count']

print("move 대상 tablet 수: %s / 받아야 할 tablet 수: %s" % (excd_result_cnt, less_result_cnt))

candidate_queue = Queue.Queue()
while excd_queue.qsize():
    excd = excd_queue.get()
    excd_addr = excd["ts_address"]
    excd_cnt = excd["result_count"]
    print("\n1. source TS: %s, 이동해야 할 수(excd_cnt): %s" % (excd_addr, excd_cnt))
    while less_queue.qsize():
        # excd_cnt 수 만큼 tablet 들을 candidate_queue 에 담은 경우 해당 순서의 TS 를 종료하고, 다음 순서의 less TS 으로 넘어간다.
        if excd_cnt == 0:
            print("excd_cnt == 0")
            break
        less = less_queue.get()
        less_addr = less["ts_address"]
        less_cnt = less["result_count"]
        print("2. 이동해야 할 수(excd_cnt): %s, 이동 시킬 수 있는 수(less_cnt): %s (%s -> %s)" % (excd_cnt, less_cnt, excd_addr, less_addr))
        # excd_cnt 가 less_cnt 보다 적은 경우는 excd_cnt 만큼 candidate_queue 에 담은 다음 나머지 TS 수를 다음 excd TS 에 대한
        # tablet 을 담는다. excd_addr 은 변경되고, less_addr 은 유지해야 한다.
        if excd_cnt < abs(less_cnt):
            print("excd_cnt < abs(less_cnt)")
            print(unicode("2-1. %s 만큼 현재 excd_addr(%s) 에서 candidate_queue 에 후보 담는다." % (excd_cnt, excd_addr)))
            excd_tablets = extract_tablets(excd_addr)
            sorted_tablets = sort_tablets(less_addr, excd_tablets)
            if candidate_queue.qsize() > 0:
                sorted_tablets = check_duplicate_tablet(candidate_queue, sorted_tablets)
            for i in range(excd_cnt):
                candidate_queue.put({"source_ts": excd_addr, "target_ts": less_addr,
                                     "tablet_id": sorted_tablets[i]})
            # 나머지 부분 다음 excd_addr 에서 추출해서 담는다.
            print("    %s 개 후보 담기 시도, 현재 candidate_queue 사이즈: %s" % (excd_cnt, candidate_queue.qsize()))
            less_cnt = less_cnt + excd_cnt
            excd = excd_queue.get()
            excd_addr = excd["ts_address"]
            excd_cnt = excd["result_count"]
            print(unicode("2-2. 나머지 부분 %s 개를 다음 순서의 excd_addr(%s) 에서 추출해서 담는다." % (abs(less_cnt), excd_addr)))
            print("2-3. 이동해야 할 수(excd_cnt): %s, 이동 시킬 수 있는 수(less_cnt): %s (%s -> %s)" % (
            excd_cnt, less_cnt, excd_addr, less_addr))
            excd_tablets = extract_tablets(excd_addr)
            sorted_tablets = sort_tablets(less_addr, excd_tablets)
            if candidate_queue.qsize() > 0:
                sorted_tablets = check_duplicate_tablet(candidate_queue, sorted_tablets)
            for i in range(abs(less_cnt)):
                candidate_queue.put({"source_ts": excd_addr, "target_ts": less_addr,
                                     "tablet_id": sorted_tablets[i]})
            print("    %s 개 후보 담기 시도, 현재 candidate_queue 사이즈: %s" % (abs(less_cnt), candidate_queue.qsize()))
            excd_cnt = excd_cnt - abs(less_cnt)
            print("2-4. 나머지 excd_cnt 는 %s 이고, 다음 less_addr 으로 이동" % excd_cnt)
            # break
        else:
            excd_tablets = extract_tablets(excd_addr)
            sorted_tablets = sort_tablets(less_addr, excd_tablets)
            if candidate_queue.qsize() > 0:
                sorted_tablets = check_duplicate_tablet(candidate_queue, sorted_tablets)
            for i in range(abs(less_cnt)):
                candidate_queue.put({"source_ts": excd_addr, "target_ts": less_addr,
                                     "tablet_id": sorted_tablets[i]})
            print("    %s 개 후보 담기 시도, 현재 candidate_queue 사이즈: %s" % (abs(less_cnt), candidate_queue.qsize()))
            excd_cnt = excd_cnt - abs(less_cnt)
            print("3. 이동 시킨 후 이동해야 할 수(excd_cnt): %s" % excd_cnt)
while candidate_queue.qsize():
    print(candidate_queue.get())
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
