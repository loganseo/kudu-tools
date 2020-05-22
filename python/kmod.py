# coding=utf-8
import sys
import json
import re
import subprocess
import time
import math
from collections import OrderedDict


# Source TS Web UI 에서 Tablet 리스트 추출
def extract_tablets(src_ts, tbl_nm, trg_prtn):
    cmd_extr = 'curl -s http://' + src_ts + ':8050/tablets | grep -w "' + tbl_nm \
               + '" -A2 | grep "PARTITION &quot;' + trg_prtn + '&quot;" -B1 | grep -o id=.*'
    tablets = subprocess.Popen(cmd_extr, stdout=subprocess.PIPE, shell=True).stdout
    extr_tlist = tablets.read().strip()
    tablets.close()
    extr_tlist = re.sub('(id=)', '', string=extr_tlist)
    extr_tlist = re.sub('".*', '', string=extr_tlist)
    extr_tlist = extr_tlist.split("\n")
    return extr_tlist


# 추출된 Tablet 리스트에서 Target TS 가 포함 되있거나, 이미 실행된 move_replica 에 포함된 경우 해당 replica 는 제외
def sort_tablets(masters, tbl_nm, target_ts, tablet_list):
    sorted_tlist = []
    for t in tablet_list:
        cmd_ksck = "kudu cluster ksck " + masters \
                   + " -ksck_format=json_pretty -tables=" + tbl_nm \
                   + " -tablets=" + t
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


def ksck_tablets(masters, tbl_nm, tlist):
    moved_tlist_str = ','.join(map(str, tlist))
    cmd_ksck = "kudu cluster ksck " + masters \
               + " -ksck_format=json_pretty -tables=" + tbl_nm \
               + " -tablets=" + moved_tlist_str
    temp_output = subprocess.Popen(cmd_ksck, stdout=subprocess.PIPE, shell=True).stdout
    output_ksck = temp_output.read().strip()
    temp_output.close()
    output_ksck = json.loads(output_ksck.decode("utf-8", "ignore"))
    return output_ksck


def ksck_single_tablet(masters, tbl_nm, a_tablet):
    cmd_ksck = "kudu cluster ksck " + masters \
               + " -ksck_format=json_pretty -tables=" + tbl_nm \
               + " -tablets=" + a_tablet
    temp_output = subprocess.Popen(cmd_ksck, stdout=subprocess.PIPE, shell=True).stdout
    output_ksck = temp_output.read().strip()
    temp_output.close()
    output_ksck = json.loads(output_ksck.decode("utf-8", "ignore"))
    return output_ksck


def extract_ts_uuid(ts_id):
    ts_uuid = subprocess.Popen('curl -s http://' + ts_id
                               + ':8050/tablets | grep -w "server uuid"', stdout=subprocess.PIPE, shell=True).stdout
    ext_ts_uuid = ts_uuid.read().strip()
    ts_uuid.close()
    ext_ts_uuid = re.sub('server uuid ', '', string=ext_ts_uuid)
    ext_ts_uuid = re.sub('</pre>', '', string=ext_ts_uuid)
    return ext_ts_uuid


def move_replica(masters, t_id, src_ts, trg_ts, cnt):
    src_ts_uuid = extract_ts_uuid(src_ts)
    trg_ts_uuid = extract_ts_uuid(trg_ts)
    cmd_move = "nohup kudu tablet change_config move_replica " + masters + " " \
               + t_id + " " + src_ts_uuid + " " + trg_ts_uuid + " & > /dev/null 2>&1"
    subprocess.call(cmd_move, shell=True)
    cnt = cnt + 1
    time.sleep(10)
    return cnt


def checking_move_replica(masters, tbl_nm, tablet_list):
    output_ksck = ksck_tablets(masters, tbl_nm, tablet_list)
    moving_cnt = len(output_ksck['tablet_summaries'])
    # move_replica 가 실행된 tablet 들이 모두 수행 완료될 때 까지 while 문 수행
    while moving_cnt > 0:
        output_ksck = ksck_tablets(masters, tbl_nm, tablet_list)
        check_count = 0
        # print("\nMoving...(%s)" % moving_cnt)
        for i in range(len(output_ksck['tablet_summaries'])):
            for j in range(len(output_ksck['tablet_summaries'][i]['replicas'])):
                state = output_ksck['tablet_summaries'][i]['replicas'][j]['state']
                if state == "INITIALIZED":
                    tablet_id = output_ksck['tablet_summaries'][i]['replicas'][j]['status_pb']['tablet_id']
                    last_status = output_ksck['tablet_summaries'][i]['replicas'][j]['status_pb']['last_status']
                    # print("[%s] %s, %s" % (state, tablet_id, last_status))
                    check_count = check_count + 1
                elif state == "UNKNOWN":
                    print("state: %s" % state)
                    check_count = check_count + 1
                sys.stdout.flush()
        # moving_cnt: 초기 move_replica 대상 tablet 수 -> 실제 move_replica 가 실행된 수
        # check_count: 실제 move_replica 가 실행된 수
        if moving_cnt > check_count:
            moving_cnt = check_count
        time.sleep(60)
    return moving_cnt


def remove_replica(masters, tbl_nm, moved_list, moved_count):
    # moved 된 tablet 삭제
    if moved_count == 0:
        for t in moved_list:
            s_ts = t["source_ts"]
            t_id = t["tablet_id"]
            s_ts = extract_ts_uuid(s_ts)
            print("completed to move this tablet: %s" % t_id)
            output_ksck = ksck_single_tablet(masters, tbl_nm, t_id)
            leader_ts_uuid = output_ksck['tablet_summaries'][0]['master_cstate']['leader_uuid']
            time.sleep(5)
            cmd_remove = "kudu tablet change_config remove_replica " + masters + " " \
                         + t_id + " " + s_ts
            if s_ts == leader_ts_uuid:
                print("Doing leader_step_down")
                cmd_leader_step_down = "kudu tablet leader_step_down " + masters + " " + t_id
                subprocess.call(cmd_leader_step_down, shell=True)
                time.sleep(5)
                remove_result = subprocess.call(cmd_remove, shell=True)
            else:
                remove_result = subprocess.call(cmd_remove, shell=True)
            if int(remove_result) == 0:
                print("Removed this tablet")
                sys.stdout.flush()
                time.sleep(5)
            elif int(remove_result) == 1:
                print("This tablet can't be removed.")
                # raise ValueError("Please check the tablets. Some Tablets can't be removed.")


# Tablet Server List 추출
def extract_tserver(masters):
    cmd_extrct = "kudu cluster ksck " + masters + " -ksck_format=json_pretty -sections=TSERVER_SUMMARIES"
    temp_output = subprocess.Popen(cmd_extrct, stdout=subprocess.PIPE, shell=True).stdout
    output_tserver = temp_output.read().strip()
    temp_output.close()
    output_tserver = json.loads(output_tserver.decode("utf-8", "ignore"))
    return output_tserver


# Table Range Partition 의 Tablet 분포도 추출(JSON)
def extract_dist_status(masters, tbl_nm, trg_prtn):
    output_tserver = extract_tserver(masters)
    tserver_list = []
    for i in range(len(output_tserver['tserver_summaries'])):
        address = output_tserver['tserver_summaries'][i]['address']
        ts_address = re.sub(':7050', '', string=address)
        # print("ts_address: %s" % ts_address)
        tserver_list.append(ts_address)
    tablet_dist_status = OrderedDict()
    tablet_dist_status["tablet_summaries"] = []
    total_count = 0
    for ts in tserver_list:
        extracted_tablets = extract_tablets(ts, tbl_nm, trg_prtn)
        if extracted_tablets[0] == "":
            extracted_tablets_count = 0
        else:
            extracted_tablets_count = len(extracted_tablets)
        # print("%s %s" % (ts, extracted_tablets_count))
        total_count = total_count + extracted_tablets_count
        tablet_dist_status["tablet_summaries"].append({"ts_address": ts, "tablet_count": extracted_tablets_count})
    tablet_dist_status["total_count"] = total_count
    # Tablet Server 당 적절한 Tablet 수
    expected_count = float(total_count) / float(len(tserver_list))
    # tablet_dist_status["expected_count"] = int(round(expected_count))
    tablet_dist_status["expected_count"] = int(math.trunc(expected_count))
    json_tablet_dist_status = json.dumps(tablet_dist_status, ensure_ascii=False, indent=4)
    return json_tablet_dist_status


# 이미 candidate_queue 존재하는 tablet 제거
def check_duplicate_tablet(c_list, s_tablets):
    temp_dup_list = []
    # source TS 에서 추출된 tablet list 중 candidate 와 중복되는 tablet 추출
    for list_num in range(len(s_tablets)):
        for c_num in range(len(c_list)):
            if s_tablets[list_num] == c_list[c_num]['tablet_id']:
                temp_dup_list.append(s_tablets[list_num])
    # source TS 에서 추출된 tablet list 에서 중복된 tablet 제거
    if len(temp_dup_list) > 0:
        for dup_cnt in range(len(temp_dup_list)):
            s_tablets.remove(temp_dup_list[dup_cnt])
    return s_tablets


# 소요시간 계산
def calc_elapse_time(start_time):
    total_elapse_time = time.time() - start_time
    day = total_elapse_time // (24 * 3600)
    total_elapse_time = total_elapse_time % (24 * 3600)
    hour = total_elapse_time // 3600
    total_elapse_time %= 3600
    minutes = total_elapse_time // 60
    total_elapse_time %= 60
    seconds = total_elapse_time
    total_elapse_time = str(int(day)) + "일 " + str(int(hour)) + "시간 " \
        + str(int(minutes)) + "분 " \
        + str(int(seconds)) + "초"
    return total_elapse_time


# Progress Bar
def print_progress(iteration, total, prefix='', suffix='', decimals=1, bar_length=100):
    format_string = "{0:." + str(decimals) + "f}"
    percent = format_string.format(100 * (iteration / float(total)))
    filled_length = int(round(bar_length * iteration / float(total)))
    bar = '#' * filled_length + '-' * (bar_length - filled_length)
    sys.stdout.write('\r%s |%s| %s%s %s' % (prefix, bar, percent, '%', suffix))
    if iteration == total:
        sys.stdout.write('\n')
    sys.stdout.flush()
