# coding=utf-8
import subprocess
import sys
import time

from python.core.extractor import Extractor
from python.core.kscks import Kscks


class ControlReplica(object):
    def __init__(self, masters, table_name, source_tserver, target_tserver, tablet_id):
        self._masters = masters
        self._table_name = table_name
        self._source_tserver = source_tserver
        self._target_tserver = target_tserver
        self._tablet_id = tablet_id

    def move_replica(self, cnt):
        src_ts_uuid = Extractor.extract_ts_uuid(self._source_tserver)
        trg_ts_uuid = Extractor.extract_ts_uuid(self._target_tserver)
        cmd_move = "nohup kudu tablet change_config move_replica " + self._masters + " " \
                   + self._tablet_id + " " + src_ts_uuid + " " + trg_ts_uuid + " & > /dev/null 2>&1"
        subprocess.call(cmd_move, shell=True)
        cnt = cnt + 1
        time.sleep(10)
        return cnt

    def checking_move_replica(self, tablet_list):
        output_ksck = Kscks.ksck_tablets(self, tablet_list)
        moving_cnt = len(output_ksck['tablet_summaries'])
        # move_replica 가 실행된 tablet 들이 모두 수행 완료될 때 까지 while 문 수행
        while moving_cnt > 0:
            output_ksck = Kscks.ksck_tablets(self, tablet_list)
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

    def remove_replica(self, moved_list, moved_count):
        # moved 된 tablet 삭제
        if moved_count == 0:
            for t in moved_list:
                s_ts = t["source_ts"]
                t_id = t["tablet_id"]
                s_ts = Extractor.extract_ts_uuid(s_ts)
                print("completed to move this tablet: %s" % t_id)
                output_ksck = Kscks.ksck_single_tablet(self)
                leader_ts_uuid = output_ksck['tablet_summaries'][0]['master_cstate']['leader_uuid']
                time.sleep(5)
                cmd_remove = "kudu tablet change_config remove_replica " + self._masters + " " \
                             + t_id + " " + s_ts
                if s_ts == leader_ts_uuid:
                    print("Doing leader_step_down")
                    cmd_leader_step_down = "kudu tablet leader_step_down " + self._masters + " " + t_id
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
