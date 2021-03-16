# coding=utf-8
import Queue
import sys
import json
import time
from collections import OrderedDict
from random import shuffle

from python.core.control_replica import ControlReplica
from python.core.extractor import Extractor
from python.core.utils import Utils


class KuduRebalancer(object):
    def __init__(self, masters, table_name, target_partition, max_moves, pre_execution):
        self._masters = masters
        self._table_name = table_name
        self._target_partition = target_partition
        self._max_moves = max_moves
        self._pre_execution = pre_execution

    def rebalance_job(self):
        start_time = time.time()

        extractor = Extractor(self._masters, self._table_name, self._target_partition)

        # 주어진 테이블과 Range Partition 에 대해 전체 tablet 수, TServer 별 tablet 수, 적정 tablet 수 추출
        json_tablet_status = extractor.extract_dist_status()
        json_tablet_status = json.loads(json_tablet_status.decode("utf-8", "ignore"))
        # print(json_tablet_status)

        # 적정수 - 보유수 = 초과인 TServer 와 미만인 TServer 분리해서 저장
        expected_count = json_tablet_status["expected_count"]  # 적정수
        exceeded_ts = OrderedDict()  # 적정수 초과 TServer
        exceeded_ts["exceeded_summaries"] = []
        less_ts = OrderedDict()  # 적정수 미만 TServer
        less_ts["less_summaries"] = []
        zero_ts = OrderedDict()  # 적정수 TServer
        zero_ts["less_summaries"] = []
        for i in range(len(json_tablet_status['tablet_summaries'])):
            # 적정수 초과/미만을 판별하고 이동/받아야함을 나타내는 변수
            result_cnt = json_tablet_status['tablet_summaries'][i]['tablet_count'] - expected_count
            if result_cnt > 0:
                exceeded_ts["exceeded_summaries"].append(
                    {"ts_address": json_tablet_status['tablet_summaries'][i]['ts_address'],
                     "tablet_count": json_tablet_status['tablet_summaries'][i][
                         'tablet_count'], "result_count": result_cnt})
            elif result_cnt < 0:
                less_ts["less_summaries"].append({"ts_address": json_tablet_status['tablet_summaries'][i]['ts_address'],
                                                  "tablet_count": json_tablet_status['tablet_summaries'][i][
                                                      'tablet_count'], "result_count": result_cnt})
            elif result_cnt == 0:
                zero_ts["less_summaries"].append({"ts_address": json_tablet_status['tablet_summaries'][i]['ts_address'],
                                                  "tablet_count": json_tablet_status['tablet_summaries'][i][
                                                      'tablet_count'], "result_count": result_cnt})

        # Sorting
        exceeded_ts["exceeded_summaries"] = sorted(exceeded_ts["exceeded_summaries"],
                                                   key=lambda item: item['result_count'], reverse=True)
        less_ts["less_summaries"] = sorted(less_ts["less_summaries"], key=lambda item: item['result_count'])
        print(json.dumps(exceeded_ts, ensure_ascii=False, indent=4))
        print(json.dumps(less_ts, ensure_ascii=False, indent=4))
        sys.stdout.flush()

        # 분리된 TServer 리스트 큐에 담기
        excd_queue = Queue.Queue()
        less_queue = Queue.Queue()
        excd_result_cnt = 0  # 이동 대상 tablet 수
        less_result_cnt = 0  # 받아야 하는 tablet 수
        for i in range(len(exceeded_ts['exceeded_summaries'])):
            excd_queue.put(exceeded_ts['exceeded_summaries'][i])
            excd_result_cnt = excd_result_cnt + exceeded_ts['exceeded_summaries'][i]['result_count']
        for j in range(len(less_ts['less_summaries'])):
            less_queue.put(less_ts['less_summaries'][j])
            less_result_cnt = less_result_cnt + less_ts['less_summaries'][j]['result_count']

        # 받아야 할 적정수 미만 TServer 가 없고, 이동 대상 Tablet 이 존재할 경우 과도하게 많은 TServer 에서 적정수 Tserver 로 이동
        if less_result_cnt == 0 and excd_result_cnt > 0 and less_queue.qsize() == 0 and len(
                zero_ts["less_summaries"]) != 0:
            less_ts = zero_ts.copy()
            for j in range(len(less_ts['less_summaries'])):
                less_queue.put(less_ts['less_summaries'][j])
                less_result_cnt = less_result_cnt + 1
            print(json.dumps(less_ts, ensure_ascii=False, indent=4))

        if excd_result_cnt <= 0:
            raise ValueError("There is nothing to move.")

        if less_queue.qsize() == 0:
            raise ValueError("There are no candidates.")

        print("이동 대상 tablet 수: %s / 받아야 할 tablet 수: %s" % (excd_result_cnt, less_result_cnt))

        # TODO: 이동 대상 tablet 이 특정 TServer 에 몰려있는 경우, 해당 TServer 에 tablet 을 받을 대상들에게
        #  적정수치까지 먼저 할당하는 로직 추가 필요
        """ 
        1. exceeded_ts 가 보유한 Tablet 리스트 추출
        2. Target TS 의 tablet 리스트 추출
        3. 추출된 tablet 리스트에서 Target 이 포함 되어 있으면 리스트에서 제거
        4. candidate_queue 생성
        - 목표 tablet 수: abs(less_result_cnt)
        - 실체 중복 tablet 은 move_replica 를 수행하지 않아 목표 수 보다 적게 수행될 수 있다. 
        """
        utils = Utils()
        source_dic = OrderedDict()  # 이동 대상 정보
        target_dic = OrderedDict()  # 받는 대상 정보
        candidate_queue = Queue.Queue()  # 이동 후보 정보
        t_key = 0  # Dictionary 에 배치되는 TServer 별 tablet 들의 key 값
        dup_tablet_list = []  # 후보 리스트 중복 tablet
        # if excd_result_cnt >= abs(less_result_cnt):
        # 1. exceeded_ts 가 보유한 tablet 리스트 추출
        for i in range(len(exceeded_ts['exceeded_summaries'])):
            excd_addr = exceeded_ts['exceeded_summaries'][i]['ts_address']
            excd_cnt = exceeded_ts['exceeded_summaries'][i]['result_count']
            print("\n1. %s 에서 이동해야 할 Tablet 수: %s" % (excd_addr, excd_cnt))
            if excd_cnt > 0:
                excd_tablets = extractor.extract_tablets(excd_addr)
                # print("- before excd_tablets: %s" % excd_tablets)
                shuffle(excd_tablets)  # 동일한 tablet(replica) 이동 최소화
                # print("- after excd_tablets: %s" % excd_tablets)
                print("  ㄴ %s 에서 보유한 Tablet 수: %s" % (excd_addr, len(excd_tablets)))
                t_key = 0
                source_dic[excd_addr] = {}
                for j in range(len(excd_tablets)):
                    source_dic[excd_addr][t_key] = excd_tablets[j]
                    t_key = t_key + 1
                # print(source_dic.get(excd_addr))
                print("  ㄴ 중복 제거 전 %s length: %s" % (excd_addr, len(source_dic.get(excd_addr))))
        sys.stdout.flush()

        # 2. Target TS 의 tablet 리스트 추출
        print("\n\n2. Target TS 의 Tablet 리스트 추출")
        for k in range(len(less_ts['less_summaries'])):
            less_addr = less_ts['less_summaries'][k]['ts_address']
            less_cnt = less_ts['less_summaries'][k]['result_count']
            if abs(less_cnt) >= 0:
                less_tablets = extractor.extract_tablets(less_addr)
                t_key = 0
                target_dic[less_addr] = {}
                for j in range(len(less_tablets)):
                    target_dic[less_addr][t_key] = less_tablets[j]
                    t_key = t_key + 1
                # print(target_dic.get(less_addr))
        sys.stdout.flush()

        # 3. 추출된 Tablet 리스트에서 Target 이 포함 되어 있으면 리스트에서 제거
        print("\n\n3. 추출된 Tablet 리스트에서 Target 이 포함 되어 있으면 리스트에서 제거")
        dup_info = {}
        for (excd_ts, excd_tablets) in source_dic.items():
            for (excd_key, excd_tablet) in excd_tablets.items():
                for (less_ts, less_tablets) in target_dic.items():
                    for less_tablet in less_tablets.items():
                        if excd_tablet == less_tablet:
                            print("  ㄴ 중복 Tablet: %s, %s" % (excd_ts, excd_tablet))
                            dup_info[excd_ts] = excd_tablet
        if len(dup_info) > 0:
            for (key, val) in dup_info.items():
                source_dic.get(key).pop(val)
            for (ts, tablets) in source_dic.items():
                print("  ㄴ 중복 제거 후 %s length: %s" % (ts, len(source_dic.get(ts))))
        else:
            print("  ㄴ 중복 없음")
        sys.stdout.flush()

        # 4. candidate_queue 생성
        print("\n\n4. candidate_queue 생성")
        tmp_src_dic = source_dic.copy()  # 이동대상정보를 담아 놓을 임시 dictionary
        copy_src_dic = OrderedDict()  # 남아 있는 이동대상정보를 담아 놓을 임시 dictionary
        add_cnt = 0
        less_cnt = 0
        less_addr = ""
        while True:
            # 아래 for 문에서 받는 TServer 의 tablet 수가 채워지지 않고 for 문이 종료된 경우
            # 다음으로 받을 대상 정보 갱신 없이 이어서 이동 대상의 tablet 추가 진행
            if add_cnt != 0 and less_cnt != 0 and add_cnt != abs(less_cnt):
                print("  이어서 받을 대상: %s" % less_addr)
            else:
                # 초기 받을 대상 셋팅 및 이후 받을 대상 셋팅 반복
                less = less_queue.get()
                less_addr = less["ts_address"]
                less_cnt = less["result_count"]
                add_cnt = 0
                print("  1) %s 는 %s 개를 받아야 함" % (less_addr, abs(less_cnt)))
            sys.stdout.flush()
            # 이동 대상 tablet 을 받는 수 만큼 candidate_queue 에 셋팅
            for (excd_ts, excd_tablets) in tmp_src_dic.items():
                # print("  2) 이동대상 %s 의 Tablet 개수 %s" % (excd_ts, len(excd_tablets)))
                if len(excd_tablets) > 0:
                    t_id = excd_tablets.popitem()  # 이동 대상 tablet 정보
                    print("    (후보) source_ts: %s, tablet_id: %s, target_ts: %s" % (excd_ts, t_id[1], less_addr))
                    candidate_queue.put({"source_ts": excd_ts, "target_ts": less_addr, "tablet_id": t_id[1]})
                    dup_tablet_list.append(t_id[1])
                    # candidate_queue 에 셋팅된 정보를 제외한 나머지 이동 대상 정보(TServer명, tablet) 복사
                    copy_src_dic[excd_ts] = tmp_src_dic.pop(excd_ts)
                    # print("  before len(tmp_excd_dic): %s" % len(tmp_excd_dic))
                    # 이동 대상 정보를 소진했을 경우 이전에 복사한 나머지 이동 대상 정보를 셋팅
                    if len(tmp_src_dic) == 0:
                        tmp_src_dic = copy_src_dic.copy()
                        copy_src_dic.clear()
                    # print("  after len(tmp_excd_dic): %s" % len(tmp_excd_dic))
                    # print("copy_excd_dic: %s" % copy_excd_dic)
                    add_cnt = add_cnt + 1
                    # 받는 대상의 수를 채운 경우 for 문 중단
                    if add_cnt == abs(less_cnt):
                        print("  2) %s 개를 받아야 하고, %s 개 받음" % (abs(less_cnt), add_cnt))
                        break
                    elif len(zero_ts['less_summaries']) != 0 and add_cnt != abs(less_cnt) and add_cnt == 1 and abs(
                            less_cnt) == 0:
                        print("  2) %s 개를 받아야 하고, %s 개 받음" % (abs(less_cnt), add_cnt))
                        break
                sys.stdout.flush()
            if candidate_queue.qsize() == abs(less_result_cnt):
                print("\n\n5. candidate_queue 생성 완료 (Queue size: %s, 소요시간: %s)\n\n"
                      % (candidate_queue.qsize(), utils.calc_elapse_time(start_time)))
                sys.stdout.flush()
                break
            else:
                print("  candidate_queue.qsize(): %s, abs(less_result_cnt): %s\n\n"
                      % (candidate_queue.qsize(), abs(less_result_cnt)))
        # elif excd_result_cnt < abs(less_result_cnt):
        #     raise Exception("Please check replicas...")
        sys.stdout.flush()

        if self._pre_execution == "false":
            if candidate_queue.qsize() <= 0:
                print("There is nothing to move...")
                sys.stdout.flush()
            elif candidate_queue.qsize() == abs(less_result_cnt):
                move_cnt = 0
                moved_list = []
                moving_replica_list = []
                # candidate_queue_size = candidate_queue.qsize()
                while candidate_queue.qsize():
                    body = candidate_queue.get()
                    source_ts = body['source_ts']
                    target_ts = body['target_ts']
                    tablet_id = body['tablet_id']
                    control = ControlReplica(self._masters, self._table_name, self._target_partition, source_ts, target_ts, tablet_id)
                    # Start moving replica!!
                    if dup_tablet_list.count(tablet_id) == 1:
                        move_cnt = control.move_replica(move_cnt)
                        print("Start moving this replica: %s ---- [%s] ----> %s" % (source_ts, tablet_id, target_ts))
                        # move_replica 가 수행된 tablet id 를 담아서 move 수행이 완료될 때 까지 체크
                        moving_replica_list.append(tablet_id)
                        moved_list.append({"source_ts": source_ts, "tablet_id": tablet_id})
                    elif dup_tablet_list.count(tablet_id) > 1:
                        print("중복 tablet %s 는 move_replica 를 수행하지 않는다." % tablet_id)
                        dup_tablet_list.remove(tablet_id)
                    print("(%s/%s) 소요시간: %s"
                          % (candidate_queue.qsize(), abs(less_result_cnt), utils.calc_elapse_time(start_time)))
                    utils.print_progress((abs(less_result_cnt) - candidate_queue.qsize()), abs(less_result_cnt)
                                         , "Progress", "Complete", 1, 50)
                    print('\n')
                    sys.stdout.flush()
                    if move_cnt != 0 \
                            and ((self._max_moves == move_cnt)
                                 or (self._max_moves > move_cnt and candidate_queue.qsize() == 0)):
                        # moving 중인 replica 체크
                        check_cnt = control.checking_move_replica(moving_replica_list)
                        # move_replica 완료 후 replica 삭제
                        if check_cnt == 0 and len(moved_list) > 0:
                            control.remove_replica(moved_list, check_cnt)
                        # moving_replica_list 초기화
                        moving_replica_list = []
                        moved_list = []
                        move_cnt = 0
                print("\nKudu Rebalance 완료 (소요시간: %s)" % utils.calc_elapse_time(start_time))
                sys.stdout.flush()
            else:
                raise Exception("Please check candidate_queue...")
        else:
            print("\nPre-execution is ended.")
            sys.stdout.flush()
