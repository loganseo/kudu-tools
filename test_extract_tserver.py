# coding=utf-8
import sys
import json
import re
import subprocess
import time
import unittest
import Queue
from collections import OrderedDict

reload(sys)
sys.setdefaultencoding('utf-8')

json_tablet_status = open("/Users/dongjin/Downloads/kudu_rebalancing_Test/tablet_summaries.json")
json_tablet_status = json_tablet_status.read().strip()
json_tablet_status = json.loads(json_tablet_status.decode("utf-8", "ignore"))

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
# Sorting
exceeded_ts["exceeded_summaries"] = sorted(exceeded_ts["exceeded_summaries"],
                                           key=lambda item: item['result_count'], reverse=True)
less_ts["less_summaries"] = sorted(less_ts["less_summaries"], key=lambda item: item['result_count'])
# print(json.dumps(exceeded_ts, ensure_ascii=False, indent=4))
# print(json.dumps(less_ts, ensure_ascii=False, indent=4))
# print(len(exceeded_ts['exceeded_summaries']))
# print(len(less_ts['less_summaries']))

excd_queue = Queue.Queue()
less_queue = Queue.Queue()
for i in range(len(exceeded_ts['exceeded_summaries'])):
    excd_queue.put(exceeded_ts['exceeded_summaries'][i])
for j in range(len(less_ts['less_summaries'])):
    less_queue.put(less_ts['less_summaries'][j])

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
            # for i in range(excd_cnt):
            #     candidate_queue.put({"source_ts": excd_addr, "target_ts": less_addr,
            #                          "tablet_id": sorted_tablets[i]})
            # 나머지 부분 다음 excd_addr 에서 추출해서 담는다.
            print("    %s 개 후보 담기 완료" % excd_cnt)
            less_cnt = less_cnt + excd_cnt
            excd = excd_queue.get()
            excd_addr = excd["ts_address"]
            excd_cnt = excd["result_count"]
            print(unicode("2-2. 나머지 부분 %s 개를 다음 순서의 excd_addr(%s) 에서 추출해서 담는다." % (abs(less_cnt), excd_addr)))
            print("2-3. 이동해야 할 수(excd_cnt): %s, 이동 시킬 수 있는 수(less_cnt): %s (%s -> %s)" % (
            excd_cnt, less_cnt, excd_addr, less_addr))
            # excd_tablets = extract_tablets(excd_addr)
            # sorted_tablets = sort_tablets(less_addr, excd_tablets)
            # for i in range(abs(less_cnt)):
            #     candidate_queue.put({"source_ts": excd_addr, "target_ts": less_addr,
            #                          "tablet_id": sorted_tablets[i]})
            print("    %s 개 후보 담기 완료" % abs(less_cnt))
            excd_cnt = excd_cnt - abs(less_cnt)
            print("2-4. 나머지 excd_cnt 는 %s 이고, 다음 less_addr 으로 이동" % excd_cnt)
            # break
        else:
            print("    %s 개 후보 담기 완료" % abs(less_cnt))
            # for i in range(less_cnt):
            #     candidate_queue.put({"source_ts": excd_addr, "target_ts": less_addr,
            #                          "tablet_id": sorted_tablets[i]})
            excd_cnt = excd_cnt - abs(less_cnt)
            print("3. 이동 시킨 후 이동해야 할 수(excd_cnt): %s" % excd_cnt)

# json_kudu_tserver_list = open("/Users/logan.seo/Downloads/kudu_tserver_list.json")
# output_tserver = json_kudu_tserver_list.read().strip()
# output_tserver = json.loads(output_tserver.decode("utf-8", "ignore"))
#
# print("tablet_summaries: %s" % len(output_tserver['tserver_summaries']))
#
# tserver_list = []
#
# for i in range(len(output_tserver['tserver_summaries'])):
#     address = output_tserver['tserver_summaries'][i]['address']
#     ts_address = re.sub(':7050', '', string=address)
#     print("ts_address: %s" % ts_address)
#     tserver_list.append(ts_address)
#
# json_rebalance_output = open("/Users/logan.seo/Downloads/kudu_rebalance_output.log")
# output_rebalance_output = json_rebalance_output.read().strip()
# output_rebalance_output = json.loads(output_rebalance_output.decode("utf-8", "ignore"))