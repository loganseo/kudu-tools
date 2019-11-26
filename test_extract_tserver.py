# coding=utf-8
import sys
import json
import re
import subprocess
import time
import unittest

json_kudu_tserver_list = open("/Users/logan.seo/Downloads/kudu_tserver_list.json")
output_tserver = json_kudu_tserver_list.read().strip()
output_tserver = json.loads(output_tserver.decode("utf-8", "ignore"))

print("tablet_summaries: %s" % len(output_tserver['tserver_summaries']))

tserver_list = []

for i in range(len(output_tserver['tserver_summaries'])):
    address = output_tserver['tserver_summaries'][i]['address']
    ts_address = re.sub(':7050', '', string=address)
    print("ts_address: %s" % ts_address)
    tserver_list.append(ts_address)

json_rebalance_output = open("/Users/logan.seo/Downloads/kudu_rebalance_output.log")
output_rebalance_output = json_rebalance_output.read().strip()
output_rebalance_output = json.loads(output_rebalance_output.decode("utf-8", "ignore"))



# while moving_cnt > 0:
#     # output_ksck = ksck_tablets(tablet_list)
#     print("tablet_summaries: %s" % len(output_ksck['tablet_summaries']))
#     check_count = 0
#     for i in range(len(output_ksck['tablet_summaries'])):
#         for j in range(len(output_ksck['tablet_summaries'][i]['replicas'])):
#             state = output_ksck['tablet_summaries'][i]['replicas'][j]['state']
#             if state == "INITIALIZED":
#                 tablet_id = output_ksck['tablet_summaries'][i]['replicas'][j]['status_pb']['tablet_id']
#                 last_status = output_ksck['tablet_summaries'][i]['replicas'][j]['status_pb']['last_status']
#                 print("state: %s" % state)
#                 print("tablet_id: %s" % tablet_id)
#                 print("last_status: %s" % last_status)
#                 check_count = check_count + 1
#             elif state == "UNKNOWN":
#                 print("state: %s" % state)
#                 check_count = check_count + 1
#     print("moving_cnt: %s, check_count: %s" % (moving_cnt, check_count))
#     if moving_cnt > check_count:
#         moving_cnt = check_count
#     # if check_count == 0:
#     #     moving_cnt = moving_cnt - 1
#     time.sleep(60)
# print("checking_move_replica::moving_cnt: %s" % moving_cnt)