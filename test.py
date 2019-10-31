# coding=utf-8
import sys
import json
import re
import subprocess
import time
import unittest

json_file = open("/Users/logan.seo/Downloads/ksck_core_1.json")
output_ksck = json_file.read().strip()
output_ksck = json.loads(output_ksck.decode("utf-8", "ignore"))

moving_cnt = len(output_ksck['tablet_summaries'])
while moving_cnt > 0:
    # output_ksck = ksck_tablets(tablet_list)
    print("tablet_summaries: %s" % len(output_ksck['tablet_summaries']))
    check_count = 0
    for i in range(len(output_ksck['tablet_summaries'])):
        for j in range(len(output_ksck['tablet_summaries'][i]['replicas'])):
            state = output_ksck['tablet_summaries'][i]['replicas'][j]['status_pb']['state']
            if state == "INITIALIZED":
                tablet_id = output_ksck['tablet_summaries'][i]['replicas'][j]['status_pb']['tablet_id']
                last_status = output_ksck['tablet_summaries'][i]['replicas'][j]['status_pb']['last_status']
                print("state: %s" % state)
                print("tablet_id: %s" % tablet_id)
                print("last_status: %s" % last_status)
                check_count = check_count + 1
    print("moving_cnt: %s, check_count: %s" % (moving_cnt, check_count))
    if moving_cnt > check_count:
        moving_cnt = check_count
    # if check_count == 0:
    #     moving_cnt = moving_cnt - 1
    time.sleep(60)
print("checking_move_replica::moving_cnt: %s" % moving_cnt)