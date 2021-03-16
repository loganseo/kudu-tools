# coding=utf-8
import json
import math
import re
import subprocess
from collections import OrderedDict


class Extractor(object):
    def __init__(self, masters, table_name, target_partition):
        self._masters = masters
        self._table_name = table_name
        self._target_partition = target_partition

    # Source TS Web UI 에서 Tablet 리스트 추출
    def extract_tablets(self, src_ts):
        cmd_extr = 'curl -s http://' + src_ts + ':8050/tablets | grep -w "' + self._table_name \
                   + '" -A2 | grep "PARTITION &quot;' + self._target_partition + '&quot;" -B1 | grep -o id=.*'
        tablets = subprocess.Popen(cmd_extr, stdout=subprocess.PIPE, shell=True).stdout
        extr_tlist = tablets.read().strip()
        tablets.close()
        extr_tlist = re.sub('(id=)', '', string=extr_tlist)
        extr_tlist = re.sub('".*', '', string=extr_tlist)
        extr_tlist = extr_tlist.split("\n")
        return extr_tlist

    def extract_ts_uuid(self, ts_id):
        ts_uuid = subprocess.Popen('curl -s http://' + ts_id
                                   + ':8050/tablets | grep -w "server uuid"', stdout=subprocess.PIPE, shell=True).stdout
        ext_ts_uuid = ts_uuid.read().strip()
        ts_uuid.close()
        ext_ts_uuid = re.sub('server uuid ', '', string=ext_ts_uuid)
        ext_ts_uuid = re.sub('</pre>', '', string=ext_ts_uuid)
        return ext_ts_uuid

    # Tablet Server List 추출
    def extract_tserver(self):
        cmd_extrct = "kudu cluster ksck " + self._masters + " -ksck_format=json_pretty -sections=TSERVER_SUMMARIES"
        temp_output = subprocess.Popen(cmd_extrct, stdout=subprocess.PIPE, shell=True).stdout
        output_tserver = temp_output.read().strip()
        temp_output.close()
        output_tserver = json.loads(output_tserver.decode("utf-8", "ignore"))
        return output_tserver

    # Table Range Partition 의 Tablet 분포도 추출(JSON)
    def extract_dist_status(self):
        output_tserver = self.extract_tserver()
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
            extracted_tablets = self.extract_tablets(ts)
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
