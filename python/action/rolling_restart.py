# coding=utf-8
import re
import time

from collections import OrderedDict
from extractor import Extractor
from maintenance import Maintenance
from control_service import ControlService


class KuduRollingRestart(object):
    def __init__(self, masters, ambari_server, ambari_pwd):
        self._masters = masters
        self._ambari_server = ambari_server
        self._ambari_pwd = ambari_pwd

    def start(self):
        extractor = Extractor(self._masters)
        maintenance = Maintenance(self._masters)
        control = ControlService(self._masters, self._ambari_server, self._ambari_pwd)

        # Tablet Server 정보 추출
        output_tserver = extractor.extract_tserver()

        uuid_list = []
        tserver_list = []
        tserver_summaries = OrderedDict()
        tserver_summaries["tserver_summaries"] = []
        for i in range(len(output_tserver['tserver_summaries'])):
            uuid = output_tserver['tserver_summaries'][i]['uuid']
            address = output_tserver['tserver_summaries'][i]['address']
            hostname = re.sub(':7050', '', string=address)
            tserver_summaries['tserver_summaries'].append({'uuid': uuid, 'hostname': hostname})
            uuid_list.append(uuid)
            tserver_list.append(hostname)

        # 1) enter_maintenance 수행
        # 2) ksck 에서 uuid 로 Tablet Server States 확인
        # 3) quiesce 수행 /usr/hdp/current/kudu/bin/kudu tserver quiesce start <hostname>
        # 4) Tablet Server 중지 -> 시작 (ambari api 사용 가능??)
        # 5) bootstrapping
        # 6) exit_maintenance 수행
        # 7) ksck 에서 uuid 로 Tablet Server States 확인
        print("Target Tablet Servers...")
        count_maintenance_mode = 0
        for i in range(len(tserver_summaries['tserver_summaries'])):
            uuid = tserver_summaries['tserver_summaries'][i]['uuid']
            hostname = tserver_summaries['tserver_summaries'][i]['hostname']
            ts_state = extractor.extract_tserver_state(uuid)
            print("hostname: %s, uuid: %s, ts_state: %s" % (hostname, uuid, ts_state))
            if ts_state == "MAINTENANCE_MODE":
                count_maintenance_mode = count_maintenance_mode + 1
        if count_maintenance_mode == 0:
            for i in range(len(tserver_summaries['tserver_summaries'])):
                uuid = tserver_summaries['tserver_summaries'][i]['uuid']
                hostname = tserver_summaries['tserver_summaries'][i]['hostname']
                ts_state = extractor.extract_tserver_state(uuid)
                if ts_state == "NON_MAINTENANCE_MODE":
                    # 1) Kudu enter_maintenance 수행
                    maintenance.enter_maintenance(uuid)
                    # 1-1) enter_maintenance 수행 완료 여부 체크
                    i = 0
                    while i < 3:
                        time.sleep(3)
                        ts_state = extractor.extract_tserver_state(uuid)
                        if ts_state == "MAINTENANCE_MODE":
                            break
                        i += 1
                    if ts_state == "MAINTENANCE_MODE":
                        # 2) quiesce 수행
                        maintenance.enter_quiesce(hostname)
                        # 2-1) quiesce 수행 완료 여부 체크
                        t_leaders = extractor.extract_tablet_leaders(uuid)
                        while t_leaders != "0":
                            time.sleep(3)
                            t_leaders = extractor.extract_tablet_leaders(uuid)
                            print("Tablet Leaders: %s" % t_leaders)
                        if t_leaders == "0":
                            cluster_name = control.get_cluster_name()
                            # 3) Ambari Maintenance Mode: ON
                            # TODO: Ambari maintenance 수행 후 Ambari API로 Tablet Server 중지 실행 무응답 현상 확인
                            # check_mode = control.set_maintenance_node(ambari_server, cluster_name, hostname, "ON")
                            # 4) Tablet Server 중지
                            # if check_mode == "ON":
                            control.stop_tserver(cluster_name, hostname)
                            # 5) Tablet Server 시작
                            control.start_tserver(cluster_name, hostname)
                            # 6) Bootstrapping 완료 확인
                            time.sleep(3*60)
                            while True:
                                bootstrap_cnt = extractor.extract_bootstrap_count()
                                if bootstrap_cnt == "0":
                                    print("Bootstrapping is completed")
                                    break
                                else:
                                    time.sleep(30)
                            # 7) Kudu exit_maintenance 수행
                            maintenance.exit_maintenance(uuid)
                            time.sleep(5)
                            # 8) Tablet Server States 확인
                            ts_state = extractor.extract_tserver_state(uuid)
                            # 9) Ambari Maintenance Mode: OFF
                            if ts_state == "NON_MAINTENANCE_MODE":
                                # check_mode = control.set_maintenance_node(ambari_server, cluster_name, hostname, "OFF")
                                print("%s restart is complete" % hostname)
                                time.sleep(60)
                            else:
                                print("Something is wrong...")
                                break
                        else:
                            raise Exception("Please check quiesce...")
                    else:
                        raise Exception("The enter_maintenance command was not performed normally.")
                else:
                    raise Exception("Please check this Tablet Server...(%s, %s, %s)" % (hostname, uuid, ts_state))
        else:
            raise Exception("There is already a Tablet Server with MAINTENANCE_MODE.")