# coding=utf-8
import json
import subprocess
import time


class ControlService(object):
    def __init__(self, masters, ambari_server, ambari_pwd):
        self._masters = masters
        self._ambari_server = ambari_server
        self._ambari_pwd = ambari_pwd

    def get_cluster_name(self):
        cmd = 'export LD_LIBRARY_PATH=/lib64:/usr/hdp/current/kudu/lib64:$LD_LIBRARY_PATH; curl -u admin:' + self._ambari_pwd + ' -sS -G "http://' + self._ambari_server + '/api/v1/clusters"'
        # print(cmd)
        temp_get_cluster_name = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True).stdout
        json_get_cluster_name = temp_get_cluster_name.read().strip()
        temp_get_cluster_name.close()
        json_get_cluster_name = json.loads(json_get_cluster_name.decode("utf-8", "ignore"))
        cluster_name = json_get_cluster_name["items"][0]["Clusters"]["cluster_name"]
        return cluster_name

    def get_tserver_info(self, cluster_name, hostname):
        cmd = 'export LD_LIBRARY_PATH=/lib64:/usr/hdp/current/kudu/lib64:$LD_LIBRARY_PATH; curl -u admin:' + self._ambari_pwd + ' -sS -G "http://' + self._ambari_server + '/api/v1/clusters/' + cluster_name + '/hosts/' + hostname + '/host_components/KUDU_TSERVER"'
        # print(cmd)
        temp_get_tserver_info = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True).stdout
        json_get_tserver_info = temp_get_tserver_info.read().strip()
        temp_get_tserver_info.close()
        json_get_tserver_info = json.loads(json_get_tserver_info.decode("utf-8", "ignore"))
        return json_get_tserver_info

    def set_maintenance_node(self, cluster_name, hostname, set_mode):
        json_get_tserver_info = self.get_tserver_info(cluster_name, hostname)
        check_mode = json_get_tserver_info["HostRoles"]["maintenance_state"]
        print("1. check_mode = %s" % check_mode)
        if check_mode == set_mode:
            raise Exception("Already Set %s" % set_mode)
        else:
            cmd = 'export LD_LIBRARY_PATH=/lib64:/usr/hdp/current/kudu/lib64:$LD_LIBRARY_PATH; curl -u admin:' + self._ambari_pwd + ' -sS -H "X-Requested-By: ambari" -X PUT -d \'' \
                  '{"RequestInfo": {"context": "turning on maintenance mode for Kudu TServer"},' \
                  '"Body":{"HostRoles":{"maintenance_state":"' + set_mode + '"}}}\' ' \
                  '"http://' + self._ambari_server + '/api/v1/clusters/' + cluster_name + '/hosts/' + hostname + '/host_components/KUDU_TSERVER"'
            # print(cmd)
            subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True).communicate()
            time.sleep(10)
            json_get_tserver_info = self.get_tserver_info(cluster_name, hostname)
            check_mode = json_get_tserver_info["HostRoles"]["maintenance_state"]
            print("2. check_mode = %s" % check_mode)
            return check_mode

    def stop_tserver(self, cluster_name, hostname):
        cmd = 'export LD_LIBRARY_PATH=/lib64:/usr/hdp/current/kudu/lib64:$LD_LIBRARY_PATH; curl -u admin:' + self._ambari_pwd + ' -sS -H "X-Requested-By: ambari" -X PUT -d \'' \
              '{"RequestInfo": {"context": "stopping Kudu TServer"},' \
              '"Body":{"HostRoles":{"state":"INSTALLED"}}}\' ' \
              '"http://' + self._ambari_server + '/api/v1/clusters/' + cluster_name + '/hosts/' + hostname + '/host_components/KUDU_TSERVER"'
        # print(cmd)
        subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True).communicate()
        i = 0
        state = ""
        while i < 3:
            time.sleep(10)
            json_get_tserver_info = self.get_tserver_info(cluster_name, hostname)
            state = json_get_tserver_info["HostRoles"]["state"]
            if state == "INSTALLED":
                print("Tablet Server of %s is stopped" % hostname)
                break
            i += 1
        if state != "INSTALLED":
            raise Exception("Tablet Server of %s is not stopped" % hostname)

    def start_tserver(self, cluster_name, hostname):
        cmd = 'export LD_LIBRARY_PATH=/lib64:/usr/hdp/current/kudu/lib64:$LD_LIBRARY_PATH; curl -u admin:' + self._ambari_pwd + ' -sS -H "X-Requested-By: ambari" -X PUT -d \'' \
              '{"RequestInfo": {"context": "starting Kudu TServer"},' \
              '"Body":{"HostRoles":{"state":"STARTED"}}}\' ' \
              '"http://' + self._ambari_server + '/api/v1/clusters/' + cluster_name + '/hosts/' + hostname + '/host_components/KUDU_TSERVER"'
        # print(cmd)
        subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True).communicate()
        i = 0
        state = ""
        while i < 3:
            time.sleep(10)
            json_get_tserver_info = self.get_tserver_info(cluster_name, hostname)
            state = json_get_tserver_info["HostRoles"]["state"]
            if state == "STARTED":
                print("Tablet Server of %s is started" % hostname)
                break
            i += 1
        if state != "STARTED":
            raise Exception("Tablet Server of %s is not started" % hostname)
