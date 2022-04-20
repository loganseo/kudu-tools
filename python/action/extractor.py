# coding=utf-8
import json
import subprocess


class Extractor(object):
    def __init__(self, masters):
        self._masters = masters

    # Tablet Server List 추출
    def extract_tserver(self):
        cmd_extrct = "export LD_LIBRARY_PATH=/lib64:/usr/hdp/current/kudu/lib64:$LD_LIBRARY_PATH; /usr/hdp/current/kudu/bin/kudu " \
                     "cluster ksck " + self._masters + " -ksck_format=json_pretty -sections=TSERVER_SUMMARIES "
        temp_output = subprocess.Popen(cmd_extrct, stdout=subprocess.PIPE, shell=True).stdout
        output_tserver = temp_output.read().strip()
        temp_output.close()
        output_tserver = json.loads(output_tserver.decode("utf-8", "ignore"))
        return output_tserver

    def extract_tserver_state(self, uuid):
        cmd_extrct = "export LD_LIBRARY_PATH=/lib64:/usr/hdp/current/kudu/lib64:$LD_LIBRARY_PATH; /usr/hdp/current/kudu/bin/kudu " \
                     "cluster ksck " + self._masters + "| grep " + uuid
        temp_output = subprocess.Popen(cmd_extrct, stdout=subprocess.PIPE, shell=True).stdout
        output_tserver_state = temp_output.read().strip()
        if output_tserver_state.find("MAINTENANCE_MODE") > 0:
            output_tserver_state = output_tserver_state.split("\n")
            output_tserver_state = output_tserver_state[0]
            ts_state_list = output_tserver_state.split("|")
            ts_state = ts_state_list[1]
            ts_state = ts_state.strip()
            temp_output.close()
        else:
            ts_state = "NON_MAINTENANCE_MODE"
        return ts_state

    def extract_tablet_leaders(self, uuid):
        cmd_extrct = "export LD_LIBRARY_PATH=/lib64:/usr/hdp/current/kudu/lib64:$LD_LIBRARY_PATH; /usr/hdp/current/kudu/bin/kudu " \
                     "cluster ksck " + self._masters + "| grep " + uuid
        temp_output = subprocess.Popen(cmd_extrct, stdout=subprocess.PIPE, shell=True).stdout
        output_tserver_state = temp_output.read().strip()
        if output_tserver_state.find("MAINTENANCE_MODE") > 0:
            output_tserver_state = output_tserver_state.split("\n")
            output_tserver_state = output_tserver_state[1]
            ts_state_list = output_tserver_state.split("|")
            t_leaders = ts_state_list[5]
            t_leaders = t_leaders.strip()
            temp_output.close()
            if not t_leaders.isdigit():
                raise Exception("t_leaders is not digit")
            return t_leaders

    def extract_bootstrap_count(self):
        cmd_extrct = 'export LD_LIBRARY_PATH=/lib64:/usr/hdp/current/kudu/lib64:$LD_LIBRARY_PATH; /usr/hdp/current/kudu/bin/kudu ' \
                     'cluster ksck ' + self._masters + '| grep -E "Bootstrap|INITIALIZED" | wc -l'
        temp_output = subprocess.Popen(cmd_extrct, stdout=subprocess.PIPE, shell=True).stdout
        output_bootstrap_count = temp_output.read().strip()
        print(output_bootstrap_count)
        return output_bootstrap_count
