# coding=utf-8
import subprocess


class Maintenance(object):
    def __init__(self, masters):
        self._masters = masters

    def enter_maintenance(self, uuid):
        cmd = "export LD_LIBRARY_PATH=/lib64:/usr/hdp/current/kudu/lib64:$LD_LIBRARY_PATH; " \
              "/usr/hdp/current/kudu/bin/kudu tserver state enter_maintenance " + self._masters + " " + uuid
        # print(cmd)
        subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True).communicate()

    def exit_maintenance(self, uuid):
        cmd = "export LD_LIBRARY_PATH=/lib64:/usr/hdp/current/kudu/lib64:$LD_LIBRARY_PATH; " \
              "/usr/hdp/current/kudu/bin/kudu tserver state exit_maintenance " + self._masters + " " + uuid
        # print(cmd)
        subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True).communicate()

    def enter_quiesce(self, hostname):
        cmd = "export LD_LIBRARY_PATH=/lib64:/usr/hdp/current/kudu/lib64:$LD_LIBRARY_PATH; " \
              "/usr/hdp/current/kudu/bin/kudu tserver quiesce start " + hostname
        # print(cmd)
        subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True).communicate()
