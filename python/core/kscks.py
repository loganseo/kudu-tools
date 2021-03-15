# coding=utf-8
import subprocess


class Kscks(object):
    def __int__(self, masters, table_name, tablet_id):
        self._masters = masters
        self._table_name = table_name
        self._tablet_id = tablet_id

    def ksck_tablets(self, tlist):
        moved_tlist_str = ','.join(map(str, tlist))
        cmd_ksck = "kudu cluster ksck " + self._masters \
                   + " -ksck_format=json_pretty -tables=" + self._table_name \
                   + " -tablets=" + moved_tlist_str
        temp_output = subprocess.Popen(cmd_ksck, stdout=subprocess.PIPE, shell=True).stdout
        output_ksck = temp_output.read().strip()
        temp_output.close()
        output_ksck = json.loads(output_ksck.decode("utf-8", "ignore"))
        return output_ksck

    def ksck_single_tablet(self):
        cmd_ksck = "kudu cluster ksck " + self._masters \
                   + " -ksck_format=json_pretty -tables=" + self._table_name \
                   + " -tablets=" + self._tablet_id
        temp_output = subprocess.Popen(cmd_ksck, stdout=subprocess.PIPE, shell=True).stdout
        output_ksck = temp_output.read().strip()
        temp_output.close()
        output_ksck = json.loads(output_ksck.decode("utf-8", "ignore"))
        return output_ksck