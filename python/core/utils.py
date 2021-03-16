# coding=utf-8
import sys
import time


class Utils(object):
    def __int__(self, masters, table_name):
        self._masters = masters
        self._table_name = table_name

    # 소요시간 계산
    def calc_elapse_time(self, start_time):
        total_elapse_time = time.time() - start_time
        day = total_elapse_time // (24 * 3600)
        total_elapse_time = total_elapse_time % (24 * 3600)
        hour = total_elapse_time // 3600
        total_elapse_time %= 3600
        minutes = total_elapse_time // 60
        total_elapse_time %= 60
        seconds = total_elapse_time
        total_elapse_time = str(int(day)) + "일 " + str(int(hour)) + "시간 " \
                            + str(int(minutes)) + "분 " \
                            + str(int(seconds)) + "초"
        return total_elapse_time

    # Progress Bar
    def print_progress(self, iteration, total, prefix='', suffix='', decimals=1, bar_length=100):
        format_string = "{0:." + str(decimals) + "f}"
        percent = format_string.format(100 * (iteration / float(total)))
        filled_length = int(round(bar_length * iteration / float(total)))
        bar = '#' * filled_length + '-' * (bar_length - filled_length)
        sys.stdout.write('\r%s |%s| %s%s %s' % (prefix, bar, percent, '%', suffix))
        if iteration == total:
            sys.stdout.write('\n')
        sys.stdout.flush()
