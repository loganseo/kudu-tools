# coding=utf-8

import sys
from python.action.rolling_restart import KuduRollingRestart


def main():
    try:
        if len(sys.argv) != 2:
            raise ValueError("Usage: kudu_rolling_restater.py <Master Hostname:7051>")

        ambari_server = "ambari.dev.toss.bz"
        ambari_pwd = raw_input("ambari password: ")
        rollingRestart = KuduRollingRestart(sys.argv[1], ambari_server, ambari_pwd)
        rollingRestart.start()

    except KeyboardInterrupt:
        print("Interrupted")
        exit(0)


if __name__ == '__main__':
    main()
