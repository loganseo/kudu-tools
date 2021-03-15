# coding=utf-8

import sys

import kudu_rebalancer

reload(sys)
sys.setdefaultencoding('utf-8')


def main():
    try:
        if len(sys.argv) != 6:
            raise ValueError("Usage: kudu_rebalance_all.py <Master Address>, <Table name>, <Range Partition("
                             "ex: 20190801)>, <max_move_replicas>, <pre_execution (true, false)>")

        r = kudu_rebalancer(sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4]), sys.argv[5])
        r.rebalace_job()

    except KeyboardInterrupt:
        print("Interrupted")
        exit(0)


if __name__ == '__main__':
    main()
