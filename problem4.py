import numpy as np
import pandas as pd
import argparse
from typing import NamedTuple
from datetime import datetime
import dateutil.parser
import ipaddress
from collections import Counter


# 条件が True になってから False になるまでの期間
def get_down_period(df, condition):
    start_time = None
    for before, after in zip(df.itertuples(), df.iloc[1:].itertuples()):
        if not condition(before) and condition(after):
            start_time = after.time
        if condition(before) and not condition(after):
            yield (start_time, after.time)
            start_time = None

    if start_time is not None:
        yield (start_time, None)


class Result(NamedTuple):
    address: str
    start: datetime
    end: datetime


def problem4(log_file, n: int):
    df = pd.read_csv(log_file, header=None, names=('time', 'addr', 'ping'))
    df.sort_values('time', inplace=True)
    df['time'] = [dateutil.parser.parse(str(t)) for t in df.time]
    df['addr'] = [ipaddress.ip_interface(x) for x in df.addr]
    df['net'] = [ip.network for ip in df.addr]
    df['ping'] = [-1 if p == '-' else int(p) for p in df.ping]
    df['loss'] = df.ping == -1
    df['loss_count'] = 0

    # 連続するパケットロスの数 (loss_count) を数える
    for addr, logs in df.groupby('addr'):
        for before, after in zip(logs.itertuples(), logs.iloc[1:].itertuples()):
            if after.loss:
                df.loc[after.Index, 'loss_count'] = df.loc[before.Index, 'loss_count'] + 1

    df['down'] = df.loss_count >= n
    df['down_net'] = False

    # サブネットごとのホストの数を数える
    counter = Counter([addr.network for addr in set(df.addr)])

    # サブネット内のダウンしたホストの数を集計
    for net, logs in df.groupby('net'):
        host_count = counter[net]
        down_hosts = set()
        for log in logs.itertuples():
            if log.down:
                down_hosts.add(log.addr)
            elif log.addr in down_hosts:
                down_hosts.remove(log.addr)
            df.loc[log.Index, 'down_net'] = len(down_hosts) == host_count

    # サブネットのダウン期間を集計
    for net, logs in df.groupby('net'):
        for start_time, end_time in get_down_period(logs, lambda x: x.down_net):
            yield Result(net, start_time, end_time)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('log_file_path')
    parser.add_argument('n')
    args = parser.parse_args()
    for result in problem4(args.log_file_path, int(args.n)):
        print('{:20}{:20}{:20}'.format(result.address.with_prefixlen, str(result.start or ''), str(result.end or '')))

