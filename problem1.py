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

def problem1(log_file):
    df = pd.read_csv(log_file, header=None, names=('time', 'addr', 'ping'))
    df.sort_values('time', inplace=True)
    df['time'] = [dateutil.parser.parse(str(t)) for t in df.time]
    df['addr'] = [ipaddress.ip_interface(x) for x in df.addr]
    df['ping'] = [-1 if p == '-' else int(p) for p in df.ping]
    df['down'] = df.ping == -1

    # down 期間を集計
    for addr, logs in df.groupby('addr'):
        for start_time, end_time in get_down_period(logs, lambda x: x.down):
            yield Result(addr, start_time, end_time)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('log_file_path')
    args = parser.parse_args()
    for result in problem1(args.log_file_path):
        print('{:20}{:20}{:20}'.format(result.address.with_prefixlen, str(result.start or ''), str(result.end or '')))

