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


def problem3(log_file, m: int, t: float):
    df = pd.read_csv(log_file, header=None, names=('time', 'addr', 'ping'))
    df.sort_values('time', inplace=True)
    df['time'] = [dateutil.parser.parse(str(t)) for t in df.time]
    df['addr'] = [ipaddress.ip_interface(x) for x in df.addr]
    df['ping'] = [np.nan if p == '-' else int(p) for p in df.ping]
    df['loss'] = df.ping == -1
    df['ping_ma'] = 0

    # ping の移動平均を集計
    for addr, logs in df.groupby('addr'):
        df.loc[logs.index, 'ping_ma'] = logs.ping.rolling(m, min_periods=1).mean(skipna=True)

    # 高負荷の期間を集計
    df['high_ping'] = df['ping_ma'] >= t
    for addr, logs in df.groupby('addr'):
        for start_time, end_time in get_down_period(logs, lambda x: x.high_ping):
            yield Result(addr, start_time, end_time)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('log_file_path')
    parser.add_argument('m')
    parser.add_argument('t')
    args = parser.parse_args()
    for result in problem3(args.log_file_path, int(args.m), float(args.t)):
        print('{:20}{:20}{:20}'.format(result.address.with_prefixlen, str(result.start or ''), str(result.end or '')))

