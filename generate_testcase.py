import pandas as pd

df = pd.read_csv('log.csv')
addrs = set(df.keys()) - {'t'}
for addr in addrs:
        df[addr] = [{'d': '-', 'f': '50', 'v': 100}.get(x, 10) for x in df[addr]]
df['t'] = [str(20201010203000 + t) for t in df.t]

dfs = []
for addr in addrs:
    n = len(df.t)
    d = pd.DataFrame({'t': df.t, 'addr': [addr] * n, 'ping': df[addr]})
    dfs.append(d)
res = pd.concat(dfs).sort_values('t')
res.to_csv('test_case.txt', header=None, index=None)

