import pandas as pd
import numpy as np


def gen_target(HOSTS, UZ):
    if (np.random.randint(0, 2) == 0):
        return f'H{np.random.randint(0, HOSTS) + 1}'
    else:
        return f'K{np.random.randint(1, UZ)}'


table = pd.DataFrame(columns=['out_host', 'key', 'target_host', 'time'])
HOSTS, UZ, PreRecvCount, TimeMAX = map(int, input('Ввод:\nХосты  УЗ  Кол-воДействий  МаксВремя\n').split())
ans = []
for _ in range(PreRecvCount):
    row = [f'H{np.random.randint(0, HOSTS) + 1}', f'K{np.random.randint(0, UZ)}',
           gen_target(HOSTS, UZ), np.random.randint(0, TimeMAX) + 1]
    while (row[0] == row[2]):
        row[2] = gen_target(HOSTS, UZ)
    table.loc[len(table.index)] = row

print(table)

table.to_csv("generated_network.csv", index=False)
