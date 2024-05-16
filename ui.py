import streamlit as st
import numpy as np
import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt

def gen_target(HOSTS, UZ):
    if np.random.randint(0, 2) == 0:
        return f'H{np.random.randint(0, HOSTS)+1}'
    else:
        return f'K{np.random.randint(1, UZ)}'

def generate_table(HOSTS, UZ, PreRecvCount, TimeMAX):
    table = pd.DataFrame(columns=['out_host', 'key', 'target_host', 'time'])
    ans = []
    for _ in range(PreRecvCount):
        row = [f'H{np.random.randint(0, HOSTS)+1}', f'K{np.random.randint(0, UZ)}',
               gen_target(HOSTS, UZ), np.random.randint(0, TimeMAX)+1]
        while row[0] == row[2]:
            row[2] = gen_target(HOSTS, UZ)
        table.loc[len(table.index)] = row

    table.sort_values(by='out_host')
    table.to_csv("generated_network.csv", index=False)
    return table

def create_graph(df):
    df.loc[:, 'time+key'] = df['time'].astype(str).map(lambda x: 'T: ' + x + ', ') + df['key']
    G = nx.from_pandas_edgelist(df, 'out_host', 'target_host', edge_attr=['time+key'], create_using=nx.DiGraph())

    while True:
        try:
            to_del = nx.find_cycle(G)[0]
            G.remove_edge(to_del[0], to_del[1])
            df = df.drop(df[(df.out_host == to_del[0]) & (df.target_host == to_del[1])].index)
        except nx.NetworkXNoCycle:
            break

    pos = nx.spring_layout(G, k=1, iterations=2)
    nx.draw_networkx_nodes(G, pos, node_size=700, node_color='lightblue')
    nx.draw_networkx_edges(G, pos, edgelist=G.edges(), edge_color='gray', arrowstyle='-|>', arrowsize=25)
    nx.draw_networkx_labels(G, pos, font_size=10, font_family='sans-serif')

    edge_labels = nx.get_edge_attributes(G, 'time+key')
    nx.draw_networkx_edge_labels(G, pos, edge_labels=edge_labels, font_size=8, font_family='sans-serif')

    plt.axis('off')
    return plt

st.title("Генерация графа")
HOSTS = st.number_input("Введите количество хостов:", min_value=1, value=10)
UZ = st.number_input("Введите количество УЗ:", min_value=1, value=5)
PreRecvCount = st.number_input("Введите количество действий:", min_value=1, value=20)
TimeMAX = st.number_input("Введите максимальное время:", min_value=1, value=10)

if st.button("Сгенерировать граф"):
    table = generate_table(HOSTS, UZ, PreRecvCount, TimeMAX)
    df = pd.read_csv('generated_network.csv')
    plt = create_graph(df)
    st.subheader("Результат")
    st.pyplot(plt)