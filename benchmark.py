#!/usr/bin/env python3
"""Compara a qualidade da solução variando o nº de ilhas (população total fixa).

ATENÇÃO: para medir speedup real, rode em máquinas/núcleos distintos. Em uma
única CPU os processos disputam o mesmo núcleo e o tempo não diminui.
"""
import json
import statistics as st
import sys

from run_local import run_cluster

NODES = [1, 2, 4, 5]
SEEDS = [1, 2, 3]
rows = {}
for n in NODES:
    lens, walls = [], []
    for s in SEEDS:
        r = run_cluster(nodes=n, pop_total=240, gens=1500, seed=s, base_port=9300 + 10 * n,
                        out=f"runs/bench_n{n}_s{s}")
        lens.append(r["global_best_len"])
        walls.append(r["wall_time"])
        print(f"nodes={n} seed={s} best={r['global_best_len']:.1f} wall={r['wall_time']:.1f}s", flush=True)
    rows[n] = {"mean_len": st.mean(lens), "std_len": st.pstdev(lens), "best_len": min(lens),
               "mean_wall": st.mean(walls), "lens": lens}
json.dump(rows, open("results_benchmark.json", "w"), indent=2)
print("\nnós | tour médio | desvio | melhor | tempo médio (s)")
for n, r in rows.items():
    print(f"{n:3d} | {r['mean_len']:9.1f} | {r['std_len']:6.1f} | {r['best_len']:8.1f} | {r['mean_wall']:.1f}")
