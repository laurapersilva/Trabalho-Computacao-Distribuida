#!/usr/bin/env python3
"""Sobe N nós em localhost, opcionalmente injeta falhas, e imprime o resultado.

Exemplos:
  python run_local.py --nodes 4
  python run_local.py --nodes 4 --kill leader@8        # mata o coordenador após 8 s
  python run_local.py --nodes 4 --kill 2@6             # mata o nó 2 após 6 s
"""
import argparse
import json
import os
import shutil
import subprocess
import sys
import time


def run_cluster(nodes=4, pop_total=240, gens=1500, cities=80, seed=1, base_port=9100,
                kill=None, out="runs/last", extra=None, quiet=True, timeout=600):
    shutil.rmtree(out, ignore_errors=True)
    os.makedirs(out)
    result_path = os.path.join(out, "result.json")
    peers = ",".join(f"{i}=127.0.0.1:{base_port + i}" for i in range(1, nodes + 1))
    procs = {}
    t0 = time.time()
    for i in range(1, nodes + 1):
        cmd = [sys.executable, "-m", "dga.node", "--id", str(i), "--peers", peers,
               "--pop", str(pop_total // nodes), "--gens", str(gens), "--cities", str(cities),
               "--seed", str(seed), "--result", result_path, "--log-dir", out] + (extra or [])
        procs[i] = subprocess.Popen(cmd, stdout=subprocess.DEVNULL if quiet else None,
                                    stderr=subprocess.STDOUT if not quiet else subprocess.DEVNULL)
    killed = False
    kill_spec = None
    if kill:
        who, at = kill.split("@")
        kill_spec = (nodes if who == "leader" else int(who), float(at))
    try:
        while time.time() - t0 < timeout:
            if kill_spec and not killed and time.time() - t0 >= kill_spec[1]:
                print(f"  >> matando nó {kill_spec[0]} em t={time.time() - t0:.1f}s")
                procs[kill_spec[0]].kill()
                killed = True
            if os.path.exists(result_path) and all(p.poll() is not None for p in procs.values()):
                break
            time.sleep(0.2)
    finally:
        for p in procs.values():
            if p.poll() is None:
                p.terminate()
    wall = time.time() - t0
    if not os.path.exists(result_path):
        return None
    with open(result_path) as f:
        res = json.load(f)
    res["wall_time"] = wall
    return res


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--nodes", type=int, default=4)
    ap.add_argument("--pop-total", type=int, default=240)
    ap.add_argument("--gens", type=int, default=1500)
    ap.add_argument("--cities", type=int, default=80)
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--base-port", type=int, default=9100)
    ap.add_argument("--kill", default=None, help="'leader@seg' ou 'ID@seg'")
    ap.add_argument("--out", default="runs/last")
    a = ap.parse_args()
    res = run_cluster(a.nodes, a.pop_total, a.gens, a.cities, a.seed, a.base_port, a.kill, a.out,
                      quiet=False)
    if res is None:
        sys.exit("execução sem resultado")
    print(f"\nNós vivos: {res['alive_nodes']} | líder final: N{res['leader']}")
    print(f"Melhor tour: {res['global_best_len']:.1f} (vizinho-mais-próximo: {res['nearest_neighbour_len']:.1f})")
    print(f"Tempo de parede total: {res['wall_time']:.1f}s | relógio de Lamport final: {res['lamport_final']}")


if __name__ == "__main__":
    main()
