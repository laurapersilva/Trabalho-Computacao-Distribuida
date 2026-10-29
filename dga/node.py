#!/usr/bin/env python3
"""Nó do Algoritmo Genético Distribuído.

Cada nó executa:
  * um servidor XML-RPC (ping, election, coordinator, migrate, report, shutdown);
  * uma thread de evolução (ilha do AG + migração em anel);
  * uma thread monitora (heartbeat, eleição Bully, agregação/finalização no líder).

Requisitos da 1ª entrega implementados: RPC, relógios lógicos (Lamport) e
eleição de líder (Bully).
"""
import argparse
import http.client
import json
import os
import socket
import sys
import threading
import time
import xmlrpc.client
from collections import defaultdict

from .ga import TSP, Island
from .lamport import LamportClock
from .rpc import QuietHandler, ThreadedXMLRPCServer, TimeoutTransport

MAX_FAILS = 3  # falhas consecutivas até considerar um nó "morto"


def parse_peers(spec: str):
    """'1=host:port,2=host:port' -> {1: (host, port), ...}"""
    peers = {}
    for item in spec.split(","):
        pid, addr = item.split("=")
        host, port = addr.rsplit(":", 1)
        peers[int(pid)] = (host, int(port))
    return peers


class Node:
    def __init__(self, a) -> None:
        self.a = a
        self.id = a.id
        self.peers = parse_peers(a.peers)
        self.others = sorted(p for p in self.peers if p != self.id)
        self.clock = LamportClock()
        self.t0 = time.time()
        self.stop = threading.Event()

        # --- AG ---
        self.tsp = TSP(a.cities, a.tsp_seed)
        self.island = Island(self.tsp, a.pop, seed=a.seed * 1000 + self.id)
        self.done = False
        self.elapsed = 0.0

        # --- liveness / eleição ---
        self.fails = defaultdict(int)
        self.last_fail = {}
        self.leader = None
        self._el_lock = threading.Lock()
        self._electing = False
        self.coord_event = threading.Event()

        # --- agregação (somente no líder) ---
        self.reports = {}
        self.reported_to = None
        self.global_best = float("inf")
        self._rep_lock = threading.Lock()
        self._finalized = False

        os.makedirs(a.log_dir, exist_ok=True)
        self._logf = open(os.path.join(a.log_dir, f"node_{self.id}.log"), "w", buffering=1)
        self._log_lock = threading.Lock()

    # ------------------------------------------------------------------ log
    def log(self, kind: str, msg: str) -> None:
        line = f"[{time.time() - self.t0:7.2f}s] N{self.id} L={self.clock.time:<5d} {kind:<9} {msg}"
        with self._log_lock:
            print(line, flush=True)
            self._logf.write(line + "\n")

    # ------------------------------------------------------------------ RPC cliente
    def call(self, pid: int, method: str, *args, timeout=None):
        """Chama `method` no nó `pid`. Retorna (ok, resultado). Nunca lança exceção de rede."""
        host, port = self.peers[pid]
        ts = self.clock.tick()                       # evento de envio
        try:
            proxy = xmlrpc.client.ServerProxy(
                f"http://{host}:{port}/RPC2",
                transport=TimeoutTransport(timeout or self.a.rpc_timeout),
                allow_none=True)
            reply = getattr(proxy, method)(self.id, ts, *args)
            self.clock.update(reply["ts"])           # evento de recebimento da resposta
            self.fails[pid] = 0
            if method != "ping":
                self.log("SEND", f"{method} -> N{pid} (ts={ts}, resp_ts={reply['ts']})")
            return True, reply["result"]
        except (OSError, xmlrpc.client.Error, http.client.HTTPException, socket.timeout):
            self.fails[pid] += 1
            self.last_fail[pid] = time.time()
            if method != "ping":
                self.log("FAIL", f"{method} -> N{pid} (falha #{self.fails[pid]})")
            return False, None

    def _suspect(self, pid: int) -> bool:
        return self.fails[pid] >= MAX_FAILS and time.time() - self.last_fail.get(pid, 0) < 5.0

    # ------------------------------------------------------------------ RPC servidor
    def rpc_ping(self, sender):
        return {"leader": self.leader if self.leader is not None else -1}

    def rpc_election(self, sender):
        if self.id > sender:
            threading.Thread(target=self.start_election, daemon=True).start()
        return "OK"

    def rpc_coordinator(self, sender):
        if sender < self.id:                 # um nó de ID menor não pode ser líder
            threading.Thread(target=self.start_election, daemon=True).start()
            return "REJECT"
        if self.leader != sender:
            self.log("LEADER", f"N{sender} é o novo coordenador")
        self.leader = sender
        self.coord_event.set()
        return "OK"

    def rpc_migrate(self, sender, individuals):
        self.island.add_immigrants(individuals)
        return len(individuals)

    def rpc_report(self, sender, info):
        if self.leader != self.id:
            return "NOT_LEADER"
        self._store_report(sender, info)
        return "OK"

    def rpc_shutdown(self, sender):
        self.log("SHUTDOWN", f"ordem de término recebida de N{sender}")
        self.stop.set()
        return "OK"

    def register(self, server) -> None:
        for name in ("ping", "election", "coordinator", "migrate", "report", "shutdown"):
            fn = getattr(self, "rpc_" + name)

            def wrapper(sender, ts, *args, _fn=fn, _name=name):
                self.clock.update(ts)                # recebimento: L = max(L, ts) + 1
                if _name != "ping":
                    self.log("RECV", f"{_name} <- N{sender} (ts={ts})")
                result = _fn(sender, *args)
                return {"ts": self.clock.tick(), "result": result}
            server.register_function(wrapper, name)

    # ------------------------------------------------------------------ eleição Bully
    def start_election(self) -> None:
        with self._el_lock:
            if self._electing:
                return
            self._electing = True
        try:
            while not self.stop.is_set():
                self.log("ELECTION", "iniciando eleição (Bully)")
                self.coord_event.clear()
                got_ok = False
                for pid in sorted(p for p in self.others if p > self.id):
                    ok, res = self.call(pid, "election")
                    if ok and res == "OK":
                        got_ok = True
                if not got_ok:
                    self.become_leader()
                    return
                if self.coord_event.wait(self.a.election_timeout):
                    return
                self.log("ELECTION", "nenhum coordenador anunciado; repetindo")
        finally:
            with self._el_lock:
                self._electing = False

    def become_leader(self) -> None:
        if self.leader != self.id:
            with self._rep_lock:
                self.reports = {}
                self.reported_to = None
        self.leader = self.id
        self.log("LEADER", f"N{self.id} venceu a eleição e se anuncia coordenador")
        for pid in self.others:
            self.call(pid, "coordinator")
        self.coord_event.set()

    # ------------------------------------------------------------------ agregação
    def _store_report(self, pid: int, info: dict) -> None:
        with self._rep_lock:
            self.reports[pid] = info
            if info["best_len"] < self.global_best - 1e-9:
                self.global_best = info["best_len"]
                self.log("GLOBAL", f"melhor global = {self.global_best:.1f} (N{pid}, ger {info['gen']})")

    def send_report(self, done: bool) -> bool:
        info = {"gen": self.island.gen, "best_len": self.island.best_len,
                "best_route": [int(x) for x in self.island.best_route],
                "done": done, "elapsed": self.elapsed}
        leader = self.leader
        if leader is None:
            return False
        if leader == self.id:
            self._store_report(self.id, info)
            return True
        ok, res = self.call(leader, "report", info)
        return ok and res == "OK"

    # ------------------------------------------------------------------ evolução
    def migrate(self) -> None:
        ids = sorted(self.peers)
        i = ids.index(self.id)
        ring = [ids[(i + k) % len(ids)] for k in range(1, len(ids))]
        migrants = self.island.take_migrants(self.a.migrants)
        for pid in ring:                       # tolerância a falhas: pula vizinhos mortos
            if self._suspect(pid):
                continue
            ok, _ = self.call(pid, "migrate", migrants)
            if ok:
                return
        self.log("MIGRATE", "nenhum vizinho alcançável; migração adiada")

    def evolve_loop(self) -> None:
        start = time.time()
        for gen in range(1, self.a.gens + 1):
            if self.stop.is_set():
                return
            self.island.step()
            if gen % self.a.migration_interval == 0:
                self.migrate()
            if gen % self.a.report_interval == 0:
                self.send_report(False)
            if self.a.delay:
                time.sleep(self.a.delay)
        self.elapsed = time.time() - start
        self.done = True
        self.log("DONE", f"{self.a.gens} gerações em {self.elapsed:.2f}s, melhor local = {self.island.best_len:.1f}")

    # ------------------------------------------------------------------ monitor
    def monitor_loop(self) -> None:
        time.sleep(self.a.startup_delay)       # dá tempo para os demais nós subirem
        while not self.stop.is_set():
            if self.leader is None:
                self.start_election()
            elif self.leader == self.id:
                for pid in self.others:        # heartbeat do líder para todos
                    self.call(pid, "ping")
            else:
                ok, _ = self.call(self.leader, "ping")
                if not ok and self.fails[self.leader] >= MAX_FAILS:
                    self.log("LEADER", f"coordenador N{self.leader} não responde; reelegendo")
                    self.leader = None
                    threading.Thread(target=self.start_election, daemon=True).start()

            if self.done and self.leader is not None and self.reported_to != self.leader:
                if self.send_report(True):
                    self.reported_to = self.leader

            if self.leader == self.id and self.done:
                self._maybe_finalize()
            self.stop.wait(self.a.heartbeat)

    def _maybe_finalize(self) -> None:
        if self._finalized:
            return
        expected = [self.id] + [p for p in self.others if self.fails[p] < MAX_FAILS]
        with self._rep_lock:
            if not all(self.reports.get(p, {}).get("done") for p in expected):
                return
            reports = dict(self.reports)
        self._finalized = True
        best_pid = min(reports, key=lambda p: reports[p]["best_len"])
        result = {
            "leader": self.id,
            "global_best_len": reports[best_pid]["best_len"],
            "global_best_route": reports[best_pid]["best_route"],
            "best_from_node": best_pid,
            "nearest_neighbour_len": self.tsp.nearest_neighbour_length(),
            "alive_nodes": expected,
            "nodes": {str(p): {"best_len": r["best_len"], "gen": r["gen"], "elapsed": r["elapsed"]}
                      for p, r in reports.items()},
            "lamport_final": self.clock.time,
            "config": {k: v for k, v in vars(self.a).items()},
        }
        os.makedirs(os.path.dirname(os.path.abspath(self.a.result)), exist_ok=True)
        with open(self.a.result, "w") as f:
            json.dump(result, f, indent=2)
        self.log("FINAL", f"melhor tour = {result['global_best_len']:.1f} "
                          f"(vizinho-mais-próximo = {result['nearest_neighbour_len']:.1f}); "
                          f"resultado em {self.a.result}")
        for pid in self.others:
            self.call(pid, "shutdown", timeout=0.5)
        self.stop.set()

    # ------------------------------------------------------------------ main
    def run(self) -> None:
        host, port = self.peers[self.id]
        server = ThreadedXMLRPCServer(("0.0.0.0", port), requestHandler=QuietHandler,
                                      logRequests=False, allow_none=True)
        self.register(server)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.log("START", f"escutando em :{port}; peers={sorted(self.peers)}; pop={self.a.pop}")
        threading.Thread(target=self.evolve_loop, daemon=True).start()
        threading.Thread(target=self.monitor_loop, daemon=True).start()
        self.stop.wait()
        time.sleep(0.4)                        # deixa a resposta do shutdown sair
        server.shutdown()
        self.log("EXIT", "encerrado")


def main(argv=None) -> None:
    p = argparse.ArgumentParser(description="Nó do AG distribuído (TSP)")
    p.add_argument("--id", type=int, required=True)
    p.add_argument("--peers", required=True, help="'1=host:porta,2=host:porta,...' (inclui este nó)")
    p.add_argument("--cities", type=int, default=80)
    p.add_argument("--tsp-seed", type=int, default=42)
    p.add_argument("--seed", type=int, default=1)
    p.add_argument("--pop", type=int, default=120, help="tamanho da subpopulação deste nó")
    p.add_argument("--gens", type=int, default=1500)
    p.add_argument("--migration-interval", type=int, default=25)
    p.add_argument("--migrants", type=int, default=4)
    p.add_argument("--report-interval", type=int, default=50)
    p.add_argument("--rpc-timeout", type=float, default=1.0)
    p.add_argument("--heartbeat", type=float, default=1.0)
    p.add_argument("--election-timeout", type=float, default=3.0)
    p.add_argument("--startup-delay", type=float, default=1.5)
    p.add_argument("--delay", type=float, default=0.0, help="pausa (s) por geração; útil para demos")
    p.add_argument("--result", default="results/result.json")
    p.add_argument("--log-dir", default="logs")
    Node(p.parse_args(argv)).run()


if __name__ == "__main__":
    main()
