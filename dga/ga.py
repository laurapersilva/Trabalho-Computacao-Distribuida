"""Algoritmo genético para o Problema do Caixeiro Viajante (TSP).

Cada nó do sistema distribuído mantém uma "ilha" (subpopulação) que evolui
de forma independente; indivíduos migram entre ilhas via RPC (ver node.py).
"""
import random
import threading
from typing import List

import numpy as np


class TSP:
    """Instância determinística: todos os nós geram as mesmas cidades pela semente."""

    def __init__(self, n_cities: int, seed: int) -> None:
        rng = np.random.default_rng(seed)
        self.n = n_cities
        self.coords = rng.random((n_cities, 2)) * 1000.0
        diff = self.coords[:, None, :] - self.coords[None, :, :]
        self.dist = np.sqrt((diff ** 2).sum(axis=2))

    def length(self, route) -> float:
        r = np.asarray(route)
        return float(self.dist[r, np.roll(r, -1)].sum())

    def nearest_neighbour_length(self) -> float:
        """Heurística gulosa, usada apenas como referência de qualidade."""
        unvisited = set(range(1, self.n))
        route = [0]
        while unvisited:
            last = route[-1]
            nxt = min(unvisited, key=lambda c: self.dist[last, c])
            route.append(nxt)
            unvisited.remove(nxt)
        return self.length(route)


def order_crossover(p1: List[int], p2: List[int], rng: random.Random) -> List[int]:
    """Order Crossover (OX): copia um segmento de p1 e completa na ordem de p2."""
    n = len(p1)
    a, b = sorted(rng.sample(range(n), 2))
    child = [-1] * n
    child[a:b] = p1[a:b]
    segment = set(p1[a:b])
    fill = [g for g in (p2[b:] + p2[:b]) if g not in segment]
    slots = list(range(b, n)) + list(range(0, a))
    for i, g in zip(slots, fill):
        child[i] = g
    return child


def mutate(route: List[int], rng: random.Random) -> None:
    n = len(route)
    i, j = sorted(rng.sample(range(n), 2))
    if rng.random() < 0.7:          # inversão (equivale a um movimento 2-opt aleatório)
        route[i:j + 1] = reversed(route[i:j + 1])
    else:                           # troca de duas cidades
        route[i], route[j] = route[j], route[i]


class Island:
    def __init__(self, tsp: TSP, pop_size: int, seed: int, elite: int = 2,
                 tournament: int = 3, p_mut: float = 0.3) -> None:
        self.tsp = tsp
        self.rng = random.Random(seed)
        self.elite, self.tournament, self.p_mut = elite, tournament, p_mut
        n = tsp.n
        self.pop = [self.rng.sample(range(n), n) for _ in range(pop_size)]
        self.fit = [tsp.length(r) for r in self.pop]
        self.gen = 0
        self._inbox: List[List[int]] = []
        self._lock = threading.Lock()
        self.best_len = float("inf")
        self.best_route: List[int] = []
        self._sort()
        self._update_best()

    # -- helpers -----------------------------------------------------------
    def _sort(self) -> None:
        order = sorted(range(len(self.pop)), key=self.fit.__getitem__)
        self.pop = [self.pop[i] for i in order]
        self.fit = [self.fit[i] for i in order]

    def _update_best(self) -> None:
        if self.fit[0] < self.best_len:
            self.best_len = self.fit[0]
            self.best_route = list(self.pop[0])

    # -- migração (chamado por threads de RPC) -----------------------------
    def add_immigrants(self, routes: List[List[int]]) -> None:
        valid = [list(r) for r in routes if sorted(r) == list(range(self.tsp.n))]
        with self._lock:
            self._inbox.extend(valid)

    def take_migrants(self, k: int) -> List[List[int]]:
        return [list(r) for r in self.pop[:k]]

    # -- uma geração -------------------------------------------------------
    def step(self) -> None:
        with self._lock:
            immigrants, self._inbox = self._inbox, []
        if immigrants:                       # imigrantes substituem os piores
            for k, r in enumerate(immigrants[:len(self.pop) // 4]):
                self.pop[-1 - k] = r
                self.fit[-1 - k] = self.tsp.length(r)
            self._sort()

        P = len(self.pop)
        new_pop = self.pop[:self.elite]
        new_fit = self.fit[:self.elite]
        pick = lambda: min(self.rng.randrange(P) for _ in range(self.tournament))  # lista ordenada
        while len(new_pop) < P:
            child = order_crossover(self.pop[pick()], self.pop[pick()], self.rng)
            if self.rng.random() < self.p_mut:
                mutate(child, self.rng)
            new_pop.append(child)
            new_fit.append(self.tsp.length(child))
        self.pop, self.fit = new_pop, new_fit
        self._sort()
        self._update_best()
        self.gen += 1
