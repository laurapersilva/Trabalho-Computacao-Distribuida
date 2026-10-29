# AG-TSP Distribuído — Algoritmo Genético em Modelo de Ilhas

Trabalho de Sistemas Distribuídos — **Tema 1: Algoritmo Genético Distribuído**.

O sistema resolve o **Problema do Caixeiro Viajante (TSP)** com um algoritmo genético
(AG) executado de forma distribuída em vários nós (processos/máquinas). Cada nó mantém uma
*ilha* (subpopulação) que evolui de forma independente; periodicamente os melhores
indivíduos **migram** para o vizinho em anel via RPC, simulando a evolução distribuída.

## Requisitos da 1ª entrega implementados

| Requisito (da lista do enunciado)   | Implementação                                                      | Arquivo        |
|-------------------------------------|--------------------------------------------------------------------|----------------|
| **RPC**                             | XML-RPC (biblioteca padrão do Python), 6 métodos, timeouts         | `dga/rpc.py`, `dga/node.py` |
| **Relógios lógicos**                | Relógio de **Lamport** implementado à mão, em todas as mensagens   | `dga/lamport.py` |
| **Eleição de líder**                | Algoritmo **Bully** implementado à mão, com reeleição por falha    | `dga/node.py` (`start_election`, `become_leader`) |

> Observação: o enunciado sugere gRPC, mas permite qualquer RPC. Usamos XML-RPC da stdlib
> para não ter dependências externas e facilitar rodar em qualquer máquina do laboratório.
> Trocar para gRPC exige apenas reescrever `Node.call` e `Node.register`.

## Arquitetura

```
        migrate (anel, a cada 25 gerações)
   N1 ───────────▶ N2 ───────────▶ N3 ───────────▶ N4 (líder, maior ID)
   ▲                                                │  ▲
   └────────────────────────────────────────────────┘  │
   N1..N3 ──── report (melhor local) ──────────────────┘
   N4 ──────── ping (heartbeat) ──────────▶ N1..N3
   Qualquer nó ── election / coordinator (Bully) ──▶ demais nós
```

* **Nós (todos iguais):** executam servidor XML-RPC + ilha do AG + thread monitora.
* **Líder (coordenador):** eleito pelo Bully (maior ID vivo). Faz *heartbeat*, agrega os
  `report` de cada ilha, detecta o fim da execução, grava `result.json` e manda `shutdown`.
* **Mensagens (todas levam `(remetente, timestamp Lamport)`; resposta devolve `{ts, result}`):**

| Método        | De → Para           | Conteúdo                                              |
|---------------|---------------------|-------------------------------------------------------|
| `ping`        | líder↔seguidores    | vazio; resposta traz quem o nó acredita ser o líder   |
| `election`    | nó → IDs maiores    | vazio; resposta `"OK"` = "há alguém maior vivo"       |
| `coordinator` | novo líder → todos  | anúncio do coordenador; `"REJECT"` se o receptor é maior |
| `migrate`     | nó → vizinho (anel) | lista dos *k* melhores tours (listas de cidades)      |
| `report`      | nó → líder          | `{gen, best_len, best_route, done, elapsed}`          |
| `shutdown`    | líder → todos       | encerra o cluster                                     |

### Tolerância a falhas de comunicação
* Todas as chamadas têm *timeout*; falhas **nunca** derrubam o nó (`Node.call` devolve `(False, None)`).
* 3 falhas consecutivas ⇒ o nó remoto é considerado morto (contagem alimentada por qualquer RPC,
  não só `ping`).
* **Migração:** se o vizinho do anel está morto, tenta o próximo do anel.
* **Líder morto:** seguidores detectam e iniciam nova eleição Bully; o novo líder reabre a agregação
  e os nós que já terminaram reenviam seu `report` final.
* O líder só finaliza quando todos os nós *vivos* reportaram `done`.

### Algoritmo genético
Permutação de cidades; seleção por torneio (k=3); *Order Crossover* (OX); mutação por inversão
(70 %) ou troca (30 %) com probabilidade 0,3; elitismo de 2; imigrantes substituem os piores.
A instância (cidades) é gerada deterministicamente por `--tsp-seed`, logo todos os nós
enxergam o mesmo problema sem transferir dados.

## Como executar

Requisitos: Python ≥ 3.9 e `numpy` (`pip install -r requirements.txt`).

### 1) Tudo em uma máquina (demonstração)
```bash
python3 run_local.py --nodes 4                    # 4 nós, população total 240
python3 run_local.py --nodes 4 --kill leader@4    # mata o líder após 4 s -> reeleição
python3 run_local.py --nodes 4 --kill 2@4         # mata o nó 2 após 4 s
python3 benchmark.py                              # compara 1, 2, 4 e 5 nós (3 sementes)
```
Os logs (com timestamps de Lamport) ficam em `runs/<nome>/node_<id>.log` e o resultado em
`runs/<nome>/result.json`.

### 2) Várias máquinas na rede local
Em **cada** máquina (ajuste `--id` e os IPs; a lista `--peers` é a mesma em todas e inclui o próprio nó):
```bash
python3 -m dga.node --id 1 --peers "1=192.168.0.11:9001,2=192.168.0.12:9001,3=192.168.0.13:9001" --pop 80
python3 -m dga.node --id 2 --peers "1=192.168.0.11:9001,2=192.168.0.12:9001,3=192.168.0.13:9001" --pop 80
python3 -m dga.node --id 3 --peers "1=192.168.0.11:9001,2=192.168.0.12:9001,3=192.168.0.13:9001" --pop 80
```
Libere a porta no firewall. Suba os nós com poucos segundos de diferença (há `--startup-delay`).
Para testar falhas de verdade, desligue um terminal ou o cabo de rede de uma máquina.

### Parâmetros principais (`python3 -m dga.node --help`)
`--pop` (subpopulação do nó), `--gens`, `--cities`, `--tsp-seed`, `--migration-interval`,
`--migrants`, `--report-interval`, `--rpc-timeout`, `--heartbeat`, `--election-timeout`,
`--result`, `--log-dir`.

## Estrutura
```
dga/lamport.py   relógio de Lamport
dga/rpc.py       servidor/cliente XML-RPC com timeout
dga/ga.py        TSP + AG (OX, mutação, ilha, migração)
dga/node.py      nó: RPCs, Bully, heartbeat, migração, agregação
run_local.py     sobe N nós locais e injeta falhas
benchmark.py     experimentos de qualidade
relatorio/       relatório (LaTeX + PDF)
```

## Resultados medidos (localhost, 80 cidades, 1500 gerações, 3 sementes)

Referência: heurística do vizinho mais próximo = **8406,7**. Menor é melhor.

| Configuração                              | Tour médio | Desvio |
|-------------------------------------------|-----------:|-------:|
| 1 nó, pop. 240 (população total)          | 9938,4     | 297,6  |
| 1 nó isolado, pop. 60 (controle)          | 7478,7     | 105,5  |
| 2 nós × 120                               | 7350,3     | 67,1   |
| 4 nós × 60                                | 7262,3     | 111,2  |
| 5 nós × 48                                | 7295,6     | 158,9  |

**Leitura honesta dos números:**
* O ganho de "1 nó (pop. 240)" para os demais é, em grande parte, efeito do *tamanho da
  subpopulação* (com geração fixa, populações menores convergem mais rápido). O controle
  "1 nó isolado, pop. 60" mostra isso.
* O efeito da **migração** isolado é modesto: 4 ilhas com migração (7262) vs. uma ilha isolada de mesmo
  tamanho (7479) ≈ 3 % melhor.
* **Speedup não foi medido**: o ambiente de teste tinha **1 núcleo de CPU**, então os processos competiam
  pelo mesmo núcleo. Rode `benchmark.py` em máquinas/núcleos distintos para medir tempo.
* Falhas injetadas (4 nós, 3000 gerações): matar o líder (N4) em ~4 s → reeleição de N3 em < 1 s e
  execução concluída (tour 7003,9); matar o nó 2 → vizinho N1 passa a migrar para N3 e a execução
  conclui (tour 7343,8). Um nó morto perde sua população local (não há persistência/checkpoint).

## Limitações conhecidas
* Associação estática (lista fixa de peers); nós que caem não são reintegrados com estado.
* Sem checkpoint: a população de um nó morto é perdida (apenas seus `report` já enviados valem).
* Sem criptografia/autenticação — uso didático em rede local.

## Divisão do trabalho (5 integrantes)

| Integrante | Responsabilidade principal |
|------------|----------------------------|
| Arthur Signorini | **Arquitetura e camada RPC:** protocolo de mensagens, `rpc.py`, `Node.call/register`, timeouts e tratamento de falhas de comunicação |
| Cauã Costa | **Eleição de líder (Bully):** `start_election`, `become_leader`, heartbeat, detecção de falha do coordenador |
| Laura Persilva | **Relógios lógicos (Lamport):** `lamport.py`, integração em todas as mensagens, formato de logs e verificação de causalidade |
| Otávio Monteiro | **Algoritmo genético:** TSP, OX, mutação, seleção, ilhas e política de migração em anel |
| Andriel Mark | **Experimentos e entrega:** `run_local.py`, `benchmark.py`, injeção de falhas, README e relatório |

