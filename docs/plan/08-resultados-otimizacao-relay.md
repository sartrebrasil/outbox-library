# Resultados da otimização do relay (2026-11-24)

Consolida as medições feitas com o `outbox-loadtest` (`06-teste-de-carga.md`) e o detalhe da query de candidatos (`07-plano-query-candidatos.md`). Ambiente: macOS, 14 cores, Postgres 14 / RabbitMQ 3.13 / LocalStack em containers (Docker Desktop) na mesma máquina do gerador de carga.

## 1. Onde há ganho (resumo)

| # | Mudança | Ganho medido | Onde | Observação |
|---|---|---|---|---|
| 1 | Pool de conexões no gerador de carga (HikariCP) | **~4× de throughput** (`inmemory-w4` 1.066 → 4.503; `rabbit-w4` 1.163 → 4.193 msg/s) | só no **teste**, não na biblioteca | Antes, cada transação abria um socket e esgotava as portas efêmeras; o teste media a si mesmo |
| 2 | `UPDATE` de PUBLISHED em lote por fluxo | **+25%** `rabbit-w4`, **+18%** `rabbit-w4-big`, +33% a +71% `inmemory` | **biblioteca** (`OutboxRelay`) | Maior ganho real da biblioteca. `rabbit-w8-cl200`: +12%, abaixo do corte, com ruído |
| 3 | `relayWorkers` 4 → 8 | **+36%** (12 filas: 5.850 → 7.936 msg/s) | configuração | Alavanca mais forte. 8 → 16 não rende mais: o gerador passa a ser o limite |
| 4 | Mais filas/destinos | +50% (1 → 3 filas), +12% (3 → 12 filas); com 8 workers, 12 filas rendem +24% sobre 3 | configuração do uso | Limite do RabbitMQ por fila: ~3,5 mil msg/s |
| 5 | Query de candidatos por `seq` + dedupe em Java | Query isolada: **12 ms → 0,08 ms** com 50k pendentes. No load test: **+23% só no `inmemory`** | **biblioteca** (`FluxoClaimer`) | Não rende com Rabbit/SQS nos backlogs do teste: o publish domina o ciclo. Protege contra degradação com backlog grande |
| 6 | Índice `idx_outbox_fluxo` | Query do fluxo: **0,72 → 0,03 ms** (10k pend.), **3,2 → 0,06 ms** (50k) | **biblioteca** (migração) | **Sem ganho de throughput** nos backlogs do teste; protege backlog grande. Sem custo de escrita mensurável (~1%) |

**Sem ganho (ou piora):** embaralhar candidatos (contenção −95%, throughput neutro), reduzir o polling (10 ms), aumentar `candidateLimit` (200 e 1000 pioram), 16 workers (menos que 8: gerador limita).

> Os percentuais **não somam**: cada A/B foi feito em sessão diferente e o nível absoluto da máquina varia entre sessões (ex.: `inmemory-w4` "antes" foi 4.265, 6.216 e 7.700 msg/s em momentos distintos). Compare só antes/depois dentro da mesma tabela.

## 2. Linha do tempo e comparações

### 2.1 Diagnóstico inicial (antes de qualquer correção)

Rodada completa travou no `rabbit-w4-c4` com `BindException: Can't assign requested address`: 16.253 sockets em `TIME_WAIT` e faixa de portas efêmeras de ~16 mil. Causa: `SimpleDriverDataSource` e `DriverManager` abriam uma conexão por transação, ciclo do relay e `count()` do monitor. O gerador era o gargalo (~1.000 msg/s), não a biblioteca.

### 2.2 Depois do pool (HikariCP, 32 conexões)

| profile | antes | depois |
|---|---|---|
| enqueue p50 | 6–7 ms | ~0,65 ms |
| inmemory-w4 | 1.066 msg/s | 4.503 |
| rabbit-w4 | 1.163 | 4.193 |
| sqs-w4 | 407 | 759 |
| sns-w4 | 275 | 426 |

Passou a valer o relay: as escritas terminam antes do relay publicar e o backlog chega a 1.500–4.500 mensagens.

### 2.3 Tuning de configuração (Rabbit, 4 workers, 2 rodadas)

| profile | msg/s (rod. 1 / 2) |
|---|---|
| rabbit-w4 (base: `candidateLimit` 50, polling 50 ms) | 4.362 / 4.243 |
| polling de 10 ms | 4.379 / 4.269 (**sem efeito**: o relay é acordado no commit) |
| `candidateLimit` 200 | 4.021 / 3.741 (**pior**) |
| `candidateLimit` 1000 | 3.271 / 2.722 (**bem pior**) |
| 8 workers + `candidateLimit` 200 | 5.532 / 5.178 (**melhor**) |

### 2.4 Análise dos índices (EXPLAIN, Postgres 14 isolado, 110k linhas)

| Query | índice usado | 10k pend. | 50k pend. |
|---|---|---|---|
| Candidatos (`FluxoClaimer`, versão antiga) | `idx_outbox_age` | 3,3 ms | 13 ms |
| Mensagens do fluxo, sem `idx_outbox_fluxo` | `idx_outbox_age` + filtro | 0,72 ms | 3,2 ms |
| Mensagens do fluxo, com `idx_outbox_fluxo` | `idx_outbox_fluxo` | 0,03 ms | 0,06 ms |
| Purga (lote de 1.000) | `idx_outbox_purge` + seq scan | 12 ms | 13 ms |

Achados: `pendingMessagesOf` e os `UPDATE`s de backoff/quarentena filtravam por `(aggregate_id, destination)` sem índice que começasse por essas colunas; `idx_outbox_pending` (líder `available_at`) não é escolhido pelo planner; a purga lê a tabela inteira (seq scan no `DELETE ... IN`).

**Custo de escrita do índice novo** (`pgbench`, 8 clientes, 15 s): só inserir 7.426 / 7.412 tx/s sem índice contra 7.412 / 7.503 com; inserir + marcar publicado 5.031 contra 4.970 (−1,2%, dentro do ruído).

### 2.5 Query de candidatos reescrita (spike)

Query atual agrega todos os PENDING a cada ciclo (O(backlog)). A nova caminha por `seq` num índice parcial (`idx_outbox_ready_seq`: `seq WHERE status='PENDING' AND available_at < 'infinity'`) e deduplica os fluxos em Java. A ordem é a mesma: a primeira aparição por `seq` equivale a `ORDER BY min(seq)`.

| Cenário (query isolada) | atual | nova |
|---|---|---|
| 50k pendentes, 1.000 fluxos | 12,2 ms | **0,08 ms** |
| 10k em backoff à frente | 2,3 ms | 0,9 ms |
| 10k em quarentena à frente | 3,6 ms | **0,16 ms** |
| Fluxo quente de 20k linhas à frente | 7,4 ms | 0,09 ms, mas só 1 fluxo distinto na janela |

Ponto fraco conhecido: **fluxo quente**. Se um fluxo ocupa toda a janela de linhas e está com lock de outro worker, o ciclo vira `all_contended` e os demais fluxos esperam. Medida a mitigação (segunda página excluindo o fluxo já visto): 2,2 ms, ainda abaixo dos 7,4 ms antigos. **Não implementada.**

A/B no load test (3 rodadas): `inmemory-w4` 4.265 → 5.248 (**+23%**); `rabbit-w4` 4.073 → 4.196 (+3%); `rabbit-w8-cl200` 5.037 → 5.022 (0%); `sqs-w4` 683 → 698 (+2%). Critério do plano (≥ 15% no Rabbit) **não atendido**, o que levou à investigação abaixo.

### 2.6 Investigação: onde o ciclo gasta tempo

`pg_stat_statements` no Postgres do load test (`rabbit-w4`, média no servidor): candidatos 0,065 ms, `UPDATE` de publicação 0,027 ms, select do fluxo 0,014 ms, `tryLock` 0,002 ms, INSERT 0,041 ms. O SQL no servidor soma menos de 10% do tempo ocupado dos workers.

**O ciclo é limitado por round trips sequenciais**, não por execução: ~16–18 por ciclo (`SET idle_in_transaction...`, candidatos, 1–3 `tryLock`, select do fluxo, **um `UPDATE` por mensagem**, commit). No `inmemory-w4` a fase de publicação custa ~0,3 ms/mensagem contra 0,027 ms no servidor: ~0,27 ms por round trip (Docker Desktop; em produção depende da rede).

Decomposição estimada do ciclo do `rabbit-w4` (~7,5 ms, ~8 mensagens): broker ~35%, `UPDATE` por mensagem ~32%, demais round trips ~27%, servidor <10%. É estimativa derivada das métricas. No `sqs-w4`: ciclo de ~52 ms, ~96% é publish (~5 ms/mensagem, sequencial).

### 2.7 `UPDATE` em lote por fluxo (maior ganho da biblioteca)

Um `UPDATE ... WHERE id = ANY(?)` por fluxo em vez de um por mensagem, gravado **antes** do backoff/quarentena (que atualizam tudo que ainda está PENDING no fluxo). As marcações continuam valendo só no commit do ciclo: at-least-once inalterado. Um erro SQL na marcação agora propaga e dá rollback do ciclo (antes virava falha transitória de publish). Teste `FalhaParcialNoFluxoIT` cobre falha transitória e permanente na 2ª de 3 mensagens; passou antes e depois.

3 rodadas, mesma sessão, query de candidatos do spike nos dois lados:

| profile | antes | depois | variação |
|---|---|---|---|
| inmemory-w4 | 6.216 | 10.651 | **+71%** |
| inmemory-w4-big | 5.626 | 7.496 | **+33%** |
| rabbit-w4 | 4.436 | 5.551 | **+25%** |
| rabbit-w4-big | 4.280 | 5.062 | **+18%** |
| rabbit-w8-cl200 | 5.445 | 6.108 | +12% (faixa 5.481–6.628) |

Os profiles `*-big` têm 4× mais mensagens (menos efeito de warmup da JVM). `sqs-w4` não foi medido (ciclo ~96% publish).

### 2.8 Embaralhar candidatos por worker (contenção cai, throughput não)

| profile | antes (msg/s) | depois | variação | locks perdidos por rodada |
|---|---|---|---|---|
| inmemory-w4 | 7.700 | 7.912 | +3% | ~830 → ~75 |
| rabbit-w4 | 5.816 | 5.359 | −8% (faixas se sobrepõem) | ~540 → 29 |
| rabbit-w8-cl200 | 6.316 | 6.260 | −1% | ~1.300 → ~110 |
| rabbit-w4-big | 5.218 | 5.156 | −1% | ~4.760 → 133 |

**Explicação do mecanismo:** todos os workers recebiam a mesma lista (mais antigo primeiro) e perdiam o `tryLock` nos fluxos que outros já processavam (~2 round trips perdidos por ciclo). Embaralhar tira a disputa (−95%). **Por que o throughput não subiu:** o custo estimado do lock perdido (~0,5 ms de ~7,5 ms) não apareceu; o limite estava em outro ponto do ciclo. Mantido por decisão do responsável (menos contenção, neutro no throughput); perde-se a preferência estrita pelo fluxo mais antigo.

### 2.9 Onde está o teto de throughput

CPU durante a carga (`rabbit-w4-xl`, 100 mil mensagens, 5.370 msg/s; 100% = 1 core): RabbitMQ pico **94%** (~1 core), Postgres 127%, host de 14 cores com 22–31% ocioso e 32–34% de CPU de sistema. Nenhum recurso esgotado.

Filas (20k mensagens, 4 workers, 3 rodadas): 1 fila 3.563 / 3.425 / 3.614; 3 filas 5.315 / 5.624 / 4.909; 12 filas 5.989 / 5.902 / 5.937.

Workers (20k mensagens, 3 rodadas, e2e p50 em ms):

| profile | rodadas (msg/s) | média | e2e p50 |
|---|---|---|---|
| 4 workers, 12 filas | 5.882 / 5.640 / 6.027 | 5.850 | ~1.130 |
| 8 workers, 12 filas | 8.080 / 7.693 / 8.036 | **7.936** | ~400 |
| 16 workers, 12 filas | 6.768 / 6.830 / 6.902 | 6.833 | **7** |
| 8 workers, 3 filas | 6.408 / 6.214 / 6.602 | 6.408 | ~1.000 |

**Leitura:**
1. Com 4 workers o **relay** é o limite (8 workers rendem +36%).
2. Com poucas filas o **RabbitMQ** limita: ~3,5 mil msg/s por fila; com 8 workers, 12 filas rendem +24% sobre 3.
3. Com 16 workers a latência fim a fim cai para 7 ms: o relay publica quase assim que a mensagem é escrita. O throughput passa a ser o ritmo de escrita do **gerador de carga** (~7 mil msg/s). Isso é inferência pela latência; o ritmo de escrita não foi medido isoladamente.

## 3. O que esperar em produção

- Ganhos **transferíveis**: `UPDATE` em lote (reduz round trips; o ganho cresce com a latência de rede ao banco) e, com backlog grande, o índice `idx_outbox_fluxo` e a query por `seq`.
- Ganhos **dependentes de uso**: `relayWorkers` e número de destinos.
- Os números absolutos (msg/s) valem para esta máquina com tudo em containers locais; não são capacidade de produção.

## 4. Pendências e riscos abertos

- **SNS fora de ordem:** `sns-w4` teve 18 mensagens fora de ordem na 1ª rodada e 94 na 2ª (os demais profiles: 0). **Não investigado**: pode ser do relay ou do consumo da fila FIFO. Verificar antes de confiar na ordenação via SNS.
- **`sqs-w4-c4` mais lento que `sqs-w4`** (548 contra 759 msg/s): uma rodada só; pode ser ruído ou competição entre consumers.
- **Fluxo quente** na query por `seq`: segunda página não implementada; sem teste de fluxo quente nem de equivalência com a query antiga.
- **`idx_outbox_pending`** deixou de servir ao claim: candidato a remoção (reduz custo de escrita); precisa de `EXPLAIN` e A/B de escrita.
- **Purga:** o `DELETE ... WHERE id IN (...)` faz seq scan; reescrever com `USING`/CTE.
- **`UPDATE`s não são HOT** (`n_tup_hot_upd` = 0): `status` e `processed_at` participam de índices parciais; cada marcação gera tupla morta e atualiza todos os índices. Dimensionar autovacuum em produção.
- **Bloat dos índices** sob rotatividade e Postgres de produção **não avaliados**.
- **SQS/SNS:** ciclo ~96% publish sequencial (~5 ms/mensagem). Publicação em lote (SendMessageBatch, até 10) é o candidato de maior ganho potencial; muda o contrato de `MessagePublisher` e a semântica de falha parcial. Não implementado.
- Medições de uma só sessão, 3 rodadas por profile, ruído de ~5%; só comparar antes/depois da mesma tabela.
