# Plano: query de candidatos do relay (`FluxoClaimer`)

Branch: `perf/claim-candidates`. Contexto: load test (`06-teste-de-carga.md`) e análise de índices de 2026-11-24.

## Problema

`FluxoClaimer.CANDIDATES_SQL` agrega todos os `PENDING` (`GROUP BY` + `ORDER BY min(seq)`) a cada ciclo de cada worker. O custo é proporcional ao backlog e o `LIMIT` não ajuda: 3,3 ms com 10k pendentes, 13 ms com 50k. Era ~80% do SQL por ciclo com 10k pendentes. O `docs/plan/02` §4.3 aceitava essa degradação; esta mudança revê a decisão.

## Restrições (de `02` §4.2, §4.2.1, §4.3)

- Ordem só importa dentro do fluxo (`seq` + advisory lock do fluxo).
- O `NOT EXISTS` de `DEAD_LETTER` continua decidindo a quarentena; `available_at='infinity'` só tira linhas do scan.
- O `tryLock` explícito, um candidato por vez, não muda.

## Decisão: Proposta 1 — caminhar por `seq` e deduplicar em Java

```sql
SELECT o.aggregate_id, o.destination, o.seq
FROM outbox_message o
WHERE o.status = 'PENDING'
  AND o.available_at < 'infinity' AND o.available_at <= now()
  AND NOT EXISTS (<DEAD_LETTER do mesmo fluxo>)
ORDER BY o.seq
LIMIT ?
```

O Java mantém a primeira ocorrência de cada fluxo até `candidateLimit` fluxos distintos. A primeira aparição em ordem de `seq` equivale a `ORDER BY min(seq)`, então a ordem de candidatos é a mesma da query atual.

Índice novo (migração própria):

```sql
CREATE INDEX idx_outbox_ready_seq ON outbox_message (seq)
    WHERE status = 'PENDING' AND available_at < 'infinity';
```

### Medições que sustentam a escolha (Postgres 14, dados sintéticos)

| Cenário | Atual | Proposta 1 |
|---|---|---|
| 50k pendentes, 1.000 fluxos | 12,2 ms | 0,08 ms |
| 10k em backoff à frente | 2,3 ms | 0,9 ms |
| 10k em quarentena (`infinity`) à frente | 3,6 ms | 0,16 ms |
| Fluxo quente de 20k linhas à frente | 7,4 ms | 0,09 ms, mas só 1 fluxo distinto em 200 linhas |

### Pontos de atenção

1. **Fluxo quente:** se a janela de linhas só traz 1 fluxo e ele está com lock de outro worker, o ciclo vira `all_contended` e os demais fluxos esperam. Precisa de segunda página excluindo os fluxos já vistos (medida: 2,2 ms, abaixo dos 7,4 ms de hoje).
2. **Backoff à frente** continua custando (filtro sobre linhas em backoff). É temporário e menor que o atual.
3. **O teste "quarentena sai do scan" (`02` §Testes)** precisa ser reescrito para o índice novo.
4. **`idx_outbox_pending`** deixa de servir ao claim e passa a custar só escrita. Decisão de removê-lo é separada, depois da medição.

## Critério de sucesso

Mesmo A/B do load test (3 rodadas antes/depois, profiles `inmemory-w4`, `rabbit-w4`, `rabbit-w8-cl200`, `sqs-w4`). **Sucesso = ganho médio de throughput ≥ 15% em `rabbit-w4` e `rabbit-w8-cl200`** (ruído medido: ~5%). Linha de base (média de 3 rodadas, índice `idx_outbox_fluxo` já aplicado): inmemory-w4 ≈ 4.265, rabbit-w4 ≈ 4.073, rabbit-w8-cl200 ≈ 5.037, sqs-w4 ≈ 683 msg/s.

## Passos

1. **Spike:** reescrever só a query + migração do índice, sem segunda página; rodar o A/B. Se < 15%, **parar** e avaliar o que fazer (ver abaixo). Não refinar antes do resultado.
2. Se passar: segunda página para fluxo quente (exclusão dos fluxos já vistos), mantendo a semântica de `candidateLimit` como número de fluxos distintos.
3. Testes: ordem preservada, equivalência com a query antiga em dados mistos, quarentena fora do scan, fluxo quente não bloqueia os demais, `all_contended`/`contended` inalterados.
4. Atualizar `02` §4.2.2 e §4.3 e o teste de plano de `02` §Testes.
5. Reavaliar `idx_outbox_pending` com `EXPLAIN` e A/B de escrita.

## Se falhar o critério

Não decidido. Hipóteses a investigar antes de escolher o caminho: custo do UPDATE por mensagem (`markPublished`, não HOT), commit/fsync por ciclo, abertura de transação + `SET idle_in_transaction_session_timeout` por ciclo, contenção entre workers. A Proposta 2 (tabela `outbox_fluxo`, uma linha por fluxo) fica descartada por ora: custou ~19% de escrita num teste simplificado e reintroduz estado derivado.

## Limites desta análise

Medições em Postgres 14 local com dados sintéticos; bloat do índice sob rotatividade e Postgres de produção não foram avaliados.

## Resultado do spike (2026-11-24)

Query reescrita (`LIMIT candidateLimit*20` linhas, dedupe em Java) + `idx_outbox_ready_seq`. 3 rodadas, comparadas à linha de base (query antiga, `idx_outbox_fluxo` aplicado). Build e testes do core verdes.

| profile | antes (média msg/s) | depois (média msg/s) | variação |
|---|---|---|---|
| inmemory-w4 | 4.265 | 5.248 | +23% |
| rabbit-w4 | 4.073 | 4.196 | +3% |
| rabbit-w8-cl200 | 5.037 | 5.022 | 0% |
| sqs-w4 | 683 | 698 | +2% |

**Critério (≥ 15% em `rabbit-w4` e `rabbit-w8-cl200`): NÃO atendido.** Conforme o plano, parou aqui; o passo 2 (segunda página) não foi feito. O ganho de 23% no inmemory (variação entre rodadas < 1%) mostra que a query era custo real quando o publish é gratuito; com Rabbit/SQS o tempo está em outro lugar do ciclo. O que fazer a partir daqui fica por decidir (hipóteses na seção anterior).

## Investigação: onde o ciclo gasta tempo (2026-11-24)

Método: `pg_stat_statements` no Postgres do load test (instrumentação temporária, já revertida) cruzado com as métricas do relay em `report.json` (`outbox_relay_cycle_duration`, `outbox_publish_duration`, `outbox_relay_claim_total`). Spike da query aplicado.

**1. O tempo de execução no servidor é desprezível.** `rabbit-w4`: candidatos 0,065 ms/chamada, `UPDATE` de publicação 0,027 ms, select do fluxo 0,014 ms, lock 0,002 ms. O total de SQL no servidor (~290 ms, incluindo os 83 ms de INSERT) é menos de 10% do tempo ocupado dos workers (~2,8 s: 1,9 s em ciclos com fluxo + 0,9 s em vazios). A query reescrita ficou barata, mas ela já era barata nos backlogs do teste (<2k pendentes).

**2. O ciclo é limitado por ida e volta (round trips) sequenciais, não por execução de SQL.** Um ciclo com fluxo faz ~16–18 round trips: `SET idle_in_transaction_session_timeout`, candidatos, 1–3 `tryLock`, select do fluxo, **um `UPDATE` por mensagem**, commit. No `inmemory-w4` (publish ≈ 0,01 ms) a fase de publicação custa ~0,3 ms/mensagem, contra 0,027 ms de execução no servidor: ~0,27 ms por round trip (Docker Desktop no macOS; em produção o valor depende da rede).

**3. Decomposição estimada do ciclo (`rabbit-w4`, ~7,5 ms com ~8 mensagens):** broker ~35% (p50 0,33 ms/msg), `UPDATE` por mensagem ~32%, demais round trips do ciclo ~27%, execução no servidor <10%. É estimativa derivada das métricas, não medição direta por fase. No `sqs-w4` o ciclo leva ~52 ms e ~96% é o publish (~5 ms/msg, sequencial).

**4. Contenção de claim:** `contended` ≈ 2 por ciclo adquirido (529 contra 251 no `rabbit-w4`). Todos os workers recebem os mesmos candidatos mais antigos e perdem o `tryLock` nos primeiros, gastando round trips.

**5. A duração dos testes é curta:** `rabbit-w4` drena 2.000 mensagens em ~0,5 s, `inmemory-w4` 5.000 em ~1 s. Warmup da JVM pesa nos números; as conclusões de ganho precisam de profiles maiores.

### Candidatos a ação (nenhum implementado)

| # | Ação | Ataca | Estimativa de ganho | Risco |
|---|---|---|---|---|
| 1 | Um único `UPDATE ... WHERE id = ANY(?)` por fluxo, em vez de um por mensagem | round trips do item 3 (~32% no Rabbit) | até ~25% no ciclo do Rabbit | Nenhum no at-least-once: as marcações já ficam na mesma transação do claim e só valem no commit, então um crash já reentrega o fluxo inteiro. Em falha parcial, gravar as mensagens publicadas antes de tratar o erro |
| 2 | Embaralhar candidatos por worker (ou escolher entre os K primeiros) | contenção do item 4 (~7%) | pequeno | Perde a preferência estrita pelo fluxo mais antigo; a ordem dentro do fluxo é preservada pelo lock |
| 3 | Eliminar o `SET idle_in_transaction_session_timeout` por ciclo (configurar na conexão do pool ou `SET LOCAL` no mesmo round trip) | 1 round trip por ciclo | pequeno | Muda como a segunda camada de proteção de `02` §4.3.1 é aplicada |
| 4 | Publicação em lote nos brokers que suportam (SQS/SNS batch de 10) | publish sequencial do item 3 | grande no SQS/SNS (~96% do ciclo); nulo no inmemory | Muda o contrato de `MessagePublisher` e a semântica de falha parcial |
| 5 | Mais `relayWorkers` | paralelismo entre fluxos | +25% já medido (4→8) | Mais conexões e mais contenção |

A reescrita da query (spike) tem ganho real só onde o publish é gratuito (+23% no `inmemory`). Decidir o que fazer com ela (manter, descartar ou segurar até haver backlog grande) é uma decisão separada.

## Resultado: `UPDATE` de publicação em lote por fluxo (2026-11-24)

`OutboxRelay.publishFluxo` acumula os ids publicados e grava `status = 'PUBLISHED'` num único `UPDATE ... WHERE id = ANY(?)` por fluxo, em vez de um por mensagem. O lote é gravado **antes** do backoff/quarentena, porque esses `UPDATE`s filtram por `status = 'PENDING'` e alcançariam mensagens já publicadas. As marcações continuam valendo só no commit do ciclo, então o at-least-once não muda. Um erro SQL na marcação agora propaga e dá rollback do ciclo (antes era tratado como falha transitória de publish).

Teste novo `FalhaParcialNoFluxoIT` (falha transitória e permanente na 2ª de 3 mensagens do fluxo): passou **antes** e **depois** da mudança. Build completo verde.

A/B com o spike da query já aplicado nos dois lados, 3 rodadas, média em msg/s. Profiles `*-big` têm 4× mais mensagens (menos efeito de warmup).

| profile | antes | depois | variação |
|---|---|---|---|
| inmemory-w4 | 6.216 | 10.651 | +71% |
| inmemory-w4-big | 5.626 | 7.496 | +33% |
| rabbit-w4 | 4.436 | 5.551 | **+25%** |
| rabbit-w4-big | 4.280 | 5.062 | **+18%** |
| rabbit-w8-cl200 | 5.445 | 6.108 | +12% (faixa depois: 5.481–6.628) |

Critério de ≥ 15% atendido em `rabbit-w4` e `rabbit-w4-big`; `rabbit-w8-cl200` ficou abaixo, com variação alta. `sqs-w4` não foi medido (o ciclo é ~96% publish, não deve mudar). O nível absoluto da máquina variou entre as sessões (o `inmemory-w4` "antes" desta rodada é maior que o de rodadas anteriores), então só vale comparar antes/depois da mesma rodada.
