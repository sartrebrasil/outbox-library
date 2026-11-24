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
