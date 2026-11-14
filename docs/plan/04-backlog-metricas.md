# Backlog de métricas

Saída da revisão das métricas de 02 §8.1, feita em 2026-11-14. Os problemas 1 e 2 da revisão
(`outbox_dead_letter_count` que não voltava a zero e `outbox_oldest_pending_age_seconds` que congelava
com o coletor falhando) já foram corrigidos no `MetricsCollector`.

A ordem é de execução: primeiro o que distorce métricas que já existem, depois as métricas novas,
por valor. Toda métrica nova respeita o contrato de cardinalidade de §8.1 (só `destination` ou
nenhum rótulo; nunca `aggregate_id`, `message_id` ou `trace_id`) e entra na tabela de §8.1 e no README.

## Correções nas métricas existentes

- [x] **M1. `outbox_relay_cycle_duration` com rótulo `outcome`** (`acquired` | `empty` | `error`).
  Hoje os ciclos vazios diluem o percentil, e isso cega o alerta de §8.2 ("percentil alto se
  aproximando do timeout"). Os ciclos com falha também caem no mesmo timer.
  Onde: `OutboxMetrics.cycleCompleted`, `OutboxRelay.runOneCycle`.
  Atualizar o exemplo de alerta no README para filtrar `outcome="acquired"`.

- [x] **M2. Rótulo `broker` de `outbox_published_total`.** Decidido: remover. Era fixo em `"core"`
  e cada `destination` já resolve para um único publisher (`OutboxRelay.resolvePublisher`), então o
  rótulo não separaria nada. Quem precisar do broker cruza `destination` com a configuração.

- [x] **M3. Separar as unidades de `outbox_relay_claim_total`.** `contended` é contado por
  candidato, `acquired` por ciclo, e `empty` cobre tanto "sem candidatos" quanto "todos em
  contenção". Proposta: `empty` só quando não havia candidato; `all_contended` quando todos
  falharam no lock. Onde: `FluxoClaimer.claimNext`.

- [x] **M4. Contar todas as falhas do `enqueue`.** `IllegalStateException` (chamada fora de
  transação) e outras `RuntimeException` não entram em `outbox_enqueue_total`.
  Onde: `JdbcOutboxWriter.enqueue`, com `outcome="error"` e relançando a exceção.

## Métricas novas (por valor)

- [x] **N1. `outbox_end_to_end_latency`**: timer `{destination}`, `published_at − created_at`.
  É o SLA real de propagação; hoje só existe a idade da mensagem mais antiga. O `created_at` já é
  lido pelo relay.

- [x] **N2. Saúde do próprio coletor**: `outbox_metrics_collector_failures_total` (counter) e
  `outbox_metrics_collector_duration` (timer). Indicam que os gauges estão velhos e quando o
  `count(*)` começar a ficar caro. Onde: `MetricsCollector.collectQuietly`.

- [x] **N3. ~~`outbox_relay_cycle_failures_total`~~ — dispensada.** Coberta por
  `outbox_relay_cycle_duration_count{outcome="error"}` (M1); o teste
  `MetricasIT.cicloQueFalhaContaOutcomeError` prova a cobertura. Sem alerta de taxa de erro, por
  §8.2 ("alertar na idade"): a falha prolongada já sobe `outbox_oldest_pending_age_seconds`.

- [x] **N4. Purga observável**: `outbox_purge_duration` (timer) e `outbox_purge_failures_total`
  (counter). A duração já é calculada para o log em `PurgeJob.runOneCycle`.

- [ ] **N5. `outbox_quarantined_total{destination}`**: counter da taxa de entrada em quarentena.
  O gauge `outbox_dead_letter_count` mostra o estoque, não o fluxo. Onde: `OutboxRelay.quarantine`.

- [ ] **N6. `outbox_publish_attempts{destination}`**: summary do `attempts` no momento do
  sucesso. Mostra quantas tentativas custa cada entrega.

- [ ] **N7. `outbox_enqueue_payload_bytes{destination}`**: summary. Mostra o quanto as mensagens
  chegam perto do limite antes de virarem `rejected`. O tamanho já é calculado em
  `JdbcOutboxWriter.validateSize`.

- [ ] **N8. `outbox_relay_last_cycle_age_seconds`**: gauge por instância. Separa "relay morto" de
  "backlog real", que hoje se misturam em `outbox_oldest_pending_age_seconds`. É o único gauge
  por instância da lista: no backend ele **não** é agregado com `max`. Documentar isso junto da
  regra de §8.1.

- [ ] **N9. `outbox_backoff_pending_count`**: gauge das pendentes com
  `available_at > now()` e diferente de `'infinity'`. Diferencia backlog saudável de backlog em
  retry. Mais uma consulta no `MetricsCollector`; verificar o custo antes, junto com N2.

## Pendências menores

- [ ] **P1. Conferir se o README traz a regra de agregação `max`** junto dos exemplos de alerta, para
  todos os gauges globais (§8.1, "Gauges globais com várias instâncias"). Essa conferência ainda não
  foi feita.
