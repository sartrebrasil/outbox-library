# Teste de carga (`outbox-loadtest`)

Mede o relay real (Postgres + `OutboxRelay` + publisher + consumidores) contra brokers reais via Testcontainers, para achar gargalos e comparar profiles de execução. É relatório, não gate: só falha se um profile não drenar dentro do `timeoutSeconds`.

## Executar

```
mvn -pl outbox-loadtest -am verify -Ploadtest                                   # todos os profiles
mvn -pl outbox-loadtest -am verify -Ploadtest -Dloadtest.profiles=inmemory-w1,rabbit-w4
mvn -pl outbox-loadtest -am verify -Ploadtest -Dloadtest.profilesDir=/meus/profiles
```

Sem `-Ploadtest` o IT é pulado (`skipITs=true`). Relatório em `outbox-loadtest/target/loadtest/<timestamp>/`: `report.json` (config, percentis, série de backlog, todas as métricas do relay) e `summary.csv` (uma linha por profile).

## Profile

Um arquivo `.properties` em `outbox-loadtest/src/test/resources/profiles/`; o nome do arquivo é o nome do profile.

| chave | padrão | |
|---|---|---|
| `broker` | `inmemory` | `inmemory`, `rabbit`, `sqs`, `sns` |
| `relayWorkers` / `candidateLimit` / `pollingIntervalMs` / `publishTimeoutMs` | 1 / 50 / 50 / 5000 | variáveis do relay a ajustar |
| `aggregates` / `eventsPerAggregate` | 200 / 10 | volume; um agregado é escrito em ordem por uma thread |
| `destinations` | 3 | agregados distribuídos entre destinos |
| `payloadBytes` / `writerThreads` | 512 / 8 | |
| `preload` | `false` | `true` escreve tudo antes de iniciar o relay (mede só drenagem) |
| `consume` / `consumers` | `true` / 1 | liga o consumo e define quantos consumidores por destino |
| `timeoutSeconds` | 120 | prazo para publicar e consumir tudo |

## Como ler

- Consumo: `consumeThroughputMsgPerSec` e `consumeE2eMs` (ocorrência → entrega ao consumidor). `duplicates` conta mensagens entregues mais de uma vez; `outOfOrder` conta quedas de `aggregateVersion` por agregado. Consumo bem abaixo da publicação = consumidor ou broker é o gargalo.
- `enqueueMs`: custo de uma transação com `enqueue` (inclui abrir conexão e commit). Se for ele que limita a vazão total, o gargalo está na escrita, não no relay.
- `publishMs` alto: o gargalo é o broker (ou o cliente dele).
- `e2eMs` alto com `publishMs` baixo: o gargalo é relay/Postgres (polling, claim, workers).
- `maxBacklog` e a série `backlog`: crescendo até o fim da escrita = relay mais lento que os escritores.
- `relayMeters`: todas as métricas Micrometer do relay, sem filtro.

## Somar um broker

Implemente `BrokerFixture` (publishers e `consume`) (ex.: Kafka com `KafkaMessagePublisher`) e registre em `BrokerFixture.create`.

## Limites conhecidos

- Consumidores são sintéticos: só contam e conferem ordem, sem processamento de negócio. No `inmemory` a entrega é instantânea (o publisher entrega o corpo), então `consumers` não vale.
- RabbitMQ com vários consumidores na mesma fila pode inverter a ordem; `outOfOrder` mostra isso.
- Escreve pelo `SpringOutboxWriter` sobre `JdbcOutboxWriter` (objeto serializado pelo `PayloadSerializer`, uma transação por evento, wake do relay no commit). O `DataSource` abre uma conexão por transação (sem pool), então os escritores limitam a vazão (~1000 msg/s com 8 threads): para estressar o relay use `preload=true` ou mais `writerThreads`.
- Uma execução por profile, sem aquecimento nem repetição: compare tendências, não diferenças pequenas.
- Kafka não entra na primeira versão.
