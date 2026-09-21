# outbox-library

Biblioteca de outbox pattern para eventos de domínio: escrita atômica na mesma transação
de negócio, relay de polling com ordenação por fluxo, retry, quarentena e observabilidade.
Requisitos completos: `docs/plan/01-requisitos-outbox-library.md`. Arquitetura e decisões:
`docs/plan/02-plano-implementacao-outbox-library.md`.

> ## ⚠️ Pré-condição de uso (RF15.1) — leia antes de usar
>
> **A garantia de ordenação desta biblioteca só se sustenta se a sua aplicação serializar
> as escritas do mesmo agregado.** Transações concorrentes sobre o mesmo agregado alocam
> números de sequência antes do commit e podem commitar fora da ordem de alocação —
> nenhum relay recupera a ordem "pretendida" depois disso.
>
> Aplicações que usam **bloqueio pessimista ou versionamento otimista na raiz do
> agregado** (o caso normal em DDD) já satisfazem essa pré-condição de graça. Se a sua
> aplicação **não** serializa escritas por agregado, trate a ordenação desta lib como
> best-effort, não como garantia.

## Módulos

| Módulo | O que é | Depende de Spring? |
|---|---|---|
| [`outbox-core`](outbox-core/README.md) | Writer, relay, retry, quarentena, purga, métricas, tracing | Não |
| [`outbox-broker-sns`](outbox-broker-sns/README.md) | `MessagePublisher` para AWS SNS FIFO | Não |
| [`outbox-broker-sqs`](outbox-broker-sqs/README.md) | `MessagePublisher` para AWS SQS (padrão ou FIFO) | Não |
| [`outbox-broker-kafka`](outbox-broker-kafka/README.md) | `MessagePublisher` para Apache Kafka | Não |
| [`outbox-broker-rabbitmq`](outbox-broker-rabbitmq/README.md) | `MessagePublisher` para RabbitMQ | Não |
| [`outbox-spring-boot-starter`](outbox-spring-boot-starter/README.md) | Autoconfiguration, `@ConfigurationProperties`, `HealthIndicator` | Sim |

## Quickstart — com Spring Boot

```xml
<dependency>
    <groupId>br.com.oobj.outbox</groupId>
    <artifactId>outbox-spring-boot-starter</artifactId>
</dependency>
```

```java
@Bean("pedidos.fifo")
MessagePublisher pedidosPublisher(SnsClient client) {
    return new SnsMessagePublisher(client, "arn:aws:sns:us-east-1:123:pedidos.fifo");
}
```

```java
@Autowired SpringOutboxWriter writer;

@Transactional
public void criarPedido(Pedido pedido) {
    repository.save(pedido); // mesma transação
    writer.enqueue("Pedido", pedido.getId(), pedido.getVersao(), "PedidoCriado", pedido, "pedidos.fifo");
}
```

Sem publisher registrado, a aplicação só enfileira — nenhum relay sobe. Detalhes,
propriedades e integração com JPA: [`outbox-spring-boot-starter/README.md`](outbox-spring-boot-starter/README.md).

## Quickstart — sem Spring

```java
OutboxConfig config = OutboxConfig.defaults();
MeterRegistry registry = new SimpleMeterRegistry();
JdbcOutboxWriter writer = new JdbcOutboxWriter(config, registry);

try (Connection connection = dataSource.getConnection()) {
    connection.setAutoCommit(false);
    // ... resto da transação de negócio na mesma connection ...
    writer.enqueue(connection, OutboxMessage.of("Pedido", pedido.getId(), pedido.getVersao(),
            "PedidoCriado", objectMapper.writeValueAsString(pedido), "pedidos.fifo"));
    connection.commit();
}

OutboxRelay relay = new OutboxRelay(dataSource, config, registry,
        Map.of("pedidos.fifo", new SnsMessagePublisher(snsClient, topicArn)));
relay.start(); // e relay.close() no shutdown

PurgeJob purgeJob = new PurgeJob(dataSource, config, registry);
purgeJob.start(); // e purgeJob.close() no shutdown
```

Detalhes: [`outbox-core/README.md`](outbox-core/README.md).

## Schema

`outbox-core` publica a migração Flyway `V1__outbox_message.sql` no próprio jar
(`classpath:db/migration`). Aponte seu `Flyway`/`flyway-maven-plugin` para
`classpath:db/migration` deste jar, ou copie o SQL — o schema é público, não um detalhe
de implementação.

## Operação

### Consulta de diagnóstico — fluxo órfão (02 §5.3)

Um desbloqueio malfeito (`UPDATE` por `id` em vez de por fluxo) pode deixar mensagens
represadas em `available_at = 'infinity'` sem nenhum `DEAD_LETTER` que as justifique —
invisíveis tanto para o scan do relay quanto para `outbox_oldest_dead_letter_age_seconds`:

```sql
SELECT DISTINCT o.aggregate_id, o.destination
FROM outbox_message o
WHERE o.status = 'PENDING'
  AND o.available_at = 'infinity'
  AND NOT EXISTS (
        SELECT 1 FROM outbox_message d
        WHERE d.aggregate_id = o.aggregate_id
          AND d.destination  = o.destination
          AND d.status = 'DEAD_LETTER');
```

### Desbloqueio de quarentena (02 §5.3) — sempre por fluxo, nunca por `id`

```sql
UPDATE outbox_message
SET status = 'PENDING', available_at = now()
WHERE aggregate_id = '...' AND destination = '...' AND status <> 'PUBLISHED';
```

### Alertas (02 §8.2)

**Regra de agregação: `max`, nunca `sum`, entre instâncias.** Os gauges abaixo descrevem
o estado do banco, não da instância que os publica — com N relays, todos reportam o mesmo
número, e somar multiplica o backlog por N.

```yaml
groups:
  - name: outbox
    rules:
      - alert: OutboxBacklogEnvelhecendo
        expr: max(outbox_oldest_pending_age_seconds) > 300   # ajuste ao SLA de propagação
        for: 5m
        labels: { severity: alta }
        annotations:
          summary: "Broker fora, relay parado, fluxo em quarentena ou fluxo órfão — apure com a consulta de diagnóstico acima."

      - alert: OutboxFluxoBloqueado
        expr: max(outbox_blocked_streams) > 0
        for: 15m
        labels: { severity: media }
        annotations:
          summary: "Quarentena aguardando decisão humana (02 §5.3)."

      - alert: OutboxQuarentenaEsquecida
        expr: max(outbox_oldest_dead_letter_age_seconds) > 86400 * 3   # dias, não horas
        labels: { severity: media }
        annotations:
          summary: "DEAD_LETTER vivo há dias — todo evento novo do fluxo se acumula em 'infinity' sem prazo."

      - alert: OutboxCicloDoRelayLento
        expr: max(outbox_relay_cycle_duration_seconds{quantile="0.99"}) > 4   # aproxime do publish-timeout
        labels: { severity: media }
        annotations:
          summary: "Transação do relay se aproximando do timeout — risco de horizonte de vacuum (02 §4.3)."

      - alert: OutboxBacklogCrescendo
        expr: deriv(max(outbox_pending_count)[15m:]) > 0
        for: 15m
        labels: { severity: media }
        annotations:
          summary: "Taxa de escrita acima da taxa de publicação."
```

Alertar na **idade**, nunca na taxa de erro: taxa de erro sobe e desce sozinha e ensina o
time a ignorá-la; a idade da mensagem mais antiga só sobe quando alguém precisa agir.
