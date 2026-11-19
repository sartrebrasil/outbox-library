# outbox-spring-boot-starter

Único módulo com Spring (02 §2). Autoconfiguração via `spring-boot-autoconfigure` — sem
`@SpringBootApplication` customizado, sem XML, só `AutoConfiguration.imports`.

## O que a autoconfiguração cria

| Bean | Sempre existe? | Condição |
|---|---|---|
| `OutboxConfig` | Sim | — |
| `MeterRegistry` | Só se a aplicação/Actuator não já tiver um | Fallback `SimpleMeterRegistry`, no-op se ninguém ler |
| `PayloadSerializer` | Sim | `JacksonPayloadSerializer` sobre o `ObjectMapper` do contexto; defina o seu para trocar |
| `JdbcOutboxWriter` | Sim | Usa o bean `JdbcClient` se existir; senão cria um sobre o `DataSource` |
| `SpringOutboxWriter` | Sim | — |
| `OutboxRelay` | Não | Só se houver pelo menos um bean `MessagePublisher` |
| `MetricsCollector` | Não | Só se houver `OutboxRelay` |
| `OutboxHealthIndicator` | Não | Só com Actuator no classpath **e** `outbox.health-max-pending-age` configurado |

Sem publisher registrado, a aplicação só enfileira — nenhum relay sobe, nenhuma thread
extra. É o caso de quem só escreve na outbox e deixa outro processo publicar.

## Publishers

Registre um bean `MessagePublisher` por destino; o Spring injeta `Map<String, MessagePublisher>`
automaticamente, chaveado pelo **nome do bean**:

```java
@Bean("pedidos.fifo")
MessagePublisher pedidosPublisher(SnsClient client) {
    return new MessageSenderPublisher(new SnsMessageSender(client, "arn:aws:sns:us-east-1:123:pedidos.fifo"));
}
```

## Propriedades (prefixo `outbox`, defaults em `02` §13)

```properties
outbox.polling-interval=1s
outbox.candidate-limit=50
outbox.publish-timeout=5s
outbox.session-timeout-margin=5s
outbox.relay-workers=1
outbox.shutdown-timeout=30s
outbox.max-attempts=-1
outbox.backoff.initial=1s
outbox.backoff.max=5m
outbox.backoff.multiplier=2.0
outbox.retention=7d
outbox.purge-interval=1h
outbox.purge-batch-size=1000
outbox.metrics-interval=15s
outbox.max-payload-bytes=256KB
outbox.health-max-pending-age=30s   # sem default (02 §13); obrigatório para o health indicator existir

# override por destino — chave com ponto precisa de colchetes
outbox.destinations[pedidos.fifo].max-payload-bytes=256KB
```

## Uso básico (sem JPA)

```java
@Autowired SpringOutboxWriter writer;

@Transactional
public void criarPedido(Pedido pedido) {
    jdbcTemplate.update(...); // grava o pedido na mesma transação
    writer.enqueue("Pedido", pedido.getId(), pedido.getVersao(), "PedidoCriado",
            pedido, "pedidos.fifo"); // serializa com o PayloadSerializer (ObjectMapper do contexto); String já em JSON seria codificada de novo, use OutboxMessage.of
}
```

`enqueue()` fora de uma transação Spring ativa lança `IllegalStateException` imediatamente
(`TransactionSynchronizationManager.isActualTransactionActive()`, 02 §3.4) — não espera o
banco reclamar do auto-commit.

## Uso com JPA/Hibernate (02 §3.3)

O `JdbcOutboxWriter` grava via `JdbcClient`, que pega a conexão pelo `DataSourceUtils`. Com
`JpaTransactionManager` + Hibernate (o padrão do Boot) a transação expõe a conexão JDBC ao
`DataSource`, então o `enqueue` entra no mesmo commit — não precisa mais de `Session.doWork`:

```java
@Autowired SpringOutboxWriter writer;
@PersistenceContext EntityManager entityManager;

@Transactional
public void criarPedido(Pedido pedido) {
    entityManager.persist(pedido);
    entityManager.flush(); // pedido.getId() só existe depois do flush — enfileirar antes grava payload com ID nulo
    writer.enqueue("Pedido", pedido.getId(), pedido.getVersao(), "PedidoCriado", pedido, "pedidos.fifo");
}
```

A linha da outbox é invisível para o `EntityManager` — nunca criar chave estrangeira dela
para a tabela de negócio (02 §3.3).

## Health indicator

Só existe com `outbox.health-max-pending-age` configurado. Sem essa propriedade, a lib
não inventa um limiar — cada aplicação tem seu próprio SLA de propagação (02 §13).
