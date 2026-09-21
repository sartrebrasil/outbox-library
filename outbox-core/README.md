# outbox-core

Núcleo sem Spring, sem framework nenhum (RNF01/RNF02.1) — só JDBC puro, SLF4J, Micrometer
e OpenTelemetry API. Uma aplicação sem Spring monta os mesmos objetos que o
`outbox-spring-boot-starter` monta por trás das cortinas; nenhum bean do starter esconde
lógica que não exista aqui.

> **Pré-condição de uso (RF15.1).** A garantia de ordenação por fluxo só se sustenta se a
> aplicação serializar as escritas do mesmo agregado. Detalhe completo no README raiz.

## Schema

`V1__outbox_message.sql` (Flyway, `classpath:db/migration`) vai dentro deste jar — aponte
sua ferramenta de migração para ele em vez de copiar o SQL.

## Escrever (dentro da transação de negócio)

```java
OutboxConfig config = OutboxConfig.defaults();
JdbcOutboxWriter writer = new JdbcOutboxWriter(config, meterRegistry);

try (Connection connection = dataSource.getConnection()) {
    connection.setAutoCommit(false);
    // ... INSERT/UPDATE da entidade de negócio na mesma connection ...
    writer.enqueue(connection, OutboxMessage.of(
            "Pedido", pedido.getId(), pedido.getVersao(), "PedidoCriado", payloadJson, "pedidos.fifo"));
    connection.commit();
}
```

`enqueue()` lança `IllegalStateException` se a conexão estiver em auto-commit (falha
rápida, não prova de atomicidade — a prova real são os testes de integração) e
`PayloadTooLargeException` se o payload exceder `outbox.max-payload-bytes` do destino.

## Publicar

```java
Map<String, MessagePublisher> publishers = Map.of(
        "pedidos.fifo", new SnsMessagePublisher(snsClient, topicArn),
        "notas.topic", new KafkaMessagePublisher(kafkaProducer, "notas"));

OutboxRelay relay = new OutboxRelay(dataSource, config, meterRegistry, publishers);
relay.start();
// ... no shutdown:
relay.close();
```

Sem publisher para um destino que aparece na tabela, o fluxo fica em backoff (falha
transitória) — nunca em quarentena, porque destino não configurado é erro de operação,
não de dado.

## Purgar

```java
PurgeJob purgeJob = new PurgeJob(dataSource, config, meterRegistry);
purgeJob.start();
// ... no shutdown:
purgeJob.close();
```

Apaga `PUBLISHED` mais antigo que `outbox.retention` (default 7 dias), em lotes de
`outbox.purge-batch-size`. `DEAD_LETTER` e as mensagens represadas atrás dele **não** têm
prazo — quarentena não se resolve por tempo, só por decisão humana (02 §5.2/§5.3, ver
README raiz para a consulta de desbloqueio).

## Métricas globais (gauges com intervalo próprio)

```java
MetricsCollector coletor = new MetricsCollector(dataSource, meterRegistry,
        config.metricsInterval(), relay::lastCycleCompletedAt);
// ... no shutdown:
coletor.close();
```

`relay::lastCycleCompletedAt` é o que faz `outbox_oldest_pending_age_seconds` continuar
subindo mesmo com o relay morto — sem essa referência, a métrica de alerta principal fica
cega justamente quando o problema existe (02 §8.1).

## O que este módulo não faz

- Não conhece Spring, Jackson, nem nenhum SDK de broker — quem serializa payload é a
  aplicação (ou o starter, se usado); quem publica de fato são os módulos `outbox-broker-*`.
- Não classifica falhas automaticamente por tipo de exceção de broker (RF27) — toda
  exceção é transitória, exceto `PermanentPublishException`.
- Não cria job de limpeza de lock órfão — não existe lock órfão. Advisory lock é escopado
  à transação; cai com ela, sempre.
