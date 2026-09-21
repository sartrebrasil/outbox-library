# outbox-broker-kafka

`MessagePublisher` para Apache Kafka. Uma instância de `KafkaMessagePublisher` publica em
**um** tópico; para publicar em vários tópicos, registre uma instância por tópico no mapa
de publishers passado ao `OutboxRelay`.

```java
Producer<String, String> producer = new KafkaProducer<>(properties);
MessagePublisher pedidos = new KafkaMessagePublisher(producer, "pedidos");
```

A aplicação constrói e configura o `Producer` (bootstrap servers, serializers, segurança,
`delivery.timeout.ms`/`request.timeout.ms` de baseline). Este módulo não conhece nada
disso — só sabe publicar.

## Como o timeout é aplicado (RF26, camada 1) — e o que este cliente não permite

O `Duration` recebido em `publish(envelope, timeout)` é aplicado **por chamada** através
do `Future` que `producer.send()` retorna:

```java
producer.send(record).get(timeout.toMillis(), TimeUnit.MILLISECONDS);
```

Isso limita quanto tempo `publish()` fica bloqueado, e é o mecanismo nativo do cliente
Kafka para isso — nenhum `ExecutorService` por fora. Se o `Future` não completar a tempo,
`get()` lança `TimeoutException`, tratada como falha transitória.

**O que isso não faz:** o cliente Kafka não expõe `request.timeout.ms`/
`delivery.timeout.ms` por chamada — são propriedades do `Producer`, fixadas na
construção. Isto **não** é um bug deste módulo: é limitação do `KafkaProducer`, registrada
aqui para não ser redescoberta. Consequência prática: se a aplicação configurar
`delivery.timeout.ms` bem maior que o `Duration` passado a `publish()`, o envio ao broker
pode continuar em voo depois que `publish()` já lançou `TimeoutException` — o mesmo modo
de falha at-least-once que o core já aceita (02 §4.5), não um caso novo. Recomenda-se que
`delivery.timeout.ms` seja próximo do `outbox.publish-timeout` configurado no relay.

## Ordenação (recomendação de uso, não garantia da lib)

`aggregateId` é usado como key do `ProducerRecord`. O particionador padrão do Kafka
garante que a mesma key sempre cai na mesma partição, e o consumidor recebe mensagens de
uma partição em ordem — a mesma ordem por agregado que a reivindicação por fluxo já
garante do lado da escrita.

## Taxonomia de erro (02 §5.4)

| Exceção | Classificação |
|---|---|
| `RecordTooLargeException` | Permanente |
| `InvalidTopicException` | Permanente |
| `SerializationException` | Permanente |
| `TimeoutException` (do `Future.get`) | Transitória |
| Qualquer outra causa de `ExecutionException` (`NotLeaderOrFollowerException`, `RetriableException` em geral, ...) | Transitória (padrão) |

Classificar errado é o modo de falha caro (02 §5.4): a lista de permanentes é
deliberadamente curta, e tudo que não está nela é transitório.
