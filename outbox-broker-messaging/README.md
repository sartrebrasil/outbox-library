# outbox-broker-messaging

`MessagePublisher` sobre um `MessageSender` da messaging-library. Substitui os antigos
`outbox-broker-sns`, `outbox-broker-sqs` e `outbox-broker-rabbitmq` e serve para qualquer
provedor que a messaging-library suporte (SQS, SNS, RabbitMQ, Pub/Sub, Service Bus, JMS). Uma instância de
`MessageSenderPublisher` publica no **único** destino do sender. Para publicar em vários
destinos, registre uma instância por destino no mapa de publishers passado ao `OutboxRelay`.

```java
SqsClient sqs = SqsClient.builder().region(Region.US_EAST_1).build();
MessagePublisher pedidos = new MessageSenderPublisher(
        new SqsMessageSender(sqs, "https://sqs.us-east-1.amazonaws.com/123/pedidos.fifo"));

SnsClient sns = SnsClient.builder().region(Region.US_EAST_1).build();
MessagePublisher notas = new MessageSenderPublisher(
        new SnsMessageSender(sns, "arn:aws:sns:us-east-1:123:notas.fifo"));

Connection rabbit = connectionFactory.newConnection();
MessagePublisher eventos = new MessageSenderPublisher(RabbitMessageSender.forExchangeByOrderingKey(
        rabbit, "pedidos.exchange", RabbitMessageSender.DEFAULT_MAX_MESSAGE_BYTES)
        .withConfirmTimeout(Duration.ofSeconds(5)));
```

O adapter do provedor (`messaging-aws`, `messaging-gcp`...) é dependência da aplicação,
assim como o cliente nativo. O publisher não fecha o sender.

## Mapeamento

| `OutboxEnvelope` | `OutgoingMessage` | SQS/SNS FIFO |
|---|---|---|
| `body` (UTF-8) | `body` | corpo em texto |
| — | `contentType = application/json` | atributo `content_type` |
| `aggregateId` | `orderingKey` | `MessageGroupId` |
| `messageId` | `deduplicationId` | `MessageDeduplicationId` (só FIFO) |

No RabbitMQ, `forExchangeByOrderingKey` usa o `orderingKey` (`aggregateId`) como routing key,
para direct ou consistent-hash exchange, e o `deduplicationId` (`messageId`) vira `message-id`.

Diferenças em relação aos módulos antigos:

- Consumidores passam a receber o atributo `content_type` na mensagem.
- Em fila SQS **padrão**, `MessageGroupId` também é enviado. Isso ativa fair queues e não
  dá ordem.
- O limite de tamanho é o do provedor na messaging-library (SQS/SNS: 1 MiB, contando os
  atributos), e não mais 256 KB.
- `aggregateId` precisa ser ASCII imprimível sem espaço, com até 128 caracteres (regra do
  `orderingKey`). Fora disso, a falha é permanente.
- RabbitMQ: a aplicação passa uma `Connection`, não um `Channel` com `confirmSelect`. O sender
  abre o próprio channel com publisher confirms e o recria depois de erro. Um sender usa um
  channel por vez; para publicar em paralelo, crie mais senders sobre a mesma `Connection`.
- RabbitMQ: o limite é `maxMessageBytes` (padrão 16 MiB, o `max_message_size` do broker), e não
  mais o frame máximo da conexão. Um nack do broker passa a ser transitório, e não permanente.

## Como o timeout é aplicado (RF26, camada 1)

`MessageSender.send` não recebe prazo, então o `Duration` passado pelo relay é
**ignorado**. O timeout é o do cliente nativo, e você precisa configurá-lo com
`apiCallTimeout <= outbox.publish-timeout`:

```java
SqsClient.builder()
        .overrideConfiguration(o -> o.apiCallTimeout(Duration.ofSeconds(5)))
        .build();
```

No RabbitMQ, o prazo é o do publisher confirm: `withConfirmTimeout(...)` no
`RabbitMessageSender` (padrão 30 s), ou `confirm-timeout` no messaging-spring-boot-starter.
Configure-o com `confirmTimeout <= outbox.publish-timeout`. O prazo vale também com o broker
em alarme de memória ou disco.

Se não configurar, a rede de segurança do banco (`idle_in_transaction_session_timeout`,
02 §4.3.1) continua valendo, mas vai abortar a transação antes de o SDK desistir.

## Taxonomia de erro (02 §5.4)

| Falha | Classificação |
|---|---|
| `MessageTooLargeException` | Permanente |
| `IllegalArgumentException` (mensagem inválida, FIFO sem `orderingKey`) | Permanente |
| Qualquer outra (`ThrottledException`, `AccessDeniedException`, `DestinationNotFoundException`, timeout, rede, sender fechado) | Transitória |

`MessagingException.retryable()` **não** é usado: o SDK da AWS marca timeout como não
repetível, e erro de permissão (inclusive KMS) ou de destino inexistente se corrige na
configuração, sem perder a mensagem. Por consequência, um 4xx genuíno, como um parâmetro
inválido rejeitado pelo serviço, entra em backoff em vez de ir direto para a quarentena.
