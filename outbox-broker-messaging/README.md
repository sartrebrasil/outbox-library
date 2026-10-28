# outbox-broker-messaging

`MessagePublisher` sobre um `MessageSender` da messaging-library. Substitui os antigos
`outbox-broker-sns` e `outbox-broker-sqs` e serve para qualquer provedor que a
messaging-library suporte (SQS, SNS, Pub/Sub, Service Bus, JMS). Uma instância de
`MessageSenderPublisher` publica no **único** destino do sender. Para publicar em vários
destinos, registre uma instância por destino no mapa de publishers passado ao `OutboxRelay`.

```java
SqsClient sqs = SqsClient.builder().region(Region.US_EAST_1).build();
MessagePublisher pedidos = new MessageSenderPublisher(
        new SqsMessageSender(sqs, "https://sqs.us-east-1.amazonaws.com/123/pedidos.fifo"));

SnsClient sns = SnsClient.builder().region(Region.US_EAST_1).build();
MessagePublisher notas = new MessageSenderPublisher(
        new SnsMessageSender(sns, "arn:aws:sns:us-east-1:123:notas.fifo"));
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

Diferenças em relação aos módulos antigos:

- Consumidores passam a receber o atributo `content_type` na mensagem.
- Em fila SQS **padrão**, `MessageGroupId` também é enviado. Isso ativa fair queues e não
  dá ordem.
- O limite de tamanho é o do provedor na messaging-library (SQS/SNS: 1 MiB, contando os
  atributos), e não mais 256 KB.
- `aggregateId` precisa ser ASCII imprimível sem espaço, com até 128 caracteres (regra do
  `orderingKey`). Fora disso, a falha é permanente.

## Como o timeout é aplicado (RF26, camada 1)

`MessageSender.send` não recebe prazo, então o `Duration` passado pelo relay é
**ignorado**. O timeout é o do cliente nativo, e você precisa configurá-lo com
`apiCallTimeout <= outbox.publish-timeout`:

```java
SqsClient.builder()
        .overrideConfiguration(o -> o.apiCallTimeout(Duration.ofSeconds(5)))
        .build();
```

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
