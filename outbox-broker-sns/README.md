# outbox-broker-sns

`MessagePublisher` para AWS SNS FIFO. Uma instância de `SnsMessagePublisher` publica em
**um** tópico; para publicar em vários tópicos, registre uma instância por tópico no mapa
de publishers passado ao `OutboxRelay`.

```java
SnsClient client = SnsClient.builder().region(Region.US_EAST_1).build();
MessagePublisher pedidos = new SnsMessagePublisher(client, "arn:aws:sns:us-east-1:123:pedidos.fifo");
```

A aplicação constrói e configura o `SnsClient` (região, credenciais, endpoint). Este
módulo não conhece nada disso — só sabe publicar.

## Como o timeout é aplicado (RF26, camada 1)

O `Duration` recebido em `publish(envelope, timeout)` é aplicado por chamada, via
`AwsRequestOverrideConfiguration`:

```java
PublishRequest.builder()
        .overrideConfiguration(o -> o.apiCallTimeout(timeout).apiCallAttemptTimeout(timeout))
```

`apiCallTimeout` limita a chamada inteira, incluindo retries do SDK. `apiCallAttemptTimeout`
limita cada tentativa individual. Os dois usam o mesmo valor porque o core já governa
retry no nível do fluxo (backoff exponencial); duplicar retry dentro do SDK só custaria
tempo sem ganhar nada.

Isso é a camada 1 do RF26 — best-effort, depende do SDK respeitar o timeout. A camada 2
(rede de segurança) é o `idle_in_transaction_session_timeout` que o `OutboxRelay` já
aplica na própria conexão, independente deste módulo.

## Ordenação (recomendação de uso, não garantia da lib)

- `MessageGroupId = aggregateId` — preserva ordem de entrega dentro do agregado no lado
  do SNS, desde que o tópico seja FIFO.
- `MessageDeduplicationId = messageId` — `messageId` é estável entre republicações
  (02 §4.5), então isso também dá deduplicação nativa do SNS para o caso de duas
  publicações da mesma mensagem — a mesma garantia de at-least-once do core, reforçada
  do lado do broker.

## Taxonomia de erro (02 §5.4)

| Exceção | Classificação |
|---|---|
| `InvalidParameterException` | Permanente |
| `KmsAccessDeniedException` | Permanente |
| Corpo acima de 256 KB | Permanente — verificado **antes** da chamada de rede, sem round-trip |
| Qualquer outra `SdkException` (`ThrottledException`, falha de rede, `InternalErrorException`, ...) | Transitória (padrão) |

Classificar errado é o modo de falha caro (02 §5.4): a lista de permanentes é
deliberadamente curta, e tudo que não está nela é transitório.
