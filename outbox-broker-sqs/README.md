# outbox-broker-sqs

`MessagePublisher` para AWS SQS, padrão ou FIFO. Uma instância de `SqsMessagePublisher`
publica numa **única** fila; para publicar em várias filas, registre uma instância por
fila no mapa de publishers passado ao `OutboxRelay`.

```java
SqsClient client = SqsClient.builder().region(Region.US_EAST_1).build();
MessagePublisher pedidos = new SqsMessagePublisher(client, "https://sqs.us-east-1.amazonaws.com/123/pedidos.fifo");
```

A aplicação constrói e configura o `SqsClient` (região, credenciais, endpoint). Este
módulo não conhece nada disso — só sabe publicar.

## FIFO é detectado pela URL, não configurado

Toda fila FIFO da AWS **tem** que terminar em `.fifo` — é restrição do serviço, não
convenção deste módulo. `SqsMessagePublisher` usa exatamente esse fato: se `queueUrl`
termina em `.fifo`, envia `MessageGroupId = aggregateId` e
`MessageDeduplicationId = messageId`; senão, não envia nenhum dos dois, porque filas
padrão rejeitam esses atributos com erro.

## Como o timeout é aplicado (RF26, camada 1)

Igual a `outbox-broker-sns`: `apiCallTimeout`/`apiCallAttemptTimeout` via
`AwsRequestOverrideConfiguration`, por chamada, nativamente.

## Taxonomia de erro (02 §5.4)

| Exceção | Classificação |
|---|---|
| `InvalidMessageContentsException` | Permanente |
| `KmsAccessDeniedException` | Permanente |
| Corpo acima de 256 KB | Permanente — verificado **antes** da chamada de rede |
| Qualquer outra `SdkException` (`RequestThrottledException`, falha de rede, ...) | Transitória (padrão) |
