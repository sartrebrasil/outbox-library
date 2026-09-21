# outbox-broker-rabbitmq

`MessagePublisher` para RabbitMQ. Uma instância de `RabbitMqMessagePublisher` publica num
**único** exchange; para publicar em vários exchanges, registre uma instância por
exchange no mapa de publishers passado ao `OutboxRelay`.

```java
Channel channel = connection.createChannel();
channel.confirmSelect(); // obrigatório — ver abaixo
MessagePublisher pedidos = new RabbitMqMessagePublisher(channel, "pedidos.exchange");
```

A aplicação constrói e configura a `Connection`/`Channel` (host, credenciais, TLS). Este
módulo não conhece nada disso — só sabe publicar.

## `channel.confirmSelect()` é obrigatório, não opcional

Sem publisher confirms habilitado no canal, `basicPublish` é fire-and-forget: o método
retorna sem que o broker tenha confirmado nada, e não há como distinguir "publicado" de
"perdido no meio do caminho" — exatamente a garantia que RF26 (timeout) e RF27
(classificação de erro) dependem de existir. Este módulo **assume** que o canal recebido
já tem confirms habilitado; não chama `confirmSelect()` por conta própria, porque isso é
estado do canal que a aplicação criou e possui, não deste módulo.

## Como o timeout é aplicado (RF26, camada 1)

O `Duration` recebido em `publish(envelope, timeout)` é aplicado via
`channel.waitForConfirms(timeout.toMillis())` — o mecanismo nativo do cliente RabbitMQ
para saber se a mensagem foi confirmada, dentro de um prazo. Se o prazo expirar sem
confirmação, `waitForConfirms` lança `TimeoutException`, tratada como falha transitória.

## Ordenação (recomendação de uso, não garantia da lib)

`aggregateId` é usado como routing key de `basicPublish`. Preservar ordem por agregado no
lado do broker exige um **exchange de consistent hashing** (plugin
`rabbitmq_consistent_hash_exchange`) configurado pela aplicação — este módulo só garante
que a mesma routing key é usada para o mesmo agregado; o roteamento em si é do exchange.

## Taxonomia de erro (02 §5.4)

| Causa | Classificação |
|---|---|
| `nack` do broker (`waitForConfirms` retorna `false`) | Permanente |
| Corpo acima do frame máximo negociado (com margem de segurança) | Permanente — verificado **antes** da chamada de rede |
| `IOException` (canal ou conexão caída) | Transitória |
| `TimeoutException` (confirmação não chegou a tempo) | Transitória |

**Sobre o frame máximo:** a verificação usa uma margem de segurança de 8 KB sobre o
frame máximo negociado da conexão, não uma contagem exata de bytes de protocolo AMQP —
é uma aproximação deliberada, não uma garantia byte-exata. Se o corpo estiver perto do
limite, teste contra o broker real.
