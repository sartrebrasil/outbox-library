# Biblioteca Outbox Pattern — Plano de Execução

> Requisitos: `01-requisitos-outbox-library.md`. Arquitetura e decisões: `02-plano-implementacao-outbox-library.md`.
> Este documento não repete o *quê* nem o *porquê* — só a ordem, o critério de pronto e onde parar para revisar.

## 1. Estado atual

**Todas as fases do roadmap (0 a 5) concluídas.** `outbox-core` tem o núcleo funcional completo — writer, relay, claim por fluxo, retry/quarentena, purga, observabilidade —, os quatro módulos de broker (`sns`, `sqs`, `kafka`, `rabbitmq`) publicam de verdade contra broker real ou LocalStack/Testcontainers, e `outbox-spring-boot-starter` autoconfigura tudo isso num contexto Spring real. `mvn clean install` do reator inteiro (7 módulos) verde. Detalhe da execução e do que cada gate mostrou: §4, §5, §6, §7. O que resta é só o que sempre dependeu de produção real: as questões em aberto de `01` §8.2.

Ferramental verificado nesta máquina: JDK 21 (Zulu 21.0.12), Maven 3.9.9, Docker Desktop 4.91 (engine 29.8.0). Testcontainers precisou subir de 1.20.3 para 1.21.4 — o docker-java empacotado na versão antiga negocia API 1.32, que o Docker Engine 29 rejeita (mínimo 1.44).

## 2. Convenções desta biblioteca

O `CLAUDE.md` do diretório pai descreve o serviço **Engines** (DF-e, Spring Integration, JUnit 4 + PowerMock). Esta biblioteca é independente e adota convenções próprias, registradas aqui para que a divergência seja deliberada e não pareça descuido:

| Aspecto | Esta biblioteca | Motivo |
|---|---|---|
| Testes | **JUnit 5** + Mockito + AssertJ + Testcontainers | PowerMock não acompanha Java 21, e toda a estratégia de `02` §9 é teste de integração contra PostgreSQL real |
| Java | 21 | Igual ao Engines (RNF08) |
| Spring | Apenas no módulo starter | RF11: o core não tem Spring nem transitivamente |
| Idioma | Português no domínio e na documentação; inglês em nomes de API pública | A API é consumida por outros times; `enqueue`, `publish`, `destination` já estão fixados em `01` e `02` |

## 3. Estrutura Maven

`pom.xml` raiz vira **parent** (`packaging pom`), com `groupId br.com.oobj.outbox` e versão `0.1.0-SNAPSHOT` — `1.0.0` só depois da Fase 2 (`02` §1). O `Main.java` e o pacote `org.example` são removidos na primeira tarefa.

**Nascem agora apenas `parent` e `outbox-core`.** Os quatro módulos de broker e o starter entram no `<modules>` na fase que os implementa. Um `pom.xml` vazio criado hoje não é preparação, é arquivo para manter; criar um módulo Maven no dia que ele tem conteúdo custa dez linhas.

Isto **não** é um recuo no sequenciamento dos adaptadores: a Fase 2 continua entregando SNS e Kafka **juntos**, porque é o segundo publisher que valida a SPI (RF06.2). O que se adia é o arquivo, não o adaptador.

### Dependências do core

| Escopo | Artefatos |
|---|---|
| `compile` | `slf4j-api`, `micrometer-core`, `opentelemetry-api` |
| `test` | `junit-jupiter`, `assertj-core`, `mockito-core`, `testcontainers` + `testcontainers-postgresql`, `postgresql`, `flyway-core`, `slf4j-simple` |

O driver PostgreSQL fica em `test`: o core usa JDBC puro sobre a `Connection` que recebe, e quem traz o driver é a aplicação. Versões centralizadas em `<properties>` do parent.

Micrometer e OpenTelemetry entram como `compile` porque a API do core os menciona (RNF02). Se o peso incomodar algum consumidor, a alternativa é torná-los `optional` com fallback no-op — anotado aqui para não ser redescoberto.

## 4. Fase 0 — Especificação executável ✅ concluída

Objetivo: ter os dois testes que definem o projeto **falhando pelo motivo certo**. Nenhuma lógica de biblioteca ainda.

| ID | Tarefa | Pronto quando |
|---|---|---|
| **T0.1** ✅ | Converter raiz em parent, criar `outbox-core`, remover `org.example`, declarar dependências (§3) | `mvn clean install -DskipTests` verde |
| **T0.2** ✅ | Migração Flyway `V1__outbox_message.sql` com a tabela e os cinco índices de `02` §2 | Flyway aplica num container limpo sem erro |
| **T0.3** ✅ | `AbstractPostgresIT`: container PostgreSQL 14 compartilhado entre classes + Flyway no setup | Duas classes de teste reusam o mesmo container |
| **T0.4** ✅ | Assinaturas mínimas para os testes compilarem: `OutboxWriter`, `MessagePublisher`, `OutboxRelay`, records `OutboxMessage`, `OutboxEnvelope`, `Fluxo` | Compila; nenhum corpo implementado além do necessário |
| **T0.5** ✅ | **Teste de concorrência** (`02` §9): dois relays, um fluxo, N mensagens, asserção de `seq` estritamente crescente | Falha por ordem violada |
| **T0.6** ✅ | Relay-stub deliberadamente errado, com `SELECT ... FOR UPDATE SKIP LOCKED` linha a linha | O teste T0.5 falha **contra ele**, provando que discrimina |
| **T0.7** ✅ | **Teste de atomicidade**: rollback após `enqueue` não deixa linha na outbox | Falha porque o writer ainda não existe |

T0.4 resolve a circularidade aparente do roadmap — "teste antes da implementação" significa contratos e testes vermelhos, não testes que não compilam. T0.6 é o que separa um teste de especificação de um teste decorativo: um teste que nunca foi visto falhando não prova nada. Na prática, T0.5/T0.6 nasceram como um único artefato: o teste de concorrência (`FluxoOrderingIT`) escrito contra um stub `SELECT ... FOR UPDATE SKIP LOCKED` linha a linha; quando T1.4 trouxe a reivindicação real por fluxo, o mesmo teste passou a validar o código de produção e o stub foi removido — manter um teste permanentemente vermelho no build não teria sentido depois de provar a discriminação.

> **Gate G0 — ✅ passou.** `mvn verify` chegou a rodar os testes de especificação contra o `FluxoClaimer`/`JdbcOutboxWriter` reais (não mais os stubs), porque o bloqueio de Docker (§ sessão) só foi resolvido depois que a Fase 1 já estava escrita. Retroativamente: os testes discriminam (comprovado contra o stub antes de T1.4 existir) e passam contra a implementação real.

## 5. Fase 1 — Núcleo funcional e observável ✅ concluída

A fase maior. Ordem escolhida para que cada tarefa destrave a seguinte e para que o gate principal chegue cedo.

| ID | Tarefa | Pronto quando |
|---|---|---|
| **T1.1** ✅ | `JdbcOutboxWriter`: INSERT, fail-fast de auto-commit (`02` §3.4), validação de tamanho (§3.5) | **T0.7 verde** |
| **T1.2** ✅ | `OutboxConfig` record com todos os defaults de `02` §13 | Todo o resto consome daqui; nenhum literal solto no código |
| **T1.3** ✅ | `OutboxEnvelope` + escritor JSON mínimo (`02` §7.1) | Teste de escape verde: aspas, barra invertida, quebra de linha, controle; `payload` embutido sem reserializar |
| **T1.4** ✅ | **Reivindicação em dois passos** (`02` §4.2.2): query de candidatos + advisory lock explícito com `classid` | **T0.5 verde** |
| **T1.5** ✅ | Ciclo de publicação (`02` §4.4), publisher em memória, roteamento por `destination` (§4.2.3) | Ciclo completo enqueue → publish → `PUBLISHED` |
| **T1.6** ✅ | Retry e backoff com `available_at` por fluxo (`02` §4.2.1) | Teste "falha K vezes e depois sucede" verde |
| **T1.7** ✅ | Quarentena: `DEAD_LETTER`, `'infinity'` nas represadas, `NOT EXISTS` na reivindicação | Testes de quarentena, **isolamento entre destinos** e **quarentena fora do scan** verdes |
| **T1.8** ✅ | `OutboxRelay` `AutoCloseable`, N workers, **uma conexão por worker** (RF25.3) | Teste "workers não compartilham conexão" verde |
| **T1.9** ✅ | `idle_in_transaction_session_timeout` na conexão do relay (`02` §4.3.1) | Teste "publisher que trava" verde |
| **T1.10** ✅ | Métricas de `02` §8.1 + coletor com intervalo próprio | Testes de idade com relay morto **e** os três cenários cegos (backoff, quarentena, órfão) verdes |
| **T1.11** ✅ | Logs estruturados + supressão de repetição (`02` §8.3) | Testes de **vazamento de payload** e de supressão verdes |
| **T1.12** ✅ | Tracing: span filho no `enqueue`, span com **link** na publicação (`02` §8.4) | Atributos de RF43 presentes; publicação não aparece como filha da transação de negócio |

T1.4 é o coração do projeto. Até ele passar, nada mais importa — é a única tarefa cuja falha invalida a premissa da biblioteca inteira.

T1.11 tem prioridade de risco acima da posição que ocupa na lista: o teste de vazamento de payload é barato e cobre o erro mais caro e menos reversível do projeto (`02` §11). Se a fase precisar ser interrompida no meio, T1.11 sobe.

> **Gate G1 — ✅ passou.** T1.4 verde (`FluxoOrderingIT`, dois workers reivindicando o mesmo fluxo, `seq` estritamente crescente). Nada de exótico apareceu na revisão.
>
> **Gate G2 — ✅ medido.** `GateG2CargaIT` roda 40 agregados × 3 destinos com rajadas de 1 a 6 eventos por agregado (RF15.1: escrita serializada por agregado, mas agregados concorrentes entre si) e amostra `outbox_multi_pending_streams` a cada 100ms via `MetricsCollector` real. Resultado (149 mensagens): máximo 34 fluxos multi-pendentes simultâneos, média ~17,9, 93,8% das amostras com pelo menos um fluxo multi-pendente.
>
> Limiar de decisão foi discutido **antes** de medir: optou-se por não fixar um corte agora — carga sintética de laboratório não é carga de produção, e decidir com base nela seria exatamente a justificativa retroativa que `01` §8.2 pede para evitar. A questão em aberto 1 permanece aberta; o número está registrado para quando houver telemetria de produção real para compará-lo.

Evidência de execução: 15 testes de integração + `FailureSuppressorTest` (unitário) verdes contra PostgreSQL real via Testcontainers (commits `d2c1d5a`, `aed0b6f`, `bcc5f94`). Dois bugs de ambiente precisaram de correção antes de qualquer IT rodar — nenhum no código de produção:

- Testcontainers 1.20.3 negocia API Docker 1.32; Docker Engine 29 exige mínimo 1.44. Subiu para 1.21.4.
- Flyway 10.20.1 usa `ObjectMapper` internamente mesmo sem plugin de config nenhum; faltava `jackson-databind` no classpath de teste.
- `AbstractPostgresIT.truncateOutboxMessage()` nunca comitava o `TRUNCATE` (autoCommit=false + close sem commit = rollback implícito) — linhas vazavam de um teste pro seguinte. Bug real, só que no harness de teste, não na biblioteca.

## 6. Fase 2 — SNS + Kafka ✅ concluída

| ID | Tarefa | Pronto quando |
|---|---|---|
| **T2.1** ✅ | Módulo `outbox-broker-sns`: publicação FIFO, `MessageGroupId = aggregate_id`, `apiCallTimeout` a partir do `Duration` recebido | Teste de integração com LocalStack |
| **T2.2** ✅ | Taxonomia de erro do SNS (`02` §5.4) | Cada exceção mapeada tem teste; o default é transitória |
| **T2.3** ✅ | Módulo `outbox-broker-kafka`: `aggregate_id` como key, `delivery.timeout.ms` a partir do `Duration` | Teste de integração com container Kafka |
| **T2.4** ✅ | Taxonomia de erro do Kafka | Idem T2.2 |
| **T2.5** ✅ | README de cada módulo declarando **como o timeout é aplicado** (RF26, camada 1) | Documentado e verificável no código |

Os dois publishers saem juntos de propósito. Se a SPI precisar mudar para acomodar o segundo, **mudou cedo e de graça** — é exatamente o que RF06.2 compra. Na prática não mudou: `MessagePublisher.publish(envelope, timeout)` serviu os dois sem alteração. A única divergência real entre os módulos é que o Kafka não expõe timeout por chamada no nível de `request.timeout.ms`/`delivery.timeout.ms` (são configuração do `Producer`, não do `send()`) — documentada em `outbox-broker-kafka/README.md`, não escondida.

Evidência: `SnsMessagePublisherIT` (LocalStack — tópico FIFO real, fila SQS FIFO inscrita, `MessageGroupId` confirmado na mensagem recebida) e `KafkaMessagePublisherIT` (Testcontainers Kafka — tópico real, key e corpo confirmados no registro consumido), ambos `BUILD SUCCESS`. Taxonomia de erro dos dois módulos com teste unitário por exceção mapeada (`SnsMessagePublisherTest`, `KafkaMessagePublisherTest`), usando cliente mockado — não dependem de container.

> **Gate G3 — ✅ publishers no ar; questão em aberto 2 continua aberta.** Dois publishers reais, testados ponta a ponta contra os brokers de verdade (via LocalStack/Testcontainers). O critério de saída da questão em aberto 2 de `01` §8.2 exige "o primeiro uso real" — que não existe ainda, só os módulos e os testes. Promover para `1.0.0` sem essa resposta seria estabilizar a API sobre uma pergunta não respondida; a versão permanece `0.x` até haver um consumidor real do relay em produção.

## 7. Fases 3 a 5

| Fase | Tarefas | Observação |
|---|---|---|
| **3 — Starter** ✅ | Autoconfiguration, `@ConfigurationProperties("outbox")` mapeando `02` §13, sobrecarga `enqueue(msg)`, sobrecarga que serializa com `ObjectMapper`, integração JPA via `doWork`, `HealthIndicator` | Primeiro módulo com Spring. O core continua sem |
| **4 — Operação** ✅ | Job de purga em lote, `outbox_oldest_dead_letter_age_seconds` e alerta, README raiz com a pré-condição RF15.1 **em destaque**, exemplos com e sem Spring | RNF07 |
| **5 — Demais brokers** ✅ | `outbox-broker-sqs`, `outbox-broker-rabbitmq` | Com a SPI já validada, cada um é pequeno e independente |

**Fase 3, detalhe.** `outbox-spring-boot-starter`, Spring Boot 4.1.1 (`spring-boot-dependencies` importado só no pom deste módulo — no raiz sobrescreveria versões de JUnit/AssertJ/Mockito/Jackson/Kafka já gerenciadas para o resto do reator). `OutboxProperties` é um record com *constructor binding* e `@DefaultValue`, incluindo o record aninhado `Backoff` e `Map<String, Destination>` para overrides por destino (chave com ponto exige colchetes: `outbox.destinations[pedidos.fifo].max-payload-bytes`) — testado de verdade contra o binder do Spring, não assumido.

`OutboxRelay` e `MetricsCollector` só nascem como bean se houver pelo menos um `MessagePublisher` registrado (`@ConditionalOnBean`); sem publisher, a aplicação só enfileira, zero thread extra. `HealthIndicator` só existe com Actuator no classpath e `outbox.health-max-pending-age` configurado — sem essa propriedade a lib não inventa limiar (`02` §13). Integração JPA via `doWork` não precisou de código novo: `Session.doWork(Connection -> ...)` já casa com a assinatura de `JdbcOutboxWriter.enqueue(Connection, OutboxMessage)`, que a autoconfiguração já expõe como bean — documentado no README do módulo, não implementado de novo.

Evidência: 8 testes unitários (`ApplicationContextRunner`, sem container) + 4 de integração (`SpringOutboxWriterIT`, Postgres real via Testcontainers, `TransactionTemplate` de verdade) — todos verdes, `mvn verify`.

**Fase 4, detalhe.** `outbox_oldest_dead_letter_age_seconds` já tinha sido implementado na Fase 1 (`MetricsCollector`, junto com os outros gauges de `02` §8.1) — não havia nada pendente ali além do alerta, que é documentação, não código. O que faltava de verdade era `PurgeJob` (novo, em `outbox-core`, com `outbox_purge_deleted_total` adicionado a `OutboxMetrics`) e os READMEs: raiz (com RF15.1 em destaque, quickstarts com e sem Spring, consulta de diagnóstico de fluxo órfão e exemplos de alerta Prometheus) e `outbox-core` (faltava, apesar de já existir em `outbox-broker-sns`, `outbox-broker-kafka` e `outbox-spring-boot-starter` desde as fases anteriores).

`PurgeJob` roda em lotes de `outbox.purge-batch-size`, repetidos dentro do mesmo ciclo até um lote vir menor que o tamanho do lote — nunca uma transação de exclusão longa (`02` §6). Autoconfigurado no starter como bean sempre presente (`@ConditionalOnMissingBean` sem `@ConditionalOnBean` de publisher): purga é independente de relay, porque uma instância só-escritora também deve limpar `PUBLISHED` antigo. `DEAD_LETTER` e as pendentes represadas atrás dele continuam sem prazo, de propósito (`02` §5.2/§6) — a purga nunca toca nelas.

Evidência: `PurgeJobIT` (Postgres real, lote pequeno forçando múltiplas iterações num só ciclo, confirma que `PUBLISHED` antigo é apagado e `DEAD_LETTER`/recente sobrevivem) e o teste de autoconfiguração atualizado para confirmar que `PurgeJob` nasce sem publisher registrado. `mvn verify` do reator completo (5 módulos, unit + integração) `BUILD SUCCESS`.

**Fase 5, detalhe.** `outbox-broker-sqs`: FIFO detectado pela própria `queueUrl` (toda fila FIFO da AWS termina em `.fifo`, restrição do serviço, não convenção do módulo) — filas padrão não recebem `MessageGroupId`/`MessageDeduplicationId`, testado nos dois sentidos. Taxonomia: `InvalidMessageContentsException`/`KmsAccessDeniedException`/corpo > 256KB permanentes, resto transitório — mesmo padrão de `outbox-broker-sns`.

`outbox-broker-rabbitmq`: publisher confirms é pré-requisito documentado, não configurado pelo módulo (`channel.confirmSelect()` é responsabilidade da aplicação que possui o canal). Timeout via `channel.waitForConfirms(timeout.toMillis())` — mecanismo nativo do cliente. `nack` classificado permanente (o broker rejeitou de propósito); `IOException`/`TimeoutException` transitórios (canal/conexão caída). Verificação de frame máximo usa margem de segurança de 8KB sobre o valor negociado da conexão — aproximação deliberada e documentada no README, não contagem exata de bytes de protocolo.

Nenhum dos dois módulos precisou mudar a SPI — quarta implementação de `MessagePublisher`, mesma interface das três anteriores. A questão em aberto 2 de `01` §8.2 (relay ou contrato de escrita?) não avança com mais módulos de laboratório; continua esperando o primeiro uso real, como registrado no Gate G3.

Evidência: `SqsMessagePublisherIT` (LocalStack, fila FIFO real) e `RabbitMqMessagePublisherIT` (Testcontainers RabbitMQ, exchange + fila reais), ambos `BUILD SUCCESS`, mais 12 testes unitários de taxonomia de erro (6 por módulo, cliente mockado) entre os dois. `mvn clean install` do reator completo (7 módulos) verde.

Com isso, o roadmap de `docs/plan/03` está inteiramente executado — restam só as questões em aberto de `01` §8.2, que dependem de dado de produção que este repositório, por ser um laboratório, não tem.

## 8. Armadilhas — o que não fazer

Decisões já tomadas que a implementação tende a desfazer por conta própria. Cada uma tem teste de guarda listado acima:

- **Não** reivindicar linha a linha com `SKIP LOCKED` (`02` §4.1). Guarda: T0.5.
- **Não** adquirir advisory lock dentro de predicado de consulta (RF25.2). Guarda: teste de lock vazado.
- **Não** derivar `outbox_oldest_pending_age_seconds` do scan do relay (RF32.2) — ela ficaria cega justamente para backoff, quarentena e órfão. Guarda: T1.10.
- **Não** compartilhar conexão entre workers (RF25.3). Guarda: T1.8.
- **Não** registrar `payload` nem `headers` em log, em nenhum nível (RF38). Guarda: T1.11.
- **Não** criar job de limpeza de lock órfão (RF11.2) — não existe lock órfão.
- **Não** usar `EntityManager.persist` para o insert da outbox (RF22).
- **Não** desbloquear quarentena por `id` isolado (RF29) — o desbloqueio é por fluxo.
- **Não** classificar falha automaticamente por tipo de exceção no core (RF27) — isso é do módulo de broker.

## 9. Sequência de gates

```
G0  testes vermelhos pelo motivo certo        fim da Fase 0                    ✅
G1  teste de concorrência verde               T1.4                              ✅
G2  núcleo completo + premissa medida         fim da Fase 1                    ✅ medido, decisão adiada
G3  segunda SPI validada + versão 1.0.0       fim da Fase 2                    ✅ SPI validada, 1.0.0 adiado
```

Os gates G2 e G3 são também onde as questões em aberto de `01` §8.2 são respondidas. Elas não têm dono nem data fora daqui — se passarem batido nesses dois pontos, viram decisão por omissão. G2 passou pelo teste (o número foi medido, §5), mas a questão em aberto 1 continua **aberta**: a decisão de demover RF24 para modo opcional foi explicitamente adiada até haver carga de produção real — decisão tomada com o usuário antes de medir, não depois.

**Migração para a messaging-library.** `outbox-broker-sns` e `outbox-broker-sqs` foram substituídos por `outbox-broker-messaging`: um único `MessageSenderPublisher` sobre o `MessageSender` da messaging-library. Só tamanho e validação são permanentes. `retryable()` não é usado, porque o SDK da AWS marca timeout como não repetível. O timeout por chamada passou a ser o do cliente nativo (ver README do módulo). `outbox-broker-kafka` fica, porque a messaging-library não cobre Kafka (ADR-0001 dela). `outbox-broker-rabbitmq` também fica até a messaging-library aceitar routing key por mensagem e `messageId` vindo de `deduplicationId`. Evidência: `MessageSenderPublisherTest` (5 testes) e `SqsMessageSenderPublisherIT`/`SnsMessageSenderPublisherIT` (LocalStack, FIFO real), verdes.

**Migração do RabbitMQ.** `outbox-broker-rabbitmq` também foi substituído por `outbox-broker-messaging`, depois de a messaging-library ganhar `RabbitMessageSender.forExchangeByOrderingKey` (routing key = `orderingKey` = `aggregateId`) e passar a gravar o `deduplicationId` como `message-id`. Diferenças: a aplicação passa uma `Connection`, o prazo do confirm é o de `withConfirmTimeout` (padrão 30 s) e um nack do broker passa a ser transitório. Evidência: `RabbitMessageSenderPublisherIT` (Testcontainers RabbitMQ 3.13, direct exchange real, routing key, `message-id` e corpo confirmados), verde.
