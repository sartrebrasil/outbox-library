# Biblioteca Outbox Pattern — Plano de Execução

> Requisitos: `01-requisitos-outbox-library.md`. Arquitetura e decisões: `02-plano-implementacao-outbox-library.md`.
> Este documento não repete o *quê* nem o *porquê* — só a ordem, o critério de pronto e onde parar para revisar.

## 1. Estado atual

O repositório é um esqueleto de IDE: `pom.xml` single-module com `groupId org.example`, um `src/main/java/org/example/Main.java` sem conteúdo, e os dois documentos de projeto. Nenhuma linha da biblioteca existe.

Ferramental verificado nesta máquina: JDK 21 (Zulu 21.0.12), Maven 3.9.9, Docker 29.6.2. Testcontainers roda, o que é pré-requisito de toda a Fase 0.

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

## 4. Fase 0 — Especificação executável

Objetivo: ter os dois testes que definem o projeto **falhando pelo motivo certo**. Nenhuma lógica de biblioteca ainda.

| ID | Tarefa | Pronto quando |
|---|---|---|
| **T0.1** | Converter raiz em parent, criar `outbox-core`, remover `org.example`, declarar dependências (§3) | `mvn clean install -DskipTests` verde |
| **T0.2** | Migração Flyway `V1__outbox_message.sql` com a tabela e os cinco índices de `02` §2 | Flyway aplica num container limpo sem erro |
| **T0.3** | `AbstractPostgresIT`: container PostgreSQL 14 compartilhado entre classes + Flyway no setup | Duas classes de teste reusam o mesmo container |
| **T0.4** | Assinaturas mínimas para os testes compilarem: `OutboxWriter`, `MessagePublisher`, `OutboxRelay`, records `OutboxMessage`, `OutboxEnvelope`, `Fluxo` | Compila; nenhum corpo implementado além do necessário |
| **T0.5** | **Teste de concorrência** (`02` §9): dois relays, um fluxo, N mensagens, asserção de `seq` estritamente crescente | Falha por ordem violada |
| **T0.6** | Relay-stub deliberadamente errado, com `SELECT ... FOR UPDATE SKIP LOCKED` linha a linha | O teste T0.5 falha **contra ele**, provando que discrimina |
| **T0.7** | **Teste de atomicidade**: rollback após `enqueue` não deixa linha na outbox | Falha porque o writer ainda não existe |

T0.4 resolve a circularidade aparente do roadmap — "teste antes da implementação" significa contratos e testes vermelhos, não testes que não compilam. T0.6 é o que separa um teste de especificação de um teste decorativo: um teste que nunca foi visto falhando não prova nada.

> **Gate G0.** Parar e revisar. `mvn verify` roda os dois testes e eles falham pela asserção, nunca por `NullPointerException` ou erro de compilação. Um teste que falha pelo motivo errado não é especificação.

## 5. Fase 1 — Núcleo funcional e observável

A fase maior. Ordem escolhida para que cada tarefa destrave a seguinte e para que o gate principal chegue cedo.

| ID | Tarefa | Pronto quando |
|---|---|---|
| **T1.1** | `JdbcOutboxWriter`: INSERT, fail-fast de auto-commit (`02` §3.4), validação de tamanho (§3.5) | **T0.7 verde** |
| **T1.2** | `OutboxConfig` record com todos os defaults de `02` §13 | Todo o resto consome daqui; nenhum literal solto no código |
| **T1.3** | `OutboxEnvelope` + escritor JSON mínimo (`02` §7.1) | Teste de escape verde: aspas, barra invertida, quebra de linha, controle; `payload` embutido sem reserializar |
| **T1.4** | **Reivindicação em dois passos** (`02` §4.2.2): query de candidatos + advisory lock explícito com `classid` | **T0.5 verde** |
| **T1.5** | Ciclo de publicação (`02` §4.4), publisher em memória, roteamento por `destination` (§4.2.3) | Ciclo completo enqueue → publish → `PUBLISHED` |
| **T1.6** | Retry e backoff com `available_at` por fluxo (`02` §4.2.1) | Teste "falha K vezes e depois sucede" verde |
| **T1.7** | Quarentena: `DEAD_LETTER`, `'infinity'` nas represadas, `NOT EXISTS` na reivindicação | Testes de quarentena, **isolamento entre destinos** e **quarentena fora do scan** verdes |
| **T1.8** | `OutboxRelay` `AutoCloseable`, N workers, **uma conexão por worker** (RF25.3) | Teste "workers não compartilham conexão" verde |
| **T1.9** | `idle_in_transaction_session_timeout` na conexão do relay (`02` §4.3.1) | Teste "publisher que trava" verde |
| **T1.10** | Métricas de `02` §8.1 + coletor com intervalo próprio | Testes de idade com relay morto **e** os três cenários cegos (backoff, quarentena, órfão) verdes |
| **T1.11** | Logs estruturados + supressão de repetição (`02` §8.3) | Testes de **vazamento de payload** e de supressão verdes |
| **T1.12** | Tracing: span filho no `enqueue`, span com **link** na publicação (`02` §8.4) | Atributos de RF43 presentes; publicação não aparece como filha da transação de negócio |

T1.4 é o coração do projeto. Até ele passar, nada mais importa — é a única tarefa cuja falha invalida a premissa da biblioteca inteira.

T1.11 tem prioridade de risco acima da posição que ocupa na lista: o teste de vazamento de payload é barato e cobre o erro mais caro e menos reversível do projeto (`02` §11). Se a fase precisar ser interrompida no meio, T1.11 sobe.

> **Gate G1.** T1.4 verde. A garantia de ordenação deixou de ser prosa. Vale uma revisão curta aqui mesmo que a fase siga.
>
> **Gate G2.** Fase 1 completa. Rodar sob carga representativa e **medir `outbox_multi_pending_streams`** — é a questão em aberto 1 de `01` §8.2, e o limiar de decisão precisa estar acordado **antes** de ver o número, ou a medição vira justificativa retroativa.

## 6. Fase 2 — SNS + Kafka

| ID | Tarefa | Pronto quando |
|---|---|---|
| **T2.1** | Módulo `outbox-broker-sns`: publicação FIFO, `MessageGroupId = aggregate_id`, `apiCallTimeout` a partir do `Duration` recebido | Teste de integração com LocalStack |
| **T2.2** | Taxonomia de erro do SNS (`02` §5.4) | Cada exceção mapeada tem teste; o default é transitória |
| **T2.3** | Módulo `outbox-broker-kafka`: `aggregate_id` como key, `delivery.timeout.ms` a partir do `Duration` | Teste de integração com container Kafka |
| **T2.4** | Taxonomia de erro do Kafka | Idem T2.2 |
| **T2.5** | README de cada módulo declarando **como o timeout é aplicado** (RF26, camada 1) | Documentado e verificável no código |

Os dois publishers saem juntos de propósito. Se a SPI precisar mudar para acomodar o segundo, **mudou cedo e de graça** — é exatamente o que RF06.2 compra.

> **Gate G3.** Dois publishers no ar. Responder à questão em aberto 2 de `01` §8.2 — o valor da biblioteca está no relay ou no contrato de escrita? — e só então promover a versão para `1.0.0`.

## 7. Fases 3 a 5

| Fase | Tarefas | Observação |
|---|---|---|
| **3 — Starter** | Autoconfiguration, `@ConfigurationProperties("outbox")` mapeando `02` §13, sobrecarga `enqueue(msg)`, sobrecarga que serializa com `ObjectMapper`, integração JPA via `doWork`, `HealthIndicator` | Primeiro módulo com Spring. O core continua sem |
| **4 — Operação** | Job de purga em lote, `outbox_oldest_dead_letter_age_seconds` e alerta, README raiz com a pré-condição RF15.1 **em destaque**, exemplos com e sem Spring | RNF07 |
| **5 — Demais brokers** | `outbox-broker-sqs`, `outbox-broker-rabbitmq` | Com a SPI já validada, cada um é pequeno e independente |

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
G0  testes vermelhos pelo motivo certo        fim da Fase 0
G1  teste de concorrência verde               T1.4
G2  núcleo completo + premissa medida         fim da Fase 1
G3  segunda SPI validada + versão 1.0.0       fim da Fase 2
```

Os gates G2 e G3 são também onde as questões em aberto de `01` §8.2 são respondidas. Elas não têm dono nem data fora daqui — se passarem batido nesses dois pontos, viram decisão por omissão.
