# Tasks da revisão do commit `c8ff0fb` (PayloadSerializer)

Origem: `/code-review max` sobre `c8ff0fb`. Achados não verificados por reprodução própria; os pré-existentes também aparecem em `9bd04b3`.

## Prioridade alta (regressão deste commit)
- [ ] **T1.** `OutboxAutoConfiguration.java:53`: trocar `ObjectMapper` por `ObjectProvider<ObjectMapper>` em `payloadSerializer`, com fallback para `JsonMapper.builder().build()`. Documentar a regra no README do starter.

## Prioridade média (pré-existentes com risco real)
- [ ] **T2.** `JacksonPayloadSerializer.java:20`: escrever com `FAIL_ON_EMPTY_BEANS` e testar com o serializer real numa classe sem propriedades. O IT atual usa um stub.
- [ ] **T3.** `OutboxAutoConfiguration.java:107`: mover `outboxHealthIndicator` para uma `@Configuration` aninhada com `@ConditionalOnClass(HealthIndicator.class)`. Testar com class loader child-first, porque `FilteredClassLoader` não reproduz o erro.
- [ ] **T4.** `JdbcOutboxWriter.java:119`: reforçar `requireTransaction()` com `isSynchronizationActive()` e conexão ligada ao `DataSource` do writer. Se não for reforçar, documentar a precondição e retirar a palavra "confiável" dos docs.
- [ ] **T5.** `JacksonPayloadSerializer.java:22`: decidir entre documentar que a causa pode conter texto da aplicação ou cortar a causa do Jackson (RF38).

## Prioridade baixa (pré-existentes, só latência)
- [ ] **T6.** `OutboxRelay.java:89`: limitar o banco de permits em `wake()` (por exemplo `availablePermits() < relayWorkers`).
- [ ] **T7.** `SpringOutboxWriter.java:38`: trocar o marcador thread-bound pela checagem de sincronização própria em `afterCommitOnce()`. Adicionar IT com `REQUIRES_NEW`.

## Nits de código
- [ ] **T8.** `JdbcOutboxWriter.java:103`: validar saída nula do `PayloadSerializer` e documentar o contrato (um valor JSON não nulo).
- [ ] **T9.** `JdbcOutboxWriter.java:106`: embrulhar só `serializer.serialize` em `OutboxSerializationException`, mantendo `requireTransaction()` fora.
- [ ] **T10.** `JdbcOutboxWriter.java:105`: `Objects.requireNonNull` em `aggregateType` e `destination` nas duas sobrecargas de `enqueue`.
- [ ] **T11.** `SpringOutboxWriter.java:59`: documentar em `OutboxWriter` o contrato para decoradores da sobrecarga de 6 argumentos.

## Testes
- [ ] **T12.** `SpringOutboxWriterIT.java:130`: novo teste de wake só com a sobrecarga de objeto (um wake por transação, nenhum no rollback) e renomear o teste atual.
- [ ] **T13.** `JdbcOutboxWriterIT.java:152`: capturar a exceção dentro do callback da transação para o commit acontecer antes de contar as linhas.

## Docs
- [ ] **T14.** `docs/plan/02` seção 3.7, `docs/plan/01` P06 e `PayloadSerializer.java:3`: reescrever em torno de `PayloadSerializer` e da sobrecarga em `OutboxWriter`.
- [ ] **T15.** `outbox-core/README.md:19`: citar as duas sobrecargas de `enqueue`, `PayloadSerializer` e `OutboxSerializationException`.
- [ ] **T16.** `README.md:135`: `outcome=error` também cobre payload que não serializa.

## Ordem sugerida
T1, depois T2 junto com T13 e T12 (testes que fecham os buracos), depois T3 a T5. T8 a T11 podem ir num commit só. T14 a T16 formam um commit de docs. T6 e T7 podem ficar para outra branch, porque são pré-existentes e só afetam latência.

## Fora do escopo da revisão (não verificado)
Transaction managers JPA/Hibernate, consumidores Gradle, CI sem Docker, latência do relay em rede real.
