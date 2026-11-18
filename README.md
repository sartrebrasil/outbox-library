# outbox-library

Biblioteca de outbox pattern para eventos de domínio: escrita atômica na mesma transação
de negócio, relay de polling com ordenação por fluxo, retry, quarentena e observabilidade.
Requisitos completos: `docs/plan/01-requisitos-outbox-library.md`. Arquitetura e decisões:
`docs/plan/02-plano-implementacao-outbox-library.md`.

> ## ⚠️ Pré-condição de uso (RF15.1) — leia antes de usar
>
> **A garantia de ordenação desta biblioteca só se sustenta se a sua aplicação serializar
> as escritas do mesmo agregado.** Transações concorrentes sobre o mesmo agregado alocam
> números de sequência antes do commit e podem commitar fora da ordem de alocação —
> nenhum relay recupera a ordem "pretendida" depois disso.
>
> Aplicações que usam **bloqueio pessimista ou versionamento otimista na raiz do
> agregado** (o caso normal em DDD) já satisfazem essa pré-condição de graça. Se a sua
> aplicação **não** serializa escritas por agregado, trate a ordenação desta lib como
> best-effort, não como garantia.

## Módulos

| Módulo | O que é | Depende de Spring? |
|---|---|---|
| [`outbox-core`](outbox-core/README.md) | Writer, relay, retry, quarentena, purga, métricas, tracing | Não |
| [`outbox-broker-messaging`](outbox-broker-messaging/README.md) | `MessagePublisher` sobre `MessageSender` da messaging-library (SQS, SNS, RabbitMQ, Pub/Sub, Service Bus, JMS) | Não |
| [`outbox-broker-kafka`](outbox-broker-kafka/README.md) | `MessagePublisher` para Apache Kafka | Não |
| [`outbox-spring-boot-starter`](outbox-spring-boot-starter/README.md) | Autoconfiguration, `@ConfigurationProperties`, `HealthIndicator` | Sim |

## Importando via BOM

```xml
<dependencyManagement>
    <dependencies>
        <dependency>
            <groupId>com.example.outbox</groupId>
            <artifactId>outbox-bom</artifactId>
            <version>0.2.0-SNAPSHOT</version>
            <type>pom</type>
            <scope>import</scope>
        </dependency>
    </dependencies>
</dependencyManagement>
```

Depois declare os módulos sem `<version>`.

## Quickstart — com Spring Boot

```xml
<dependency>
    <groupId>com.example.outbox</groupId>
    <artifactId>outbox-spring-boot-starter</artifactId>
</dependency>
```

```java
@Bean("pedidos.fifo")
MessagePublisher pedidosPublisher(SnsClient client) {
    return new MessageSenderPublisher(new SnsMessageSender(client, "arn:aws:sns:us-east-1:123:pedidos.fifo"));
}
```

```java
@Autowired SpringOutboxWriter writer;

@Transactional
public void criarPedido(Pedido pedido) {
    repository.save(pedido); // mesma transação
    writer.enqueue("Pedido", pedido.getId(), pedido.getVersao(), "PedidoCriado", pedido, "pedidos.fifo");
}
```

Sem publisher registrado, a aplicação só enfileira — nenhum relay sobe. Detalhes,
propriedades e integração com JPA: [`outbox-spring-boot-starter/README.md`](outbox-spring-boot-starter/README.md).

## Quickstart — sem Spring

```java
OutboxConfig config = OutboxConfig.defaults();
MeterRegistry registry = new SimpleMeterRegistry();
JdbcOutboxWriter writer = new JdbcOutboxWriter(config, registry);

try (Connection connection = dataSource.getConnection()) {
    connection.setAutoCommit(false);
    // ... resto da transação de negócio na mesma connection ...
    writer.enqueue(connection, OutboxMessage.of("Pedido", pedido.getId(), pedido.getVersao(),
            "PedidoCriado", objectMapper.writeValueAsString(pedido), "pedidos.fifo"));
    connection.commit();
}

OutboxRelay relay = new OutboxRelay(dataSource, config, registry,
        Map.of("pedidos.fifo", new MessageSenderPublisher(new SnsMessageSender(snsClient, topicArn))));
relay.start(); // e relay.close() no shutdown

PurgeJob purgeJob = new PurgeJob(dataSource, config, registry);
purgeJob.start(); // e purgeJob.close() no shutdown
```

Detalhes: [`outbox-core/README.md`](outbox-core/README.md).

## Schema

`outbox-core` publica a migração Flyway `V202609211401__outbox_message.sql` no próprio jar,
em `classpath:db/outbox/postgresql`. Inclua esse location na sua configuração do Flyway
(no Spring Boot: `spring.flyway.locations=...,classpath:db/outbox/{vendor}`), ou copie o
SQL — o schema é público, não um detalhe de implementação.

A versão é um timestamp (`V<yyyyMMddHHmm>`) para não colidir com as migrações da aplicação
nem de outras bibliotecas no mesmo histórico do Flyway. Use timestamp também nas suas
migrações e ligue `out-of-order` (`spring.flyway.out-of-order=true`).

## Métricas

Todas via Micrometer, no `MeterRegistry` da aplicação (sem Actuator, a autoconfiguração cai num
`SimpleMeterRegistry` e a lib continua funcionando, só que sem exportar nada). Detalhe de desenho
e justificativa de cada uma em 02 §8.1.

**Nomes no Prometheus.** Os nomes abaixo são os do Micrometer. Counters saem como `_total`, timers
como `<nome>_seconds_count`, `_sum` e `_max`, e summaries como `<nome>_count`, `_sum` e `_max`.
Timers **não publicam quantis por padrão**: para `{quantile="0.99"}` configure
`management.metrics.distribution.percentiles.<nome>=0.99` (Spring Boot) ou o equivalente do seu registry.

**Cardinalidade.** Nenhum rótulo é `aggregate_id`, `message_id` ou `trace_id` — são ilimitados e
derrubam o backend de métricas. Investigar um agregado é trabalho de log e de consulta à tabela.

**Agregação entre instâncias** (coluna *Entre instâncias*):

- **`sum`**: counters, timers e summaries — cada instância conta o que fez; some as taxas.
- **`max`**: gauges globais — descrevem o **banco**, não a instância. Com N relays todos reportam o
  mesmo número; `sum` multiplica o backlog por N, justamente quando alguém escala o relay num incidente.
- **por instância**: descrevem a instância. Não use `max` nem `sum` sobre o gauge.

Séries com rótulo nascem no primeiro evento (não há `outbox_quarantined_total{destination="x"}` antes da
primeira quarentena daquele destino), então `increase()` não enxerga o primeiro incremento. As séries de
saúde da purga e do coletor, sem rótulo, nascem em zero.

### Escrita — instâncias que chamam `enqueue`

| Métrica | Tipo | Rótulos | O que mostra | Entre instâncias |
|---|---|---|---|---|
| `outbox_enqueue_total` | counter | `aggregate_type`, `destination`, `outcome` = `success` \| `rejected` (payload acima do limite) \| `error` (falha de banco ou chamada fora de transação) | Volume de escrita e quantas foram recusadas | `sum` |
| `outbox_enqueue_duration` | timer | `aggregate_type`, `destination` | Latência do `enqueue` dentro da transação de negócio: separa degradação da outbox de degradação do domínio | `sum` das taxas |
| `outbox_enqueue_payload_bytes` | summary | `destination` | Tamanho do payload em bytes, medido **antes** da validação: as rejeitadas também entram e mostram por quanto estouraram o limite | `sum` |

### Relay — instâncias com `MessagePublisher`

| Métrica | Tipo | Rótulos | O que mostra | Entre instâncias |
|---|---|---|---|---|
| `outbox_published_total` | counter | `destination` | Throughput de publicação | `sum` |
| `outbox_end_to_end_latency` | timer | `destination` | Do `created_at` à publicação: o SLA de propagação como distribuição. Inclui backoff e espera em fila. Compara o relógio de quem fez o `enqueue` com o do relay: desvio entre hosts entra no valor | `sum` das taxas |
| `outbox_publish_duration` | timer | `destination`, `outcome` = `success` \| `permanent_failure` \| `transient_failure` | Latência do broker | `sum` das taxas |
| `outbox_publish_failures_total` | counter | `destination`, `classification` = `permanent` \| `transient` | Distribuição de falhas de publicação | `sum` |
| `outbox_retry_scheduled_total` | counter | `destination` | Volume de reprocessamento (falhas transitórias que viraram backoff) | `sum` |
| `outbox_publish_attempts` | summary | `destination` | Tentativas até o sucesso, **contando a bem-sucedida** (1 = de primeira). Não inclui o que ainda não saiu nem o que foi para quarentena | `sum` |
| `outbox_quarantined_total` | counter | `destination` | Taxa de **entrada** em quarentena. O gauge `outbox_dead_letter_count` mostra o estoque; este mostra quando começou e a que ritmo | `sum` |
| `outbox_relay_claim_total` | counter | `outcome` = `acquired` \| `contended` \| `all_contended` \| `empty` | Saúde do paralelismo. `contended` conta **candidatos** que perderam o lock; os outros contam **ciclos**. `all_contended` alto = colisão de `hashtext` ou poucos fluxos quentes; `empty` alto é só ociosidade | `sum` |
| `outbox_relay_batch_size` | summary | — | Mensagens publicadas por reivindicação de fluxo (só registra quando publicou algo) | `sum` |
| `outbox_relay_cycle_duration` | timer | `outcome` = `acquired` \| `empty` \| `error` | Duração da transação do relay. Alerte sobre `outcome="acquired"`: ciclos vazios, a maioria no polling ocioso, diluem o máximo e os percentis. `outcome="error"` (em `_count`) é o registro de ciclos que falharam | `sum` das taxas |

### Purga — toda instância com `PurgeJob`, inclusive só-escritora

| Métrica | Tipo | Rótulos | O que mostra | Entre instâncias |
|---|---|---|---|---|
| `outbox_purge_deleted_total` | counter | — | Linhas `PUBLISHED` apagadas: eficácia da retenção | `sum` |
| `outbox_purge_duration` | timer | — | Duração de cada ciclo de purga, com sucesso ou falha | `sum` das taxas |
| `outbox_purge_failures_total` | counter | — | Ciclos de purga que falharam. Sem ele a tabela cresce sem aviso e o único rastro é um `ERROR` no log | por instância (`sum by (instance)`) |

### Gauges globais — estado do banco, atualizados a cada `outbox.metrics-interval` (15s)

Só existem em instâncias com relay (é o `MetricsCollector` que os alimenta, com uma consulta por rodada).

| Métrica | Tipo | Rótulos | O que mostra | Entre instâncias |
|---|---|---|---|---|
| `outbox_pending_count` | gauge | — | Tamanho do backlog | **`max`** |
| `outbox_oldest_pending_age_seconds` | gauge | — | **Alerta principal.** O maior de: idade da pendente mais antiga (sem filtro de disponibilidade) e tempo desde o último ciclo de relay. Continua subindo com relay morto **e** com o coletor falhando. É o que o health indicator lê | **`max`** |
| `outbox_backoff_pending_count` | gauge | — | Pendentes adiadas por backoff (`available_at` no futuro, sem as represadas por quarentena). Conta a que falhou **e** as que esperam atrás dela no fluxo, porque o backoff adia o fluxo inteiro. Diferencia backlog saudável de backlog em retry | **`max`** |
| `outbox_blocked_streams` | gauge | — | Fluxos `(aggregate_id, destination)` inelegíveis por quarentena | **`max`** |
| `outbox_dead_letter_count` | gauge | `destination` | Quarentenas vivas por destino. Volta a 0 quando a última é resolvida. Acompanhamento, não alerta | **`max`** |
| `outbox_oldest_dead_letter_age_seconds` | gauge | — | Idade da quarentena mais antiga não resolvida: distingue "duas de ontem" de "duas de março" | **`max`** |
| `outbox_multi_pending_streams` | gauge | — | Fluxos com dois ou mais pendentes ao mesmo tempo. Se ficar perto de zero, a reivindicação por fluxo está sendo paga por um caso que quase não ocorre | **`max`** |

### Saúde da própria instância

| Métrica | Tipo | Rótulos | O que mostra | Entre instâncias |
|---|---|---|---|---|
| `outbox_relay_last_cycle_age_seconds` | gauge | — | Tempo desde o último ciclo de relay concluído **nesta instância**: separa "este relay morreu" de "há backlog". Só existe com relay | por instância (ou `min`, para saber se algum relay está vivo) |
| `outbox_metrics_collector_failures_total` | counter | — | Rodadas do coletor que falharam. Enquanto sobe, os gauges globais estão velhos (só a idade do backlog continua subindo sozinha) | por instância (`sum by (instance)`) |
| `outbox_metrics_collector_duration` | timer | — | Duração de cada rodada do coletor. Vigia o custo do `count(*)` conforme o backlog cresce | por instância |

## Operação

### Consulta de diagnóstico — fluxo órfão (02 §5.3)

Um desbloqueio malfeito (`UPDATE` por `id` em vez de por fluxo) pode deixar mensagens
represadas em `available_at = 'infinity'` sem nenhum `DEAD_LETTER` que as justifique —
invisíveis tanto para o scan do relay quanto para `outbox_oldest_dead_letter_age_seconds`:

```sql
SELECT DISTINCT o.aggregate_id, o.destination
FROM outbox_message o
WHERE o.status = 'PENDING'
  AND o.available_at = 'infinity'
  AND NOT EXISTS (
        SELECT 1 FROM outbox_message d
        WHERE d.aggregate_id = o.aggregate_id
          AND d.destination  = o.destination
          AND d.status = 'DEAD_LETTER');
```

### Desbloqueio de quarentena (02 §5.3) — sempre por fluxo, nunca por `id`

```sql
UPDATE outbox_message
SET status = 'PENDING', available_at = now()
WHERE aggregate_id = '...' AND destination = '...' AND status <> 'PUBLISHED';
```

### Alertas (02 §8.2)

**Regra de agregação: `max`, nunca `sum`, entre instâncias.** Os gauges abaixo descrevem
o estado do banco, não da instância que os publica — com N relays, todos reportam o mesmo
número, e somar multiplica o backlog por N. As exceções (métricas por instância) estão marcadas
na seção [Métricas](#métricas), coluna *Entre instâncias*.

```yaml
groups:
  - name: outbox
    rules:
      - alert: OutboxBacklogEnvelhecendo
        expr: max(outbox_oldest_pending_age_seconds) > 300   # ajuste ao SLA de propagação
        for: 5m
        labels: { severity: alta }
        annotations:
          summary: "Broker fora, relay parado, fluxo em quarentena ou fluxo órfão — apure com a consulta de diagnóstico acima."

      - alert: OutboxFluxoBloqueado
        expr: max(outbox_blocked_streams) > 0
        for: 15m
        labels: { severity: media }
        annotations:
          summary: "Quarentena aguardando decisão humana (02 §5.3)."

      - alert: OutboxQuarentenaEsquecida
        expr: max(outbox_oldest_dead_letter_age_seconds) > 86400 * 3   # dias, não horas
        labels: { severity: media }
        annotations:
          summary: "DEAD_LETTER vivo há dias — todo evento novo do fluxo se acumula em 'infinity' sem prazo."

      # _max existe por padrão (quantis não): é o maior ciclo da janela recente do Micrometer, não um percentil.
      - alert: OutboxCicloDoRelayLento
        expr: max(outbox_relay_cycle_duration_seconds_max{outcome="acquired"}) > 4   # aproxime do publish-timeout
        labels: { severity: media }
        annotations:
          summary: "Transação do relay se aproximando do timeout — risco de horizonte de vacuum (02 §4.3)."

      # Contador POR INSTÂNCIA (cada coletor tem o seu): aqui sum/rate por instância, não a regra do max.
      - alert: OutboxColetorDeMetricasFalhando
        expr: sum by (instance) (increase(outbox_metrics_collector_failures_total[5m])) > 0
        for: 10m
        labels: { severity: media }
        annotations:
          summary: "Os gauges globais estão velhos; só a idade do backlog continua subindo sozinha."

      - alert: OutboxBacklogCrescendo
        expr: deriv(max(outbox_pending_count)[15m:]) > 0
        for: 15m
        labels: { severity: media }
        annotations:
          summary: "Taxa de escrita acima da taxa de publicação."
```

Alertar na **idade**, nunca na taxa de erro: taxa de erro sobe e desce sozinha e ensina o
time a ignorá-la; a idade da mensagem mais antiga só sobe quando alguém precisa agir.
