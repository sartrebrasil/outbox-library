# Biblioteca Outbox Pattern — Plano de Implementação

> Revisão 3 — acompanha o fechamento das dez pendências levantadas na revisão da revisão 2. A de maior alcance é a adoção do **fluxo** `(aggregate_id, destination)` como unidade de ordenação, reivindicação e bloqueio (§4.2). Registro completo em §12; configuração em §13.
> Requisitos: `01-requisitos-outbox-library.md`.

## 1. Visão Geral

Core agnóstico com módulos plugáveis.

```
outbox-core                → contratos, modelo, envelope, motor de retry,
                             implementação JDBC e relay de polling
outbox-broker-kafka        → MessagePublisher para Apache Kafka
outbox-broker-messaging    → MessagePublisher sobre MessageSender da messaging-library
                             (substitui outbox-broker-sns, -sqs e -rabbitmq)
outbox-spring-boot-starter → autoconfiguration + sobrecarga enqueue(msg)
```

Coordenadas: `groupId` `br.com.oobj.outbox`, artifactIds iguais aos nomes acima, versionamento semântico, licença proprietária interna — coerente com a decisão de não publicar em repositório remoto.

**A versão só chega a `1.0.0` depois da Fase 2.** Enquanto existir um único `MessagePublisher`, a SPI não foi validada (RF06.2) e estabilizar a versão seria prometer compatibilidade sobre uma interface que ninguém tentou implementar duas vezes. Até lá, `0.x`, onde quebrar é permitido e esperado.

A implementação JDBC e o relay de polling ficam dentro do core porque não há, hoje, uma segunda implementação de nenhum dos dois. Separá-los em módulos é uma refatoração de cinco minutos no dia em que existir — a diferença em relação aos brokers é que ali a segunda implementação já está prevista e tem conteúdo próprio (§5.4).

Ficam fora de escopo o módulo de CDC via Debezium e um módulo de persistência JPA separado, com os motivos em `01-requisitos-outbox-library.md` §6. O backend MongoDB está adiado (§7 do mesmo documento). Os brokers não estão em nenhuma das duas listas.

## 2. Modelo de Dados

Baseline: PostgreSQL 14 ou superior (requisitos RNF09). Tabela simples, sem particionamento (§2.4).

```sql
CREATE TABLE outbox_message (
    id                UUID PRIMARY KEY,
    seq               BIGINT GENERATED ALWAYS AS IDENTITY,
    aggregate_type    VARCHAR(255) NOT NULL,
    aggregate_id      VARCHAR(255) NOT NULL,
    aggregate_version BIGINT,                  -- opcional, fornecido pela aplicação (§3.6)
    event_type        VARCHAR(255) NOT NULL,
    payload           TEXT         NOT NULL,
    headers           TEXT,
    trace_id          VARCHAR(64),
    destination       VARCHAR(255) NOT NULL,
    status            VARCHAR(20)  NOT NULL,   -- PENDING | PUBLISHED | DEAD_LETTER
    attempts          INT          NOT NULL DEFAULT 0,
    last_error        TEXT,
    created_at        TIMESTAMPTZ  NOT NULL DEFAULT now(),
    available_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    processed_at      TIMESTAMPTZ
);

-- Scan do relay. available_at lidera para que o range scan pare antes das linhas
-- em backoff e nunca alcance as bloqueadas por quarentena, marcadas 'infinity' (§4.2).
-- (aggregate_id, destination) é o fluxo, unidade de reivindicação e de ordem (§4.2).
CREATE INDEX idx_outbox_pending
    ON outbox_message (available_at, aggregate_id, destination, seq)
    WHERE status = 'PENDING';

-- Verificação de bloqueio por quarentena. Minúsculo: DEAD_LETTER deve ser raro.
CREATE INDEX idx_outbox_dead_letter
    ON outbox_message (aggregate_id, destination)
    WHERE status = 'DEAD_LETTER';

CREATE INDEX idx_outbox_trace
    ON outbox_message (trace_id)
    WHERE trace_id IS NOT NULL;

-- Varredura da purga (§6). Sem ele, a retenção faz seq scan na tabela inteira
-- a cada ciclo — exatamente na tabela que a purga existe para manter pequena.
CREATE INDEX idx_outbox_purge
    ON outbox_message (processed_at)
    WHERE status = 'PUBLISHED';

-- Métrica de idade (§8.1). Deliberadamente SEM filtro de available_at: precisa
-- enxergar o que o scan do relay não enxerga — backoff, quarentena e órfãos.
CREATE INDEX idx_outbox_age
    ON outbox_message (created_at)
    WHERE status = 'PENDING';
```

### 2.1 Por que `seq` e não `created_at`

`created_at` não é determinístico: duas mensagens do mesmo agregado gravadas na mesma transação podem ter timestamp idêntico, e a ordenação entre elas fica indefinida. `seq` é monotônico dentro de uma sessão e resolve o empate.

### 2.2 O que `seq` não resolve

`GENERATED AS IDENTITY` aloca o número **antes** do commit. Uma transação lenta pega 100, uma rápida pega 101 e commita primeiro. Um relay que lesse `ORDER BY seq` globalmente publicaria 101 antes de 100 existir.

Duas coisas neutralizam isso:

1. O relay nunca usa `seq` como marca d'água global. Ele varre por `status = 'PENDING'` e ordena por `seq` **apenas dentro do fluxo já reivindicado**. Um registro que commita tarde simplesmente aparece numa rodada posterior.
2. Dentro de um agregado, a ordem de `seq` só coincide com a ordem de commit se as escritas daquele agregado forem serializadas pela aplicação — a pré-condição RF15.1. Essa é uma propriedade do domínio, não da biblioteca, e está documentada como tal.

### 2.3 Status removidos

Não existem `FAILED`, `PROCESSING`, `IN_FLIGHT` nem `BLOCKED`. Falha transitória mantém o registro em `PENDING` com `attempts` incrementado e `available_at` adiado. Processamento em curso é representado pelo advisory lock (§4.2.2), não por coluna. Bloqueio é derivado da existência de um `DEAD_LETTER` no mesmo fluxo (§5.2).

### 2.4 Por que não particionar

A revisão anterior previa particionamento por tempo em `created_at` desde o início. Foi retirado do MVP (requisitos RF31). Três razões, em ordem de peso:

1. **O ganho pretendido não se realiza.** A ideia era trocar `DELETE` em massa por `DROP PARTITION`. Mas o drop exige que a partição inteira seja descartável, e a tabela retém por construção registros que não expiram: um `DEAD_LETTER` fica até alguém agir (§5.3) e congela atrás de si todos os pendentes do mesmo fluxo (§5.2). Uma linha em quarentena imobiliza a partição, e o caso em que a purga mais importa — tabela grande, incidente antigo — é justamente o que produz essas linhas.
2. **Custo de schema.** Tabela particionada exige que a chave de partição participe de toda constraint unique, transformando a chave primária em `(id, created_at)`.
3. **Custo de baseline.** `GENERATED ALWAYS AS IDENTITY` em tabela particionada só é aceito em versões recentes do PostgreSQL. Manter o particionamento obrigaria a elevar a baseline acima de 14 ou a trocar a coluna por sequência explícita com `DEFAULT nextval(...)`.

Nada disso é intransponível; o que falta é a medição que justifique pagar por isso. A medida inicial é índice parcial mais purga em lote (§6), e o caminho de evolução — sequência explícita, chave primária composta e uma partição de retidos separada da de publicados — fica registrado para o dia em que a purga não sustentar o volume.

## 3. Contrato de Escrita

### 3.1 Assinatura canônica

```java
public interface OutboxWriter {
    void enqueue(Connection connection, OutboxMessage message);
}
```

A `Connection` é sempre do chamador. O core não possui `ConnectionProvider`, `TransactionContext`, `ThreadLocal` nem nada que resolva a conexão implicitamente. Uma SPI ali esconderia exatamente a linha que justifica a existência da biblioteca — e, se mal configurada, abriria uma conexão nova e quebraria a atomicidade sem barulho.

### 3.2 Uso em Spring

O starter fornece a sobrecarga conveniente:

```java
public final class SpringOutboxWriter {

    private final OutboxWriter delegate;
    private final DataSource dataSource;

    public void enqueue(OutboxMessage message) {
        Connection connection = DataSourceUtils.getConnection(dataSource);
        try {
            delegate.enqueue(connection, message);
        } finally {
            DataSourceUtils.releaseConnection(connection, dataSource);
        }
    }
}
```

Quinze linhas no starter, zero abstração no core.

### 3.3 Uso com JPA/Hibernate

```java
session.doWork(connection -> writer.enqueue(connection, message));
```

Mesma `Connection` física, mesmo commit. A ordem de flush do Hibernate é irrelevante porque os dois inserts commitam juntos ou nenhum commita. A linha da outbox é invisível para o contexto de persistência, o que é intencional — ninguém lê a outbox via JPA. Não criar chave estrangeira da outbox para a tabela de negócio: o flush da entidade pode ocorrer depois do insert do outbox.

**Armadilha a documentar em destaque:** enfileirar o evento antes do flush da entidade produz payload com ID gerado ainda nulo.

### 3.4 Fail-fast

```java
if (connection.getAutoCommit()) {
    throw new IllegalStateException("enqueue() chamado fora de uma transação ativa");
}
```

Três linhas, pega a maioria dos erros reais. **Não é uma prova de atomicidade:** uma conexão pega direto do pool também tem auto-commit desativado e passaria. No starter, complementar com `TransactionSynchronizationManager.isActualTransactionActive()`, que é confiável dentro do Spring. A garantia de verdade é o teste de integração (§9).

### 3.5 Validação no enqueue

Serialização e verificação de tamanho acontecem aqui, dentro da transação de negócio. Payload que não serializa ou que excede o limite do destino lança exceção e aborta o commit. Esse é o único lugar onde o erro ainda tem contexto e dono; deixá-lo chegar ao relay o transforma num registro morto sem culpado.

### 3.6 Origem do `aggregate_version`

Quem fornece o número é a **aplicação**, na própria chamada de `enqueue`. A biblioteca transporta e não valida (requisitos RF30 e RF30.1).

```java
writer.enqueue(connection, OutboxMessage.of(
        "Pedido", pedido.getId(), pedido.getVersion(), "PedidoCriado", payload, "pedidos.fifo"));
```

Em DDD com versionamento otimista, `pedido.getVersion()` já é exatamente o número que se quer: monotônico por agregado, incrementado a cada escrita e protegido pelo mesmo mecanismo que satisfaz a pré-condição de ordenação (requisitos RF15.1 e RF15.2). Pedir à aplicação um valor que ela já tem em mãos custa um parâmetro.

O campo é **opcional**. Quando nulo, a coluna fica nula, o envelope omite `aggregateVersion` (§7) e o consumidor perde a detecção de lacunas — nada mais muda. Uma aplicação sem versionamento de agregado continua usando a biblioteca inteira.

As alternativas foram descartadas na revisão 3:

- **Calcular na lib.** `SELECT max(aggregate_version) + 1 FROM outbox_message WHERE aggregate_id = ?` dentro da transação adiciona um round-trip no caminho crítico da requisição do usuário — o mesmo caminho que §8.1 instrumenta separadamente justamente por ser sensível. Pior: duas transações concorrentes sobre o mesmo agregado leem o mesmo máximo e gravam a mesma versão, silenciosamente. Só funciona onde a aplicação já serializa as escritas — e onde ela serializa, o número já existe no domínio. É custo onde é desnecessário e defeito onde seria necessário.
- **Reaproveitar `seq`.** `seq` é global à tabela: entre duas mensagens do mesmo agregado cabem centenas de outras. Lacuna é o caso normal, então o consumidor não consegue distinguir "faltou mensagem" de "passou gente no meio", que é a única coisa que `aggregateVersion` existe para responder.

### 3.7 `OutboxMessage` e serialização

```java
public record OutboxMessage(
        UUID                messageId,
        String              aggregateType,
        String              aggregateId,
        Long                aggregateVersion,   // opcional (§3.6)
        String              eventType,
        String              payload,            // JSON já serializado
        Map<String, String> headers,            // opcional
        String              destination,
        Instant             occurredAt) {

    public static OutboxMessage of(String aggregateType, String aggregateId, Long aggregateVersion,
                                   String eventType, String payload, String destination) {
        return new OutboxMessage(UUID.randomUUID(), aggregateType, aggregateId, aggregateVersion,
                eventType, payload, Map.of(), destination, Instant.now());
    }
}
```

`messageId` e `occurredAt` são preenchidos pela factory quando não informados. O construtor canônico permanece acessível para quem precisa reconstruir uma mensagem — em teste, por exemplo — sem que a biblioteca invente valores por trás.

**O `payload` chega como `String` já serializada.** O core não conhece Jackson nem qualquer biblioteca de serialização, o que é o que mantém RNF01 verdadeiro em vez de aspiracional. O starter oferece a sobrecarga conveniente, usando o `ObjectMapper` que já existe no contexto:

```java
// no starter
public void enqueue(String aggregateType, String aggregateId, Long version,
                    String eventType, Object payload, String destination) {
    enqueue(OutboxMessage.of(aggregateType, aggregateId, version, eventType,
            objectMapper.writeValueAsString(payload), destination));
}
```

Isso preserva RF21: a serialização continua acontecendo dentro da transação de negócio, porque é o código da aplicação que a dispara ao chamar `enqueue`. O que muda é onde a dependência mora. A validação de tamanho, essa sim, é do core — ela opera sobre a `String` e não precisa saber como ela foi produzida.

A alternativa de uma SPI de serialização foi descartada: seria uma interface com uma implementação, para um problema que a fronteira do tipo `String` já resolve.

**Nota sobre `messageId` e bloat.** `UUID.randomUUID()` gera v4, aleatório, o que espalha inserções por toda a extensão do índice da chave primária e amplifica a fragmentação numa tabela já descrita como quente em §6. Um identificador ordenável no tempo (UUID v7 ou ULID) mantém as inserções à direita do índice. Java 21 não traz v7 nativo e a implementação são poucas linhas; a decisão de pagá-las fica registrada aqui, junto do motivo, para ser tomada com o resto do trabalho de retenção.

## 4. Relay de Polling

### 4.1 Por que não `SKIP LOCKED` linha a linha

Com `SELECT ... FOR UPDATE SKIP LOCKED` sobre linhas individuais, um segundo worker pula a linha travada e pega a próxima — que pode ser a mensagem N+1 do mesmo fluxo cuja mensagem N está em voo. A ordenação morre na própria query, antes de qualquer discussão sobre quarentena. A reivindicação não pode ser por linha; qual é a unidade certa está em §4.2.

### 4.2 Fluxo: a unidade é o par `(aggregate_id, destination)`

**Fluxo** é o par agregado × destino. É a unidade de reivindicação, de ordenação e de bloqueio — não o agregado sozinho, como nas revisões anteriores.

A razão é o modo de falha que o agregado sozinho produzia: uma mensagem de `Pedido/123` para o Kafka e outra para o SNS ficam na mesma fila de publicação, então o SNS fora do ar segura também o que ia para o Kafka, e uma quarentena num broker congela o outro. Acoplar dois brokers que não têm relação é caro, e o que se compra com isso é a ordem entre destinos diferentes — uma garantia que **nenhum consumidor observa**, já que ninguém consome dois destinos e compara a sequência entre eles. Garantia que ninguém consome, cobrada de todo mundo.

Com o fluxo como unidade, a promessa da §3 dos requisitos passa a ser: mensagens do mesmo agregado **para o mesmo destino** são publicadas na ordem de `seq`. Entre destinos não há ordem, o que já era verdade na prática do lado do broker.

Custo de implementação: o `objid` do advisory lock passa a ser o hash do par, e as cláusulas de bloqueio e de carga ganham `destination`. Colisão de hash entre pares distintos continua sendo perda de paralelismo, nunca incorretude.

### 4.2.1 `available_at` é propriedade do fluxo

Regra da qual todo o resto depende: **`available_at` descreve o fluxo, não a mensagem isolada.** Toda transição que adia um fluxo adia todas as suas mensagens pendentes no mesmo `UPDATE`:

| Evento | Efeito em `available_at` das pendentes do fluxo |
|---|---|
| Falha transitória | `now() + backoff` |
| Quarentena (`PermanentPublishException`) | `'infinity'` |
| Desbloqueio manual (§5.3) | `now()` |

Isso decorre de algo que já era verdade: a publicação para no primeiro erro e nunca pula mensagem (§4.4), então um fluxo com a mensagem N adiada já tinha, na prática, N+1 em diante indisponíveis. A revisão anterior mantinha isso implícito e pagava duas vezes por isso — precisava de `DISTINCT ON` para descobrir qual era a primeira pendente só para ler o `available_at` dela, e deixava as mensagens bloqueadas no índice quente para sempre (pendência P02). Escrever a indisponibilidade em todas as linhas elimina as duas coisas.

O custo é um `UPDATE` de N linhas em vez de uma quando um fluxo falha. N é pequeno por hipótese — é a mesma premissa que a questão em aberto 1 dos requisitos existe para medir — e só ocorre no caminho de falha.

**`'infinity'` não é um estado.** A verdade sobre a quarentena continua sendo a existência de uma linha `DEAD_LETTER` no fluxo, e a query abaixo continua verificando isso com `NOT EXISTS` (§5.2). O `available_at = 'infinity'` é otimização de índice: se divergir da verdade, o `NOT EXISTS` ainda impede a publicação, e o pior caso é a linha voltar a ser percorrida no scan — o comportamento da revisão anterior. É cache, e falha como cache.

O índice do relay lidera por `available_at` e fecha pelo fluxo:

```sql
CREATE INDEX idx_outbox_pending
    ON outbox_message (available_at, aggregate_id, destination, seq)
    WHERE status = 'PENDING';
```

O scan vira um range scan `available_at <= now()` que **para** antes de alcançar as linhas em backoff e nunca chega perto das linhas em `'infinity'`. Mensagens de fluxos em quarentena continuam fisicamente na tabela e no índice, mas deixam de ser percorridas — é isso que fecha P02. O custo de cada incidente de quarentena não resolvido deixa de crescer com o tempo e passa a ser apenas espaço, tratado por §6.

### 4.2.2 Reivindicação em dois passos

**Passo 1 — candidatos, sem lock nenhum:**

```sql
SELECT o.aggregate_id, o.destination, min(o.seq) AS primeiro_seq
FROM outbox_message o
WHERE o.status = 'PENDING'
  AND o.available_at <= now()
  AND NOT EXISTS (
        SELECT 1
        FROM outbox_message d
        WHERE d.aggregate_id = o.aggregate_id
          AND d.destination  = o.destination
          AND d.status       = 'DEAD_LETTER')
GROUP BY o.aggregate_id, o.destination
ORDER BY min(o.seq)
LIMIT 50;
```

**Passo 2 — lock explícito, no cliente, um candidato por vez:**

```java
for (Fluxo fluxo : candidatos) {
    if (tryAdvisoryLock(connection, fluxo)) {
        metrics.claim("acquired");
        return fluxo;
    }
    metrics.claim("contended");
}
metrics.claim("empty");
return null;
```

```sql
-- tryAdvisoryLock
SELECT pg_try_advisory_xact_lock(1329748044, hashtext(? || '|' || ?));
```

Os dois passos existem por causa da segunda metade de P03: chamar `pg_try_advisory_xact_lock` dentro de um predicado combinado com `LIMIT 1` **não** garante que apenas o fluxo retornado seja travado. O executor pode avaliar o predicado em vários candidatos antes de parar, e um lock de escopo transacional adquirido sobre um candidato descartado só é liberado no commit — travando fluxos que este worker não vai processar. Não há formulação declarativa confiável para isso: a ordem de avaliação de uma função volátil em um predicado não é um contrato do planner. Adquirir o lock explicitamente, uma chamada por candidato, é determinístico e torna `outbox_relay_claim_total{outcome="contended"}` um número real em vez de uma estimativa.

O laço custa um round-trip por candidato disputado. Sem contenção, é um. Com contenção alta, é o sinal que a métrica existe para mostrar.

**A constante `1329748044` é o `classid` reservado da biblioteca** — os bytes ASCII de `OBXL`. Essa é a primeira metade de P03: advisory locks são globais ao banco de dados, não ao schema, e a forma de um argumento usada na revisão anterior colocava o relay no mesmo espaço de nomes que qualquer advisory lock da própria aplicação. Uma colisão ali não custa paralelismo, custa bloqueio mútuo entre a biblioteca e o domínio, sem relação causal visível de nenhum dos dois lados. Com a variante de dois argumentos, o `classid` isola a biblioteca e o `objid` distingue os fluxos.

Duas instalações da biblioteca no mesmo banco compartilham o `classid` e podem serializar entre si quando têm fluxos de mesmo nome. É perda de paralelismo, nunca incorretude — o mesmo patamar da colisão de `hashtext`, já aceito em §4.3. Se algum dia houver duas outbox no mesmo banco com contenção medida, o `classid` vira propriedade de configuração; até lá, é uma constante.

Depois de reivindicado, o worker carrega as mensagens do fluxo com `WHERE aggregate_id = ? AND destination = ? AND status = 'PENDING' ORDER BY seq` e publica em sequência.

### 4.2.3 Seleção do publisher

O relay resolve o `MessagePublisher` pelo `destination` do fluxo reivindicado, num `Map<String, MessagePublisher>` montado na inicialização. Quando há um único publisher registrado, ele atende todos os destinos e nenhuma configuração de roteamento é necessária — o caso de quem usa um broker só não paga nada pela existência do mapa.

Destino sem publisher correspondente é erro de configuração, não de runtime: o relay falha na inicialização listando os destinos órfãos, em vez de descobrir o problema quando a primeira mensagem daquele destino aparecer. Como o conjunto de destinos vivos não é conhecido na partida — vem das linhas da tabela —, a verificação cobre o que está configurado e, no ciclo, um destino desconhecido é tratado como falha transitória do fluxo: ele fica em backoff, não em quarentena, e o alerta de idade expõe a situação sem que nada seja descartado.

### 4.3 Por que advisory lock em vez de lease

`pg_try_advisory_xact_lock` é escopado à transação: liberado no commit, no rollback **e na queda da conexão**. Isso resolve de graça a janela de crash do relay — se o processo morre no meio da publicação, o lock cai junto e o fluxo volta a ser elegível na rodada seguinte, sem coluna de lease, sem job de expiração e sem fluxo órfão congelado.

O preço é manter uma transação aberta durante a chamada de rede ao broker. Consequências e mitigações:

- Transação longa no PostgreSQL segura o horizonte de vacuum. Mitigação: batches pequenos e timeout de publicação curto e obrigatório, imposto conforme §4.3.1.
- `hashtext` pode colidir entre dois fluxos distintos, seja por colisão do próprio hash, seja pela concatenação `aggregate_id || '|' || destination` quando um `aggregate_id` contém `|`. Nos dois casos o efeito é serialização desnecessária entre dois fluxos, nunca incorretude. Aceitável.
- O scan de candidatos varre os registros pendentes **disponíveis** do índice parcial (§4.2.1). Com backlog pequeno é barato; com backlog grande degrada. O backlog grande já é a condição que `outbox_oldest_pending_age_seconds` alerta, então a degradação acompanha um alarme que já deveria ter disparado. O que não degrada mais com o tempo é o acúmulo de quarentena, que sai do range scan por construção.

Se a transação longa se mostrar inviável sob carga, o caminho de evolução é trocar o advisory lock por lease com expiração — pagando, aí sim, o custo do órfão e da limpeza.

### 4.3.1 Como o timeout de publicação é imposto

A mitigação acima depende inteiramente de a publicação terminar rápido, e o core **não consegue garantir isso sozinho**. `publish()` é uma chamada síncrona para código de terceiro: envolvê-la num `ExecutorService` e chamar `Future.cancel(true)` não desbloqueia um socket em espera; a thread continua presa e a transação continua aberta. O que se ganharia é um pool de threads que parece resolver o problema e não resolve — e que esconde o vazamento atrás de um `TimeoutException` no relay enquanto a thread real segue viva.

A imposição fica em duas camadas (requisitos RF26):

**Camada 1 — contrato do SPI.** O timeout é parâmetro da chamada:

```java
public interface MessagePublisher {
    void publish(OutboxEnvelope envelope, Duration timeout) throws PermanentPublishException;
}
```

Cada módulo de broker traduz o `Duration` para o mecanismo nativo do seu client: `request.timeout.ms` e `delivery.timeout.ms` no Kafka, `apiCallTimeout` e `apiCallAttemptTimeout` no AWS SDK, timeout de confirmação no RabbitMQ. É o mesmo raciocínio de §5.4 — quem conhece o broker é o módulo. O README de cada módulo declara como o parâmetro é aplicado, e ignorá-lo é violação de contrato.

**Camada 2 — rede de segurança no banco.** A conexão que o relay usa é aberta com:

```sql
SET idle_in_transaction_session_timeout = '<timeout de publicação + margem>';
```

Essa camada existe porque a primeira depende da boa-fé de código que a biblioteca não controla. A observação que a torna possível: durante a chamada de rede ao broker, a sessão PostgreSQL do relay está **ociosa dentro de uma transação aberta** — não executando nada. É exatamente a condição que `idle_in_transaction_session_timeout` detecta. Se o publisher travar apesar do contrato, o próprio banco encerra a sessão, aborta a transação e libera o advisory lock.

O que acontece depois é o modo de falha já aceito: a mensagem permanece `PENDING`, o fluxo volta a ser elegível na rodada seguinte e, se a publicação de fato chegou ao broker antes da sessão morrer, o consumidor recebe uma duplicata — at-least-once (§4.5). A degradação cai precisamente onde a arquitetura já a previa, e o horizonte de vacuum fica protegido por um mecanismo que não depende de nenhuma implementação de publisher se comportar.

O relay complementa com um log em `ERROR` quando um ciclo excede o timeout configurado: não interrompe nada, mas é o que torna um publisher mal-comportado visível antes de ele aparecer como bloat de tabela.

### 4.4 Ciclo de publicação

1. Reivindica um fluxo (§4.2.2) e resolve o publisher do destino (§4.2.3).
2. Carrega as mensagens pendentes do fluxo em ordem de `seq`.
3. Para cada mensagem, chama `publisher.publish(envelope, timeout)` (§4.3.1).
   - Sucesso: `status = 'PUBLISHED'`, `processed_at = now()`.
   - `PermanentPublishException`: `status = 'DEAD_LETTER'` na mensagem, grava `last_error`, marca as demais pendentes do fluxo com `available_at = 'infinity'`, **interrompe o processamento do fluxo**.
   - Qualquer outra exceção: incrementa `attempts` da mensagem, calcula o backoff exponencial limitado e aplica o novo `available_at` **a todas as pendentes do fluxo**, **interrompe o processamento do fluxo**.
4. Commit — libera o advisory lock.

Interromper no primeiro erro é o que preserva a ordem. Nunca pular para a próxima mensagem do mesmo fluxo. Mensagens do mesmo agregado para **outros** destinos seguem em fluxos próprios e não são afetadas (§4.2).

Os dois caminhos de erro escrevem `available_at` no fluxo inteiro, não só na mensagem que falhou (§4.2.1):

```sql
-- falha transitória
UPDATE outbox_message
SET available_at = now() + ?::interval,
    attempts    = attempts + CASE WHEN id = ? THEN 1 ELSE 0 END,
    last_error  = CASE WHEN id = ? THEN ? ELSE last_error END
WHERE aggregate_id = ? AND destination = ? AND status = 'PENDING';
```

`attempts` continua sendo por mensagem — é o contador de tentativas daquela publicação, e só a mensagem que de fato falhou tentou. O que é do fluxo é a indisponibilidade.

### 4.5 Idempotência do relay

O `publish()` e o `UPDATE status = 'PUBLISHED'` não são atômicos entre si. Crash entre os dois republica a mensagem na rodada seguinte. Isso é at-least-once e é aceito. Note que a republicação **não quebra a ordem**: é a mesma mensagem reenviada, não um salto. O consumidor deduplica por `message_id` e, quando a aplicação produtora fornece o campo (§3.6), pode usar `aggregate_version` para detectar lacunas.

A sessão encerrada pelo `idle_in_transaction_session_timeout` (§4.3.1) cai neste mesmo caso: é um crash de relay visto do lado do banco, e não introduz um modo de falha novo.

### 4.6 Ciclo de vida e concorrência

```java
public final class OutboxRelay implements AutoCloseable {
    public void start();
    @Override public void close();
}
```

Sem Spring, a aplicação instancia e chama `start()` (RF11). Com Spring, o starter registra o bean e o container cuida do ciclo. Por dentro é um `ScheduledExecutorService` com `relay.workers` threads (default `1`), cada uma executando o ciclo de §4.4 a cada `polling-interval`.

Não há coordenação entre workers, nem dentro da instância nem entre instâncias: o advisory lock já é o mecanismo de exclusão mútua, e ele não distingue duas threads de dois processos. Subir o paralelismo é mudar um número.

**Cada worker precisa da própria conexão.** Advisory locks são de sessão: dois workers compartilhando uma conexão veriam os locks um do outro como próprios, e ambos processariam o mesmo fluxo simultaneamente — exatamente a falha de ordenação que toda a §4 existe para impedir. É a única regra de implementação aqui que não é negociável, e o teste de concorrência de §9 a cobre por consequência.

**Encerramento: não há nada a construir.** `close()` interrompe o agendamento e aguarda o ciclo em voo até um limite. Se o limite estourar, ou se o processo morrer de forma abrupta, ou se o banco encerrar a sessão por `idle_in_transaction_session_timeout`, o resultado é o mesmo nos três casos: a transação sofre rollback, o advisory lock cai com a conexão e o fluxo volta a ser elegível na rodada seguinte.

Isto está escrito porque a ausência de mecanismo é fácil de confundir com esquecimento: **não existe lock órfão, e portanto não deve existir job de limpeza de lock órfão.** Foi precisamente por isso que o advisory lock foi escolhido em vez de lease com expiração (§4.3). Adicionar a limpeza depois seria reintroduzir o custo que a escolha evitou.

## 5. Retry e Quarentena

### 5.1 Política padrão

- `max-attempts = -1`, ou seja, retry infinito.
- Backoff exponencial com teto (padrão: 5 minutos).
- Broker fora do ar não é motivo para descartar mensagem. É motivo para alerta.

A biblioteca **não classifica** falhas. Toda exceção é transitória, exceto `PermanentPublishException`, lançada pelo `MessagePublisher` do usuário. A tentação de classificar automaticamente por tipo de erro do broker foi descartada: "tópico inexistente" e "schema ainda não propagado" parecem permanentes e frequentemente são transitórios, e classificar errado produz perda silenciosa.

### 5.2 Bloqueio derivado

Não existe status `BLOCKED`. O bloqueio é a cláusula `NOT EXISTS` da query de candidatos (§4.2.2): enquanto houver um registro em `DEAD_LETTER` para o fluxo, o fluxo inteiro fica inelegível. Estado derivado, não duplicado — nada para manter em sincronia. O bloqueio é do fluxo, não do agregado: uma quarentena no Kafka não impede a publicação do mesmo agregado no SNS (§4.2).

O `available_at = 'infinity'` gravado nas mensagens pendentes do fluxo em quarentena (§4.2.1) **não** é um segundo estado. É o que tira essas linhas do range scan do índice quente, e nada mais depende dele: se a marcação faltar ou ficar defasada, o `NOT EXISTS` continua impedindo a publicação e o único efeito é a linha voltar a ser percorrida no scan. A ordem de precedência importa e está registrada aqui para não ser invertida numa otimização futura: o `NOT EXISTS` decide, o `'infinity'` apenas economiza leitura.

A alternativa de avançar e deixar a lacuna foi descartada: produz um consumidor recebendo `PedidoAtualizado` sem nunca ter recebido `PedidoCriado`, e essa é a única classe de falha que ninguém descobre. Um fluxo congelado gera um chamado; um buraco na sequência gera um bug irreparável.

### 5.3 Desbloqueio

O desbloqueio é **por fluxo**, não por linha:

```sql
UPDATE outbox_message
SET status       = 'PENDING',
    available_at = now()
WHERE aggregate_id = '...'
  AND destination  = '...'
  AND status <> 'PUBLISHED';
```

Uma única instrução devolve a quarentena para `PENDING` e traz de volta do `'infinity'` as mensagens que ficaram represadas atrás dela. É também a semântica correta: RF28 bloqueia o fluxo, então é o fluxo que se desbloqueia.

O `UPDATE` por `id` da revisão anterior deixou de ser suficiente e não deve ser usado — ele limparia o `DEAD_LETTER` sem tirar as demais do `'infinity'`, produzindo um fluxo **órfão**: passa no `NOT EXISTS`, mas nunca aparece no range scan do relay. O sintoma é um fluxo parado depois de um desbloqueio aparentemente bem-sucedido.

É o estado que motivou a forma de cálculo de `outbox_oldest_pending_age_seconds` em §8.1. Como as mensagens órfãs não estão em quarentena, `outbox_oldest_dead_letter_age_seconds` não as vê; e como estão em `'infinity'`, o scan do relay também não. Só uma métrica de idade calculada sobre **todas** as pendentes, sem filtro de disponibilidade, expõe esse caso — e por isso ela não pode ser derivada do scan. Consulta de diagnóstico, no README:

```sql
-- pendentes represadas sem quarentena que as justifique
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

Documentado no README. CLI e endpoint administrativo ficam para quando houver demanda — e são a resposta definitiva para o risco acima, já que uma operação de duas linhas de SQL feita à mão durante um incidente é exatamente onde se erra o `WHERE`.

### 5.4 Conteúdo de um módulo de broker

A chamada de publicação em si são umas quarenta linhas. O que justifica cada módulo existir é a **taxonomia de erro**: traduzir as exceções nativas do broker em permanente ou transitória, cumprindo o RF27 no único lugar que tem conhecimento para isso.

| Broker | Permanente (`PermanentPublishException`) | Transitória (retry) |
|---|---|---|
| Kafka | `RecordTooLargeException`, `InvalidTopicException`, `SerializationException` | `TimeoutException`, `NotLeaderOrFollowerException`, `RetriableException` em geral |
| SNS / SQS | `InvalidParameterException`, `KmsAccessDenied`, payload acima do limite | `ThrottlingException`, `ServiceUnavailable`, falha de rede |
| RabbitMQ | `nack` com rejeição definitiva, mensagem acima do frame máximo | canal ou conexão caída, broker indisponível |

Classificar errado é o modo de falha caro: marcar como permanente algo transitório descarta mensagem, e o inverso trava um fluxo para sempre. Na dúvida, transitória — o alerta de idade (§8.2) expõe o problema sem perder dado.

Cada módulo também define a chave de agrupamento recomendada para preservar a ordem no lado do broker, que é recomendação de uso e não garantia da lib (requisitos §3): `aggregate_id` como key do `ProducerRecord` no Kafka, `MessageGroupId` em SNS e SQS FIFO, consistent hashing exchange no RabbitMQ.

## 6. Retenção e Saúde da Tabela

A tabela outbox é uma tabela quente com muitos `UPDATE`, o que no PostgreSQL significa tuplas mortas, amplificação de escrita, bloat de índice e pressão de autovacuum. Esse é o problema que derruba implementações de outbox em produção — mais do que qualquer política de quarentena.

Medidas previstas desde o início:

- Índice parcial sobre `status = 'PENDING'`: registros publicados saem do índice do relay automaticamente.
- Índice parcial `idx_outbox_purge` sobre `processed_at WHERE status = 'PUBLISHED'`: sem ele, cada ciclo de purga faz seq scan na tabela que a purga existe para manter pequena.
- Job de purga com retenção configurável, operando em lote limitado e repetido até não haver mais elegíveis, para não produzir uma transação de exclusão longa — que causaria o mesmo problema de horizonte de vacuum que §4.3 evita no relay.
- Monitoramento de `n_dead_tup` na tabela.

O particionamento foi retirado do MVP; as razões estão em §2.4 e o caminho de evolução também.

**O que a purga não cobre, e por que continua assim.** A retenção elimina `PUBLISHED`. Registros `DEAD_LETTER` e os `PENDING` represados atrás deles (§5.2) não têm prazo: permanecem até alguém decidir sobre eles (§5.3). A revisão 3 fechou P01 decidindo **não criar política de retenção para eles**, e sim expor a idade:

- Descartar quarentena por tempo contradiz a decisão central da biblioteca. Toda a §5 existe para nunca perder mensagem; uma cláusula que apaga o que ficou parado tempo demais devolve pela porta dos fundos exatamente a perda silenciosa que §5.2 recusou pela porta da frente.
- Arquivar em tabela fria resolve espaço, e espaço não é o problema: uma linha em quarentena são alguns KB, e desde §4.2.1 elas também não custam mais tempo de scan.
- O problema real é **ninguém agir**. Uma quarentena viva bloqueia o fluxo, e todo evento novo daquele fluxo se empilha em `'infinity'` sem limite. É um vazamento lento cuja causa é humana, e a contagem de fluxos bloqueados não a revela: dois fluxos bloqueados parecem iguais tendo a quarentena um dia ou seis meses.

Daí `outbox_oldest_dead_letter_age_seconds` (§8.1) e o alerta correspondente em §8.2, medidos em dias. Arquivamento volta à mesa se a métrica mostrar quarentenas vivendo semanas como situação normal — aí o problema deixou de ser um incidente esquecido e virou volume, que é outra conversa.

**O que a quarentena custa no caminho quente.** Resolvido na revisão 3 (§4.2 e §4.2.1): as mensagens represadas atrás de uma quarentena recebem `available_at = 'infinity'` e ficam fora do range scan do relay. Elas continuam ocupando espaço na tabela e no índice — o que é assunto da retenção, acima — mas deixam de custar tempo em todo ciclo. O custo de um incidente de quarentena não resolvido parou de crescer com o tempo.

## 7. Envelope

```json
{
  "messageId": "uuid",
  "aggregateType": "Pedido",
  "aggregateId": "123",
  "aggregateVersion": 7,
  "eventType": "PedidoCriado",
  "occurredAt": "2026-09-20T12:00:00Z",
  "headers": { "traceparent": "00-...-01" },
  "payload": { }
}
```

`messageId` é estável entre republicações — é a chave de deduplicação do consumidor e é sempre emitido.

`aggregateVersion` é **opcional e fornecido pela aplicação** (§3.6). Quando presente, permite ao consumidor detectar lacunas e descartar reprocessamentos; quando ausente, o campo é omitido do envelope — não emitido como `null`, para que o consumidor distinga "esta aplicação não versiona agregados" de "esta mensagem perdeu a versão".

### 7.1 `OutboxEnvelope`

O JSON acima é o **corpo**. O que o `MessagePublisher` recebe é o envelope como objeto, porque um publisher precisa de metadados de entrega que não fazem parte do corpo:

```java
public record OutboxEnvelope(
        UUID                messageId,
        String              aggregateType,
        String              aggregateId,
        Long                aggregateVersion,   // pode ser null
        String              eventType,
        Instant             occurredAt,
        Map<String, String> headers,
        String              payload,            // JSON já serializado
        String              destination,        // roteamento (§4.2.3)
        int                 attempts,           // atributo de span (RF43)
        String              body) {             // o JSON de §7, pronto para envio
}
```

`OutboxEnvelope` não é `OutboxMessage`. `OutboxMessage` é o que a aplicação escreve; `OutboxEnvelope` é o que o relay leu da tabela, acrescido do estado de entrega — `attempts` não existe no momento do `enqueue` e é o que RF43 exige no span de publicação.

`destination` está no envelope, e não só no roteamento, porque o publisher precisa dele para escolher o tópico ou a fila. `body` vem pronto: o publisher normalmente envia essa string e usa `aggregateId` como chave de agrupamento (§5.4), sem precisar reconstruir nada.

**Quem monta o `body`.** O core, com um escritor JSON mínimo próprio — cerca de trinta linhas. Não é uma decisão confortável, e a alternativa foi pesada: trazer Jackson para o core contradiz RF02.1 e RNF01, que é justamente o que permite usar a biblioteca fora do Spring sem arrastar dependências. Delegar a montagem a cada módulo de broker seria pior: o mesmo formato reimplementado quatro vezes, divergindo na primeira correção.

O escopo do escritor é pequeno e fixo, e é isso que torna a opção defensável: um objeto plano de campos conhecidos, sem aninhamento, com duas exceções controladas — `headers`, que é `Map<String, String>` e vira um objeto de um nível; e `payload`, que já é JSON válido e é **embutido bruto**, sem reserialização. O escape recai apenas sobre valores de string, conforme RFC 8259. É o único ponto do core que toca JSON, e leva teste próprio com aspas, barras invertidas, quebras de linha e caracteres de controle — o conjunto exato onde escritor artesanal costuma falhar.

A definição de `OutboxMessage`, a origem de `occurredAt` e o mecanismo de serialização do `payload` estão em §3.7.

## 8. Observabilidade

Entregue na Fase 1, junto com o núcleo. O relay é assíncrono, distribuído entre instâncias e não tem interface: sem instrumentação, o primeiro sintoma de falha chega como "o cliente não recebeu o evento", horas depois e sem rastro. Instrumentar depois significa depurar produção às cegas exatamente na primeira vez que o componente falha.

### 8.1 Métricas

| Métrica | Tipo | Rótulos | Para que serve |
|---|---|---|---|
| `outbox_enqueue_duration` | timer | `aggregate_type`, `destination` | Latência dentro da transação de negócio. Separa degradação da outbox de degradação do domínio |
| `outbox_enqueue_total` | counter | `aggregate_type`, `destination`, `outcome` | Volume de escrita. `outcome` distingue rejeição na validação (§3.5) |
| `outbox_pending_count` | gauge | — | Tamanho do backlog |
| `outbox_oldest_pending_age_seconds` | gauge | — | **Alerta principal** |
| `outbox_blocked_streams` | gauge | — | Fluxos inelegíveis por quarentena (§5.2) |
| `outbox_dead_letter_count` | gauge | `destination` | Acompanhamento, não alerta |
| `outbox_oldest_dead_letter_age_seconds` | gauge | — | Idade da quarentena mais antiga não resolvida. É o que distingue "duas quarentenas de ontem" de "duas quarentenas de março" (§6) |
| `outbox_relay_claim_total` | counter | `outcome` (`acquired`, `contended`, `empty`) | Saúde do paralelismo. `contended` alto indica colisão de `hashtext` ou poucos fluxos quentes |
| `outbox_relay_batch_size` | summary | — | Mensagens publicadas por reivindicação |
| `outbox_relay_cycle_duration` | timer | — | Duração da transação do relay. Vigia o risco de transação longa (§4.3) |
| `outbox_published_total` | counter | `destination`, `broker` | Throughput |
| `outbox_publish_failures_total` | counter | `destination`, `classification` (`permanent`, `transient`) | Distribuição de falhas |
| `outbox_publish_duration` | timer | `destination`, `outcome` | Latência do broker |
| `outbox_retry_scheduled_total` | counter | `destination` | Volume de reprocessamento |
| `outbox_purge_deleted_total` | counter | — | Eficácia da retenção (§6) |
| `outbox_multi_pending_streams` | gauge | — | Fluxos com dois ou mais pendentes simultâneos. Existe para responder à questão em aberto 1 dos requisitos: se este número for consistentemente próximo de zero, toda a reivindicação por fluxo está sendo paga por um caso que quase não ocorre |

**Cardinalidade.** Nenhum rótulo pode ser `aggregate_id`, `message_id` ou `trace_id`. São campos ilimitados e derrubam o backend de métricas. Investigação de um agregado específico é trabalho de log e de consulta à tabela, nunca de métrica.

**Como calcular `outbox_oldest_pending_age_seconds`.** Esta é a métrica de alerta principal, e o cálculo dela tem duas armadilhas que se corrigem de formas opostas.

*Primeira: não pode ser derivada do scan do relay.* A tentação é economizar aproveitando a varredura que o ciclo já faz. Não funciona mais desde que o scan passou a filtrar por `available_at <= now()` e a excluir fluxos em quarentena (§4.2.2): mensagens em backoff, mensagens represadas em `'infinity'` e o estado órfão de §5.3 ficam todos fora do que o relay enxerga. A métrica veria exatamente o backlog saudável e ficaria cega para os três casos que precisam de alerta. Ela é calculada pelo coletor de gauges, numa consulta própria **sem nenhum filtro de disponibilidade**:

```sql
SELECT now() - min(created_at) FROM outbox_message WHERE status = 'PENDING';
```

É para isso que existe `idx_outbox_age` (§2), um índice parcial só sobre pendentes, separado do índice do relay porque tem propósito oposto: o do relay serve para ignorar o que não está disponível, este serve para não ignorar nada.

*Segunda: não pode congelar quando o relay morre.* Se o valor vier apenas do último cálculo em cache, ele para de subir justamente quando o problema existe. O gauge reporta:

```
max(agora − min(created_at) das pendentes,
    agora − instante do último ciclo de relay concluído)
```

O segundo termo faz a métrica crescer sozinha com o relay parado. Uma métrica que emudece quando o componente que ela vigia morre não serve para nada.

Com o primeiro termo sobre todas as pendentes, a promessa de §8.2 se sustenta: um único gauge cobre broker fora do ar, relay parado, fluxo em quarentena e fluxo órfão por desbloqueio incompleto.

**Gauges globais com várias instâncias.** `outbox_pending_count`, `outbox_blocked_streams`, `outbox_dead_letter_count`, `outbox_multi_pending_streams` e as duas de idade descrevem o **estado do banco**, não o da instância que os publica. Com N relays no ar, todos reportam o mesmo número, e a regra de agregação no backend é **`max`, nunca `sum`** — somar multiplica o backlog por N e produz um painel que erra por um fator igual ao número de réplicas, justamente no momento em que alguém escala o relay para lidar com um incidente. A regra vale para todos os gauges desta tabela e precisa estar junto dos exemplos de alerta no README, porque é o tipo de detalhe que ninguém confere antes de acreditar no gráfico.

Como todas as instâncias medem a mesma coisa, `max` é exato — é o valor da leitura mais recente, não uma estimativa.

Esses gauges são calculados por um coletor com **intervalo próprio** (`metrics.interval`, default 15s), desacoplado do `polling-interval`. Uma consulta única alimenta todos eles. O intervalo separado existe porque `count(*)` sobre um backlog grande não deve rodar a cada ciclo de polling de um segundo — e porque a frequência útil dessas métricas é a do scrape, não a do relay.

### 8.2 Alertas

| Condição | Severidade | Significado provável |
|---|---|---|
| `outbox_oldest_pending_age_seconds` acima do SLA de propagação | alta | Broker fora, relay parado, fluxo em quarentena ou fluxo órfão (§5.3) — os quatro de uma vez |
| `outbox_blocked_streams > 0` sustentado | média | Existe quarentena aguardando decisão humana (§5.3) |
| `outbox_oldest_dead_letter_age_seconds` acima de alguns dias | média | Quarentena esquecida. Enquanto ela viver, todo evento novo daquele fluxo se acumula em `'infinity'` sem prazo (§6) |
| `outbox_relay_cycle_duration` no percentil alto se aproximando do timeout | média | Transações longas segurando o horizonte de vacuum |
| `outbox_pending_count` com crescimento monotônico | média | Taxa de escrita acima da taxa de publicação |

Alertar na idade, nunca na taxa de erro: taxa de erro sobe e desce sozinha e ensina o time a ignorá-la; a idade da mensagem mais antiga só sobe quando alguém precisa agir.

### 8.3 Logs

Estruturados, com argumentos nomeados — nunca concatenação. Campos obrigatórios em todo evento do relay: `messageId`, `aggregateType`, `aggregateId`, `eventType`, `destination`, `attempts`, `traceId`.

```java
log.warn("Falha transitória na publicação",
        kv("messageId", m.id()), kv("aggregateId", m.aggregateId()),
        kv("destination", m.destination()), kv("attempts", m.attempts()),
        kv("nextAttemptAt", next), kv("traceId", m.traceId()),
        kv("error", e.getClass().getSimpleName()));
```

**O `payload` nunca vai para o log.** Em nenhum nível, nem em caso de erro, nem truncado. A outbox de um sistema fiscal carrega dados pessoais e documentos autorizados, e log é o vazamento mais fácil de causar e o mais difícil de reverter. O mesmo vale para `headers`, que podem carregar credenciais. Erro de serialização registra o tipo do objeto e o tamanho em bytes, nunca o valor.

Níveis, calibrados para que INFO em produção não gere ruído:

| Nível | Eventos |
|---|---|
| `INFO` | Partida e parada do relay, configuração efetiva na inicialização, resultado de cada ciclo de purga |
| `WARN` | Falha transitória com número da tentativa e próximo instante; fluxo tornado inelegível por quarentena |
| `ERROR` | Falha permanente levando a `DEAD_LETTER`; erro de banco na reivindicação; ciclo excedendo o timeout de publicação (§4.3.1) |
| `DEBUG` | `enqueue` realizado, publicação bem-sucedida, fluxo reivindicado, tamanho do lote |
| `TRACE` | Contenção de lock, fluxos examinados e descartados |

**Supressão de repetição.** Com retry infinito, um broker indisponível por três horas gera um `WARN` por mensagem por ciclo — dezenas de milhares de linhas idênticas que escondem o problema em vez de revelá-lo, e que custam dinheiro no agregador de logs. Falhas consecutivas da mesma mensagem ou do mesmo destino são registradas em frequência decrescente: a primeira ocorrência, depois em intervalos crescentes até um teto. O log de recuperação informa quantas tentativas foram suprimidas no intervalo:

```
INFO  Publicação recuperada destination=pedidos.fifo tentativasSuprimidas=8421 duracaoFalhaSegundos=10847
```

### 8.4 Rastreamento

O `enqueue` abre um span filho do span de negócio ativo e grava o `traceparent` completo em `headers`, para o consumidor continuar o trace.

A publicação pelo relay abre um span próprio que **referencia o trace de origem por link**, não por parentesco. Relay e transação de negócio são assíncronos e podem estar separados por minutos ou horas; encadeá-los como pai e filho produz no APM transações de negócio com duração aparente de horas. O link preserva a navegabilidade sem distorcer a métrica.

Atributos mínimos: `messaging.destination`, `messaging.message_id`, `outbox.aggregate_type`, `outbox.attempts` e, no span de publicação, o resultado da tentativa.

A coluna indexada `trace_id` existe para a pergunta inversa — "quais mensagens saíram desta requisição" — respondida direto no banco, sem parsear JSON.

### 8.5 Health indicator

O starter registra um `HealthIndicator` para o Actuator que reporta o componente como degradado quando `outbox_oldest_pending_age_seconds` ultrapassa um limite configurável. É o que permite a um orquestrador ou a um painel de plantão enxergar a outbox sem depender do backend de métricas estar de pé.

## 9. Estratégia de Testes

O teste de concorrência é escrito **antes** da implementação e funciona como especificação executável:

> Duas instâncias de relay apontando para o mesmo PostgreSQL. Um único fluxo — mesmo `aggregate_id`, mesmo `destination` — com N mensagens pendentes. Um `MessagePublisher` que registra a ordem de invocação. Asserção: a sequência registrada é estritamente crescente em `seq`.

Esse teste falha contra um relay baseado em `SKIP LOCKED` linha a linha, o que é precisamente o ponto.

Demais testes:

- Atomicidade: transação de negócio que faz rollback após o `enqueue` não deixa registro na outbox. Roda com PostgreSQL real.
- Retry e backoff: publisher que falha K vezes e depois tem sucesso.
- Quarentena: `PermanentPublishException` marca `DEAD_LETTER` e bloqueia as mensagens seguintes do mesmo fluxo; mensagens de outros fluxos continuam fluindo.
- Crash do relay: processo encerrado no meio da publicação; fluxo volta a ser processável na rodada seguinte.
- Ciclo completo com Testcontainers: PostgreSQL + broker real.
- **Vazamento em log:** publisher que sempre falha, com payload contendo um marcador reconhecível; asserção de que o marcador não aparece em nenhuma linha capturada, em nenhum nível (RF38). É um teste barato para o risco mais caro do projeto.
- **Supressão de repetição:** broker indisponível por N ciclos; asserção de que o número de linhas de log cresce de forma sublinear em relação ao número de tentativas, e de que a linha de recuperação informa o total suprimido.
- **Métrica de idade com relay morto:** relay parado com backlog pendente; asserção de que `outbox_oldest_pending_age_seconds` continua crescendo em vez de congelar (§8.1).
- **Métrica de idade enxerga o que o relay não enxerga:** três cenários — fluxo em backoff, fluxo em quarentena e fluxo órfão (pendentes em `'infinity'` sem `DEAD_LETTER`, §5.3) — com o relay vivo e ciclando normalmente. Asserção de que `outbox_oldest_pending_age_seconds` sobe nos três. É o teste que impede alguém de "otimizar" a métrica de volta para o scan do relay, que não vê nenhum deles.
- **Escritor JSON do envelope:** `payload` embutido bruto sem reserialização, e valores de `headers` e campos de domínio contendo aspas, barra invertida, quebra de linha e caracteres de controle. Asserção de que o `body` resultante é JSON válido e reparseável (§7.1). É o único lugar do core que produz JSON à mão, e o teste é o que justifica não ter trazido uma dependência.
- **Publisher que trava:** `MessagePublisher` que ignora o `Duration` recebido e bloqueia indefinidamente. Asserção de que o `idle_in_transaction_session_timeout` encerra a sessão, a transação é abortada, o advisory lock é liberado e o fluxo volta a ser processável na rodada seguinte (§4.3.1). É o teste que sustenta a garantia de RF26 — sem ele, a segunda camada é só uma linha de configuração que ninguém verificou.
- **`aggregateVersion` ausente:** `enqueue` sem versão; asserção de que a mensagem é gravada, publicada normalmente e que o envelope **omite** o campo em vez de emitir `null` (§7).
- **Lock não vaza para candidatos descartados:** dois workers, vários fluxos elegíveis, o primeiro segurando o lock do candidato mais antigo. Asserção de que o segundo worker reivindica outro fluxo e de que, ao final da sua transação, `pg_locks` não contém nenhum advisory lock de fluxo que ele não processou (§4.2.2). É o teste que impede a volta da formulação declarativa.
- **Quarentena sai do scan:** fluxo em quarentena com mensagens represadas; asserção de que o plano da query de candidatos não as alcança — o range scan `available_at <= now()` não as lê — e de que mensagens de outros fluxos continuam fluindo (§4.2.1).
- **Desbloqueio devolve o fluxo:** após o `UPDATE` por fluxo de §5.3, asserção de que todas as mensagens voltam a `PENDING` com `available_at` no passado e são publicadas **na ordem original de `seq`**, incluindo a que estava em `DEAD_LETTER`.
- **Isolamento entre destinos:** um agregado com mensagens para dois destinos, o publisher de um deles sempre falhando. Asserção de que as mensagens do outro destino são publicadas normalmente e de que a quarentena de um destino não bloqueia o outro (§4.2). É o teste que sustenta a escolha do fluxo como unidade — sem ele, é fácil alguém "simplificar" a query de volta para o agregado.
- **Workers não compartilham conexão:** dois workers na mesma instância, um fluxo, N mensagens. Asserção da ordem de `publish()` (§4.6). Com conexão compartilhada, os advisory locks deixariam de isolar e este teste falha — é a razão de ele existir separado do teste entre instâncias.

## 10. Roadmap

| Fase | Entregável |
|---|---|
| **0 — Especificação executável** | Teste de concorrência, teste de atomicidade e schema Flyway. Nenhuma lógica de biblioteca ainda |
| **1 — Núcleo funcional e observável** | `OutboxWriter` JDBC com validação e fail-fast, relay de polling com reivindicação por fluxo, retry, quarentena, publisher em memória, **e toda a instrumentação da §8**: métricas, logs estruturados com supressão de repetição, spans. Ponta a ponta testável e depurável. Inclui o que é contrato público e não sobrevive a ser decidido por acidente durante a codificação: modelo de mensagem (§3.7), configuração (§13), ciclo de vida (§4.6) e coletor de gauges (§8.1) |
| **2 — SNS + Kafka** | `outbox-broker-sns` (FIFO) e `outbox-broker-kafka`, cada um com sua taxonomia de erro e com a aplicação documentada do timeout (§4.3.1). Os dois juntos nesta fase de propósito: é o segundo publisher que valida a SPI (RF06.2). É também onde o roteamento por destino (§4.2.3) deixa de ser hipotético, e onde a versão pode chegar a `1.0.0` (§1) |
| **3 — Starter** | Autoconfiguration, sobrecarga `enqueue(msg)`, integração JPA via `doWork`, health indicator do Actuator |
| **4 — Operação** | Purga com retenção em lote e índice de apoio, alerta de idade da quarentena (§6), documentação e exemplos |
| **5 — Demais brokers** | `outbox-broker-sqs` e `outbox-broker-rabbitmq`. Depois da SPI validada, cada um é um módulo pequeno e independente |

A Fase 0 existe porque as duas falhas mais caras deste projeto — ordenação que não ordena e atomicidade que não é atômica — são invisíveis em teste manual e óbvias em teste automatizado.

A observabilidade está na Fase 1 e não na 4 pelo mesmo motivo: instrumentação é o que torna as Fases 2 a 5 depuráveis. Um relay sem métrica de contenção de lock e sem log estruturado de falha é uma caixa preta durante exatamente o período em que se está integrando brokers novos.

O desdobramento destas fases em tarefas, com critério de pronto e pontos de verificação, está em `03-plano-execucao-outbox-library.md`.

## 11. Riscos e Pontos de Atenção

- **Transação longa durante a publicação** (§4.3). O advisory lock resolve a recuperação de crash ao custo de segurar uma transação durante uma chamada de rede. Timeout de publicação curto é obrigatório, não opcional — e como o core não consegue interromper a chamada, o que de fato sustenta esse risco é o `idle_in_transaction_session_timeout` da sessão do relay (§4.3.1), não a boa-fé do publisher.
- **Publisher que ignora o timeout do contrato** (§4.3.1). Modo de falha conhecido e contido: a sessão morre, o lock cai e a mensagem é republicada. O custo é uma duplicata e um `ERROR` no log, nunca uma transação presa. Vale registrar que é o único ponto da arquitetura onde a biblioteca depende de código de terceiro se comportar, e por isso é o único com rede de segurança imposta por fora.
- **Custo do scan de candidatos com backlog grande** (§4.3). Degrada junto com a condição que o alerta de idade já monitora. Diferente da revisão anterior, a degradação acompanha o backlog realmente disponível e não cresce mais com a quarentena acumulada (§4.2.1).
- **Bloat da tabela** (§6). O risco operacional mais provável de todos.
- **Ordenação depende da aplicação** (RF15.1). Se o domínio não serializa escritas por agregado, a garantia vira best-effort. Precisa estar no README, não numa nota de rodapé.
- **`hashtext` colide.** Perda de paralelismo ocasional, nunca incorretude.
- **Volume e custo de log sob falha prolongada.** Retry infinito somado a log por tentativa é a combinação que gera fatura de agregador de logs e enterra o sinal no ruído. A supressão de repetição (§8.3) não é refinamento: é a contrapartida obrigatória da decisão de nunca descartar mensagem.
- **Vazamento de dado sensível em log.** A outbox carrega payload fiscal e dados pessoais. A regra de nunca registrar `payload` precisa de teste automatizado (§9), não de disciplina — disciplina falha no primeiro `log.error("...", message)` escrito às pressas durante um incidente.
- **Premissa não medida.** Se fluxos com múltiplos eventos pendentes simultâneos forem raros na carga real, todo o custo da reivindicação por fluxo está sendo pago por um caso que quase não ocorre. A métrica `outbox_multi_pending_streams` (§8.1) existe para medir isso na Fase 1. O limiar que decide precisa ser acordado **antes** do primeiro número aparecer, ou a medição vira justificativa retroativa do que já foi construído.
- **Quarentena esquecida** (§6). Registros retidos não expiram, por decisão. Desde a revisão 3 eles não custam mais tempo de scan (§4.2.1) e a idade da mais antiga é medida e alertada, mas o acúmulo continua dependendo de alguém agir sobre o alerta. É o risco que a biblioteca conscientemente devolve à operação, em vez de resolver descartando dado.
- **Um único worker por default** (§4.6). Com `relay.workers = 1`, um fluxo lento atrasa todos os outros da instância. É o default certo para começar — paralelismo sem medição é palpite — mas é o primeiro número a revisar quando `outbox_oldest_pending_age_seconds` subir sem que o broker esteja fora.
- **Desbloqueio manual incompleto** (§5.3). O `UPDATE` de desbloqueio precisa cobrir o fluxo, não a linha; feito pela metade durante um incidente, deixa o fluxo parado sem nenhuma quarentena que explique. É detectável em `outbox_oldest_pending_age_seconds` **porque** a métrica é calculada sobre todas as pendentes (§8.1) — com o cálculo derivado do scan do relay, este estado seria invisível. Continua sendo o argumento mais forte a favor da ferramenta administrativa que hoje está fora de escopo (requisitos RF29).
- **O relay é a parte cara.** Decodificação lógica via Debezium entrega ordem por LSN de commit sem reivindicação, sem advisory lock e sem varredura. A decisão foi não competir com isso: quem precisa de CDC usa o Debezium diretamente (requisitos §6). A consequência é que o diferencial desta biblioteca precisa estar no contrato de escrita atômica e no envelope, não no motor de polling — que é a parte com mais risco de correção e menos valor único (requisitos §8.2, item 2). Note-se que o roadmap aloca toda a Fase 1 justamente a esse componente; a tensão está registrada, não resolvida.

## 12. Pendências

Todas as pendências levantadas na revisão foram fechadas. O catálogo, com a decisão de cada uma, está em `01-requisitos-outbox-library.md` §8.1.

| Pendência | Onde foi resolvida |
|---|---|
| P01 — retenção dos registros retidos | §6 e §8.1: alerta de idade em vez de política de descarte |
| P02 — custo permanente da quarentena no scan | §4.2.1 |
| P03 — namespace e aquisição do advisory lock | §4.2.2 |
| P04 — roteamento de destino | §4.2 e §4.2.3: o fluxo passa a ser a unidade |
| P05 — contrato de configuração | §13 |
| P06 — modelo de mensagem e serialização | §3.7 |
| P07 — ciclo de vida e concorrência do relay | §4.6 |
| P08 — gauges globais e multi-instância | §8.1 |
| P09 — identidade do artefato | §1 |
| P10 — encerramento do relay | §4.6 |

Permanecem abertas apenas as três **questões** de `01-requisitos-outbox-library.md` §8.2, que dependem de medição em produção e não de decisão de projeto.

## 13. Configuração

Prefixo `outbox.`. O core recebe um record imutável já montado e **não lê propriedades**: quem conhece `Environment`, `@Value` e relaxed binding é o starter. É o que mantém RF11 honesto — uma aplicação sem Spring constrói o record na mão e não descobre, três camadas abaixo, que faltava um arquivo de propriedades.

| Propriedade | Default | Efeito |
|---|---|---|
| `polling-interval` | `1s` | Intervalo entre ciclos de reivindicação (§4.4) |
| `candidate-limit` | `50` | Teto de candidatos por scan (§4.2.2) |
| `publish-timeout` | `5s` | Passado a cada `publish()` (§4.3.1) |
| `session-timeout-margin` | `5s` | Somada a `publish-timeout` para formar o `idle_in_transaction_session_timeout` da sessão do relay (§4.3.1). A margem existe para que a rede de segurança do banco dispare depois do timeout do publisher, e não no lugar dele |
| `relay.workers` | `1` | Threads de relay por instância, cada uma com sua conexão (§4.6) |
| `shutdown-timeout` | `30s` | Quanto `close()` aguarda o ciclo em voo antes de desistir (§4.6). Desistir é seguro: o rollback e a queda do lock acontecem de qualquer forma |
| `max-attempts` | `-1` | Retry infinito (§5.1) |
| `backoff.initial` | `1s` | Primeiro adiamento após falha transitória |
| `backoff.max` | `5min` | Teto do backoff exponencial |
| `backoff.multiplier` | `2` | Fator de crescimento |
| `retention` | `7d` | Idade a partir da qual um `PUBLISHED` é purgado (§6) |
| `purge.interval` | `1h` | Frequência do job de purga |
| `purge.batch-size` | `1000` | Linhas por lote, para não produzir transação longa (§6) |
| `metrics.interval` | `15s` | Frequência do coletor de gauges globais (§8.1) |
| `health.max-pending-age` | — | Limiar de degradação do health indicator (§8.5). Obrigatório quando o indicador está ativo |
| `max-payload-bytes` | `256KB` | Limite global verificado no `enqueue` (§3.5) |
| `destinations.<nome>.max-payload-bytes` | herda o global | Sobrescreve o limite para um destino. SNS e SQS aceitam 256KB; Kafka depende de `max.message.bytes` do cluster |

Os defaults valem como produto: são a configuração que este documento descreve, e alterá-los é sair do caminho testado. O único obrigatório sem default é `health.max-pending-age`, porque um limiar de saúde inventado pela biblioteca seria pior que a ausência dele — depende do SLA de propagação de cada aplicação, que a biblioteca não conhece.

O limite de payload tem default global justamente para que destino não configurado não fique sem verificação: RF21 exige que a validação aconteça sempre, e um limite ausente viraria silenciosamente "sem limite" — o caminho por onde uma mensagem grande demais chega ao relay e vira um registro morto, que é exatamente o que RF21 existe para impedir.
