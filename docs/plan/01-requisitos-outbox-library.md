# Biblioteca Outbox Pattern — Especificação de Requisitos

> Revisão 3 — fecha todas as pendências levantadas na revisão da revisão 2, com o registro das decisões em §8.1. A mudança de maior alcance é a adoção do **fluxo** `(aggregate_id, destination)` como unidade de ordenação, reivindicação e bloqueio (§3, RF24.1), que desacopla brokers ao preço de uma garantia que nenhum consumidor observava. Permanecem abertas apenas as três questões de §8.2, que dependem de medição em produção.
> Plano de implementação correspondente: `02-plano-implementacao-outbox-library.md`.

## 1. Contexto e Objetivo

Implementar o **Transactional Outbox Pattern** como biblioteca Java reutilizável, garantindo que a escrita de um registro de domínio e o enfileiramento da mensagem correspondente ocorram de forma **atômica** (mesma transação de banco de dados), eliminando o problema de dual-write entre banco e broker de mensageria.

A biblioteca deve ser:

- **Agnóstica de framework**: utilizável em qualquer aplicação Java, com ou sem Spring Boot.
- **Agnóstica de broker**: publicação plugável via SPI.
- **Publicável como dependência Maven**, para uso em múltiplos projetos/microsserviços.

## 2. Decisões Arquiteturais

| Decisão | Escolha |
|---|---|
| Broker de mensageria | Abstração agnóstica via SPI — sem acoplamento a um broker específico no core |
| Estratégia de relay | Polling Publisher. CDC/Debezium fora de escopo (ver §6) |
| Persistência (MVP) | JDBC puro sobre PostgreSQL. JPA é atendido pela mesma implementação via `Session.doWork` |
| Formato de mensagem | JSON, envelope próprio (sem CloudEvents/Avro/Protobuf) |
| Garantia de entrega | **At-least-once.** Exactly-once não é oferecido nem simulado |
| Retry | Infinito por padrão, com backoff exponencial e teto |
| Quarentena (DLQ) | Estado `DEAD_LETTER` na própria tabela outbox, alcançado **somente** por `PermanentPublishException` |
| Baseline de versão | JDK 21 + Spring Boot 3.x |
| Ordenação de mensagens | Por **fluxo** `(aggregate_id, destination)`, no escopo restrito definido em §3 e RF15 |
| Contrato transacional | `Connection` explícita como assinatura canônica do core (RF19) |
| Publicação da lib | Uso local (`mvn install`); sem publicação em repositório remoto |
| Migração de schema | Flyway |
| Broker prioritário | AWS SNS FIFO |
| Baseline PostgreSQL | 14 ou superior (RNF09) |
| Origem do `aggregate_version` | Fornecido pela aplicação, campo opcional. A biblioteca não deriva nem calcula (RF30) |
| Imposição do timeout de publicação | Contrato do `MessagePublisher`, com rede de segurança no banco. O core não interrompe a chamada (RF26) |
| Particionamento da tabela | Fora do MVP. Índice parcial e purga em lote como medida inicial (RF31) |
| Escopo de `available_at` | Propriedade do fluxo, gravada em todas as pendentes a cada adiamento (RF28.1) |
| Espaço de nomes do lock | `classid` reservado da biblioteca, com aquisição explícita fora de predicado (RF25.1 e RF25.2) |
| Retenção da quarentena | Sem prazo. Alerta de idade em vez de descarte (RF14.1) |
| Serialização do payload | `String` já serializada no core; Jackson apenas no starter (RF02.1) |
| Montagem do envelope | Corpo JSON produzido pelo core, com escritor mínimo próprio (RF02.2, RF06.3) |
| Cálculo da métrica de idade | Sobre todas as pendentes, nunca derivado do scan do relay (RF32.2) |
| Configuração | Record imutável no core; properties só no starter (RF10.1) |
| Coordenadas do artefato | `br.com.oobj.outbox`, SemVer, `1.0.0` só depois da Fase 2 |

## 3. Escopo da Garantia de Ordenação

Este é o ponto mais mal compreendido do projeto e por isso é definido antes dos requisitos.

A unidade de ordenação é o **fluxo**: o par `(aggregate_id, destination)`.

**O que a biblioteca garante:** se dois registros do mesmo fluxo foram commitados na tabela outbox em sequência, o relay invoca `publish()` para eles nessa mesma sequência.

**O que a biblioteca não garante:**

- Ordem de entrega do broker para o consumidor. Isso depende do broker (chave de partição no Kafka, `MessageGroupId` em filas FIFO) e é responsabilidade da aplicação consumidora.
- Ordem entre agregados diferentes. Não existe ordem total.
- **Ordem entre destinos diferentes do mesmo agregado.** Nenhum consumidor observa essa ordem — ninguém consome dois destinos e compara a sequência entre eles —, e garanti-la custaria acoplar brokers sem relação: um deles fora do ar seguraria as mensagens do outro (RF24.1).
- Ausência de duplicatas. A entrega é at-least-once por construção.

**Pré-condição de uso (RF15.1):** a ordenação só se sustenta se a aplicação **serializar as escritas do mesmo agregado**. Transações concorrentes sobre o mesmo agregado alocam números de sequência antes do commit e podem commitar fora da ordem de alocação; nenhuma implementação de relay consegue recuperar a ordem "pretendida" depois disso. Aplicações que usam bloqueio pessimista ou versionamento otimista na raiz do agregado — o caso normal em DDD — já satisfazem essa pré-condição. Aplicações que não satisfazem devem tratar a ordenação como best-effort.

**Pré-condição de uso (RF15.2):** a detecção de lacunas pelo consumidor depende de a aplicação fornecer um `aggregate_version` monotônico no `enqueue` (RF30). É a mesma pré-condição de RF15.1 vista do outro lado: quem serializa as escritas do agregado já possui esse número — tipicamente a coluna de versão da raiz do agregado — e quem não serializa também não tem como produzi-lo de forma confiável. Aplicações que não fornecem o campo continuam recebendo a ordenação de RF15, apenas sem a verificação independente no consumidor.

## 4. Requisitos Funcionais

### 4.1 Escrita

- **RF01** — Persistir um outbox record na mesma transação JDBC/JPA da operação de negócio, sem boilerplate repetido pelo consumidor da lib.
- **RF02** — Expor uma API central (`OutboxWriter`) para registrar mensagens a partir de código de aplicação, independente de framework.
- **RF02.1** — `OutboxMessage` é um record cujo `payload` é uma `String` JSON **já serializada**. O core não depende de Jackson nem de qualquer biblioteca de serialização — é o que torna RNF01 verdadeiro em vez de aspiracional. O starter oferece a sobrecarga que aceita um objeto e serializa com o `ObjectMapper` do contexto. RF21 continua satisfeito: a serialização segue ocorrendo dentro da transação de negócio, porque é o código da aplicação que a dispara. Uma SPI de serialização foi descartada — seria uma interface com uma implementação para um problema que a fronteira do tipo já resolve.
- **RF02.2** — A única produção de JSON dentro do core é a montagem do corpo do envelope (RF06.3), feita por um escritor mínimo próprio. É consequência direta de RF02.1: como o formato do envelope é único da biblioteca e o core não pode depender de Jackson, alguém no core precisa escrevê-lo. O escopo é fixo e pequeno — objeto plano de campos conhecidos, `headers` como mapa de um nível e `payload` embutido bruto por já ser JSON válido —, e precisa de teste de escape dedicado. Escritor JSON artesanal é aceitável nessa escala e só nessa; qualquer crescimento do formato reabre a decisão.
- **RF19** — A assinatura canônica do core é `enqueue(Connection, OutboxMessage)`. A `Connection` é sempre fornecida pelo chamador; o core não possui SPI de resolução de conexão, `ThreadLocal` nem qualquer mecanismo implícito. A sobrecarga `enqueue(OutboxMessage)` existe apenas no starter Spring, que resolve a conexão via `DataSourceUtils.getConnection(dataSource)`.
- **RF20** — O `enqueue` deve falhar rápido quando a conexão recebida estiver em auto-commit (`connection.getAutoCommit() == true`), lançando `IllegalStateException`. Essa verificação é uma **heurística documentada, não uma garantia**: uma conexão obtida diretamente do pool também tem auto-commit desativado e passaria no teste. A garantia real de atomicidade é o teste de integração descrito em RNF05.
- **RF21** — O `enqueue` deve validar a mensagem **dentro da transação de negócio**: serialização do payload e verificação do tamanho máximo configurado por destino. Falha nessa validação lança exceção e aborta o commit do negócio. O objetivo é que erros permanentes explodam na face de quem escreveu o código, em vez de virarem um registro morto na outbox às três da manhã.
- **RF22** — Em aplicações Spring com JPA/Hibernate, o insert do outbox deve usar a mesma `Connection` física da transação, obtida via `Session.doWork(...)` ou `DataSourceUtils`, **nunca** via `EntityManager.persist`. A ordem de flush do Hibernate é irrelevante porque o commit é atômico; a invisibilidade da linha para o contexto de persistência é intencional.
- **RF23** — A documentação deve alertar explicitamente que enfileirar um evento antes do flush da entidade resulta em payload com o ID gerado ainda nulo.

### 4.2 Relay e publicação

- **RF03** — Suportar Polling Publisher: leitura periódica de registros pendentes e publicação no broker configurado.
- **RF06** — Expor um SPI `MessagePublisher` para integração com brokers específicos, implementável como módulos separados. A assinatura é `publish(OutboxEnvelope envelope, Duration timeout)`: o timeout faz parte do contrato porque o core não tem como impô-lo por fora (RF26). Implementações previstas: AWS SNS, Apache Kafka, AWS SQS e RabbitMQ.
- **RF06.3** — `OutboxEnvelope` é o tipo que o publisher recebe, e **não** é o mesmo que `OutboxMessage` (RF02.1): além dos campos da mensagem, carrega `destination`, `attempts` — que não existe no momento do `enqueue` e é exigido por RF43 no span de publicação — e o corpo JSON já montado, pronto para envio. O corpo é produzido pelo core a partir do formato de envelope único da biblioteca; deixar cada módulo de broker montá-lo produziria quatro implementações do mesmo formato, divergindo na primeira correção.
- **RF06.1** — Cada módulo de broker é responsável por traduzir os erros nativos daquele broker em `PermanentPublishException` ou em falha transitória. Esse mapeamento é o conteúdo substantivo do módulo — a chamada de publicação em si é trivial — e não é transferível entre brokers: no Kafka, `RecordTooLargeException` e `InvalidTopicException` são permanentes enquanto `TimeoutException` e `NotLeaderOrFollowerException` não são.
- **RF06.2** — A segunda implementação de `MessagePublisher` deve ser entregue cedo, e não ao final do roadmap. Uma biblioteca com um único publisher nunca valida a própria SPI: é o segundo broker que demonstra que a interface não foi moldada em torno do primeiro.
- **RF15** — Garantir a ordenação definida na §3, respeitada a pré-condição RF15.1.
- **RF24** — A exclusividade de processamento é obtida por **lock por fluxo**, não por linha. Um worker que reivindica um fluxo processa todas as mensagens pendentes dele em ordem de sequência, e nenhum outro worker toca aquele fluxo enquanto isso. `SELECT ... FOR UPDATE SKIP LOCKED` aplicado linha a linha é explicitamente **insuficiente**: ele permite que um segundo worker publique N+1 enquanto N está em voo.
- **RF24.1** — A unidade de reivindicação, de ordenação e de bloqueio é o **fluxo**, o par `(aggregate_id, destination)`, e não o agregado isolado. Com o agregado como unidade, mensagens do mesmo agregado destinadas a brokers diferentes compartilham a mesma fila de publicação: um broker indisponível segura o que iria para o outro, e uma quarentena num deles congela o outro. O que se compraria com esse acoplamento é a ordem entre destinos — garantia que nenhum consumidor observa (§3).
- **RF24.2** — O relay resolve o `MessagePublisher` pelo `destination` do fluxo. Quando houver um único publisher registrado, ele atende todos os destinos sem configuração de roteamento. Destino sem publisher correspondente é tratado como falha **transitória** do fluxo, nunca como quarentena: erro de configuração se corrige reiniciando com a configuração certa, e descartar mensagem por causa dele seria punir o dado por um erro do deploy.
- **RF25** — O lock por fluxo deve ser liberado automaticamente em caso de crash do processo relay, sem intervenção humana e sem depender de job de limpeza.
- **RF25.1** — O lock por fluxo usa espaço de nomes próprio da biblioteca, via a variante de dois argumentos `pg_try_advisory_xact_lock(classid, objid)` com `classid` reservado e `objid` derivado do par `(aggregate_id, destination)`. Advisory locks são globais ao banco de dados, não ao schema: sem `classid` próprio, o relay disputa o mesmo espaço de nomes que qualquer advisory lock da aplicação hospedeira, e uma colisão ali não custa paralelismo — causa bloqueio mútuo entre a biblioteca e o domínio, sem relação causal visível de nenhum dos dois lados.
- **RF25.2** — A aquisição do lock é feita **explicitamente, um candidato por vez**, e nunca dentro de um predicado de consulta. Chamar a função de lock num `WHERE` combinado com `LIMIT` não garante que apenas a linha retornada seja travada: o executor pode avaliar o predicado sobre candidatos que descarta, e um lock de escopo transacional adquirido sobre um deles só é liberado no commit, imobilizando fluxos que aquele worker não vai processar. A ordem de avaliação de uma função volátil em um predicado não é contrato do planner e não pode ser tratada como tal.
- **RF25.3** — Cada worker de relay opera sobre a **sua própria conexão**. Advisory locks são de sessão: dois workers compartilhando uma conexão enxergariam os locks um do outro como próprios e processariam o mesmo fluxo em paralelo, que é exatamente a falha de ordenação que RF24 existe para impedir.
- **RF26** — A publicação deve ter timeout configurável. Nenhuma operação de rede pode bloquear indefinidamente um fluxo. Como `publish()` é uma chamada síncrona para código de terceiro, **o core não consegue interrompê-la**: `Future.cancel` não desbloqueia um socket, e um pool de threads em volta da chamada daria a impressão de resolver o problema sem resolvê-lo. O timeout é imposto em duas camadas:
  1. **Contrato do publisher.** O `Duration` é passado em cada chamada (RF06) e cada módulo de broker o traduz para o mecanismo nativo do seu client — `request.timeout.ms` e `delivery.timeout.ms` no Kafka, `apiCallTimeout` no AWS SDK, timeout de confirmação no RabbitMQ. Publisher que ignora o parâmetro viola o contrato do SPI, e isso precisa estar no README de cada módulo.
  2. **Rede de segurança imposta pelo banco.** A conexão usada pelo relay é aberta com `idle_in_transaction_session_timeout` configurado acima do timeout de publicação. Durante a chamada de rede ao broker, a sessão PostgreSQL está ociosa dentro de uma transação aberta; se o publisher travar apesar do contrato, o próprio banco encerra a sessão, aborta a transação e libera o advisory lock. A mensagem volta a `PENDING` e é republicada na rodada seguinte — exatamente o modo de falha já aceito por RF13. É a única camada que não depende da boa-fé de código de terceiro, e é ela que protege a garantia de RNF03.
- **RF17** — Fornecer implementação de `MessagePublisher` para AWS SNS, com suporte a tópicos FIFO (`MessageGroupId = aggregate_id`). A ordenação broker→consumidor resultante é uma recomendação de uso, não uma garantia da lib (§3).

### 4.3 Falha, retry e quarentena

- **RF08** — Aplicar retry com backoff exponencial e teto configuráveis. **O padrão é retry infinito** (`max-attempts = -1`): broker indisponível é um problema operacional que se resolve sozinho quando o broker volta, não um caso de descarte de mensagem.
- **RF27** — A biblioteca **não classifica** falhas como transitórias ou permanentes. Toda falha é transitória por padrão. O único caminho para a quarentena é o `MessagePublisher` do usuário lançar `PermanentPublishException` — quem implementa o publisher é quem sabe distinguir "tópico ainda não propagado" de "payload rejeitado pelo schema registry".
- **RF09** — Ao receber `PermanentPublishException`, o registro assume `status = DEAD_LETTER`, preservando `last_error` e o instante da falha.
- **RF28** — Enquanto existir qualquer registro em `DEAD_LETTER` para um fluxo, nenhuma mensagem subsequente daquele fluxo é publicada. Outros destinos do mesmo agregado seguem normalmente (RF24.1). O bloqueio é **derivado do estado de quarentena**, não um estado adicional na máquina de estados: não existe status `BLOCKED`. A alternativa — avançar e deixar um buraco permanente na sequência — foi descartada porque produz perda silenciosa, a única classe de falha que não é descoberta em produção.
- **RF28.1** — `available_at` é propriedade **do fluxo**: toda transição que adia um fluxo — backoff transitório ou quarentena — grava o novo valor em todas as suas mensagens pendentes, no mesmo `UPDATE`. Isso apenas torna explícito o que já era verdade, já que a publicação para no primeiro erro e nunca pula mensagem (RF24), e permite que o índice do relay exclua do scan tanto as mensagens em backoff quanto as represadas por quarentena. Sem isso, cada incidente de quarentena não resolvido deixaria um custo permanente e crescente na varredura de todo ciclo.
- **RF28.2** — A marcação de indisponibilidade de RF28.1 é **otimização de leitura, não estado**. A verdade sobre o bloqueio continua sendo a existência de um registro `DEAD_LETTER` no fluxo, verificada a cada reivindicação. Se a marcação divergir, a verificação ainda impede a publicação e o único efeito é a linha voltar a ser percorrida. A precedência não pode ser invertida por otimização posterior.
- **RF29** — O desbloqueio é uma operação humana explícita e documentada, **por fluxo** (`UPDATE ... SET status = 'PENDING', available_at = now() WHERE aggregate_id = ... AND destination = ... AND status <> 'PUBLISHED'`). Desbloquear por `id` isolado é insuficiente: limparia a quarentena sem liberar as mensagens represadas atrás dela (RF28.1), deixando o fluxo parado sem nenhum registro em `DEAD_LETTER` que explicasse o porquê. O README documenta o comando correto e uma consulta de diagnóstico para esse estado. Ferramenta de linha de comando ou endpoint administrativo ficam para uma versão posterior, quando houver demanda concreta — e esse risco é o principal argumento a favor deles.

### 4.4 Idempotência e retenção

- **RF13** — A entrega é **at-least-once**. Um crash entre o `publish()` bem-sucedido e o commit da atualização de status causa republicação, e isso é aceito por construção. A biblioteca fornece um `message_id` estável no envelope para que o consumidor faça deduplicação; ela não promete ausência de duplicatas.
- **RF30** — O envelope carrega `aggregate_version`, **fornecido pela aplicação** na chamada de `enqueue`. A biblioteca não deriva nem calcula esse número. O campo é **opcional**: quando ausente, a coluna fica nula, o envelope omite `aggregateVersion` e o consumidor não dispõe de detecção de lacunas — todas as demais garantias permanecem. As duas alternativas foram descartadas:
  - *Calcular na lib* (`SELECT max(aggregate_version) + 1 WHERE aggregate_id = ?`) custa um round-trip no caminho crítico da transação de negócio (RF34) e só produz valores corretos se a aplicação já serializar as escritas do agregado (RF15.1). Onde a pré-condição vale, o número já existe no domínio e a consulta é desperdício; onde não vale, a consulta gera versões duplicadas em silêncio — falsa confiança, que é pior que campo ausente.
  - *Reaproveitar `seq`* não serve: `seq` é global à tabela e salta entre agregados, então lacuna na numeração é o caso normal e não sinaliza nada.
- **RF30.1** — O `enqueue` não valida a monotonicidade do `aggregate_version` recebido. Validar exigiria ler o último valor gravado, que é precisamente o round-trip descartado em RF30. A aplicação é a dona do número; a biblioteca apenas o transporta.
- **RF14** — Fornecer limpeza/arquivamento de registros publicados com retenção configurável. Não é item opcional: sem purga, a tabela cresce até degradar o scan do relay.
- **RF14.1** — Registros em `DEAD_LETTER` e as mensagens represadas atrás deles **não têm prazo de retenção**. Descartá-los por tempo devolveria, por uma cláusula de limpeza, a perda silenciosa que RF28 recusa explicitamente. Em vez de política de descarte, a biblioteca expõe a **idade da quarentena mais antiga não resolvida** (RF32.1): o risco real não é espaço — uma linha em quarentena são poucos KB e, desde RF28.1, ela nem custa tempo de scan —, é ninguém agir enquanto todo evento novo daquele fluxo se acumula sem limite.
- **RF31** — O schema deve ser desenhado para minimizar bloat no PostgreSQL: índice parcial sobre registros pendentes, índice de apoio à purga sobre registros já publicados, e purga em lote em vez de `DELETE` linha a linha. **O particionamento por tempo fica fora do MVP**, revertendo a decisão da revisão anterior por três motivos concretos:
  - Tabela particionada exige que a chave de partição participe de toda constraint unique, o que transformaria a chave primária em `(id, created_at)` e mudaria o contrato de identidade do registro.
  - `GENERATED ALWAYS AS IDENTITY` em tabela particionada só é aceito em versões recentes do PostgreSQL. Manter o particionamento exigiria ou elevar a baseline (RNF09), ou trocar a coluna por uma sequência explícita com `DEFAULT nextval(...)` — decisão que não se justifica antes de haver volume medido.
  - O ganho pretendido era trocar `DELETE` em massa por `DROP PARTITION`, mas o drop só é possível quando a partição inteira é descartável. Como um registro `DEAD_LETTER` permanece na tabela até intervenção humana (RF29) e bloqueia todos os pendentes seguintes do mesmo fluxo (RF28), uma única linha em quarentena imobiliza a partição indefinidamente. O particionamento entregaria a complexidade sem o benefício.

  A purga em lote é a medida inicial. O particionamento é reavaliado quando houver medição mostrando que ela não sustenta o volume — não antes, conforme o mesmo princípio aplicado em §8.2, item 1.

### 4.5 Observabilidade

A observabilidade é requisito de **primeira fase**, não de polimento. O relay é um componente assíncrono, distribuído entre instâncias e sem interface: quando ele para, o sintoma que chega ao time é "o cliente não recebeu o evento", horas depois, sem nenhum rastro de onde a mensagem parou. Instrumentação retroativa neste tipo de componente significa depurar produção às cegas na primeira vez que ele falha.

#### Métricas

- **RF32** — Expor `outbox_oldest_pending_age_seconds`. **Esta é a métrica de alerta principal**, não a contagem de erros nem a de mensagens em quarentena: ela captura simultaneamente broker fora do ar, relay parado, fluxo bloqueado por quarentena e fluxo órfão por desbloqueio incompleto (RF29), e é a única que só sobe quando alguém precisa agir.
- **RF32.2** — O cálculo de RF32 é feito sobre **todas** as mensagens `PENDING`, sem filtro de disponibilidade, e **não** pode ser derivado do scan de reivindicação do relay. O scan filtra por `available_at` e exclui fluxos em quarentena (RF28.1), de modo que mensagens em backoff, represadas ou órfãs ficam fora do que ele enxerga — precisamente os estados que a métrica existe para revelar. Derivar do scan produziria um alerta que observa apenas o backlog saudável. O schema prevê índice parcial dedicado para essa consulta.
- **RF32.1** — Expor `outbox_oldest_dead_letter_age_seconds`, com alerta na casa de dias. É a métrica que distingue duas quarentenas de ontem de duas quarentenas de seis meses atrás — distinção que a contagem de fluxos bloqueados não faz e sem a qual RF14.1 seria apenas a decisão de não limpar.
- **RF33** — Expor métricas que cubram as três camadas do caminho: escrita (`enqueue`), reivindicação (relay) e publicação (broker). O conjunto mínimo está em `02-plano-implementacao-outbox-library.md` §8.
- **RF33.1** — Os gauges que descrevem o estado do banco são publicados por todas as instâncias de relay, que medem a mesma coisa. A regra de agregação no backend é **`max`, nunca `sum`**, e precisa estar documentada junto dos exemplos de alerta: somar multiplica o backlog pelo número de réplicas e erra exatamente quando alguém escala o relay para reagir a um incidente. Esses gauges são calculados por um coletor com intervalo próprio, desacoplado do intervalo de polling.
- **RF34** — O `enqueue` está dentro da transação de negócio e, portanto, no caminho crítico da requisição do usuário. Sua latência deve ser medida separadamente, para que uma degradação da outbox seja distinguível de uma degradação do domínio.
- **RF35** — **Nenhuma métrica pode ser rotulada por `aggregate_id`, `message_id` ou `trace_id`.** São campos de cardinalidade ilimitada e derrubam o backend de métricas. Rótulos permitidos: `destination`, `aggregate_type`, `event_type`, `broker` e classificação de falha (`permanent` ou `transient`). Onde for preciso investigar um agregado específico, a ferramenta é o log e a consulta à tabela, não a métrica.
- **RF36** — Fornecer health indicator para o Spring Boot Actuator, reportando o componente como degradado quando a idade da mensagem pendente mais antiga ultrapassar um limite configurável.

#### Logs

- **RF37** — Logs estruturados (pares chave-valor ou JSON), nunca mensagens concatenadas. Campos obrigatórios em todo evento de log do relay: `messageId`, `aggregateType`, `aggregateId`, `eventType`, `destination`, `attempts` e `traceId`. Esses campos são o que torna possível responder "onde parou a mensagem do pedido X" sem abrir o banco.
- **RF38** — **O conteúdo do `payload` nunca é registrado em log**, em nenhum nível, nem em caso de erro. A outbox de um sistema fiscal carrega dados pessoais e documentos autorizados; o log é o vazamento mais fácil de causar e o mais difícil de reverter. O mesmo vale para `headers` que possam conter credenciais. Em caso de erro de serialização, registrar o tipo do objeto e o tamanho, nunca o valor.
- **RF39** — Níveis de log por evento, definidos de forma que o nível padrão de produção (INFO) não gere ruído:
  - `INFO` — início e parada do relay, mudança de configuração efetiva, resultado de cada ciclo de purga.
  - `WARN` — falha transitória de publicação, com o número da tentativa e o instante da próxima; fluxo tornado inelegível por quarentena.
  - `ERROR` — falha permanente levando a `DEAD_LETTER`; falha de reivindicação por erro de banco; ciclo excedendo o timeout de publicação (RF26).
  - `DEBUG` — `enqueue` realizado, publicação bem-sucedida, fluxo reivindicado, tamanho do lote.
  - `TRACE` — contenção de lock, fluxos examinados e descartados no scan.
- **RF40** — **Supressão de repetição obrigatória.** Com retry infinito (RF08), um broker fora do ar por três horas produz um `WARN` por mensagem por ciclo — dezenas de milhares de linhas idênticas que escondem o problema real em vez de revelá-lo. Falhas consecutivas da mesma mensagem ou do mesmo destino devem ser registradas de forma decrescente (primeira ocorrência, depois em intervalos crescentes), com o log de recuperação informando quantas tentativas foram suprimidas.
- **RF41** — Toda linha de log emitida pelo relay deve conter o `traceId` da mensagem em processamento, permitindo correlacionar a publicação assíncrona com a requisição de origem que a enfileirou.

#### Rastreamento distribuído

- **RF18** — No `enqueue`, capturar o contexto de tracing ativo (W3C `traceparent`) e persistir de duas formas: completo em `headers`, para o consumidor continuar o trace como span filho; e o `trace_id` isolado em coluna indexada, para consulta direta na tabela durante troubleshooting, sem parsear JSON.
- **RF42** — O `enqueue` abre um span filho do span de negócio ativo. A publicação pelo relay abre um span próprio que **referencia o trace de origem por link**, e não por parentesco: relay e transação de negócio são assíncronos e frequentemente separados por minutos, e tratá-los como um único span encadeado distorce a duração das transações no APM.
- **RF43** — Atributos mínimos dos spans: `messaging.destination`, `messaging.message_id`, `outbox.aggregate_type`, `outbox.attempts` e, no span de publicação, o resultado da tentativa.

### 4.6 Integração

- **RF10** — Fornecer autoconfiguration Spring Boot (starter) que registra os beans necessários com base em propriedades.
- **RF10.1** — A configuração do core é um **record imutável**, montado por quem instancia a biblioteca. O core não lê propriedades, variáveis de ambiente nem arquivos: quem conhece `Environment` e relaxed binding é o starter. A lista completa de propriedades, com defaults, está em `02-plano-implementacao-outbox-library.md` §13, e faz parte da API pública — nomes de configuração inventados durante a implementação só podem ser corrigidos quebrando quem já os usa.
- **RF11** — Ser utilizável em aplicações sem Spring, via configuração programática, sem dependência transitiva de Spring no core. RF10.1 é o que torna isso verdadeiro na prática: uma aplicação sem Spring constrói o record na mão, em vez de descobrir três camadas abaixo que faltava um arquivo de propriedades.
- **RF11.1** — O relay é `AutoCloseable`, com partida e parada explícitas, e executa com `workers` threads configuráveis (default `1`), cada uma com sua conexão (RF25.3). Não há coordenação entre workers, nem dentro da instância nem entre instâncias: o lock por fluxo já é o mecanismo de exclusão mútua e não distingue duas threads de dois processos.
- **RF11.2** — O encerramento **não requer mecanismo próprio**. Parada ordenada, crash do processo e sessão encerrada pelo banco convergem para o mesmo resultado: rollback, liberação do lock com a queda da conexão e fluxo elegível na rodada seguinte. Este requisito existe na forma negativa — não deve ser implementado job de limpeza de lock órfão, porque não existe lock órfão. Essa é a razão pela qual RF25 escolheu advisory lock em vez de lease.
- **RF12** — Fornecer migração Flyway de referência para a tabela outbox e seus índices.

## 5. Requisitos Não Funcionais

- **RNF01** — Baixo acoplamento: módulos de broker não devem vazar dependências entre si nem para o core.
- **RNF02** — Observabilidade via Micrometer (métricas), SLF4J com argumentos estruturados (logs) e OpenTelemetry (tracing), entregues junto com o núcleo funcional e não como fase posterior. A biblioteca não pode assumir um backend específico: expõe `MeterRegistry` e logger padrão, e cabe à aplicação decidir para onde vai.
- **RNF03** — O polling não deve gerar contenção significativa. A reivindicação por fluxo mantém uma transação aberta durante a publicação; batches devem ser pequenos e o timeout de publicação curto, porque transações longas no PostgreSQL seguram o horizonte de vacuum.
- **RNF04** — Nenhuma mensagem perdida entre a escrita do outbox record e a confirmação de publicação.
- **RNF05** — **Teste de concorrência como especificação executável.** Duas instâncias de relay, um fluxo, N mensagens, asserção da ordem das chamadas a `publish()`. Esse teste precede a implementação: sem ele, a garantia de ordenação é apenas prosa no README. Testes de integração com Testcontainers (PostgreSQL real + broker real) cobrindo o ciclo completo.
- **RNF06** — Extensibilidade: novos brokers como módulos externos, sem alterar o core.
- **RNF07** — Documentação: README por módulo, exemplos com e sem Spring Boot, e a pré-condição RF15.1 em destaque.
- **RNF08** — Java 21 como baseline única.
- **RNF09** — **PostgreSQL 14 ou superior** como baseline única. Fixada por ser a versão mais antiga ainda em suporte oficial e por cobrir com folga tudo que o relay usa: `pg_try_advisory_xact_lock` em duas variantes, índices parciais, `GENERATED ALWAYS AS IDENTITY` e `idle_in_transaction_session_timeout` (RF26). Exigir uma versão mais nova só se justificaria pelo particionamento, que está fora do MVP (RF31).

## 6. Fora de Escopo

- Exactly-once de ponta a ponta.
- Schema registry (Avro/Protobuf).
- Multi-tenancy nativo.
- UI/dashboard de monitoramento.
- Classificação automática de falhas pela biblioteca (ver RF27).
- Política de retry/quarentena configurável por destino ou por agregado. O padrão é o produto; configuração que ninguém altera é complexidade sem retorno.
- API administrativa de desbloqueio (ver RF29).
- **CDC via Debezium.** A decodificação lógica do PostgreSQL entrega ordem por LSN de commit nativamente, mas o Debezium **já possui** o Outbox Event Router como SMT pronto. Entregar um módulo equivalente significaria manter dois motores de relay com duas provas de correção independentes, para reimplementar algo que existe. A orientação da biblioteca é: quem precisa de CDC usa o Debezium diretamente, sem esta lib no caminho do relay.
- **Módulo de persistência JPA separado.** O insert do outbox é uma única linha sem mapeamento de domínio, e `Session.doWork(...)` entrega a mesma `Connection` física à implementação JDBC (RF22). Um módulo JPA seria uma segunda implementação do mesmo INSERT.

## 7. Item Adiado

### 7.1 Backend MongoDB

Existia na revisão anterior e foi adiado. Permanece registrado porque a decisão de reintroduzi-lo não está fechada.

Motivação do adiamento: exige replica set como pré-requisito de infraestrutura, não possui equivalente ao lock por fluxo com liberação automática em crash (RF25), e a atomicidade depende de transação multi-documento via sessão do driver. É superfície grande para uma demanda que ainda não existe.

Se for reintroduzido, o equivalente à reivindicação por fluxo é um `findOneAndUpdate` atômico filtrado por `aggregateId` e `destination`, e a recuperação de crash precisa de lease com expiração — o mecanismo que o advisory lock do PostgreSQL torna desnecessário.

## 8. Pendências e Questões em Aberto

Pendências eram lacunas conhecidas de projeto; questões em aberto são decisões que dependem de medição que ainda não existe. **As dez pendências foram fechadas na revisão 3** e viraram requisitos. A tabela abaixo é o registro de como cada uma foi decidida, e vale mais que o enunciado original da lacuna porque preserva a alternativa descartada — que é o que alguém vai querer reabrir daqui a um ano.

### 8.1 Pendências fechadas na revisão 3

| ID | Lacuna | Decisão | Virou |
|---|---|---|---|
| **P01** | Retenção dos registros em quarentena e das mensagens represadas atrás deles | Sem prazo de retenção. Em vez de descartar, expor a idade da quarentena mais antiga e alertar em dias. Descartar por tempo devolveria a perda silenciosa que RF28 recusa; arquivar resolve espaço, que não é o problema — o problema é ninguém agir | RF14.1, RF32.1 |
| **P02** | Custo permanente da quarentena no scan | `available_at` vira propriedade do fluxo; as represadas recebem `'infinity'` e saem do range scan do índice quente | RF28.1, RF28.2 |
| **P03** | Namespace e aquisição do advisory lock | `classid` reservado da biblioteca, e aquisição explícita fora de predicado, um candidato por vez | RF25.1, RF25.2 |
| **P04** | Roteamento `destination` → publisher | A unidade passa a ser o fluxo `(aggregate_id, destination)`. Desacopla brokers ao preço da ordem entre destinos, que nenhum consumidor observa | RF24.1, RF24.2, §3 |
| **P05** | Contrato de configuração | Record imutável no core, properties só no starter. Lista completa com defaults no plano §13 | RF10.1 |
| **P06** | Modelo de mensagem e serialização | `payload` como `String` já serializada, Jackson só no starter. SPI de serialização descartada como interface de uma implementação só | RF02.1 |
| **P07** | Ciclo de vida e concorrência do relay | `AutoCloseable` com `workers` threads (default `1`), cada uma com sua conexão, sem coordenação além do lock | RF11.1, RF25.3 |
| **P08** | Gauges globais e multi-instância | Todas as instâncias reportam; agregação documentada como `max`, nunca `sum`; coletor com intervalo próprio | RF33.1 |
| **P09** | Identidade do artefato | `br.com.oobj.outbox`, SemVer, licença interna. `1.0.0` só depois da Fase 2, quando um segundo publisher tiver validado a SPI | Plano §1 |
| **P10** | Encerramento do relay | Requisito na forma negativa: não construir limpeza de lock órfão, porque não existe lock órfão | RF11.2 |

### 8.2 Questões em aberto

1. **Volumetria da premissa.** Quantos fluxos de fato têm dois ou mais eventos pendentes simultaneamente na carga real? Se a janela for rara, o custo da reivindicação por fluxo (RF24) está sendo pago por um caso quase inexistente, e a ordenação poderia ser oferecida como modo opcional em vez de padrão. **Critério de saída:** `outbox_multi_pending_streams`, instrumentada na Fase 1, e um limiar acordado antes de medir — abaixo dele, a reivindicação por fluxo passa a modo opcional. Medir antes de otimizar só funciona se o número que decide for escolhido antes do resultado aparecer.
2. **Onde está o valor da biblioteca.** Com o CDC fora de escopo, a orientação para quem precisa de ordem por LSN é usar o Debezium diretamente. Vale manter a pergunta viva: o valor desta lib está no relay de polling ou no contrato de escrita atômica? Se for no segundo, o relay é acessório e o investimento deveria se concentrar no `enqueue` e no envelope. **Critério de saída:** responder ao final da Fase 2, quando houver dois publishers e o primeiro uso real. Enquanto a resposta não existir, note-se que o roadmap aloca toda a Fase 1 ao componente que este documento descreve como o de maior risco de correção e menor valor único.
3. **Confirmação do adiamento do backend MongoDB** (§7). Os módulos de broker permanecem em escopo, apenas sequenciados em fases posteriores à primeira (RF06.2). **Critério de saída:** reavaliar apenas mediante demanda concreta de um serviço que já use MongoDB como banco primário; sem isso, o adiamento se torna exclusão na próxima revisão.

## 9. Glossário

- **Outbox record**: linha na tabela outbox representando uma mensagem pendente de publicação.
- **Relay**: processo que lê registros da outbox e os publica.
- **Quarentena (`DEAD_LETTER`)**: estado terminal de um registro cuja publicação falhou de forma declaradamente permanente. Apesar do nome herdado "DLQ", não é uma fila nem um tópico — é uma coluna de status.
- **Fluxo**: o par `(aggregate_id, destination)`. É a unidade de ordenação, de reivindicação e de bloqueio (RF24.1). O mesmo agregado publicando em dois destinos tem dois fluxos independentes.
- **Reivindicação (claim)**: aquisição de exclusividade sobre todas as mensagens pendentes de um fluxo pelo tempo de uma rodada de publicação.
