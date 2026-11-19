package com.example.outbox.spring;

import com.example.outbox.core.OutboxConfig;
import com.example.outbox.core.OutboxMessage;
import com.example.outbox.core.OutboxSerializationException;
import com.example.outbox.core.PayloadSerializer;
import com.example.outbox.core.PayloadTooLargeException;
import tools.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.SimpleDriverDataSource;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** T1.1 e T0.7 (atomicidade) contra o escritor real, dentro de transação Spring. */
@Testcontainers
class JdbcOutboxWriterIT {

	@Container
	private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:14")
			.withDatabaseName("outbox").withUsername("outbox").withPassword("outbox");

	private static final PayloadSerializer SERIALIZER = new JacksonPayloadSerializer(new ObjectMapper());

	private static JdbcClient jdbc;
	private static TransactionTemplate tx;

	private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
	private JdbcOutboxWriter writer;

	@BeforeAll
	static void setup() throws Exception {
		Flyway.configure()
				.dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
				.locations("classpath:db/outbox/postgresql")
				.load()
				.migrate();
		SimpleDriverDataSource dataSource = new SimpleDriverDataSource(
				(java.sql.Driver) Class.forName("org.postgresql.Driver").getDeclaredConstructor().newInstance(),
				POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
		jdbc = JdbcClient.create(dataSource);
		tx = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
	}

	@BeforeEach
	void truncate() {
		jdbc.sql("TRUNCATE TABLE outbox_message").update();
		writer = new JdbcOutboxWriter(OutboxConfig.defaults(), registry, jdbc, SERIALIZER);
	}

	@Test
	void enqueueGravaLinhaPendente() {
		OutboxMessage message = new OutboxMessage(
				UUID.randomUUID(), "Pedido", "pedido-1", 3L, "PedidoCriado",
				"{\"total\":10}", Map.of("traceparent", "00-abc"), "pedidos.fifo", Instant.now());

		tx.executeWithoutResult(status -> writer.enqueue(message));

		// {"total":10} = 12 bytes
		var bytes = registry.get("outbox_enqueue_payload_bytes").tag("destination", "pedidos.fifo").summary();
		assertThat(bytes.count()).isEqualTo(1);
		assertThat(bytes.totalAmount()).isEqualTo(12.0);
		assertThat(registry.get("outbox_enqueue_total").tag("outcome", "success").counter().count()).isEqualTo(1.0);

		var row = jdbc.sql("SELECT status, aggregate_version, headers FROM outbox_message WHERE id = ?")
				.param(message.messageId()).query().singleRow();
		assertThat(row.get("status")).isEqualTo("PENDING");
		assertThat(((Number) row.get("aggregate_version")).longValue()).isEqualTo(3L);
		// T1.12: o enqueue sobrescreve qualquer traceparent do chamador pelo do span que abriu (02 §8.4).
		assertThat((String) row.get("headers")).containsPattern("\"traceparent\":\"00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}\"");
	}

	@Test
	void versaoNulaEhGravadaComoNull() {
		tx.executeWithoutResult(status ->
				writer.enqueue(OutboxMessage.of("Pedido", "pedido-1", null, "PedidoCriado", "{}", "pedidos.fifo")));

		assertThat(jdbc.sql("SELECT count(*) FROM outbox_message WHERE aggregate_version IS NULL").query(Long.class).single())
				.isEqualTo(1L);
	}

	@Test
	void rollbackAposEnqueueNaoDeixaLinhaNaOutbox() {
		tx.executeWithoutResult(status -> {
			writer.enqueue(OutboxMessage.of("Pedido", "pedido-1", null, "PedidoCriado", "{}", "pedidos.fifo"));
			status.setRollbackOnly();
		});

		assertThat(jdbc.sql("SELECT count(*) FROM outbox_message").query(Long.class).single()).isZero();
	}

	@Test
	void enqueueFalhaRapidoForaDeTransacao() {
		OutboxMessage message = OutboxMessage.of("Pedido", "pedido-1", null, "PedidoCriado", "{}", "pedidos.fifo");

		assertThatThrownBy(() -> writer.enqueue(message)).isInstanceOf(IllegalStateException.class);
		assertThat(registry.get("outbox_enqueue_total").tag("outcome", "error").counter().count()).isEqualTo(1.0);
	}

	@Test
	void enqueueRejeitaPayloadMaiorQueOLimiteDoDestino() {
		OutboxConfig config = OutboxConfig.defaults().withPayloadLimits(new OutboxConfig.PayloadLimits(10L, Map.of()));
		SimpleMeterRegistry smallRegistry = new SimpleMeterRegistry();
		JdbcOutboxWriter smallLimitWriter = new JdbcOutboxWriter(config, smallRegistry, jdbc, SERIALIZER);
		OutboxMessage message = OutboxMessage.of(
				"Pedido", "pedido-1", null, "PedidoCriado", "{\"total\":\"muito grande pro limite\"}", "pedidos.fifo");

		assertThatThrownBy(() -> tx.executeWithoutResult(status -> smallLimitWriter.enqueue(message)))
				.isInstanceOf(PayloadTooLargeException.class);
		// a rejeitada também é medida: mostra por quanto estourou o limite de 10 bytes
		assertThat(smallRegistry.get("outbox_enqueue_payload_bytes").summary().totalAmount()).isGreaterThan(10.0);
	}

	@Test
	void sobrecargaComObjetoSerializaEGrava() {
		record Pedido(String id, int total) {
		}

		tx.executeWithoutResult(status ->
				writer.enqueue("Pedido", "pedido-1", 1L, "PedidoCriado", new Pedido("pedido-1", 42), "pedidos.fifo"));

		assertThat(jdbc.sql("SELECT payload FROM outbox_message").query(String.class).single())
				.isEqualTo("{\"id\":\"pedido-1\",\"total\":42}");
	}

	@Test
	void payloadQueNaoSerializaContaErroENaoGrava() {
		PayloadSerializer quebrado = payload -> {
			throw new OutboxSerializationException("não serializou", null);
		};
		JdbcOutboxWriter quebradoWriter = new JdbcOutboxWriter(OutboxConfig.defaults(), registry, jdbc, quebrado);

		assertThatThrownBy(() -> tx.executeWithoutResult(status ->
				quebradoWriter.enqueue("Pedido", "pedido-1", null, "PedidoCriado", new Object(), "pedidos.fifo")))
				.isInstanceOf(OutboxSerializationException.class);

		assertThat(registry.get("outbox_enqueue_total").tag("outcome", "error").counter().count()).isEqualTo(1.0);
		assertThat(jdbc.sql("SELECT count(*) FROM outbox_message").query(Long.class).single()).isZero();
	}
}
