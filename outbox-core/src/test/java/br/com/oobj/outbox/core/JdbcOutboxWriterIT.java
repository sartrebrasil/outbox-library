package br.com.oobj.outbox.core;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Cobre os três comportamentos de T1.1 que {@link EnqueueAtomicidadeIT} não cobre. */
class JdbcOutboxWriterIT extends AbstractPostgresIT {

	private final JdbcOutboxWriter writer = new JdbcOutboxWriter(OutboxConfig.defaults(), new SimpleMeterRegistry());

	@Test
	void enqueueGravaLinhaPendente() throws Exception {
		OutboxMessage message = new OutboxMessage(
				java.util.UUID.randomUUID(), "Pedido", "pedido-1", 3L, "PedidoCriado",
				"{\"total\":10}", Map.of("traceparent", "00-abc"), "pedidos.fifo", java.time.Instant.now());

		try (Connection connection = newConnection()) {
			writer.enqueue(connection, message);
			connection.commit();
		}

		try (Connection connection = newConnection();
				PreparedStatement select = connection.prepareStatement(
						"SELECT status, aggregate_version, headers FROM outbox_message WHERE id = ?")) {
			select.setObject(1, message.messageId());
			try (ResultSet resultSet = select.executeQuery()) {
				assertThat(resultSet.next()).isTrue();
				assertThat(resultSet.getString("status")).isEqualTo("PENDING");
				assertThat(resultSet.getLong("aggregate_version")).isEqualTo(3L);
				String headersJson = resultSet.getString("headers");
				assertThat(headersJson).contains("traceparent");
				// T1.12: o enqueue sobrescreve qualquer traceparent do chamador pelo do span que abriu (02 §8.4).
				assertThat(headersJson).containsPattern("\"traceparent\":\"00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}\"");
			}
		}
	}

	@Test
	void enqueueFalhaRapidoForaDeTransacao() throws Exception {
		try (Connection connection = newConnection()) {
			connection.setAutoCommit(true);
			OutboxMessage message = OutboxMessage.of(
					"Pedido", "pedido-1", null, "PedidoCriado", "{}", "pedidos.fifo");

			assertThatThrownBy(() -> writer.enqueue(connection, message))
					.isInstanceOf(IllegalStateException.class);
		}
	}

	@Test
	void enqueueRejeitaPayloadMaiorQueOLimiteDoDestino() throws Exception {
		OutboxConfig config = OutboxConfig.defaults().withPayloadLimits(new OutboxConfig.PayloadLimits(10L, Map.of()));
		JdbcOutboxWriter smallLimitWriter = new JdbcOutboxWriter(config, new SimpleMeterRegistry());
		OutboxMessage message = OutboxMessage.of(
				"Pedido", "pedido-1", null, "PedidoCriado", "{\"total\":\"muito grande pro limite\"}", "pedidos.fifo");

		try (Connection connection = newConnection()) {
			assertThatThrownBy(() -> smallLimitWriter.enqueue(connection, message))
					.isInstanceOf(PayloadTooLargeException.class);
		}
	}
}
