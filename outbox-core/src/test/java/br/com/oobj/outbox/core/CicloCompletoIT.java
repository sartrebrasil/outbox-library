package br.com.oobj.outbox.core;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;


import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** T1.5: ciclo completo enqueue -> publish -> PUBLISHED, com o publisher em memória. */
class CicloCompletoIT extends AbstractPostgresIT {

	@Test
	void mensagemEnfileiradaChegaAoPublisherEFicaPublicada() throws Exception {
		OutboxWriter writer = new JdbcOutboxWriter(OutboxConfig.defaults(), new SimpleMeterRegistry());
		OutboxMessage message = OutboxMessage.of(
				"Pedido", "pedido-1", 1L, "PedidoCriado", "{\"total\":10}", "pedidos.fifo");
		try (Connection connection = newConnection()) {
			writer.enqueue(connection, message);
			connection.commit();
		}

		InMemoryMessagePublisher publisher = new InMemoryMessagePublisher();
		OutboxConfig config = OutboxConfig.defaults().withPollingInterval(Duration.ofMillis(50));
		OutboxRelay relay = new OutboxRelay(dataSource(), config, new SimpleMeterRegistry(), Map.of("pedidos.fifo", publisher));
		try {
			relay.start();
			awaitPublished(message.messageId());
		} finally {
			relay.close();
		}

		assertThat(publisher.published()).hasSize(1);
		OutboxEnvelope envelope = publisher.published().get(0);
		assertThat(envelope.messageId()).isEqualTo(message.messageId());
		assertThat(envelope.payload()).isEqualTo("{\"total\":10}");
		assertThat(envelope.body()).contains("\"eventType\":\"PedidoCriado\"").contains("\"payload\":{\"total\":10}");
	}

	private void awaitPublished(UUID messageId) throws Exception {
		Instant deadline = Instant.now().plusSeconds(5);
		while (Instant.now().isBefore(deadline)) {
			if ("PUBLISHED".equals(statusOf(messageId))) {
				return;
			}
			Thread.sleep(50);
		}
		throw new AssertionError("mensagem não foi publicada dentro do prazo");
	}

	private String statusOf(UUID messageId) throws Exception {
		try (Connection connection = newConnection();
				PreparedStatement select = connection.prepareStatement("SELECT status FROM outbox_message WHERE id = ?")) {
			select.setObject(1, messageId);
			try (ResultSet resultSet = select.executeQuery()) {
				resultSet.next();
				return resultSet.getString("status");
			}
		}
	}
}
