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

/** T1.6 (retry/backoff) e T1.7 (quarentena, isolamento entre destinos). */
class RetryQuarentenaIT extends AbstractPostgresIT {

	private final OutboxWriter writer = new JdbcOutboxWriter(OutboxConfig.defaults(), new SimpleMeterRegistry());

	@Test
	void publisherQueFalhaKvezesDepoisSucede() throws Exception {
		OutboxMessage message = OutboxMessage.of(
				"Pedido", "pedido-1", null, "PedidoCriado", "{}", "pedidos.fifo");
		enqueueAndCommit(message);

		FlakyMessagePublisher publisher = new FlakyMessagePublisher(3);
		OutboxConfig config = OutboxConfig.defaults()
				.withPollingInterval(Duration.ofMillis(30))
				.withBackoff(new OutboxConfig.Backoff(Duration.ofMillis(20), Duration.ofMillis(200), 1.0));
		OutboxRelay relay = new OutboxRelay(dataSource(), config, new SimpleMeterRegistry(), Map.of("pedidos.fifo", publisher));
		try {
			relay.start();
			awaitStatus(message.messageId(), "PUBLISHED");
		} finally {
			relay.close();
		}

		assertThat(publisher.published()).hasSize(1);
		assertThat(attemptsOf(message.messageId())).isEqualTo(3);
	}

	@Test
	void quarentenaBloqueiaFluxoSemAfetarOutroDestino() throws Exception {
		OutboxMessage bloqueada = OutboxMessage.of(
				"Pedido", "pedido-1", null, "PedidoCriado", "{}", "pedidos.fifo");
		OutboxMessage represada = OutboxMessage.of(
				"Pedido", "pedido-1", null, "PedidoAtualizado", "{}", "pedidos.fifo");
		OutboxMessage outroDestino = OutboxMessage.of(
				"Pedido", "pedido-1", null, "PedidoCriado", "{}", "outros.topico");
		enqueueAndCommit(bloqueada);
		enqueueAndCommit(represada);
		enqueueAndCommit(outroDestino);

		InMemoryMessagePublisher publisherOk = new InMemoryMessagePublisher();
		OutboxConfig config = OutboxConfig.defaults().withPollingInterval(Duration.ofMillis(30));
		OutboxRelay relay = new OutboxRelay(dataSource(), config, new SimpleMeterRegistry(),
				Map.of("pedidos.fifo", new AlwaysFailPublisher(), "outros.topico", publisherOk));
		try {
			relay.start();
			awaitStatus(bloqueada.messageId(), "DEAD_LETTER");
			awaitStatus(outroDestino.messageId(), "PUBLISHED");
			Thread.sleep(200); // dá tempo pro relay tentar de novo o fluxo em quarentena, se for tentar
		} finally {
			relay.close();
		}

		assertThat(statusOf(represada.messageId())).isEqualTo("PENDING");
		assertThat(publisherOk.published()).hasSize(1);
	}

	private void enqueueAndCommit(OutboxMessage message) throws Exception {
		try (Connection connection = newConnection()) {
			writer.enqueue(connection, message);
			connection.commit();
		}
	}

	private void awaitStatus(UUID messageId, String expected) throws Exception {
		Instant deadline = Instant.now().plusSeconds(5);
		while (Instant.now().isBefore(deadline)) {
			if (expected.equals(statusOf(messageId))) {
				return;
			}
			Thread.sleep(30);
		}
		throw new AssertionError("mensagem não chegou a " + expected + " dentro do prazo");
	}

	private String statusOf(UUID messageId) throws Exception {
		return singleColumn(messageId, "status");
	}

	private int attemptsOf(UUID messageId) throws Exception {
		return Integer.parseInt(singleColumn(messageId, "attempts"));
	}

	private String singleColumn(UUID messageId, String column) throws Exception {
		try (Connection connection = newConnection();
				PreparedStatement select = connection.prepareStatement(
						"SELECT " + column + " FROM outbox_message WHERE id = ?")) {
			select.setObject(1, messageId);
			try (ResultSet resultSet = select.executeQuery()) {
				resultSet.next();
				return resultSet.getString(column);
			}
		}
	}
}
