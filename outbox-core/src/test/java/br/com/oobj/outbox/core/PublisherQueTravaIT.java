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

/**
 * T1.9: publisher que ignora o Duration e trava. O idle_in_transaction_session_timeout
 * encerra a sessão, a transação aborta, o advisory lock cai e o fluxo volta a ser
 * elegível na rodada seguinte — sem esse mecanismo, a mensagem nunca seria publicada.
 */
class PublisherQueTravaIT extends AbstractPostgresIT {

	@Test
	void sessaoOciosaEhEncerradaEFluxoVoltaAServirNaRodadaSeguinte() throws Exception {
		OutboxWriter writer = new JdbcOutboxWriter(OutboxConfig.defaults(), new SimpleMeterRegistry());
		OutboxMessage message = OutboxMessage.of(
				"Pedido", "pedido-1", null, "PedidoCriado", "{}", "pedidos.fifo");
		try (Connection connection = newConnection()) {
			writer.enqueue(connection, message);
			connection.commit();
		}

		SlowOnceThenFastPublisher publisher = new SlowOnceThenFastPublisher(1500);
		OutboxConfig config = OutboxConfig.defaults()
				.withPollingInterval(Duration.ofMillis(50))
				.withPublishTimeout(Duration.ofMillis(200))
				.withSessionTimeoutMargin(Duration.ofMillis(100));
		OutboxRelay relay = new OutboxRelay(dataSource(), config, new SimpleMeterRegistry(), Map.of("pedidos.fifo", publisher));
		try {
			relay.start();
			awaitStatus(message.messageId(), "PUBLISHED", Duration.ofSeconds(8));
		} finally {
			relay.close();
		}

		assertThat(publisher.calls()).isGreaterThanOrEqualTo(2);
	}

	private void awaitStatus(UUID messageId, String expected, Duration timeout) throws Exception {
		Instant deadline = Instant.now().plus(timeout);
		while (Instant.now().isBefore(deadline)) {
			if (expected.equals(statusOf(messageId))) {
				return;
			}
			Thread.sleep(50);
		}
		throw new AssertionError("mensagem não chegou a " + expected + " dentro do prazo");
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
