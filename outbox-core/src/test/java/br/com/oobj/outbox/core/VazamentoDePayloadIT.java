package br.com.oobj.outbox.core;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * T1.11: o payload nunca vai para o log, em nenhum nível, mesmo em falha permanente
 * (02 §8.3). Publisher que sempre falha, payload com marcador reconhecível; o marcador
 * não pode aparecer em nenhuma linha capturada.
 */
class VazamentoDePayloadIT extends AbstractPostgresIT {

	private static final String MARCADOR = "SEGREDO-FISCAL-9f8e7d6c";

	@Test
	void payloadComMarcadorNuncaApareceNoLog() throws Exception {
		OutboxWriter writer = new JdbcOutboxWriter(OutboxConfig.defaults(), new SimpleMeterRegistry());
		OutboxMessage message = OutboxMessage.of(
				"Pedido", "pedido-1", null, "PedidoCriado", "{\"cpf\":\"" + MARCADOR + "\"}", "pedidos.fifo");
		try (Connection connection = newConnection()) {
			writer.enqueue(connection, message);
			connection.commit();
		}

		PrintStream originalErr = System.err;
		ByteArrayOutputStream captured = new ByteArrayOutputStream();
		System.setErr(new PrintStream(captured, true, StandardCharsets.UTF_8));

		OutboxConfig config = OutboxConfig.defaults().withPollingInterval(Duration.ofMillis(30));
		OutboxRelay relay = new OutboxRelay(dataSource(), config, new SimpleMeterRegistry(),
				Map.of("pedidos.fifo", new AlwaysFailPublisher()));
		try {
			relay.start();
			awaitStatus(message.messageId(), "DEAD_LETTER");
		} finally {
			relay.close();
			System.setErr(originalErr);
		}

		String logged = captured.toString(StandardCharsets.UTF_8);
		assertThat(logged).doesNotContain(MARCADOR);
		assertThat(logged).contains("pedidos.fifo"); // confere que algo de fato foi logado, não um teste vazio
	}

	private void awaitStatus(java.util.UUID messageId, String expected) throws Exception {
		Instant deadline = Instant.now().plusSeconds(5);
		while (Instant.now().isBefore(deadline)) {
			if (expected.equals(statusOf(messageId))) {
				return;
			}
			Thread.sleep(30);
		}
		throw new AssertionError("mensagem não chegou a " + expected + " dentro do prazo");
	}

	private String statusOf(java.util.UUID messageId) throws Exception {
		try (Connection connection = newConnection();
				var select = connection.prepareStatement("SELECT status FROM outbox_message WHERE id = ?")) {
			select.setObject(1, messageId);
			try (var resultSet = select.executeQuery()) {
				resultSet.next();
				return resultSet.getString("status");
			}
		}
	}
}
