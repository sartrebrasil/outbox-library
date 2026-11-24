package com.example.outbox.core;

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
 * Falha no meio de um fluxo de 3 mensagens: as já publicadas ficam PUBLISHED e o efeito da falha
 * (backoff ou quarentena) alcança só o que sobrou. Guarda a marcação em lote de PUBLISHED, que
 * precisa ser gravada antes do UPDATE de backoff/quarentena (que filtra por status = 'PENDING').
 */
class FalhaParcialNoFluxoIT extends AbstractPostgresIT {

	private final OutboxSeeder writer = new OutboxSeeder();

	@Test
	void falhaTransitoriaNaSegundaMantemPrimeiraPublicadaEAdiaResto() throws Exception {
		OutboxMessage m1 = enqueue("E1");
		OutboxMessage m2 = enqueue("E2");
		OutboxMessage m3 = enqueue("E3");

		runUntil(m2.messageId(), "attempts", "1", (envelope, timeout) -> {
			if (envelope.eventType().equals("E2")) {
				throw new RuntimeException("falha transitória simulada");
			}
		}, Duration.ofSeconds(60));

		assertThat(column(m1.messageId(), "status")).isEqualTo("PUBLISHED");
		assertThat(column(m2.messageId(), "status")).isEqualTo("PENDING");
		assertThat(column(m2.messageId(), "attempts")).isEqualTo("1");
		assertThat(column(m3.messageId(), "status")).isEqualTo("PENDING");
		assertThat(column(m3.messageId(), "attempts")).isEqualTo("0");
		assertThat(column(m1.messageId(), "available_at > now() AS x", "x")).isEqualTo("f");
		assertThat(column(m3.messageId(), "available_at > now() AS x", "x")).isEqualTo("t");
	}

	@Test
	void falhaPermanenteNaSegundaPublicaPrimeiraQuarentenaSegundaERepresaTerceira() throws Exception {
		OutboxMessage m1 = enqueue("E1");
		OutboxMessage m2 = enqueue("E2");
		OutboxMessage m3 = enqueue("E3");

		runUntil(m2.messageId(), "status", "DEAD_LETTER", (envelope, timeout) -> {
			if (envelope.eventType().equals("E2")) {
				throw new PermanentPublishException("erro permanente simulado");
			}
		}, Duration.ofSeconds(1));

		assertThat(column(m1.messageId(), "status")).isEqualTo("PUBLISHED");
		assertThat(column(m2.messageId(), "status")).isEqualTo("DEAD_LETTER");
		assertThat(column(m3.messageId(), "status")).isEqualTo("PENDING");
		assertThat(column(m3.messageId(), "available_at = 'infinity' AS x", "x")).isEqualTo("t");
	}

	private OutboxMessage enqueue(String eventType) throws Exception {
		OutboxMessage message = OutboxMessage.of("Pedido", "pedido-1", null, eventType, "{}", "pedidos.fifo");
		try (Connection connection = newConnection()) {
			writer.enqueue(connection, message);
			connection.commit();
		}
		return message;
	}

	private void runUntil(UUID id, String column, String expected, MessagePublisher publisher, Duration backoff)
			throws Exception {
		OutboxConfig config = OutboxConfig.defaults()
				.withPollingInterval(Duration.ofMillis(30))
				.withBackoff(new OutboxConfig.Backoff(backoff, backoff, 1.0));
		OutboxRelay relay = new OutboxRelay(dataSource(), config, new SimpleMeterRegistry(),
				Map.of("pedidos.fifo", publisher));
		try {
			relay.start();
			Instant deadline = Instant.now().plusSeconds(5);
			while (!expected.equals(column(id, column)) && Instant.now().isBefore(deadline)) {
				Thread.sleep(30);
			}
			Thread.sleep(150); // dá ao relay a chance de errar algo a mais (ex.: retentar fora do backoff)
		} finally {
			relay.close();
		}
		assertThat(column(id, column)).isEqualTo(expected);
	}

	private String column(UUID id, String column) throws Exception {
		return column(id, column, column);
	}

	private String column(UUID id, String expression, String label) throws Exception {
		try (Connection connection = newConnection();
				PreparedStatement select = connection.prepareStatement(
						"SELECT " + expression + " FROM outbox_message WHERE id = ?")) {
			select.setObject(1, id);
			try (ResultSet resultSet = select.executeQuery()) {
				resultSet.next();
				return resultSet.getString(label);
			}
		}
	}
}
