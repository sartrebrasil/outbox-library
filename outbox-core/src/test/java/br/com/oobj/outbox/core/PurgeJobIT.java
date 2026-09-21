package br.com.oobj.outbox.core;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * T4: purga em lote (02 §6). PUBLISHED antigo é apagado, PUBLISHED recente e DEAD_LETTER
 * sobrevivem — quarentena não tem prazo, de propósito (§6). Lote pequeno força mais de
 * uma iteração dentro do mesmo ciclo, provando o "repetido até não haver mais elegíveis".
 */
class PurgeJobIT extends AbstractPostgresIT {

	@Test
	void apagaPublishedAntigoPreservaRecenteEDeadLetter() throws Exception {
		UUID antigo1 = insertRow("PUBLISHED", Instant.now().minus(Duration.ofDays(10)));
		UUID antigo2 = insertRow("PUBLISHED", Instant.now().minus(Duration.ofDays(9)));
		UUID antigo3 = insertRow("PUBLISHED", Instant.now().minus(Duration.ofDays(8)));
		UUID recente = insertRow("PUBLISHED", Instant.now().minus(Duration.ofHours(1)));
		UUID deadLetter = insertRow("DEAD_LETTER", Instant.now().minus(Duration.ofDays(30)));

		SimpleMeterRegistry registry = new SimpleMeterRegistry();
		OutboxConfig config = OutboxConfig.defaults()
				.withRetention(Duration.ofDays(7))
				.withPurgeInterval(Duration.ofMillis(50))
				.withPurgeBatchSize(2); // força 2 lotes pra apagar os 3 antigos
		PurgeJob purgeJob = new PurgeJob(dataSource(), config, registry);
		try {
			purgeJob.start();
			awaitRowGone(antigo3);
		} finally {
			purgeJob.close();
		}

		assertThat(existe(antigo1)).isFalse();
		assertThat(existe(antigo2)).isFalse();
		assertThat(existe(antigo3)).isFalse();
		assertThat(existe(recente)).isTrue();
		assertThat(existe(deadLetter)).isTrue();
		assertThat(registry.get("outbox_purge_deleted_total").counter().count()).isEqualTo(3.0);
	}

	private void awaitRowGone(UUID id) throws Exception {
		Instant deadline = Instant.now().plusSeconds(5);
		while (Instant.now().isBefore(deadline)) {
			if (!existe(id)) {
				return;
			}
			Thread.sleep(30);
		}
		throw new AssertionError("linha não foi apagada dentro do prazo");
	}

	private boolean existe(UUID id) throws Exception {
		try (Connection connection = newConnection();
				PreparedStatement select = connection.prepareStatement("SELECT 1 FROM outbox_message WHERE id = ?")) {
			select.setObject(1, id);
			try (ResultSet resultSet = select.executeQuery()) {
				return resultSet.next();
			}
		}
	}

	private UUID insertRow(String status, Instant processedAt) throws Exception {
		UUID id = UUID.randomUUID();
		try (Connection connection = newConnection();
				PreparedStatement insert = connection.prepareStatement(
						"INSERT INTO outbox_message (id, aggregate_type, aggregate_id, event_type, payload, destination, status, processed_at) "
								+ "VALUES (?, 'Pedido', 'pedido-1', 'PedidoCriado', '{}', 'pedidos.fifo', ?, ?)")) {
			insert.setObject(1, id);
			insert.setString(2, status);
			insert.setObject(3, OffsetDateTime.ofInstant(processedAt, ZoneOffset.UTC));
			insert.executeUpdate();
			connection.commit();
		}
		return id;
	}
}
