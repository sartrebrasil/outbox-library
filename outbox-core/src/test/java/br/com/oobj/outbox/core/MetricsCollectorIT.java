package br.com.oobj.outbox.core;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** T1.10: gauges globais de 02 §8.1, calculados pelo coletor com intervalo próprio. */
class MetricsCollectorIT extends AbstractPostgresIT {

	@Test
	void gaugesRefletemPendentesEQuarentena() throws Exception {
		insertRow("PENDING", "pedidos.fifo");
		insertRow("DEAD_LETTER", "pedidos.fifo");

		SimpleMeterRegistry registry = new SimpleMeterRegistry();
		MetricsCollector collector = new MetricsCollector(dataSource(), registry, Duration.ofMillis(50), Instant::now);
		try {
			awaitGaugeAtLeast(registry, "outbox_pending_count", 1.0);
			awaitGaugeAtLeast(registry, "outbox_dead_letter_count", 1.0);
		} finally {
			collector.close();
		}

		assertThat(registry.get("outbox_pending_count").gauge().value()).isGreaterThanOrEqualTo(1.0);
		assertThat(registry.get("outbox_dead_letter_count").tag("destination", "pedidos.fifo").gauge().value())
				.isEqualTo(1.0);
	}

	@Test
	void oldestPendingAgeCresceComRelayMorto() throws Exception {
		insertRow("PENDING", "pedidos.fifo");
		Instant relayMortoDesde = Instant.now().minusSeconds(120);

		SimpleMeterRegistry registry = new SimpleMeterRegistry();
		MetricsCollector collector = new MetricsCollector(dataSource(), registry, Duration.ofMillis(50), () -> relayMortoDesde);
		try {
			awaitGaugeAtLeast(registry, "outbox_oldest_pending_age_seconds", 100.0);
		} finally {
			collector.close();
		}
	}

	private void awaitGaugeAtLeast(SimpleMeterRegistry registry, String name, double minimo) throws InterruptedException {
		Instant deadline = Instant.now().plusSeconds(5);
		while (Instant.now().isBefore(deadline)) {
			var meter = registry.find(name).gauge();
			if (meter != null && meter.value() >= minimo) {
				return;
			}
			Thread.sleep(30);
		}
		throw new AssertionError("gauge " + name + " não chegou a " + minimo + " dentro do prazo");
	}

	private void insertRow(String status, String destination) throws Exception {
		try (Connection connection = newConnection();
				var insert = connection.prepareStatement(
						"INSERT INTO outbox_message (id, aggregate_type, aggregate_id, event_type, payload, destination, status) "
								+ "VALUES (?, 'Pedido', 'pedido-1', 'PedidoCriado', '{}', ?, ?)")) {
			insert.setObject(1, UUID.randomUUID());
			insert.setString(2, destination);
			insert.setString(3, status);
			insert.executeUpdate();
			connection.commit();
		}
	}
}
