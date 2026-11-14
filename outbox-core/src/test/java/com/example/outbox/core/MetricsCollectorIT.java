package com.example.outbox.core;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
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
			// por instância: mesmo valor do segundo termo da idade, mas sem misturar o backlog
			assertThat(registry.get("outbox_relay_last_cycle_age_seconds").gauge().value()).isGreaterThanOrEqualTo(120.0);
		} finally {
			collector.close();
		}
	}

	@Test
	void backoffPendingCountSoContaPendentesAdiadasNaoQuarentena() throws Exception {
		insertRow("PENDING", "pedidos.fifo");                       // disponível agora: não conta
		insertRowAvailableAt("now() + interval '1 hour'");          // em backoff: conta
		insertRowAvailableAt("'infinity'");                         // represada por quarentena: não conta

		SimpleMeterRegistry registry = new SimpleMeterRegistry();
		MetricsCollector collector = new MetricsCollector(dataSource(), registry, Duration.ofMillis(50), Instant::now);
		try {
			awaitGaugeAtLeast(registry, "outbox_backoff_pending_count", 1.0);
			Thread.sleep(150); // mais uma rodada: o valor tem que estabilizar em 1, não passar de 1
			assertThat(registry.get("outbox_backoff_pending_count").gauge().value()).isEqualTo(1.0);
		} finally {
			collector.close();
		}
	}

	@Test
	void deadLetterCountVoltaAZeroQuandoQuarentenaEResolvida() throws Exception {
		insertRow("DEAD_LETTER", "resolvido.fifo");

		SimpleMeterRegistry registry = new SimpleMeterRegistry();
		MetricsCollector collector = new MetricsCollector(dataSource(), registry, Duration.ofMillis(50), Instant::now);
		try {
			awaitGauge(registry, "resolvido.fifo", v -> v == 1.0);
			try (Connection connection = newConnection();
					var delete = connection.prepareStatement("DELETE FROM outbox_message WHERE destination = 'resolvido.fifo'")) {
				delete.executeUpdate();
				connection.commit();
			}
			awaitGauge(registry, "resolvido.fifo", v -> v == 0.0);
		} finally {
			collector.close();
		}
	}

	@Test
	void oldestPendingAgeNaoCongelaComColetorFalhando() throws Exception {
		DataSource bancoFora = (DataSource) Proxy.newProxyInstance(getClass().getClassLoader(),
				new Class<?>[] {DataSource.class}, (proxy, method, args) -> {
					throw new SQLException("banco fora");
				});
		Instant relayMortoDesde = Instant.now().minusSeconds(120);

		SimpleMeterRegistry registry = new SimpleMeterRegistry();
		MetricsCollector collector = new MetricsCollector(bancoFora, registry, Duration.ofMillis(50), () -> relayMortoDesde);
		try {
			awaitGaugeAtLeast(registry, "outbox_oldest_pending_age_seconds", 100.0);
		} finally {
			collector.close();
		}
	}

	@Test
	void coletorContaFalhasEMedeDuracao() throws Exception {
		DataSource bancoFora = (DataSource) Proxy.newProxyInstance(getClass().getClassLoader(),
				new Class<?>[] {DataSource.class}, (proxy, method, args) -> {
					throw new SQLException("banco fora");
				});
		SimpleMeterRegistry registry = new SimpleMeterRegistry();
		MetricsCollector collector = new MetricsCollector(bancoFora, registry, Duration.ofMillis(50), Instant::now);
		try {
			Instant deadline = Instant.now().plusSeconds(5);
			while (Instant.now().isBefore(deadline)
					&& registry.get("outbox_metrics_collector_failures_total").counter().count() < 2) {
				Thread.sleep(30);
			}
		} finally {
			collector.close();
		}
		// >= 2: prova que a falha não cancelou o agendamento
		assertThat(registry.get("outbox_metrics_collector_failures_total").counter().count()).isGreaterThanOrEqualTo(2.0);
		assertThat(registry.get("outbox_metrics_collector_duration").timer().count()).isGreaterThanOrEqualTo(2);
	}

	@Test
	void coletorSaudavelNaoContaFalha() throws Exception {
		SimpleMeterRegistry registry = new SimpleMeterRegistry();
		MetricsCollector collector = new MetricsCollector(dataSource(), registry, Duration.ofMillis(50), Instant::now);
		try {
			Instant deadline = Instant.now().plusSeconds(5);
			while (Instant.now().isBefore(deadline)
					&& registry.get("outbox_metrics_collector_duration").timer().count() < 2) {
				Thread.sleep(30);
			}
		} finally {
			collector.close();
		}
		assertThat(registry.get("outbox_metrics_collector_duration").timer().count()).isGreaterThanOrEqualTo(2);
		assertThat(registry.get("outbox_metrics_collector_failures_total").counter().count()).isZero();
	}

	private void awaitGauge(SimpleMeterRegistry registry, String destination, java.util.function.DoublePredicate condicao)
			throws InterruptedException {
		Instant deadline = Instant.now().plusSeconds(5);
		while (Instant.now().isBefore(deadline)) {
			var meter = registry.find("outbox_dead_letter_count").tag("destination", destination).gauge();
			if (meter != null && condicao.test(meter.value())) {
				return;
			}
			Thread.sleep(30);
		}
		throw new AssertionError("outbox_dead_letter_count{destination=" + destination + "} não atingiu a condição");
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

	private void insertRowAvailableAt(String availableAtSql) throws Exception {
		try (Connection connection = newConnection();
				var insert = connection.prepareStatement(
						"INSERT INTO outbox_message (id, aggregate_type, aggregate_id, event_type, payload, destination, status, available_at) "
								+ "VALUES (?, 'Pedido', ?, 'PedidoCriado', '{}', 'pedidos.fifo', 'PENDING', " + availableAtSql + ")")) {
			insert.setObject(1, UUID.randomUUID());
			insert.setString(2, "pedido-" + UUID.randomUUID());
			insert.executeUpdate();
			connection.commit();
		}
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
