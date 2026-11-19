package com.example.outbox.core;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** T1.10: métricas de reivindicação e publicação de 02 §8.1 (as de escrita ficam no JdbcOutboxWriterIT, no starter). */
class MetricasIT extends AbstractPostgresIT {

	@Test
	void publicacaoAlimentaAsMetricasBasicas() throws Exception {
		SimpleMeterRegistry registry = new SimpleMeterRegistry();
		OutboxSeeder writer = new OutboxSeeder();
		OutboxMessage message = OutboxMessage.of(
				"Pedido", "pedido-1", null, "PedidoCriado", "{}", "pedidos.fifo");
		try (Connection connection = newConnection()) {
			writer.enqueue(connection, message);
			connection.commit();
		}

		InMemoryMessagePublisher publisher = new InMemoryMessagePublisher();
		OutboxConfig config = OutboxConfig.defaults().withPollingInterval(Duration.ofMillis(30));
		OutboxRelay relay = new OutboxRelay(dataSource(), config, registry, Map.of("pedidos.fifo", publisher));
		try {
			relay.start();
			awaitPublished(publisher);
		} finally {
			relay.close();
		}

		assertThat(registry.get("outbox_published_total").counter().count()).isEqualTo(1.0);
		assertThat(registry.get("outbox_end_to_end_latency").tag("destination", "pedidos.fifo").timer().count()).isEqualTo(1);
		assertThat(registry.get("outbox_relay_claim_total").tag("outcome", "acquired").counter().count()).isGreaterThanOrEqualTo(1.0);
		assertThat(registry.get("outbox_relay_batch_size").summary().count()).isGreaterThanOrEqualTo(1);
		assertThat(registry.get("outbox_relay_cycle_duration").tag("outcome", "acquired").timer().count())
				.isGreaterThanOrEqualTo(1);
	}

	/** N3: falha de ciclo (aqui, sem conexão) já aparece em outbox_relay_cycle_duration{outcome="error"} — sem counter à parte. */
	@Test
	void cicloQueFalhaContaOutcomeError() throws Exception {
		DataSource bancoFora = (DataSource) Proxy.newProxyInstance(getClass().getClassLoader(),
				new Class<?>[] {DataSource.class}, (proxy, method, args) -> {
					throw new SQLException("banco fora");
				});
		SimpleMeterRegistry registry = new SimpleMeterRegistry();
		OutboxConfig config = OutboxConfig.defaults().withPollingInterval(Duration.ofMillis(30));
		OutboxRelay relay = new OutboxRelay(bancoFora, config, registry, Map.of());
		try {
			relay.start();
			Instant deadline = Instant.now().plusSeconds(5);
			while (Instant.now().isBefore(deadline)
					&& (registry.find("outbox_relay_cycle_duration").tag("outcome", "error").timer() == null)) {
				Thread.sleep(30);
			}
		} finally {
			relay.close();
		}
		assertThat(registry.get("outbox_relay_cycle_duration").tag("outcome", "error").timer().count())
				.isGreaterThanOrEqualTo(1);
		assertThat(registry.find("outbox_relay_cycle_duration").tag("outcome", "acquired").timer()).isNull();
	}

	private void awaitPublished(InMemoryMessagePublisher publisher) throws InterruptedException {
		Instant deadline = Instant.now().plusSeconds(5);
		while (Instant.now().isBefore(deadline)) {
			if (!publisher.published().isEmpty()) {
				return;
			}
			Thread.sleep(30);
		}
		throw new AssertionError("mensagem não foi publicada dentro do prazo");
	}
}
