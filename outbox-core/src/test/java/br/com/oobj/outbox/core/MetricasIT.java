package br.com.oobj.outbox.core;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** T1.10: métricas de escrita, reivindicação e publicação de 02 §8.1. */
class MetricasIT extends AbstractPostgresIT {

	@Test
	void enqueueEPublicacaoAlimentamAsMetricasBasicas() throws Exception {
		SimpleMeterRegistry registry = new SimpleMeterRegistry();
		OutboxWriter writer = new JdbcOutboxWriter(OutboxConfig.defaults(), registry);
		OutboxMessage message = OutboxMessage.of(
				"Pedido", "pedido-1", null, "PedidoCriado", "{}", "pedidos.fifo");
		try (Connection connection = newConnection()) {
			writer.enqueue(connection, message);
			connection.commit();
		}

		assertThat(registry.get("outbox_enqueue_total").tag("outcome", "success").counter().count()).isEqualTo(1.0);
		assertThat(registry.get("outbox_enqueue_duration").timer().count()).isEqualTo(1);

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
		assertThat(registry.get("outbox_relay_claim_total").tag("outcome", "acquired").counter().count()).isGreaterThanOrEqualTo(1.0);
		assertThat(registry.get("outbox_relay_batch_size").summary().count()).isGreaterThanOrEqualTo(1);
		assertThat(registry.get("outbox_relay_cycle_duration").timer().count()).isGreaterThanOrEqualTo(1);
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
