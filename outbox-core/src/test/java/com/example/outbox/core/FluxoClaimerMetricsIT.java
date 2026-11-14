package com.example.outbox.core;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** M3: {@code empty} (nada a fazer) e {@code all_contended} (tudo travado) são sinais opostos e não podem se misturar. */
class FluxoClaimerMetricsIT extends AbstractPostgresIT {

	@Test
	void semCandidatosContaEmpty() throws Exception {
		SimpleMeterRegistry registry = new SimpleMeterRegistry();
		try (Connection connection = newConnection()) {
			assertThat(FluxoClaimer.claimNext(connection, 10, new OutboxMetrics(registry))).isEmpty();
		}
		assertThat(registry.get("outbox_relay_claim_total").tag("outcome", "empty").counter().count()).isEqualTo(1.0);
		assertThat(registry.find("outbox_relay_claim_total").tag("outcome", "all_contended").counter()).isNull();
	}

	@Test
	void todosOsCandidatosTravadosContamAllContended() throws Exception {
		String aggregateId = "pedido-" + UUID.randomUUID();
		try (Connection insert = newConnection();
				var statement = insert.prepareStatement(
						"INSERT INTO outbox_message (id, aggregate_type, aggregate_id, event_type, payload, destination, status) "
								+ "VALUES (?, 'Pedido', ?, 'PedidoCriado', '{}', 'contencao.fifo', 'PENDING')")) {
			statement.setObject(1, UUID.randomUUID());
			statement.setString(2, aggregateId);
			statement.executeUpdate();
			insert.commit();
		}

		SimpleMeterRegistry registry = new SimpleMeterRegistry();
		try (Connection holder = newConnection(); Connection claimer = newConnection()) {
			assertThat(FluxoClaimer.claimNext(holder, 10, new OutboxMetrics(new SimpleMeterRegistry()))).isPresent();

			assertThat(FluxoClaimer.claimNext(claimer, 10, new OutboxMetrics(registry))).isEmpty();
			holder.rollback();
			claimer.rollback();
		}

		assertThat(registry.get("outbox_relay_claim_total").tag("outcome", "contended").counter().count()).isEqualTo(1.0);
		assertThat(registry.get("outbox_relay_claim_total").tag("outcome", "all_contended").counter().count()).isEqualTo(1.0);
		assertThat(registry.find("outbox_relay_claim_total").tag("outcome", "empty").counter()).isNull();
	}
}
