package com.example.outbox.core;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Candidatos embaralhados: workers concorrentes não disputam sempre o mesmo fluxo (probabilidade de falha ~20^-99). */
class FluxoClaimerEmbaralhaIT extends AbstractPostgresIT {

	@Test
	void reivindicaFluxosDiferentesEmChamadasSucessivas() throws Exception {
		Set<String> inseridos = new HashSet<>();
		try (Connection insert = newConnection();
				var statement = insert.prepareStatement(
						"INSERT INTO outbox_message (id, aggregate_type, aggregate_id, event_type, payload, destination, status) "
								+ "VALUES (?, 'Pedido', ?, 'PedidoCriado', '{}', 'embaralha.fifo', 'PENDING')")) {
			for (int i = 0; i < 20; i++) {
				String aggregateId = "embaralha-" + UUID.randomUUID();
				inseridos.add(aggregateId);
				statement.setObject(1, UUID.randomUUID());
				statement.setString(2, aggregateId);
				statement.executeUpdate();
			}
			insert.commit();
		}

		Set<String> reivindicados = new HashSet<>();
		OutboxMetrics metrics = new OutboxMetrics(new SimpleMeterRegistry());
		try (Connection connection = newConnection()) {
			for (int i = 0; i < 100; i++) {
				reivindicados.add(FluxoClaimer.claimNext(connection, 50, metrics).orElseThrow().aggregateId());
				connection.rollback(); // libera o advisory lock de transação
			}
		}

		assertThat(reivindicados).isSubsetOf(inseridos).hasSizeGreaterThan(1);
	}
}
