package br.com.oobj.outbox.core;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Especificação executável de docs/plan/02 §9 (T0.5): duas instâncias apontando para o
 * mesmo PostgreSQL, um único fluxo, N mensagens pendentes — a sequência de invocação do
 * publisher deve ser estritamente crescente em seq. Valida {@link FluxoClaimer} (T1.4):
 * o lock é por fluxo, então só um dos dois workers reivindica e ele drena o fluxo inteiro
 * em ordem; o outro não reivindica nada. Um relay que travasse linha a linha (SKIP LOCKED)
 * deixaria os dois workers reivindicarem linhas distintas do mesmo fluxo e este teste
 * discriminaria isso pela mesma asserção — é o que T0.6 comprovou antes de T1.4 existir.
 */
class FluxoOrderingIT extends AbstractPostgresIT {

	@Test
	void doisWorkersDevemPublicarEmOrdemEstritaDeSeq() throws Exception {
		String aggregateId = "pedido-1";
		String destination = "pedidos.fifo";
		int totalMessages = 20;
		for (int i = 0; i < totalMessages; i++) {
			insertPendingMessage(aggregateId, destination);
		}

		List<Long> invocationOrder = new CopyOnWriteArrayList<>();
		ExecutorService workers = Executors.newFixedThreadPool(2);
		try {
			Callable<Void> worker = () -> claimAndDrainFluxo(invocationOrder);
			Future<Void> workerA = workers.submit(worker);
			Future<Void> workerB = workers.submit(worker);
			workerA.get();
			workerB.get();
		} finally {
			workers.shutdown();
		}

		assertThat(invocationOrder).hasSize(totalMessages).isSorted();
	}

	private Void claimAndDrainFluxo(List<Long> invocationOrder) throws Exception {
		try (Connection connection = newConnection()) {
			Optional<Fluxo> claimed = FluxoClaimer.claimNext(connection, 50, new OutboxMetrics(new SimpleMeterRegistry()));
			if (claimed.isEmpty()) {
				connection.commit();
				return null;
			}
			for (PendingRow row : pendingRowsOf(connection, claimed.get())) {
				invocationOrder.add(row.seq());
				markPublished(connection, row.id());
			}
			connection.commit();
		}
		return null;
	}

	private record PendingRow(UUID id, long seq) {
	}

	private List<PendingRow> pendingRowsOf(Connection connection, Fluxo fluxo) throws SQLException {
		List<PendingRow> rows = new ArrayList<>();
		try (PreparedStatement select = connection.prepareStatement(
				"SELECT id, seq FROM outbox_message WHERE aggregate_id = ? AND destination = ? "
						+ "AND status = 'PENDING' ORDER BY seq")) {
			select.setString(1, fluxo.aggregateId());
			select.setString(2, fluxo.destination());
			try (ResultSet resultSet = select.executeQuery()) {
				while (resultSet.next()) {
					rows.add(new PendingRow((UUID) resultSet.getObject("id"), resultSet.getLong("seq")));
				}
			}
		}
		return rows;
	}

	private void markPublished(Connection connection, UUID id) throws SQLException {
		try (PreparedStatement update = connection.prepareStatement(
				"UPDATE outbox_message SET status = 'PUBLISHED', processed_at = now() WHERE id = ?")) {
			update.setObject(1, id);
			update.executeUpdate();
		}
	}

	private void insertPendingMessage(String aggregateId, String destination) throws SQLException {
		try (Connection connection = newConnection();
				PreparedStatement insert = connection.prepareStatement(
						"INSERT INTO outbox_message (id, aggregate_type, aggregate_id, event_type, payload, destination, status) "
								+ "VALUES (?, 'Pedido', ?, 'PedidoCriado', '{}', ?, 'PENDING')")) {
			insert.setObject(1, UUID.randomUUID());
			insert.setString(2, aggregateId);
			insert.setString(3, destination);
			insert.executeUpdate();
			connection.commit();
		}
	}
}
