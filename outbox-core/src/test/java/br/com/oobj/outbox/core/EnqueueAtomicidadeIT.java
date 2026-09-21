package br.com.oobj.outbox.core;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;


import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Especificação executável de docs/plan/02 §9 (T0.7): rollback da transação de negócio
 * após enqueue() não deixa linha na outbox.
 */
class EnqueueAtomicidadeIT extends AbstractPostgresIT {

	private final OutboxWriter writer = new JdbcOutboxWriter(OutboxConfig.defaults(), new SimpleMeterRegistry());

	@Test
	void rollbackAposEnqueueNaoDeixaLinhaNaOutbox() throws Exception {
		try (Connection connection = newConnection()) {
			OutboxMessage message = OutboxMessage.of(
					"Pedido", "pedido-1", null, "PedidoCriado", "{}", "pedidos.fifo");
			writer.enqueue(connection, message);
			connection.rollback();
		}

		assertThat(countRows()).isZero();
	}

	private long countRows() throws Exception {
		try (Connection connection = newConnection();
				PreparedStatement select = connection.prepareStatement("SELECT count(*) FROM outbox_message");
				ResultSet resultSet = select.executeQuery()) {
			resultSet.next();
			return resultSet.getLong(1);
		}
	}
}
