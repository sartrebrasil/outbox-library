package br.com.oobj.outbox.spring;

import br.com.oobj.outbox.core.JdbcOutboxWriter;
import br.com.oobj.outbox.core.OutboxConfig;
import br.com.oobj.outbox.core.OutboxMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.SimpleDriverDataSource;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * T3: {@link SpringOutboxWriter} dentro de uma transação Spring real (não a mão, como em
 * outbox-core). Confirma que {@code DataSourceUtils} amarra a mesma conexão física da
 * transação (02 §3.2) e que o fail-fast usa {@code TransactionSynchronizationManager}.
 */
@Testcontainers
class SpringOutboxWriterIT {

	@Container
	private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:14")
			.withDatabaseName("outbox").withUsername("outbox").withPassword("outbox");

	private static SimpleDriverDataSource dataSource;
	private static TransactionTemplate transactionTemplate;
	private static SpringOutboxWriter writer;

	@BeforeAll
	static void setup() throws Exception {
		Flyway.configure()
				.dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
				.locations("classpath:db/migration")
				.load()
				.migrate();

		dataSource = new SimpleDriverDataSource(
				(java.sql.Driver) Class.forName("org.postgresql.Driver").getDeclaredConstructor().newInstance(),
				POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
		transactionTemplate = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
		JdbcOutboxWriter delegate = new JdbcOutboxWriter(OutboxConfig.defaults(), new SimpleMeterRegistry());
		writer = new SpringOutboxWriter(delegate, dataSource, new ObjectMapper());
	}

	@AfterAll
	static void teardown() {
		// nada a fechar: SimpleDriverDataSource não é pool
	}

	@BeforeEach
	void truncate() throws SQLException {
		try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
			statement.execute("TRUNCATE TABLE outbox_message");
		}
	}

	@Test
	void enqueueDentroDeTransacaoSpringGravaLinha() {
		OutboxMessage message = OutboxMessage.of("Pedido", "pedido-1", null, "PedidoCriado", "{}", "pedidos.fifo");

		transactionTemplate.executeWithoutResult(status -> writer.enqueue(message));

		assertThat(countRows()).isEqualTo(1);
	}

	@Test
	void rollbackDaTransacaoSpringNaoDeixaLinha() {
		OutboxMessage message = OutboxMessage.of("Pedido", "pedido-1", null, "PedidoCriado", "{}", "pedidos.fifo");

		transactionTemplate.executeWithoutResult(status -> {
			writer.enqueue(message);
			status.setRollbackOnly();
		});

		assertThat(countRows()).isZero();
	}

	@Test
	void enqueueForaDeTransacaoSpringFalhaRapido() {
		OutboxMessage message = OutboxMessage.of("Pedido", "pedido-1", null, "PedidoCriado", "{}", "pedidos.fifo");

		assertThatThrownBy(() -> writer.enqueue(message)).isInstanceOf(IllegalStateException.class);
	}

	@Test
	void overloadComObjectMapperSerializaEGrava() {
		record Pedido(String id, int total) {
		}

		transactionTemplate.executeWithoutResult((TransactionStatus status) ->
				writer.enqueue("Pedido", "pedido-2", 1L, "PedidoCriado", new Pedido("pedido-2", 42), "pedidos.fifo"));

		assertThat(countRows()).isEqualTo(1);
	}

	private long countRows() {
		try (Connection connection = dataSource.getConnection();
				PreparedStatement select = connection.prepareStatement("SELECT count(*) FROM outbox_message");
				ResultSet resultSet = select.executeQuery()) {
			resultSet.next();
			return resultSet.getLong(1);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
}
