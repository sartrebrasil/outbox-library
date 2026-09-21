package br.com.oobj.outbox.core;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.PostgreSQLContainer;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Container PostgreSQL compartilhado entre todas as subclasses de teste de integração.
 * O container é único por JVM de teste (Testcontainers reusa a mesma instância); cada
 * teste recebe o schema limpo via TRUNCATE, evitando o custo de recriar o container.
 */
public abstract class AbstractPostgresIT {

	private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:14")
			.withDatabaseName("outbox")
			.withUsername("outbox")
			.withPassword("outbox");

	@BeforeAll
	static void startContainerAndMigrate() {
		if (!POSTGRES.isRunning()) {
			POSTGRES.start();
		}
		Flyway.configure()
				.dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
				.locations("classpath:db/migration")
				.load()
				.migrate();
	}

	@BeforeEach
	void truncateOutboxMessage() throws SQLException {
		try (Connection connection = newConnection(); Statement statement = connection.createStatement()) {
			statement.execute("TRUNCATE TABLE outbox_message");
			connection.commit(); // sem isto, o close() com autoCommit=false faz rollback implícito do TRUNCATE
		}
	}

	protected Connection newConnection() throws SQLException {
		Connection connection = DriverManager.getConnection(
				POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
		connection.setAutoCommit(false);
		return connection;
	}

	/** DataSource mínimo para exercitar {@link OutboxRelay}, que abre as próprias conexões. */
	protected DataSource dataSource() {
		return new DataSource() {

			@Override
			public Connection getConnection() throws SQLException {
				return DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
			}

			@Override
			public Connection getConnection(String username, String password) throws SQLException {
				return getConnection();
			}

			@Override
			public <T> T unwrap(Class<T> iface) {
				throw new UnsupportedOperationException();
			}

			@Override
			public boolean isWrapperFor(Class<?> iface) {
				return false;
			}

			@Override
			public java.io.PrintWriter getLogWriter() {
				return null;
			}

			@Override
			public void setLogWriter(java.io.PrintWriter out) {
				// não usado nos testes
			}

			@Override
			public void setLoginTimeout(int seconds) {
				// não usado nos testes
			}

			@Override
			public int getLoginTimeout() {
				return 0;
			}

			@Override
			public java.util.logging.Logger getParentLogger() {
				throw new UnsupportedOperationException();
			}
		};
	}
}
