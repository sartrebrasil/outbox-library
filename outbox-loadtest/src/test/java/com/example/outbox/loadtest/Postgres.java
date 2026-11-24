package com.example.outbox.loadtest;

import org.flywaydb.core.Flyway;
import org.testcontainers.containers.PostgreSQLContainer;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Logger;

/** Postgres único por execução; o schema vem da migração do core. */
final class Postgres implements AutoCloseable {

	private final PostgreSQLContainer<?> container = new PostgreSQLContainer<>("postgres:14")
			.withDatabaseName("outbox").withUsername("outbox").withPassword("outbox");

	Postgres() {
		container.start();
		Flyway.configure().dataSource(container.getJdbcUrl(), container.getUsername(), container.getPassword())
				.locations("classpath:db/outbox/postgresql").load().migrate();
	}

	Connection connection() throws SQLException {
		Connection c = DriverManager.getConnection(container.getJdbcUrl(), container.getUsername(), container.getPassword());
		c.setAutoCommit(false);
		return c;
	}

	/** DataSource do Spring (nova conexão por transação), para o writer real participar de transações. */
	javax.sql.DataSource springDataSource() {
		return new org.springframework.jdbc.datasource.SimpleDriverDataSource(new org.postgresql.Driver(),
				container.getJdbcUrl(), container.getUsername(), container.getPassword());
	}

	void truncate() throws SQLException {
		try (Connection c = connection(); Statement s = c.createStatement()) {
			s.execute("TRUNCATE TABLE outbox_message");
			c.commit();
		}
	}

	/** DataSource mínimo (uma conexão nova por chamada), como nos ITs do core: o relay abre as dele. */
	DataSource dataSource() {
		return new DataSource() {
			@Override public Connection getConnection() throws SQLException {
				return DriverManager.getConnection(container.getJdbcUrl(), container.getUsername(), container.getPassword());
			}
			@Override public Connection getConnection(String u, String p) throws SQLException { return getConnection(); }
			@Override public PrintWriter getLogWriter() { return null; }
			@Override public void setLogWriter(PrintWriter out) { }
			@Override public void setLoginTimeout(int seconds) { }
			@Override public int getLoginTimeout() { return 0; }
			@Override public Logger getParentLogger() { return Logger.getGlobal(); }
			@Override public <T> T unwrap(Class<T> iface) throws SQLException { throw new SQLException(); }
			@Override public boolean isWrapperFor(Class<?> iface) { return false; }
		};
	}

	@Override
	public void close() {
		container.stop();
	}
}
