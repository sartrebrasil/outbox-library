package com.example.outbox.loadtest;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.flywaydb.core.Flyway;
import org.testcontainers.containers.PostgreSQLContainer;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/** Postgres único por execução; o schema vem da migração do core. */
final class Postgres implements AutoCloseable {

	private final PostgreSQLContainer<?> container = new PostgreSQLContainer<>("postgres:14")
			.withDatabaseName("outbox").withUsername("outbox").withPassword("outbox");

	Postgres() {
		container.start();
		pool = newPool();
		Flyway.configure().dataSource(container.getJdbcUrl(), container.getUsername(), container.getPassword())
				.locations("classpath:db/outbox/postgresql").load().migrate();
	}

	/** Pool único: sem ele cada transação abre um socket novo e a faixa de portas efêmeras esgota (TIME_WAIT). */
	private final HikariDataSource pool;

	private HikariDataSource newPool() {
		HikariConfig cfg = new HikariConfig();
		cfg.setJdbcUrl(container.getJdbcUrl());
		cfg.setUsername(container.getUsername());
		cfg.setPassword(container.getPassword());
		cfg.setMaximumPoolSize(32);
		return new HikariDataSource(cfg);
	}

	Connection connection() throws SQLException {
		Connection c = pool.getConnection();
		c.setAutoCommit(false);
		return c;
	}

	/** DataSource do Spring, para o writer real participar de transações. */
	DataSource springDataSource() {
		return pool;
	}

	void truncate() throws SQLException {
		try (Connection c = connection(); Statement s = c.createStatement()) {
			s.execute("TRUNCATE TABLE outbox_message");
			c.commit();
		}
	}

	/** DataSource do relay (abre as conexões dele). */
	DataSource dataSource() {
		return pool;
	}

	@Override
	public void close() {
		pool.close();
		container.stop();
	}
}
