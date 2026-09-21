package br.com.oobj.outbox.core;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * Coletor dos gauges globais de 02 §8.1, com intervalo próprio (metrics.interval),
 * desacoplado do polling do relay: uma consulta única alimenta cada gauge.
 *
 * <p>{@code outbox_oldest_pending_age_seconds} soma dois termos deliberadamente (§8.1):
 * o backlog real de pendentes, sem filtro de disponibilidade, e o tempo desde o último
 * ciclo de relay concluído — para que a métrica continue subindo mesmo com o relay morto.
 */
public final class MetricsCollector implements AutoCloseable {

	private static final Logger LOG = LoggerFactory.getLogger(MetricsCollector.class);

	private final DataSource dataSource;
	private final MeterRegistry registry;
	private final Supplier<Instant> lastRelayCycleCompletedAt;
	private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

	private final AtomicLong pendingCount = new AtomicLong();
	private final AtomicLong oldestPendingAgeSeconds = new AtomicLong();
	private final AtomicLong blockedStreams = new AtomicLong();
	private final AtomicLong oldestDeadLetterAgeSeconds = new AtomicLong();
	private final AtomicLong multiPendingStreams = new AtomicLong();
	private final Map<String, AtomicLong> deadLetterCountByDestination = new ConcurrentHashMap<>();

	public MetricsCollector(DataSource dataSource, MeterRegistry registry, Duration interval,
			Supplier<Instant> lastRelayCycleCompletedAt) {
		this.dataSource = dataSource;
		this.registry = registry;
		this.lastRelayCycleCompletedAt = lastRelayCycleCompletedAt;
		registry.gauge("outbox_pending_count", pendingCount);
		registry.gauge("outbox_oldest_pending_age_seconds", oldestPendingAgeSeconds);
		registry.gauge("outbox_blocked_streams", blockedStreams);
		registry.gauge("outbox_oldest_dead_letter_age_seconds", oldestDeadLetterAgeSeconds);
		registry.gauge("outbox_multi_pending_streams", multiPendingStreams);
		executor.scheduleWithFixedDelay(this::collectQuietly, 0, interval.toMillis(), TimeUnit.MILLISECONDS);
	}

	@Override
	public void close() {
		executor.shutdownNow();
	}

	private void collectQuietly() {
		try {
			collect();
		} catch (SQLException e) {
			LOG.error("coletor de métricas falhou", e);
		}
	}

	private void collect() throws SQLException {
		try (Connection connection = dataSource.getConnection()) {
			pendingCount.set(scalarLong(connection, "SELECT count(*) FROM outbox_message WHERE status = 'PENDING'"));

			long oldestPendingSecondsFromTable = scalarLong(connection, """
					SELECT COALESCE(EXTRACT(EPOCH FROM now() - min(created_at)), 0)
					FROM outbox_message WHERE status = 'PENDING'
					""");
			long secondsSinceLastRelayCycle = Duration.between(lastRelayCycleCompletedAt.get(), Instant.now()).getSeconds();
			oldestPendingAgeSeconds.set(Math.max(oldestPendingSecondsFromTable, secondsSinceLastRelayCycle));

			blockedStreams.set(scalarLong(connection, """
					SELECT count(DISTINCT (aggregate_id, destination)) FROM outbox_message WHERE status = 'DEAD_LETTER'
					"""));

			oldestDeadLetterAgeSeconds.set(scalarLong(connection, """
					SELECT COALESCE(EXTRACT(EPOCH FROM now() - min(created_at)), 0)
					FROM outbox_message WHERE status = 'DEAD_LETTER'
					"""));

			multiPendingStreams.set(scalarLong(connection, """
					SELECT count(*) FROM (
					    SELECT 1 FROM outbox_message WHERE status = 'PENDING'
					    GROUP BY aggregate_id, destination HAVING count(*) > 1
					) fluxos_com_multiplas_pendentes
					"""));

			collectDeadLetterByDestination(connection);
		}
	}

	private void collectDeadLetterByDestination(Connection connection) throws SQLException {
		try (PreparedStatement select = connection.prepareStatement(
				"SELECT destination, count(*) AS total FROM outbox_message WHERE status = 'DEAD_LETTER' GROUP BY destination");
				ResultSet resultSet = select.executeQuery()) {
			while (resultSet.next()) {
				String destination = resultSet.getString("destination");
				long total = resultSet.getLong("total");
				deadLetterCountByDestination.computeIfAbsent(destination, this::registerDeadLetterGauge).set(total);
			}
		}
	}

	private AtomicLong registerDeadLetterGauge(String destination) {
		AtomicLong value = new AtomicLong();
		registry.gauge("outbox_dead_letter_count", Tags.of("destination", destination), value);
		return value;
	}

	private long scalarLong(Connection connection, String sql) throws SQLException {
		try (PreparedStatement statement = connection.prepareStatement(sql);
				ResultSet resultSet = statement.executeQuery()) {
			resultSet.next();
			return resultSet.getLong(1);
		}
	}
}
