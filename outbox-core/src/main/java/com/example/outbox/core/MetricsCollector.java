package com.example.outbox.core;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
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
 * <p>{@code outbox_oldest_pending_age_seconds} é o maior de dois termos (§8.1): a idade
 * da pendente mais antiga, sem filtro de disponibilidade, e o tempo desde o último ciclo
 * de relay concluído. Ambos são calculados no scrape, a partir da última leitura, para que
 * a métrica continue subindo com o relay morto — e também com o coletor falhando.
 */
public final class MetricsCollector implements AutoCloseable {

	private static final Logger LOG = LoggerFactory.getLogger(MetricsCollector.class);

	private final DataSource dataSource;
	private final MeterRegistry registry;
	private final Supplier<Instant> lastRelayCycleCompletedAt;
	private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

	private final AtomicLong pendingCount = new AtomicLong();
	private volatile long oldestPendingAgeSecondsAtCollect;
	private volatile Instant lastCollectedAt = Instant.now();
	private final AtomicLong blockedStreams = new AtomicLong();
	private final AtomicLong oldestDeadLetterAgeSeconds = new AtomicLong();
	private final AtomicLong multiPendingStreams = new AtomicLong();
	private final AtomicLong backoffPendingCount = new AtomicLong();
	private final Counter collectorFailures;
	private final Timer collectorDuration;
	private final Map<String, AtomicLong> deadLetterCountByDestination = new ConcurrentHashMap<>();

	public MetricsCollector(DataSource dataSource, MeterRegistry registry, Duration interval,
			Supplier<Instant> lastRelayCycleCompletedAt) {
		this.dataSource = dataSource;
		this.registry = registry;
		this.lastRelayCycleCompletedAt = lastRelayCycleCompletedAt;
		this.collectorFailures = registry.counter("outbox_metrics_collector_failures_total");
		this.collectorDuration = registry.timer("outbox_metrics_collector_duration");
		registry.gauge("outbox_pending_count", pendingCount);
		Gauge.builder("outbox_oldest_pending_age_seconds", this, MetricsCollector::oldestPendingAgeSeconds)
				.strongReference(true)
				.register(registry);
		registry.gauge("outbox_blocked_streams", blockedStreams);
		registry.gauge("outbox_oldest_dead_letter_age_seconds", oldestDeadLetterAgeSeconds);
		registry.gauge("outbox_multi_pending_streams", multiPendingStreams);
		registry.gauge("outbox_backoff_pending_count", backoffPendingCount);
		// POR INSTÂNCIA, ao contrário dos demais: separa "este relay morreu" de "há backlog" (02 §8.1)
		Gauge.builder("outbox_relay_last_cycle_age_seconds", this, c -> c.secondsSinceLastRelayCycle(Instant.now()))
				.strongReference(true)
				.register(registry);
		executor.scheduleWithFixedDelay(this::collectQuietly, 0, interval.toMillis(), TimeUnit.MILLISECONDS);
	}

	@Override
	public void close() {
		executor.shutdownNow();
	}

	/** Extrapola a partir da última leitura: se o coletor parar de ler, o valor não congela. */
	private double oldestPendingAgeSeconds() {
		Instant now = Instant.now();
		long fromTable = oldestPendingAgeSecondsAtCollect;
		if (fromTable > 0) {
			fromTable += Duration.between(lastCollectedAt, now).getSeconds();
		}
		return Math.max(fromTable, secondsSinceLastRelayCycle(now));
	}

	private long secondsSinceLastRelayCycle(Instant now) {
		return Duration.between(lastRelayCycleCompletedAt.get(), now).getSeconds();
	}

	private void collectQuietly() {
		Timer.Sample sample = Timer.start(registry);
		try {
			collect();
		} catch (SQLException | RuntimeException e) {
			// RuntimeException também: sem capturá-la o scheduleWithFixedDelay cancela as execuções seguintes em silêncio
			collectorFailures.increment();
			LOG.error("coletor de métricas falhou", e);
		} finally {
			sample.stop(collectorDuration);
		}
	}

	private void collect() throws SQLException {
		try (Connection connection = dataSource.getConnection()) {
			pendingCount.set(scalarLong(connection, "SELECT count(*) FROM outbox_message WHERE status = 'PENDING'"));

			oldestPendingAgeSecondsAtCollect = scalarLong(connection, """
					SELECT COALESCE(EXTRACT(EPOCH FROM now() - min(created_at)), 0)
					FROM outbox_message WHERE status = 'PENDING'
					""");
			lastCollectedAt = Instant.now();

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

			// só o que está em backoff: o range scan de idx_outbox_pending para antes das 'infinity' (quarentena)
			// e não olha o backlog disponível, então o custo acompanha o número de retries, não o do backlog
			backoffPendingCount.set(scalarLong(connection, """
					SELECT count(*) FROM outbox_message
					WHERE status = 'PENDING' AND available_at > now() AND available_at < 'infinity'
					"""));

			collectDeadLetterByDestination(connection);
		}
	}

	private void collectDeadLetterByDestination(Connection connection) throws SQLException {
		Map<String, Long> current = new HashMap<>();
		try (PreparedStatement select = connection.prepareStatement(
				"SELECT destination, count(*) AS total FROM outbox_message WHERE status = 'DEAD_LETTER' GROUP BY destination");
				ResultSet resultSet = select.executeQuery()) {
			while (resultSet.next()) {
				current.put(resultSet.getString("destination"), resultSet.getLong("total"));
			}
		}
		current.keySet().forEach(destination -> deadLetterCountByDestination.computeIfAbsent(destination, this::registerDeadLetterGauge));
		// destino que saiu do GROUP BY teve a quarentena resolvida: zera em vez de manter o último valor
		deadLetterCountByDestination.forEach((destination, value) -> value.set(current.getOrDefault(destination, 0L)));
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
