package br.com.oobj.outbox.core;

import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Job de purga (02 §6): apaga {@code PUBLISHED} mais antigos que a retenção, em lotes
 * limitados e repetidos até não haver mais elegíveis — nunca uma transação de exclusão
 * longa, que causaria o mesmo problema de horizonte de vacuum que o relay evita (§4.3).
 * `DEAD_LETTER` e as pendentes represadas atrás dele não têm prazo aqui, de propósito
 * (§6): quem resolve isso é gente, não a purga.
 */
public final class PurgeJob implements AutoCloseable {

	private static final Logger LOG = LoggerFactory.getLogger(PurgeJob.class);

	private static final String DELETE_BATCH_SQL = """
			DELETE FROM outbox_message
			WHERE id IN (
			    SELECT id FROM outbox_message
			    WHERE status = 'PUBLISHED' AND processed_at < ?
			    ORDER BY processed_at
			    LIMIT ?
			)
			""";

	private final DataSource dataSource;
	private final OutboxConfig config;
	private final OutboxMetrics metrics;
	private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

	public PurgeJob(DataSource dataSource, OutboxConfig config, MeterRegistry registry) {
		this.dataSource = dataSource;
		this.config = config;
		this.metrics = new OutboxMetrics(registry);
	}

	public void start() {
		executor.scheduleWithFixedDelay(this::runOneCycleQuietly, 0,
				config.purgeInterval().toMillis(), TimeUnit.MILLISECONDS);
	}

	@Override
	public void close() {
		executor.shutdown();
		try {
			if (!executor.awaitTermination(config.shutdownTimeout().toMillis(), TimeUnit.MILLISECONDS)) {
				executor.shutdownNow();
			}
		} catch (InterruptedException e) {
			executor.shutdownNow();
			Thread.currentThread().interrupt();
		}
	}

	private void runOneCycleQuietly() {
		try {
			runOneCycle();
		} catch (SQLException e) {
			LOG.error("ciclo de purga falhou", e);
		}
	}

	private void runOneCycle() throws SQLException {
		Instant start = Instant.now();
		long totalApagado = 0;
		int apagadoNoLote;
		do {
			apagadoNoLote = deleteBatch();
			totalApagado += apagadoNoLote;
		} while (apagadoNoLote == config.purgeBatchSize());

		if (totalApagado > 0) {
			metrics.purgeDeleted(totalApagado);
		}
		LOG.atInfo().setMessage("ciclo de purga concluído")
				.addKeyValue("linhasApagadas", totalApagado)
				.addKeyValue("duracaoMs", java.time.Duration.between(start, Instant.now()).toMillis())
				.log();
	}

	private int deleteBatch() throws SQLException {
		try (Connection connection = dataSource.getConnection();
				PreparedStatement statement = connection.prepareStatement(DELETE_BATCH_SQL)) {
			statement.setObject(1, OffsetDateTime.now(ZoneOffset.UTC).minus(config.retention()));
			statement.setInt(2, config.purgeBatchSize());
			return statement.executeUpdate();
		}
	}
}
