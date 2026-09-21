package br.com.oobj.outbox.core;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.api.trace.Tracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Relay de polling (02 §4). Sem Spring, a aplicação instancia e chama {@link #start()}
 * (RF11). Cada worker tem sua própria conexão — advisory locks são de sessão, e
 * compartilhar conexão entre workers os faria ver o lock um do outro como próprio (§4.6).
 */
public final class OutboxRelay implements AutoCloseable {

	private static final Logger LOG = LoggerFactory.getLogger(OutboxRelay.class);
	private static final Tracer TRACER = GlobalOpenTelemetry.getTracer("br.com.oobj.outbox.core");
	private static final Pattern TRACEPARENT = Pattern.compile("^00-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})$");

	private final DataSource dataSource;
	private final OutboxConfig config;
	private final Map<String, MessagePublisher> publishers;
	private final ScheduledExecutorService executor;
	private final OutboxMetrics metrics;
	private final FailureSuppressor failureSuppressor = new FailureSuppressor();
	private final AtomicReference<Instant> lastCycleCompletedAt = new AtomicReference<>(Instant.now());

	public OutboxRelay(DataSource dataSource, OutboxConfig config, MeterRegistry registry,
			Map<String, MessagePublisher> publishers) {
		this.dataSource = dataSource;
		this.config = config;
		this.publishers = publishers;
		this.metrics = new OutboxMetrics(registry);
		this.executor = Executors.newScheduledThreadPool(config.relayWorkers());
	}

	/** Alimenta {@link MetricsCollector}: instante do último ciclo concluído, para detectar relay morto. */
	public Instant lastCycleCompletedAt() {
		return lastCycleCompletedAt.get();
	}

	public void start() {
		LOG.atInfo().setMessage("relay iniciado")
				.addKeyValue("relayWorkers", config.relayWorkers())
				.addKeyValue("pollingInterval", config.pollingInterval())
				.addKeyValue("candidateLimit", config.candidateLimit())
				.log();
		long intervalMillis = config.pollingInterval().toMillis();
		for (int worker = 0; worker < config.relayWorkers(); worker++) {
			executor.scheduleWithFixedDelay(this::runOneCycleQuietly, 0, intervalMillis, TimeUnit.MILLISECONDS);
		}
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
		LOG.atInfo().setMessage("relay parado").log();
	}

	private void runOneCycleQuietly() {
		try {
			runOneCycle();
		} catch (Exception e) {
			LOG.error("ciclo do relay falhou", e);
		}
	}

	private void runOneCycle() throws SQLException {
		Timer.Sample cycleSample = metrics.startCycle();
		try (Connection connection = dataSource.getConnection()) {
			connection.setAutoCommit(false);
			applyIdleTransactionTimeout(connection);
			Optional<Fluxo> claimed = FluxoClaimer.claimNext(connection, config.candidateLimit(), metrics);
			if (claimed.isPresent()) {
				publishFluxo(connection, claimed.get());
			}
			connection.commit();
			lastCycleCompletedAt.set(Instant.now());
		} finally {
			metrics.cycleCompleted(cycleSample);
		}
	}

	/**
	 * Rede de segurança do banco (02 §4.3.1): se o publisher travar apesar do timeout que
	 * recebe como parâmetro, o próprio Postgres encerra a sessão ociosa-dentro-de-transação,
	 * o que aborta a transação e libera o advisory lock sem depender do publisher se comportar.
	 */
	private void applyIdleTransactionTimeout(Connection connection) throws SQLException {
		long timeoutMillis = config.publishTimeout().plus(config.sessionTimeoutMargin()).toMillis();
		try (Statement statement = connection.createStatement()) {
			statement.execute("SET idle_in_transaction_session_timeout = '" + timeoutMillis + "ms'");
		}
	}

	/**
	 * Publica em sequência e interrompe no primeiro erro (02 §4.4) — nunca pula para a
	 * próxima mensagem do fluxo, é isso que preserva a ordem.
	 */
	private void publishFluxo(Connection connection, Fluxo fluxo) throws SQLException {
		int published = 0;
		for (PendingMessage message : pendingMessagesOf(connection, fluxo)) {
			Instant start = Instant.now();
			Span span = startPublishSpan(fluxo, message);
			try {
				resolvePublisher(fluxo.destination()).publish(toEnvelope(message), config.publishTimeout());
				markPublished(connection, message.id());
				published++;
				span.setAttribute("outbox.outcome", "success");
				metrics.published(fluxo.destination(), "core");
				metrics.publishDuration(fluxo.destination(), "success", Duration.between(start, Instant.now()));
				logPublishSuccessOrRecovery(fluxo, message);
			} catch (PermanentPublishException e) {
				span.setAttribute("outbox.outcome", "permanent_failure");
				span.recordException(e);
				metrics.publishFailure(fluxo.destination(), "permanent");
				metrics.publishDuration(fluxo.destination(), "permanent_failure", Duration.between(start, Instant.now()));
				quarantine(connection, fluxo, message, e.getMessage());
				return;
			} catch (Exception e) {
				span.setAttribute("outbox.outcome", "transient_failure");
				span.recordException(e);
				metrics.publishFailure(fluxo.destination(), "transient");
				metrics.publishDuration(fluxo.destination(), "transient_failure", Duration.between(start, Instant.now()));
				metrics.retryScheduled(fluxo.destination());
				backoff(connection, fluxo, message, e);
				return;
			} finally {
				span.end();
			}
		}
		if (published > 0) {
			metrics.relayBatchSize(published);
		}
	}

	private Span startPublishSpan(Fluxo fluxo, PendingMessage message) {
		var builder = TRACER.spanBuilder("outbox.publish")
				.setSpanKind(SpanKind.PRODUCER)
				.setAttribute("messaging.destination", fluxo.destination())
				.setAttribute("messaging.message_id", message.id().toString())
				.setAttribute("outbox.aggregate_type", message.aggregateType())
				.setAttribute("outbox.attempts", message.attempts());
		parseTraceparent(message.headers().get("traceparent")).ifPresent(builder::addLink);
		return builder.startSpan();
	}

	/** Parseia o traceparent gravado pelo enqueue (T1.12) só para linkar o span, nunca como pai (02 §8.4). */
	private static Optional<SpanContext> parseTraceparent(String traceparent) {
		if (traceparent == null) {
			return Optional.empty();
		}
		Matcher matcher = TRACEPARENT.matcher(traceparent);
		if (!matcher.matches()) {
			return Optional.empty();
		}
		TraceFlags flags = TraceFlags.fromHex(matcher.group(3), 0);
		return Optional.of(SpanContext.createFromRemoteParent(
				matcher.group(1), matcher.group(2), flags, TraceState.getDefault()));
	}

	private void logPublishSuccessOrRecovery(Fluxo fluxo, PendingMessage message) {
		LOG.atDebug().setMessage("publicação bem-sucedida")
				.addKeyValue("aggregateId", message.aggregateId())
				.addKeyValue("destination", fluxo.destination())
				.addKeyValue("attempts", message.attempts())
				.log();
		failureSuppressor.recover(fluxo.destination()).ifPresent(recovery ->
				LOG.atInfo().setMessage("Publicação recuperada")
						.addKeyValue("destination", fluxo.destination())
						.addKeyValue("tentativasSuprimidas", recovery.suppressedAttempts())
						.addKeyValue("duracaoFalhaSegundos", recovery.duration().toSeconds())
						.log());
	}

	private MessagePublisher resolvePublisher(String destination) {
		MessagePublisher publisher = publishers.get(destination);
		if (publisher == null) {
			throw new IllegalStateException("nenhum publisher configurado para o destino '" + destination + "'");
		}
		return publisher;
	}

	/** Quarentena (02 §5.2): a mensagem que falhou vai a DEAD_LETTER, o resto do fluxo congela. */
	private void quarantine(Connection connection, Fluxo fluxo, PendingMessage message, String error) throws SQLException {
		try (PreparedStatement update = connection.prepareStatement(
				"UPDATE outbox_message SET status = 'DEAD_LETTER', last_error = ? WHERE id = ?")) {
			update.setString(1, error);
			update.setObject(2, message.id());
			update.executeUpdate();
		}
		try (PreparedStatement update = connection.prepareStatement(
				"UPDATE outbox_message SET available_at = 'infinity' "
						+ "WHERE aggregate_id = ? AND destination = ? AND status = 'PENDING'")) {
			update.setString(1, fluxo.aggregateId());
			update.setString(2, fluxo.destination());
			update.executeUpdate();
		}
		LOG.atError().setMessage("Falha permanente na publicação, mensagem em DEAD_LETTER")
				.addKeyValue("aggregateId", message.aggregateId())
				.addKeyValue("destination", fluxo.destination())
				.addKeyValue("attempts", message.attempts())
				.log();
		LOG.atWarn().setMessage("Fluxo tornado inelegível por quarentena")
				.addKeyValue("aggregateId", fluxo.aggregateId())
				.addKeyValue("destination", fluxo.destination())
				.log();
	}

	/** Falha transitória (02 §4.2.1, §5.1): adia o fluxo inteiro; attempts é só da mensagem que falhou. */
	private void backoff(Connection connection, Fluxo fluxo, PendingMessage message, Exception error) throws SQLException {
		Duration delay = backoffDelay(message.attempts());
		Instant nextAttemptAt = Instant.now().plus(delay);
		try (PreparedStatement update = connection.prepareStatement("""
				UPDATE outbox_message
				SET available_at = now() + (? || ' milliseconds')::interval,
				    attempts    = attempts + CASE WHEN id = ? THEN 1 ELSE 0 END,
				    last_error  = CASE WHEN id = ? THEN ? ELSE last_error END
				WHERE aggregate_id = ? AND destination = ? AND status = 'PENDING'
				""")) {
			update.setLong(1, delay.toMillis());
			update.setObject(2, message.id());
			update.setObject(3, message.id());
			update.setString(4, error.getMessage());
			update.setString(5, fluxo.aggregateId());
			update.setString(6, fluxo.destination());
			update.executeUpdate();
		}
		if (failureSuppressor.shouldLog(fluxo.destination())) {
			LOG.atWarn().setMessage("Falha transitória na publicação")
					.addKeyValue("aggregateId", message.aggregateId())
					.addKeyValue("destination", fluxo.destination())
					.addKeyValue("attempts", message.attempts() + 1)
					.addKeyValue("nextAttemptAt", nextAttemptAt)
					.addKeyValue("error", error.getClass().getSimpleName())
					.log();
		}
	}

	private Duration backoffDelay(int attemptsSoFar) {
		OutboxConfig.Backoff backoff = config.backoff();
		double factor = Math.pow(backoff.multiplier(), attemptsSoFar);
		Duration delay = Duration.ofMillis((long) (backoff.initial().toMillis() * factor));
		return delay.compareTo(backoff.max()) > 0 ? backoff.max() : delay;
	}

	private OutboxEnvelope toEnvelope(PendingMessage message) {
		OutboxEnvelope withoutBody = new OutboxEnvelope(
				message.id(), message.aggregateType(), message.aggregateId(), message.aggregateVersion(),
				message.eventType(), message.occurredAt(), message.headers(), message.payload(),
				message.destination(), message.attempts(), null);
		String body = EnvelopeJsonWriter.writeBody(withoutBody);
		return new OutboxEnvelope(
				withoutBody.messageId(), withoutBody.aggregateType(), withoutBody.aggregateId(),
				withoutBody.aggregateVersion(), withoutBody.eventType(), withoutBody.occurredAt(),
				withoutBody.headers(), withoutBody.payload(), withoutBody.destination(),
				withoutBody.attempts(), body);
	}

	private record PendingMessage(
			UUID id, String aggregateType, String aggregateId, Long aggregateVersion, String eventType,
			String payload, Map<String, String> headers, String destination, Instant occurredAt, int attempts) {
	}

	private List<PendingMessage> pendingMessagesOf(Connection connection, Fluxo fluxo) throws SQLException {
		List<PendingMessage> messages = new ArrayList<>();
		try (PreparedStatement select = connection.prepareStatement("""
				SELECT id, aggregate_type, aggregate_id, aggregate_version, event_type,
				       payload, headers, destination, created_at, attempts
				FROM outbox_message
				WHERE aggregate_id = ? AND destination = ? AND status = 'PENDING'
				ORDER BY seq
				""")) {
			select.setString(1, fluxo.aggregateId());
			select.setString(2, fluxo.destination());
			try (ResultSet resultSet = select.executeQuery()) {
				while (resultSet.next()) {
					Long aggregateVersion = resultSet.getObject("aggregate_version", Long.class);
					messages.add(new PendingMessage(
							(UUID) resultSet.getObject("id"),
							resultSet.getString("aggregate_type"),
							resultSet.getString("aggregate_id"),
							aggregateVersion,
							resultSet.getString("event_type"),
							resultSet.getString("payload"),
							EnvelopeJsonWriter.parseHeaders(resultSet.getString("headers")),
							resultSet.getString("destination"),
							resultSet.getObject("created_at", OffsetDateTime.class).toInstant(),
							resultSet.getInt("attempts")));
				}
			}
		}
		return messages;
	}

	private void markPublished(Connection connection, UUID id) throws SQLException {
		try (PreparedStatement update = connection.prepareStatement(
				"UPDATE outbox_message SET status = 'PUBLISHED', processed_at = now() WHERE id = ?")) {
			update.setObject(1, id);
			update.executeUpdate();
		}
	}
}
