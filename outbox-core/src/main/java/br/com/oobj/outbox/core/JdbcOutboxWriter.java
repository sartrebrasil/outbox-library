package br.com.oobj.outbox.core;

import io.micrometer.core.instrument.MeterRegistry;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;

/**
 * Escritor JDBC do core (02 §3): INSERT dentro da transação do chamador, fail-fast de
 * auto-commit (§3.4) e validação de tamanho de payload por destino (§3.5).
 */
public final class JdbcOutboxWriter implements OutboxWriter {

	private static final Logger LOG = LoggerFactory.getLogger(JdbcOutboxWriter.class);
	private static final Tracer TRACER = GlobalOpenTelemetry.getTracer("br.com.oobj.outbox.core");

	private static final String INSERT_SQL = """
			INSERT INTO outbox_message
				(id, aggregate_type, aggregate_id, aggregate_version, event_type,
				 payload, headers, trace_id, destination, status, created_at, available_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', ?, ?)
			""";

	private final OutboxConfig.PayloadLimits payloadLimits;
	private final OutboxMetrics metrics;

	public JdbcOutboxWriter(OutboxConfig config, MeterRegistry registry) {
		this.payloadLimits = config.payloadLimits();
		this.metrics = new OutboxMetrics(registry);
	}

	@Override
	public void enqueue(Connection connection, OutboxMessage message) {
		Instant start = Instant.now();
		Span span = TRACER.spanBuilder("outbox.enqueue")
				.setSpanKind(SpanKind.INTERNAL)
				.setAttribute("messaging.destination", message.destination())
				.setAttribute("messaging.message_id", message.messageId().toString())
				.setAttribute("outbox.aggregate_type", message.aggregateType())
				.startSpan();
		try (Scope scope = span.makeCurrent()) {
			if (connection.getAutoCommit()) {
				throw new IllegalStateException("enqueue() chamado fora de uma transação ativa");
			}
			validateSize(message);
			insert(connection, message, span);
			metrics.enqueue(message.aggregateType(), message.destination(), "success", Duration.between(start, Instant.now()));
			LOG.atDebug()
					.setMessage("enqueue realizado")
					.addKeyValue("aggregateType", message.aggregateType())
					.addKeyValue("destination", message.destination())
					.log();
		} catch (PayloadTooLargeException e) {
			metrics.enqueue(message.aggregateType(), message.destination(), "rejected", Duration.between(start, Instant.now()));
			throw e;
		} catch (SQLException e) {
			metrics.enqueue(message.aggregateType(), message.destination(), "error", Duration.between(start, Instant.now()));
			throw new OutboxWriteException("falha ao gravar mensagem na outbox", e);
		} finally {
			span.end();
		}
	}

	private void validateSize(OutboxMessage message) {
		int payloadBytes = message.payload().getBytes(StandardCharsets.UTF_8).length;
		long maxBytes = payloadLimits.maxBytesFor(message.destination());
		if (payloadBytes > maxBytes) {
			throw new PayloadTooLargeException(message.destination(), payloadBytes, maxBytes);
		}
	}

	private void insert(Connection connection, OutboxMessage message, Span span) throws SQLException {
		String traceId = span.getSpanContext().getTraceId();
		String traceparent = formatTraceparent(span);
		Map<String, String> headersWithTraceparent = mergeTraceparent(message.headers(), traceparent);

		try (PreparedStatement statement = connection.prepareStatement(INSERT_SQL)) {
			statement.setObject(1, message.messageId());
			statement.setString(2, message.aggregateType());
			statement.setString(3, message.aggregateId());
			if (message.aggregateVersion() != null) {
				statement.setLong(4, message.aggregateVersion());
			} else {
				statement.setNull(4, Types.BIGINT);
			}
			statement.setString(5, message.eventType());
			statement.setString(6, message.payload());
			statement.setString(7, EnvelopeJsonWriter.headersToJson(headersWithTraceparent));
			statement.setString(8, traceId);
			statement.setString(9, message.destination());
			statement.setObject(10, message.occurredAt().atOffset(ZoneOffset.UTC));
			statement.setObject(11, message.occurredAt().atOffset(ZoneOffset.UTC));
			statement.executeUpdate();
		}
	}

	private static Map<String, String> mergeTraceparent(Map<String, String> headers, String traceparent) {
		Map<String, String> merged = headers == null ? new HashMap<>() : new HashMap<>(headers);
		merged.put("traceparent", traceparent);
		return merged;
	}

	/** Formata o {@code traceparent} W3C (https://www.w3.org/TR/trace-context/#traceparent-header) na mão: o core não traz o propagador como dependência só para isso. */
	private static String formatTraceparent(Span span) {
		var context = span.getSpanContext();
		return "00-" + context.getTraceId() + "-" + context.getSpanId() + "-" + context.getTraceFlags().asHex();
	}
}
