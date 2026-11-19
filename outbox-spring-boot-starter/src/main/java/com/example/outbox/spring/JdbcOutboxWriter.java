package com.example.outbox.spring;

import com.example.outbox.core.EnvelopeJsonWriter;
import com.example.outbox.core.OutboxConfig;
import com.example.outbox.core.OutboxMessage;
import com.example.outbox.core.OutboxMetrics;
import com.example.outbox.core.OutboxSerializationException;
import com.example.outbox.core.OutboxWriteException;
import com.example.outbox.core.OutboxWriter;
import com.example.outbox.core.PayloadSerializer;
import com.example.outbox.core.PayloadTooLargeException;
import io.micrometer.core.instrument.MeterRegistry;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.SqlParameterValue;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.nio.charset.StandardCharsets;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;

/**
 * Escritor JDBC (02 §3): INSERT via {@link JdbcClient}, que participa da transação Spring do chamador, fail-fast
 * fora de transação (§3.4) e validação de tamanho de payload por destino (§3.5).
 */
public final class JdbcOutboxWriter implements OutboxWriter {

	private static final Logger LOG = LoggerFactory.getLogger(JdbcOutboxWriter.class);
	private static final Tracer TRACER = GlobalOpenTelemetry.getTracer("com.example.outbox.core");

	private static final String INSERT_SQL = """
			INSERT INTO outbox_message
				(id, aggregate_type, aggregate_id, aggregate_version, event_type,
				 payload, headers, trace_id, destination, status, created_at, available_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', ?, ?)
			""";

	private final OutboxConfig.PayloadLimits payloadLimits;
	private final OutboxMetrics metrics;
	private final JdbcClient jdbcClient;
	private final PayloadSerializer serializer;

	public JdbcOutboxWriter(OutboxConfig config, MeterRegistry registry, JdbcClient jdbcClient, PayloadSerializer serializer) {
		this.payloadLimits = config.payloadLimits();
		this.metrics = new OutboxMetrics(registry);
		this.jdbcClient = jdbcClient;
		this.serializer = serializer;
	}

	@Override
	public void enqueue(OutboxMessage message) {
		Instant start = Instant.now();
		Span span = TRACER.spanBuilder("outbox.enqueue")
				.setSpanKind(SpanKind.INTERNAL)
				.setAttribute("messaging.destination", message.destination())
				.setAttribute("messaging.message_id", message.messageId().toString())
				.setAttribute("outbox.aggregate_type", message.aggregateType())
				.startSpan();
		try (Scope scope = span.makeCurrent()) {
			requireTransaction();
			validateSize(message);
			insert(message, span);
			metrics.enqueue(message.aggregateType(), message.destination(), "success", Duration.between(start, Instant.now()));
			LOG.atDebug()
					.setMessage("enqueue realizado")
					.addKeyValue("aggregateType", message.aggregateType())
					.addKeyValue("destination", message.destination())
					.log();
		} catch (PayloadTooLargeException e) {
			metrics.enqueue(message.aggregateType(), message.destination(), "rejected", Duration.between(start, Instant.now()));
			throw e;
		} catch (DataAccessException e) {
			metrics.enqueue(message.aggregateType(), message.destination(), "error", Duration.between(start, Instant.now()));
			throw new OutboxWriteException("falha ao gravar mensagem na outbox", e);
		} catch (RuntimeException e) {
			// ex.: IllegalStateException fora de transação — também é falha de escrita e não pode sumir da métrica
			metrics.enqueue(message.aggregateType(), message.destination(), "error", Duration.between(start, Instant.now()));
			throw e;
		} finally {
			span.end();
		}
	}

	/** Serializa antes de abrir o span do INSERT; a falha (ou a ausência de transação) também conta como {@code error}. */
	@Override
	public void enqueue(String aggregateType, String aggregateId, Long aggregateVersion,
			String eventType, Object payload, String destination) {
		Instant start = Instant.now();
		String json;
		try {
			requireTransaction();
			json = serializer.serialize(payload);
		} catch (RuntimeException e) {
			metrics.enqueue(aggregateType, destination, "error", Duration.between(start, Instant.now()));
			if (e instanceof OutboxSerializationException) {
				// só o tipo, nunca o valor (02 §8.3)
				LOG.atError().setMessage("payload não serializou")
						.addKeyValue("aggregateType", aggregateType)
						.addKeyValue("payloadType", payload == null ? "null" : payload.getClass().getName())
						.log();
			}
			throw e;
		}
		enqueue(OutboxMessage.of(aggregateType, aggregateId, aggregateVersion, eventType, json, destination));
	}

	private static void requireTransaction() {
		if (!TransactionSynchronizationManager.isActualTransactionActive()) {
			throw new IllegalStateException("enqueue() chamado fora de uma transação Spring ativa");
		}
	}

	private void validateSize(OutboxMessage message) {
		int payloadBytes = message.payload().getBytes(StandardCharsets.UTF_8).length;
		metrics.enqueuePayloadBytes(message.destination(), payloadBytes);
		long maxBytes = payloadLimits.maxBytesFor(message.destination());
		if (payloadBytes > maxBytes) {
			throw new PayloadTooLargeException(message.destination(), payloadBytes, maxBytes);
		}
	}

	private void insert(OutboxMessage message, Span span) {
		String traceId = span.getSpanContext().getTraceId();
		String traceparent = formatTraceparent(span);
		Map<String, String> headersWithTraceparent = mergeTraceparent(message.headers(), traceparent);

		jdbcClient.sql(INSERT_SQL)
				.param(message.messageId())
				.param(message.aggregateType())
				.param(message.aggregateId())
				.param(new SqlParameterValue(Types.BIGINT, message.aggregateVersion()))
				.param(message.eventType())
				.param(message.payload())
				.param(EnvelopeJsonWriter.headersToJson(headersWithTraceparent))
				.param(traceId)
				.param(message.destination())
				.param(message.occurredAt().atOffset(ZoneOffset.UTC))
				.param(message.occurredAt().atOffset(ZoneOffset.UTC))
				.update();
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
