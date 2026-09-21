package br.com.oobj.outbox.core;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

public record OutboxEnvelope(
		UUID messageId,
		String aggregateType,
		String aggregateId,
		Long aggregateVersion,
		String eventType,
		Instant occurredAt,
		Map<String, String> headers,
		String payload,
		String destination,
		int attempts,
		String body) {
}
