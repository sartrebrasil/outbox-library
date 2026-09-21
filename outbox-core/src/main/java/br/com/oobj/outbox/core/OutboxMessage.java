package br.com.oobj.outbox.core;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

public record OutboxMessage(
		UUID messageId,
		String aggregateType,
		String aggregateId,
		Long aggregateVersion,
		String eventType,
		String payload,
		Map<String, String> headers,
		String destination,
		Instant occurredAt) {

	public static OutboxMessage of(String aggregateType, String aggregateId, Long aggregateVersion,
			String eventType, String payload, String destination) {
		return new OutboxMessage(UUID.randomUUID(), aggregateType, aggregateId, aggregateVersion,
				eventType, payload, Map.of(), destination, Instant.now());
	}
}
