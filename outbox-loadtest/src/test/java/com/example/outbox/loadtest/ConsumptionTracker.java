package com.example.outbox.loadtest;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Recebe os corpos entregues pelos consumidores e confere o que o relay promete: cada mensagem
 * chega, sem duplicata e, por agregado, em ordem de {@code aggregateVersion} (RF24).
 * A ordem só é garantida por agregado, e só se o broker e o número de consumidores a preservam:
 * várias consumidoras na mesma fila RabbitMQ, por exemplo, podem inverter, e isso aparece em {@code outOfOrder}.
 */
final class ConsumptionTracker {

	private static final ObjectMapper JSON = new ObjectMapper();

	private final ConcurrentHashMap<String, Long> lastVersion = new ConcurrentHashMap<>();
	final ConcurrentLinkedQueue<Long> latencyMillis = new ConcurrentLinkedQueue<>();
	final AtomicLong unique = new AtomicLong();
	final AtomicLong duplicates = new AtomicLong();
	final AtomicLong outOfOrder = new AtomicLong();
	final AtomicLong malformed = new AtomicLong();
	volatile long lastConsumeNanos;

	void accept(String body) {
		long now = System.nanoTime();
		try {
			JsonNode n = JSON.readTree(body);
			String aggregateId = n.get("aggregateId").asString();
			long version = n.get("aggregateVersion").asLong();
			Instant occurredAt = Instant.parse(n.get("occurredAt").asString());
			boolean[] kind = new boolean[2]; // [0]=duplicata, [1]=fora de ordem
			lastVersion.merge(aggregateId, version, (last, v) -> {
				if (v == last) {
					kind[0] = true;
					return last;
				}
				if (v < last) {
					kind[1] = true;
					return last;
				}
				return v;
			});
			if (kind[0]) {
				duplicates.incrementAndGet();
			} else if (kind[1]) {
				outOfOrder.incrementAndGet();
				unique.incrementAndGet();
			} else {
				unique.incrementAndGet();
			}
			latencyMillis.add(Duration.between(occurredAt, Instant.now()).toMillis());
		} catch (Exception e) {
			malformed.incrementAndGet();
		}
		lastConsumeNanos = now;
	}
}
