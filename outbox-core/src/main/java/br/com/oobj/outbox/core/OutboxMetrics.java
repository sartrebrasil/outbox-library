package br.com.oobj.outbox.core;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;

import java.time.Duration;

/**
 * Fachada das métricas de escrita/publicação de 02 §8.1. Cardinalidade proibida por
 * contrato: nenhum rótulo pode ser {@code aggregate_id}, {@code message_id} ou
 * {@code trace_id} — são campos ilimitados e derrubam o backend de métricas.
 */
final class OutboxMetrics {

	private final MeterRegistry registry;

	OutboxMetrics(MeterRegistry registry) {
		this.registry = registry;
	}

	MeterRegistry registry() {
		return registry;
	}

	void enqueue(String aggregateType, String destination, String outcome, Duration duration) {
		registry.timer("outbox_enqueue_duration", Tags.of("aggregate_type", aggregateType, "destination", destination))
				.record(duration);
		registry.counter("outbox_enqueue_total",
				Tags.of("aggregate_type", aggregateType, "destination", destination, "outcome", outcome))
				.increment();
	}

	void claim(String outcome) {
		registry.counter("outbox_relay_claim_total", Tags.of("outcome", outcome)).increment();
	}

	void relayBatchSize(int size) {
		registry.summary("outbox_relay_batch_size").record(size);
	}

	Timer.Sample startCycle() {
		return Timer.start(registry);
	}

	void cycleCompleted(Timer.Sample sample) {
		sample.stop(registry.timer("outbox_relay_cycle_duration"));
	}

	void published(String destination, String broker) {
		registry.counter("outbox_published_total", Tags.of("destination", destination, "broker", broker)).increment();
	}

	void publishFailure(String destination, String classification) {
		registry.counter("outbox_publish_failures_total",
				Tags.of("destination", destination, "classification", classification)).increment();
	}

	void publishDuration(String destination, String outcome, Duration duration) {
		registry.timer("outbox_publish_duration", Tags.of("destination", destination, "outcome", outcome))
				.record(duration);
	}

	void retryScheduled(String destination) {
		registry.counter("outbox_retry_scheduled_total", Tags.of("destination", destination)).increment();
	}

	void purgeDeleted(long count) {
		registry.counter("outbox_purge_deleted_total").increment(count);
	}
}
