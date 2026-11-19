package com.example.outbox.core;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;

import java.time.Duration;

/**
 * Fachada das métricas de escrita/publicação de 02 §8.1. Cardinalidade proibida por
 * contrato: nenhum rótulo pode ser {@code aggregate_id}, {@code message_id} ou
 * {@code trace_id} — são campos ilimitados e derrubam o backend de métricas.
 */
public final class OutboxMetrics {

	private final MeterRegistry registry;

	public OutboxMetrics(MeterRegistry registry) {
		this.registry = registry;
	}

	MeterRegistry registry() {
		return registry;
	}

	public void enqueue(String aggregateType, String destination, String outcome, Duration duration) {
		registry.timer("outbox_enqueue_duration", Tags.of("aggregate_type", aggregateType, "destination", destination))
				.record(duration);
		registry.counter("outbox_enqueue_total",
				Tags.of("aggregate_type", aggregateType, "destination", destination, "outcome", outcome))
				.increment();
	}

	/** Tamanho do payload em bytes, medido antes da validação: as rejeitadas também entram, e mostram por quanto estouraram. */
	public void enqueuePayloadBytes(String destination, int bytes) {
		registry.summary("outbox_enqueue_payload_bytes", Tags.of("destination", destination)).record(bytes);
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

	/** {@code outcome}: {@code acquired}, {@code empty} ou {@code error} — ciclos vazios não podem diluir o percentil. */
	void cycleCompleted(Timer.Sample sample, String outcome) {
		sample.stop(registry.timer("outbox_relay_cycle_duration", Tags.of("outcome", outcome)));
	}

	void published(String destination) {
		registry.counter("outbox_published_total", Tags.of("destination", destination)).increment();
	}

	/** Do {@code created_at} (relógio de quem fez o enqueue) até a publicação — inclui backoff e espera em fila. */
	void endToEndLatency(String destination, Duration duration) {
		registry.timer("outbox_end_to_end_latency", Tags.of("destination", destination)).record(duration);
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

	/** Registra as séries da purga já em zero: sem isso, a primeira falha não gera {@code increase()}. */
	void registerPurgeMeters() {
		registry.counter("outbox_purge_failures_total");
		registry.timer("outbox_purge_duration");
	}

	void purgeDuration(Duration duration) {
		registry.timer("outbox_purge_duration").record(duration);
	}

	void purgeFailed() {
		registry.counter("outbox_purge_failures_total").increment();
	}

	/** Entradas em quarentena: o gauge de dead letter mostra o estoque, este mostra a taxa de entrada. */
	void quarantined(String destination) {
		registry.counter("outbox_quarantined_total", Tags.of("destination", destination)).increment();
	}

	/** Tentativas até o sucesso, contando a bem-sucedida: 1 = entregue de primeira. */
	void publishAttempts(String destination, int attempts) {
		registry.summary("outbox_publish_attempts", Tags.of("destination", destination)).record(attempts);
	}

	void purgeDeleted(long count) {
		registry.counter("outbox_purge_deleted_total").increment(count);
	}
}
