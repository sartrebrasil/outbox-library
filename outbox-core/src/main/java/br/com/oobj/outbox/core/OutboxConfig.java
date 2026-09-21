package br.com.oobj.outbox.core;

import java.time.Duration;
import java.util.Map;

/**
 * Configuração imutável do core (02 §13). O core não lê propriedades: quem conhece
 * {@code Environment}/{@code @Value} é o starter, que monta este record.
 */
public record OutboxConfig(
		Duration pollingInterval,
		int candidateLimit,
		Duration publishTimeout,
		Duration sessionTimeoutMargin,
		int relayWorkers,
		Duration shutdownTimeout,
		int maxAttempts,
		Backoff backoff,
		Duration retention,
		Duration purgeInterval,
		int purgeBatchSize,
		Duration metricsInterval,
		Duration healthMaxPendingAge,
		PayloadLimits payloadLimits) {

	public record Backoff(Duration initial, Duration max, double multiplier) {

		public static Backoff defaults() {
			return new Backoff(Duration.ofSeconds(1), Duration.ofMinutes(5), 2.0);
		}
	}

	public record PayloadLimits(long globalMaxBytes, Map<String, Long> perDestinationMaxBytes) {

		public static PayloadLimits defaults() {
			return new PayloadLimits(256 * 1024L, Map.of());
		}

		public long maxBytesFor(String destination) {
			return perDestinationMaxBytes.getOrDefault(destination, globalMaxBytes);
		}
	}

	public OutboxConfig withPollingInterval(Duration pollingInterval) {
		return new OutboxConfig(pollingInterval, candidateLimit, publishTimeout, sessionTimeoutMargin,
				relayWorkers, shutdownTimeout, maxAttempts, backoff, retention, purgeInterval,
				purgeBatchSize, metricsInterval, healthMaxPendingAge, payloadLimits);
	}

	public OutboxConfig withPublishTimeout(Duration publishTimeout) {
		return new OutboxConfig(pollingInterval, candidateLimit, publishTimeout, sessionTimeoutMargin,
				relayWorkers, shutdownTimeout, maxAttempts, backoff, retention, purgeInterval,
				purgeBatchSize, metricsInterval, healthMaxPendingAge, payloadLimits);
	}

	public OutboxConfig withSessionTimeoutMargin(Duration sessionTimeoutMargin) {
		return new OutboxConfig(pollingInterval, candidateLimit, publishTimeout, sessionTimeoutMargin,
				relayWorkers, shutdownTimeout, maxAttempts, backoff, retention, purgeInterval,
				purgeBatchSize, metricsInterval, healthMaxPendingAge, payloadLimits);
	}

	public OutboxConfig withRelayWorkers(int relayWorkers) {
		return new OutboxConfig(pollingInterval, candidateLimit, publishTimeout, sessionTimeoutMargin,
				relayWorkers, shutdownTimeout, maxAttempts, backoff, retention, purgeInterval,
				purgeBatchSize, metricsInterval, healthMaxPendingAge, payloadLimits);
	}

	public OutboxConfig withBackoff(Backoff backoff) {
		return new OutboxConfig(pollingInterval, candidateLimit, publishTimeout, sessionTimeoutMargin,
				relayWorkers, shutdownTimeout, maxAttempts, backoff, retention, purgeInterval,
				purgeBatchSize, metricsInterval, healthMaxPendingAge, payloadLimits);
	}

	public OutboxConfig withPayloadLimits(PayloadLimits payloadLimits) {
		return new OutboxConfig(pollingInterval, candidateLimit, publishTimeout, sessionTimeoutMargin,
				relayWorkers, shutdownTimeout, maxAttempts, backoff, retention, purgeInterval,
				purgeBatchSize, metricsInterval, healthMaxPendingAge, payloadLimits);
	}

	public static OutboxConfig defaults() {
		return new OutboxConfig(
				Duration.ofSeconds(1),
				50,
				Duration.ofSeconds(5),
				Duration.ofSeconds(5),
				1,
				Duration.ofSeconds(30),
				-1,
				Backoff.defaults(),
				Duration.ofDays(7),
				Duration.ofHours(1),
				1000,
				Duration.ofSeconds(15),
				null,
				PayloadLimits.defaults());
	}
}
