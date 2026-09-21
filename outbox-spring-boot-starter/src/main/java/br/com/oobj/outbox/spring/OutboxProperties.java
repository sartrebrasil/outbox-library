package br.com.oobj.outbox.spring;

import br.com.oobj.outbox.core.OutboxConfig;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.util.unit.DataSize;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * Espelha os defaults de 02 §13 em propriedades Spring (prefixo {@code outbox}). O core
 * não lê propriedades (RF11) — só este record, que o {@code OutboxAutoConfiguration}
 * converte em {@link OutboxConfig}.
 */
@ConfigurationProperties(prefix = "outbox")
public record OutboxProperties(
		@DefaultValue("1s") Duration pollingInterval,
		@DefaultValue("50") int candidateLimit,
		@DefaultValue("5s") Duration publishTimeout,
		@DefaultValue("5s") Duration sessionTimeoutMargin,
		@DefaultValue("1") int relayWorkers,
		@DefaultValue("30s") Duration shutdownTimeout,
		@DefaultValue("-1") int maxAttempts,
		@DefaultValue Backoff backoff,
		@DefaultValue("7d") Duration retention,
		@DefaultValue("1h") Duration purgeInterval,
		@DefaultValue("1000") int purgeBatchSize,
		@DefaultValue("15s") Duration metricsInterval,
		Duration healthMaxPendingAge,
		@DefaultValue("256KB") DataSize maxPayloadBytes,
		Map<String, Destination> destinations) {

	public record Backoff(
			@DefaultValue("1s") Duration initial,
			@DefaultValue("5m") Duration max,
			@DefaultValue("2.0") double multiplier) {
	}

	public record Destination(DataSize maxPayloadBytes) {
	}

	public OutboxConfig toOutboxConfig() {
		Map<String, Long> perDestinationMaxBytes = new HashMap<>();
		if (destinations != null) {
			destinations.forEach((name, destination) -> {
				if (destination.maxPayloadBytes() != null) {
					perDestinationMaxBytes.put(name, destination.maxPayloadBytes().toBytes());
				}
			});
		}
		return new OutboxConfig(
				pollingInterval, candidateLimit, publishTimeout, sessionTimeoutMargin,
				relayWorkers, shutdownTimeout, maxAttempts,
				new OutboxConfig.Backoff(backoff.initial(), backoff.max(), backoff.multiplier()),
				retention, purgeInterval, purgeBatchSize, metricsInterval, healthMaxPendingAge,
				new OutboxConfig.PayloadLimits(maxPayloadBytes.toBytes(), perDestinationMaxBytes));
	}
}
