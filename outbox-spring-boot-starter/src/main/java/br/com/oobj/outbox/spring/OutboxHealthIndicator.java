package br.com.oobj.outbox.spring;

import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;

import java.time.Duration;

/**
 * Degrada quando {@code outbox_oldest_pending_age_seconds} ultrapassa
 * {@code outbox.health-max-pending-age} (02 §8.5). Lê o gauge do mesmo {@link MeterRegistry}
 * que o {@code MetricsCollector} alimenta — não duplica a consulta ao banco.
 */
public class OutboxHealthIndicator implements HealthIndicator {

	static final String METRIC_NAME = "outbox_oldest_pending_age_seconds";

	private final MeterRegistry registry;
	private final Duration maxPendingAge;

	public OutboxHealthIndicator(MeterRegistry registry, Duration maxPendingAge) {
		this.registry = registry;
		this.maxPendingAge = maxPendingAge;
	}

	@Override
	public Health health() {
		var gauge = registry.find(METRIC_NAME).gauge();
		if (gauge == null) {
			return Health.up().withDetail("motivo", "coletor de métricas ainda não rodou um ciclo").build();
		}
		double idadeSegundos = gauge.value();
		Health.Builder builder = idadeSegundos > maxPendingAge.toSeconds() ? Health.down() : Health.up();
		return builder
				.withDetail("oldestPendingAgeSeconds", idadeSegundos)
				.withDetail("maxPendingAgeSeconds", maxPendingAge.toSeconds())
				.build();
	}
}
