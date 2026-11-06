package br.com.outbox.spring;

import br.com.outbox.core.JdbcOutboxWriter;
import br.com.outbox.core.MessagePublisher;
import br.com.outbox.core.MetricsCollector;
import br.com.outbox.core.OutboxConfig;
import br.com.outbox.core.OutboxRelay;
import br.com.outbox.core.PurgeJob;
import tools.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import javax.sql.DataSource;
import java.util.Map;

/**
 * Único módulo com Spring (02 §2): monta os beans a partir de {@link OutboxProperties} e
 * deixa o container gerenciar o ciclo de vida do relay e do coletor de métricas (02 §4.6).
 * Sem Spring, a aplicação monta os mesmos objetos na mão — nenhum bean aqui esconde lógica
 * que não exista também no core.
 */
@AutoConfiguration
@EnableConfigurationProperties(OutboxProperties.class)
@ConditionalOnClass(DataSource.class)
public class OutboxAutoConfiguration {

	@Bean
	@ConditionalOnMissingBean
	public OutboxConfig outboxConfig(OutboxProperties properties) {
		return properties.toOutboxConfig();
	}

	/** Fallback no-op (02 §3, nota de bloat): sem Actuator/backend de métricas, a lib ainda funciona. */
	@Bean
	@ConditionalOnMissingBean
	public MeterRegistry meterRegistry() {
		return new SimpleMeterRegistry();
	}

	@Bean
	@ConditionalOnMissingBean
	public JdbcOutboxWriter jdbcOutboxWriter(OutboxConfig config, MeterRegistry registry) {
		return new JdbcOutboxWriter(config, registry);
	}

	@Bean
	@ConditionalOnMissingBean
	public SpringOutboxWriter springOutboxWriter(JdbcOutboxWriter delegate, DataSource dataSource, ObjectMapper objectMapper,
			ObjectProvider<OutboxRelay> relay) {
		// sem relay neste contexto (sem MessagePublisher) o aviso não faz nada; o provider evita a dependência circular
		return new SpringOutboxWriter(delegate, dataSource, objectMapper, () -> relay.ifAvailable(OutboxRelay::wake));
	}

	/** Só existe se a aplicação registrou pelo menos um {@link MessagePublisher} (02 §4.2.3). */
	@Bean(initMethod = "start", destroyMethod = "close")
	@ConditionalOnMissingBean
	@ConditionalOnBean(MessagePublisher.class)
	public OutboxRelay outboxRelay(DataSource dataSource, OutboxConfig config, MeterRegistry registry,
			Map<String, MessagePublisher> publishers) {
		return new OutboxRelay(dataSource, config, registry, publishers);
	}

	@Bean(destroyMethod = "close")
	@ConditionalOnMissingBean
	@ConditionalOnBean(OutboxRelay.class)
	public MetricsCollector metricsCollector(DataSource dataSource, MeterRegistry registry, OutboxConfig config,
			OutboxRelay relay) {
		return new MetricsCollector(dataSource, registry, config.metricsInterval(), relay::lastCycleCompletedAt);
	}

	/** Independe de relay/publisher: purga PUBLISHED antigo mesmo em instância só-escritora (02 §6). */
	@Bean(initMethod = "start", destroyMethod = "close")
	@ConditionalOnMissingBean
	public PurgeJob purgeJob(DataSource dataSource, OutboxConfig config, MeterRegistry registry) {
		return new PurgeJob(dataSource, config, registry);
	}

	/**
	 * Só existe com Actuator no classpath e {@code outbox.health-max-pending-age}
	 * configurado (02 §13: sem default, porque um limiar inventado pela lib seria pior
	 * que a ausência do indicador).
	 */
	@Bean
	@ConditionalOnMissingBean
	@ConditionalOnClass(HealthIndicator.class)
	@ConditionalOnProperty(prefix = "outbox", name = "health-max-pending-age")
	public OutboxHealthIndicator outboxHealthIndicator(MeterRegistry registry, OutboxProperties properties) {
		return new OutboxHealthIndicator(registry, properties.healthMaxPendingAge());
	}
}
