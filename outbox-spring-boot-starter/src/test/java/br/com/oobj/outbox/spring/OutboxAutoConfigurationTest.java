package br.com.oobj.outbox.spring;

import br.com.oobj.outbox.core.JdbcOutboxWriter;
import br.com.oobj.outbox.core.MessagePublisher;
import br.com.oobj.outbox.core.MetricsCollector;
import br.com.oobj.outbox.core.OutboxConfig;
import br.com.oobj.outbox.core.OutboxRelay;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.SimpleDriverDataSource;

import javax.sql.DataSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * T3: beans condicionais da autoconfiguração — relay e coletor só existem com pelo menos
 * um {@link MessagePublisher} registrado (02 §4.2.3); health indicator só com Actuator no
 * classpath (sempre, no build deste módulo) e o limiar configurado (02 §13).
 */
class OutboxAutoConfigurationTest {

	private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
			.withBean(DataSource.class, SimpleDriverDataSource::new)
			.withBean(ObjectMapper.class, ObjectMapper::new)
			.withConfiguration(AutoConfigurations.of(OutboxAutoConfiguration.class));

	@Test
	void semPublisherNaoCriaRelayNemColetor() {
		contextRunner.run(context -> {
			assertThat(context).hasSingleBean(OutboxConfig.class);
			assertThat(context).hasSingleBean(JdbcOutboxWriter.class);
			assertThat(context).hasSingleBean(SpringOutboxWriter.class);
			assertThat(context).doesNotHaveBean(OutboxRelay.class);
			assertThat(context).doesNotHaveBean(MetricsCollector.class);
			assertThat(context).doesNotHaveBean(OutboxHealthIndicator.class);
		});
	}

	@Test
	void comPublisherCriaRelayEColetor() {
		contextRunner.withUserConfiguration(PublisherDeTeste.class).run(context -> {
			assertThat(context).hasSingleBean(OutboxRelay.class);
			assertThat(context).hasSingleBean(MetricsCollector.class);
		});
	}

	@Test
	void semLimiarConfiguradoNaoCriaHealthIndicator() {
		contextRunner.run(context -> assertThat(context).doesNotHaveBean(OutboxHealthIndicator.class));
	}

	@Test
	void comLimiarConfiguradoCriaHealthIndicator() {
		contextRunner.withPropertyValues("outbox.health-max-pending-age=1m")
				.run(context -> assertThat(context).hasSingleBean(OutboxHealthIndicator.class));
	}

	@Configuration
	static class PublisherDeTeste {

		@Bean
		MessagePublisher pedidosPublisher() {
			return (envelope, timeout) -> { };
		}
	}
}
