package br.com.oobj.outbox.spring;

import br.com.oobj.outbox.core.OutboxConfig;
import tools.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.jdbc.datasource.SimpleDriverDataSource;

import javax.sql.DataSource;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/** Vincula defaults de 02 §13 e overrides, incluindo o record aninhado (backoff) e mapas com chave pontuada. */
class OutboxPropertiesTest {

	private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
			.withBean(DataSource.class, SimpleDriverDataSource::new)
			.withBean(ObjectMapper.class, ObjectMapper::new)
			.withConfiguration(AutoConfigurations.of(OutboxAutoConfiguration.class));

	@Test
	void defaultsBatemComOsDeOutboxConfig() {
		contextRunner.run(context -> {
			OutboxConfig config = context.getBean(OutboxConfig.class);
			OutboxConfig esperado = OutboxConfig.defaults();
			assertThat(config.pollingInterval()).isEqualTo(esperado.pollingInterval());
			assertThat(config.candidateLimit()).isEqualTo(esperado.candidateLimit());
			assertThat(config.publishTimeout()).isEqualTo(esperado.publishTimeout());
			assertThat(config.sessionTimeoutMargin()).isEqualTo(esperado.sessionTimeoutMargin());
			assertThat(config.relayWorkers()).isEqualTo(esperado.relayWorkers());
			assertThat(config.shutdownTimeout()).isEqualTo(esperado.shutdownTimeout());
			assertThat(config.maxAttempts()).isEqualTo(esperado.maxAttempts());
			assertThat(config.backoff()).isEqualTo(esperado.backoff());
			assertThat(config.retention()).isEqualTo(esperado.retention());
			assertThat(config.purgeInterval()).isEqualTo(esperado.purgeInterval());
			assertThat(config.purgeBatchSize()).isEqualTo(esperado.purgeBatchSize());
			assertThat(config.metricsInterval()).isEqualTo(esperado.metricsInterval());
			assertThat(config.healthMaxPendingAge()).isNull();
			assertThat(config.payloadLimits().globalMaxBytes()).isEqualTo(esperado.payloadLimits().globalMaxBytes());
		});
	}

	@Test
	void overridesDeBackoffEDestinoComChavePontuadaSaoAplicados() {
		contextRunner
				.withPropertyValues(
						"outbox.backoff.initial=2s",
						"outbox.backoff.max=10m",
						"outbox.backoff.multiplier=3.0",
						"outbox.relay-workers=4",
						"outbox.destinations[pedidos.fifo].max-payload-bytes=10KB")
				.run(context -> {
					OutboxConfig config = context.getBean(OutboxConfig.class);
					assertThat(config.backoff().initial()).isEqualTo(Duration.ofSeconds(2));
					assertThat(config.backoff().max()).isEqualTo(Duration.ofMinutes(10));
					assertThat(config.backoff().multiplier()).isEqualTo(3.0);
					assertThat(config.relayWorkers()).isEqualTo(4);
					assertThat(config.payloadLimits().maxBytesFor("pedidos.fifo")).isEqualTo(10 * 1024L);
					assertThat(config.payloadLimits().maxBytesFor("outro.destino"))
							.isEqualTo(config.payloadLimits().globalMaxBytes());
				});
	}

	@Test
	void healthMaxPendingAgeSemDefaultFicaNuloQuandoAusente() {
		contextRunner.run(context ->
				assertThat(context.getBean(OutboxConfig.class).healthMaxPendingAge()).isNull());
	}

	@Test
	void healthMaxPendingAgeEhLidoQuandoConfigurado() {
		contextRunner.withPropertyValues("outbox.health-max-pending-age=45s")
				.run(context -> assertThat(context.getBean(OutboxConfig.class).healthMaxPendingAge())
						.isEqualTo(Duration.ofSeconds(45)));
	}
}
