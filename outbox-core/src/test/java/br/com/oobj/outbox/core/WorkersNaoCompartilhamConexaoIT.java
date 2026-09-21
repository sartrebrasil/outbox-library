package br.com.oobj.outbox.core;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;


import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * T1.8: relay com dois workers na mesma instância continua publicando em ordem estrita de
 * seq. Se os workers compartilhassem conexão, os advisory locks de um apareceriam como
 * próprios do outro e este teste falharia (02 §4.6).
 */
class WorkersNaoCompartilhamConexaoIT extends AbstractPostgresIT {

	@Test
	void doisWorkersNaMesmaInstanciaPublicamEmOrdem() throws Exception {
		OutboxWriter writer = new JdbcOutboxWriter(OutboxConfig.defaults(), new SimpleMeterRegistry());
		int totalMessages = 15;
		try (Connection connection = newConnection()) {
			for (int i = 0; i < totalMessages; i++) {
				writer.enqueue(connection, OutboxMessage.of(
						"Pedido", "pedido-1", null, "Evento-" + i, "{}", "pedidos.fifo"));
			}
			connection.commit();
		}

		InMemoryMessagePublisher publisher = new InMemoryMessagePublisher();
		OutboxConfig config = OutboxConfig.defaults()
				.withPollingInterval(Duration.ofMillis(20))
				.withRelayWorkers(2);
		OutboxRelay relay = new OutboxRelay(dataSource(), config, new SimpleMeterRegistry(), Map.of("pedidos.fifo", publisher));
		try {
			relay.start();
			awaitCount(publisher, totalMessages);
		} finally {
			relay.close();
		}

		List<String> eventTypesRecebidos = publisher.published().stream()
				.map(OutboxEnvelope::eventType)
				.collect(Collectors.toList());
		List<String> ordemEsperada = IntStream.range(0, totalMessages)
				.mapToObj(i -> "Evento-" + i)
				.collect(Collectors.toList());
		assertThat(eventTypesRecebidos).isEqualTo(ordemEsperada);
	}

	private void awaitCount(InMemoryMessagePublisher publisher, int expected) throws InterruptedException {
		Instant deadline = Instant.now().plusSeconds(5);
		while (Instant.now().isBefore(deadline)) {
			if (publisher.published().size() >= expected) {
				return;
			}
			Thread.sleep(30);
		}
		throw new AssertionError("nem todas as mensagens foram publicadas dentro do prazo");
	}
}
