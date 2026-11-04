package br.com.oobj.outbox.core;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * O relay acordado (engine-tax T-114): {@link OutboxRelay#wake()} publica sem esperar o {@code pollingInterval}, um ciclo
 * que achou fluxo roda de novo já, e sem trabalho cada worker faz uma consulta por intervalo, não um laço quente.
 */
class RelayAcordadoIT extends AbstractPostgresIT {

	private static final Duration LONG_INTERVAL = Duration.ofSeconds(10);

	@Test
	void wakePublicaSemEsperarOIntervalo() throws Exception {
		InMemoryMessagePublisher publisher = new InMemoryMessagePublisher();
		OutboxRelay relay = relay(LONG_INTERVAL, 2, publisher, new SimpleMeterRegistry());
		try {
			relay.start();
			Thread.sleep(200); // os workers fazem o ciclo da partida, vazio, e passam a esperar
			UUID id = enqueue("pedido-1");
			Instant woken = Instant.now();
			relay.wake();
			awaitPublished(List.of(id), Duration.ofSeconds(2));
			assertThat(Duration.between(woken, Instant.now())).isLessThan(Duration.ofSeconds(2));
		} finally {
			relay.close();
		}
	}

	@Test
	void cicloQueAchouFluxoRodaDeNovoSemEsperar() throws Exception {
		List<UUID> ids = new ArrayList<>();
		for (int i = 0; i < 20; i++) {
			ids.add(enqueue("pedido-" + i)); // 20 agregados, 20 fluxos: um por ciclo
		}
		OutboxRelay relay = relay(LONG_INTERVAL, 1, new InMemoryMessagePublisher(), new SimpleMeterRegistry());
		try {
			relay.start(); // sem wake: com o intervalo antigo, 20 ciclos levariam 190 s
			awaitPublished(ids, Duration.ofSeconds(5));
		} finally {
			relay.close();
		}
	}

	@Test
	void ociosoFazUmaConsultaPorIntervaloPorWorker() throws Exception {
		SimpleMeterRegistry registry = new SimpleMeterRegistry();
		OutboxRelay relay = relay(Duration.ofMillis(300), 2, new InMemoryMessagePublisher(), registry);
		try {
			relay.start();
			Thread.sleep(1500);
		} finally {
			relay.close();
		}
		// 2 workers × (1 ciclo na partida + 1500 / 300) = 12; folga para o agendamento, nunca centenas
		assertThat(registry.timer("outbox_relay_cycle_duration").count()).isBetween(4L, 16L);
	}

	@Test
	void closeNaoEsperaOIntervalo() throws Exception {
		OutboxRelay relay = relay(LONG_INTERVAL, 4, new InMemoryMessagePublisher(), new SimpleMeterRegistry());
		relay.start();
		Thread.sleep(200);
		Instant closing = Instant.now();
		relay.close();
		assertThat(Duration.between(closing, Instant.now())).isLessThan(Duration.ofSeconds(2));
	}

	private OutboxRelay relay(Duration interval, int workers, MessagePublisher publisher, SimpleMeterRegistry registry) {
		OutboxConfig config = OutboxConfig.defaults().withPollingInterval(interval).withRelayWorkers(workers);
		return new OutboxRelay(dataSource(), config, registry, Map.of("pedidos.fifo", publisher));
	}

	private UUID enqueue(String aggregateId) throws Exception {
		OutboxMessage message = OutboxMessage.of("Pedido", aggregateId, 1L, "PedidoCriado", "{}", "pedidos.fifo");
		try (Connection connection = newConnection()) {
			new JdbcOutboxWriter(OutboxConfig.defaults(), new SimpleMeterRegistry()).enqueue(connection, message);
			connection.commit();
		}
		return message.messageId();
	}

	private void awaitPublished(List<UUID> ids, Duration timeout) throws Exception {
		Instant deadline = Instant.now().plus(timeout);
		while (Instant.now().isBefore(deadline)) {
			if (publishedCount(ids) == ids.size()) {
				return;
			}
			Thread.sleep(20);
		}
		throw new AssertionError("publicadas " + publishedCount(ids) + " de " + ids.size() + " dentro de " + timeout);
	}

	private int publishedCount(List<UUID> ids) throws Exception {
		try (Connection connection = newConnection();
				PreparedStatement select = connection.prepareStatement(
						"SELECT count(*) FROM outbox_message WHERE status = 'PUBLISHED' AND id = ANY (?)")) {
			select.setArray(1, connection.createArrayOf("uuid", ids.toArray()));
			try (ResultSet resultSet = select.executeQuery()) {
				resultSet.next();
				return resultSet.getInt(1);
			}
		}
	}
}
