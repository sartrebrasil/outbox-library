package br.com.oobj.outbox.core;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Gate G2 (docs/plan/03 §5, questão em aberto 1 de docs/plan/01 §8.2): mede
 * outbox_multi_pending_streams sob carga sintética que estressa exatamente o cenário em
 * questão — rajadas de eventos no mesmo agregado, para responder "quantos fluxos de fato
 * têm dois ou mais pendentes simultâneos". Não compara contra limiar: a decisão de
 * demover RF24 para modo opcional foi explicitamente adiada para quando houver carga de
 * produção real, não uma carga sintética de laboratório (decisão registrada na sessão).
 *
 * <p>Este teste é um relatório, não um gate de build: falha só se mensagens não forem
 * publicadas dentro do prazo (defeito), nunca por o número medido ser alto ou baixo.
 */
class GateG2CargaIT extends AbstractPostgresIT {

	private static final Logger LOG = LoggerFactory.getLogger(GateG2CargaIT.class);

	private static final int AGREGADOS = 40;
	private static final String[] DESTINOS = {"pedidos.fifo", "notas.fifo", "pagamentos.fifo"};
	private static final int WRITER_THREADS = 10;

	@Test
	void medeMultiPendingStreamsSobCargaComRajadasPorAgregado() throws Exception {
		int totalMensagens = gerarCargaComRajadas();

		SimpleMeterRegistry registry = new SimpleMeterRegistry();
		InMemoryMessagePublisher publisherPedidos = new InMemoryMessagePublisher();
		InMemoryMessagePublisher publisherNotas = new InMemoryMessagePublisher();
		InMemoryMessagePublisher publisherPagamentos = new InMemoryMessagePublisher();
		OutboxConfig config = OutboxConfig.defaults()
				.withPollingInterval(Duration.ofMillis(15))
				.withRelayWorkers(4);
		OutboxRelay relay = new OutboxRelay(dataSource(), config, registry, Map.of(
				"pedidos.fifo", publisherPedidos,
				"notas.fifo", publisherNotas,
				"pagamentos.fifo", publisherPagamentos));

		List<Double> amostras = new CopyOnWriteArrayList<>();
		try (MetricsCollector coletor = new MetricsCollector(dataSource(), registry, Duration.ofMillis(100), Instant::now)) {
			relay.start();
			Instant deadline = Instant.now().plusSeconds(30);
			while (Instant.now().isBefore(deadline)) {
				var gauge = registry.find("outbox_multi_pending_streams").gauge();
				if (gauge != null) {
					amostras.add(gauge.value());
				}
				int publicadas = publisherPedidos.published().size() + publisherNotas.published().size()
						+ publisherPagamentos.published().size();
				if (publicadas >= totalMensagens) {
					break;
				}
				Thread.sleep(50);
			}
			int publicadas = publisherPedidos.published().size() + publisherNotas.published().size()
					+ publisherPagamentos.published().size();
			assertThat(publicadas).as("carga precisa drenar inteira pro relatório ser confiável").isEqualTo(totalMensagens);
		} finally {
			relay.close();
		}

		relatarResultado(totalMensagens, amostras);
	}

	private int gerarCargaComRajadas() throws Exception {
		OutboxWriter writer = new JdbcOutboxWriter(OutboxConfig.defaults(), new SimpleMeterRegistry());
		ExecutorService writers = Executors.newFixedThreadPool(WRITER_THREADS);
		CountDownLatch done = new CountDownLatch(AGREGADOS);
		java.util.concurrent.atomic.AtomicInteger totalMensagens = new java.util.concurrent.atomic.AtomicInteger();
		for (int a = 0; a < AGREGADOS; a++) {
			String aggregateId = "agregado-" + a;
			String destination = DESTINOS[a % DESTINOS.length];
			writers.submit(() -> {
				try {
					// RF15.1: escritas do mesmo agregado são serializadas pela aplicação — uma
					// rajada de 1 a 6 eventos, back-to-back, simulando um caso de uso real
					// (pedido criado + N itens) em vez de um evento isolado por vez.
					int rajada = ThreadLocalRandom.current().nextInt(1, 7);
					for (int i = 0; i < rajada; i++) {
						try (Connection connection = newConnection()) {
							writer.enqueue(connection, OutboxMessage.of(
									"Pedido", aggregateId, null, "Evento-" + i, "{}", destination));
							connection.commit();
						}
						totalMensagens.incrementAndGet();
					}
				} catch (Exception e) {
					throw new RuntimeException(e);
				} finally {
					done.countDown();
				}
			});
		}
		done.await();
		writers.shutdown();
		return totalMensagens.get();
	}

	private void relatarResultado(int totalMensagens, List<Double> amostras) {
		List<Double> naoVazio = new ArrayList<>(amostras);
		double max = naoVazio.stream().mapToDouble(Double::doubleValue).max().orElse(0);
		double media = naoVazio.stream().mapToDouble(Double::doubleValue).average().orElse(0);
		long amostrasComMultiPending = naoVazio.stream().filter(v -> v > 0).count();
		double fracaoTempoComMultiPending = naoVazio.isEmpty() ? 0 : (double) amostrasComMultiPending / naoVazio.size();

		LOG.atInfo().setMessage("Gate G2 — outbox_multi_pending_streams sob carga sintética")
				.addKeyValue("totalMensagens", totalMensagens)
				.addKeyValue("totalAgregados", AGREGADOS)
				.addKeyValue("amostras", naoVazio.size())
				.addKeyValue("maxMultiPendingStreams", max)
				.addKeyValue("mediaMultiPendingStreams", String.format("%.3f", media))
				.addKeyValue("fracaoTempoComMultiPendingMaiorQueZero", String.format("%.3f", fracaoTempoComMultiPending))
				.log();

		// Sanidade do harness, não do limiar de decisão (adiado para carga de produção real).
		assertThat(max).isGreaterThanOrEqualTo(0);
	}
}
