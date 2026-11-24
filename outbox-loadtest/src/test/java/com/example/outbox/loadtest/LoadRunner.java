package com.example.outbox.loadtest;

import com.example.outbox.core.MessagePublisher;
import com.example.outbox.core.OutboxConfig;
import com.example.outbox.core.OutboxEnvelope;
import com.example.outbox.core.OutboxRelay;
import com.example.outbox.core.OutboxWriter;
import com.example.outbox.spring.JacksonPayloadSerializer;
import com.example.outbox.spring.JdbcOutboxWriter;
import com.example.outbox.spring.SpringOutboxWriter;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

/**
 * Roda um profile: escritores concorrentes inserem linhas PENDING (um agregado = uma tarefa, em ordem,
 * RF15.1), o relay real publica no broker, e mede onde o tempo vai.
 *
 * <p>Leitura do resultado: {@code publishMs} alto = o gargalo é o broker; {@code e2eMs} alto com
 * {@code publishMs} baixo = é o relay/Postgres (polling, claim, workers); {@code backlog} crescendo
 * até o fim da escrita = relay mais lento que os escritores.
 */
final class LoadRunner {

	private record LoadPayload(String d) {
	}

	private final Postgres db;

	LoadRunner(Postgres db) {
		this.db = db;
	}

	Map<String, Object> run(LoadProfile p, BrokerFixture fixture, List<String> destinations) throws Exception {
		Map<String, MessagePublisher> delegates = fixture.publishers(destinations);
		db.truncate();
		int aggregates = p.num("aggregates", 200);
		int perAggregate = p.num("eventsPerAggregate", 10);
		long total = (long) aggregates * perAggregate;
		LoadPayload payload = new LoadPayload("x".repeat(Math.max(0, p.num("payloadBytes", 512) - 8)));
		ConcurrentLinkedQueue<Long> enqueueNanos = new ConcurrentLinkedQueue<>();

		ConcurrentLinkedQueue<Long> publishNanos = new ConcurrentLinkedQueue<>();
		ConcurrentLinkedQueue<Long> e2eMillis = new ConcurrentLinkedQueue<>();
		AtomicLong published = new AtomicLong();
		AtomicLong lastPublishNanos = new AtomicLong();
		Map<String, MessagePublisher> timed = new TreeMap<>();
		delegates.forEach((dest, d) -> timed.put(dest, (OutboxEnvelope e, Duration timeout) -> {
			long t0 = System.nanoTime();
			d.publish(e, timeout);
			long t1 = System.nanoTime();
			publishNanos.add(t1 - t0);
			e2eMillis.add(Duration.between(e.occurredAt(), Instant.now()).toMillis());
			lastPublishNanos.set(t1);
			published.incrementAndGet();
		}));

		OutboxConfig config = OutboxConfig.defaults()
				.withPollingInterval(Duration.ofMillis(p.num("pollingIntervalMs", 50)))
				.withRelayWorkers(p.num("relayWorkers", 1))
				.withPublishTimeout(Duration.ofMillis(p.num("publishTimeoutMs", 5000)))
				.withRetention(Duration.ofDays(7));
		config = new OutboxConfig(config.pollingInterval(), p.num("candidateLimit", config.candidateLimit()),
				config.publishTimeout(), config.sessionTimeoutMargin(), config.relayWorkers(), config.shutdownTimeout(),
				config.maxAttempts(), config.backoff(), config.retention(), config.purgeInterval(),
				config.purgeBatchSize(), config.metricsInterval(), config.healthMaxPendingAge(), config.payloadLimits());

		boolean consume = p.flag("consume", true);
		ConsumptionTracker tracker = new ConsumptionTracker();
		AutoCloseable consumers = consume
				? fixture.consume(destinations, p.num("consumers", 1), tracker::accept) : () -> { };

		SimpleMeterRegistry registry = new SimpleMeterRegistry();
		List<Map<String, Object>> backlog = new ArrayList<>();
		long start = System.nanoTime();
		long firstWriteNanos = start;
		long writeDoneNanos;

		try (consumers; OutboxRelay relay = new OutboxRelay(db.dataSource(), config, registry, timed)) {
			boolean preload = p.flag("preload", false);
			if (!preload) {
				relay.start();
			}
			// o caminho real da aplicação: SpringOutboxWriter (acorda o relay no commit) sobre JdbcOutboxWriter
			var dataSource = db.springDataSource();
			var tx = new org.springframework.transaction.support.TransactionTemplate(
					new org.springframework.jdbc.datasource.DataSourceTransactionManager(dataSource));
			OutboxWriter writer = new SpringOutboxWriter(new JdbcOutboxWriter(config, registry,
					org.springframework.jdbc.core.simple.JdbcClient.create(dataSource),
					new JacksonPayloadSerializer(new tools.jackson.databind.ObjectMapper())), relay::wake);
			ExecutorService writers = Executors.newFixedThreadPool(p.num("writerThreads", 8));
			List<Future<?>> futures = IntStream.range(0, aggregates).mapToObj(a -> writers.submit(() -> {
				String aggregateId = "agregado-" + a;
				String dest = destinations.get(a % destinations.size());
				for (int v = 0; v < perAggregate; v++) {
					long t0 = System.nanoTime();
					final long version = v;
					tx.executeWithoutResult(status -> writer.enqueue("Pedido", aggregateId, version, "Evento", payload, dest));
					enqueueNanos.add(System.nanoTime() - t0);
				}
			})).collect(java.util.stream.Collectors.toList());
			for (Future<?> f : futures) {
				f.get();
			}
			writers.shutdown();
			writeDoneNanos = System.nanoTime();
			long relayStart = writeDoneNanos;
			if (preload) {
				relay.start();
				relayStart = System.nanoTime();
				firstWriteNanos = relayStart;
			}

			long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(p.num("timeoutSeconds", 120));
			while ((published.get() < total || (consume && tracker.unique.get() < total)) && System.nanoTime() < deadline) {
				backlog.add(Map.of("tMs", (System.nanoTime() - start) / 1_000_000, "pending", count("PENDING"),
						"published", published.get(), "consumed", tracker.unique.get()));
				Thread.sleep(500);
			}
			boolean published_ = published.get() >= total;
			boolean drained = published_ && (!consume || tracker.unique.get() >= total);
			long endNanos = published_ ? lastPublishNanos.get() : System.nanoTime();
			double seconds = Math.max(1e-9, (endNanos - firstWriteNanos) / 1e9);

			Map<String, Object> r = new LinkedHashMap<>();
			r.put("drained", drained);
			r.put("total", total);
			r.put("published", published.get());
			r.put("writeSeconds", round((writeDoneNanos - start) / 1e9));
			r.put("wallSeconds", round(seconds));
			r.put("throughputMsgPerSec", round(published.get() / seconds));
			r.put("enqueueMs", percentiles(enqueueNanos, 1e6));
			r.put("publishMs", percentiles(publishNanos, 1e6));
			r.put("e2eMs", percentiles(e2eMillis, 1));
			if (consume) {
				double consumeSeconds = Math.max(1e-9, (Math.max(tracker.lastConsumeNanos, endNanos) - firstWriteNanos) / 1e9);
				r.put("consumed", tracker.unique.get());
				r.put("consumeThroughputMsgPerSec", round(tracker.unique.get() / consumeSeconds));
				r.put("consumeE2eMs", percentiles(tracker.latencyMillis, 1));
				r.put("duplicates", tracker.duplicates.get());
				r.put("outOfOrder", tracker.outOfOrder.get());
				r.put("malformed", tracker.malformed.get());
			}
			r.put("maxBacklog", backlog.stream().mapToLong(b -> (Long) b.get("pending")).max().orElse(0));
			r.put("statusCounts", statusCounts());
			r.put("backlog", backlog);
			r.put("relayMeters", meters(registry));
			return r;
		}
	}

	private long count(String status) throws Exception {
		try (Connection c = db.connection(); PreparedStatement s = c.prepareStatement(
				"SELECT count(*) FROM outbox_message WHERE status = ?")) {
			s.setString(1, status);
			try (ResultSet rs = s.executeQuery()) {
				rs.next();
				return rs.getLong(1);
			}
		}
	}

	private Map<String, Long> statusCounts() throws Exception {
		Map<String, Long> m = new TreeMap<>();
		try (Connection c = db.connection(); Statement s = c.createStatement();
				ResultSet rs = s.executeQuery("SELECT status, count(*) FROM outbox_message GROUP BY status")) {
			while (rs.next()) {
				m.put(rs.getString(1), rs.getLong(2));
			}
		}
		return m;
	}

	/** Despeja todas as métricas do relay sem precisar conhecer os nomes: o gargalo costuma estar numa delas. */
	private static Map<String, Double> meters(SimpleMeterRegistry registry) {
		Map<String, Double> m = new TreeMap<>();
		for (Meter meter : registry.getMeters()) {
			String tags = meter.getId().getTags().stream().map(t -> t.getKey() + "=" + t.getValue())
					.collect(java.util.stream.Collectors.joining(","));
			meter.measure().forEach(ms -> m.put(meter.getId().getName() + "{" + tags + "}." + ms.getStatistic().name().toLowerCase(),
					round(ms.getValue())));
		}
		return m;
	}

	private static Map<String, Double> percentiles(ConcurrentLinkedQueue<Long> values, double divisor) {
		long[] a = values.stream().mapToLong(Long::longValue).sorted().toArray();
		Map<String, Double> m = new LinkedHashMap<>();
		if (a.length == 0) {
			return m;
		}
		m.put("p50", round(a[Math.min(a.length - 1, (int) (a.length * 0.50))] / divisor));
		m.put("p95", round(a[Math.min(a.length - 1, (int) (a.length * 0.95))] / divisor));
		m.put("p99", round(a[Math.min(a.length - 1, (int) (a.length * 0.99))] / divisor));
		m.put("max", round(a[a.length - 1] / divisor));
		return m;
	}

	private static double round(double v) {
		return Math.round(v * 100.0) / 100.0;
	}
}
