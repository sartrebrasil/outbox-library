package com.example.outbox.loadtest;

import tools.jackson.databind.SerializationFeature;
import tools.jackson.databind.json.JsonMapper;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Teste de carga do relay contra brokers reais. Só roda com {@code -Ploadtest} (docs/plan/06).
 *
 * <p>É um relatório, não um gate de build: falha só se um profile não drenar dentro do próprio
 * {@code timeoutSeconds}. Nunca compara throughput ou latência contra limiar.
 *
 * <pre>
 * mvn -pl outbox-loadtest -am verify -Ploadtest                         # todos os profiles do diretório
 * mvn -pl outbox-loadtest -am verify -Ploadtest -Dloadtest.profiles=inmemory-w1,rabbit-w4
 * mvn ... -Dloadtest.profilesDir=/caminho/meus-profiles
 * </pre>
 */
class OutboxLoadIT {

	private static final Logger LOG = LoggerFactory.getLogger(OutboxLoadIT.class);

	@Test
	void executaProfilesEGeraRelatorio() throws Exception {
		List<LoadProfile> profiles = carregarProfiles();
		assertThat(profiles).as("nenhum profile encontrado").isNotEmpty();

		List<Map<String, Object>> results = new ArrayList<>();
		try (Postgres db = new Postgres()) {
			LoadRunner runner = new LoadRunner(db);
			// um container de broker por tipo, reaproveitado entre os profiles do mesmo broker
			Map<String, List<LoadProfile>> porBroker = profiles.stream()
					.collect(Collectors.groupingBy(LoadProfile::broker, LinkedHashMap::new, Collectors.toList()));
			for (var entry : porBroker.entrySet()) {
				try (BrokerFixture fixture = BrokerFixture.create(entry.getKey())) {
					for (LoadProfile p : entry.getValue()) {
						List<String> destinations = IntStream.range(0, p.num("destinations", 3))
								.mapToObj(i -> p.name().replaceAll("[^A-Za-z0-9_-]", "-") + "-" + i + ".fifo").toList();
						LOG.info("profile {} ({})", p.name(), p.broker());
						Map<String, Object> r = runner.run(p, fixture, destinations);
						Map<String, Object> row = new LinkedHashMap<>();
						row.put("profile", p.name());
						row.put("config", p.props());
						row.put("result", r);
						results.add(row);
						LOG.info("profile {}: drained={} publish={} msg/s consume={} msg/s publishMs={} e2eMs={} consumeE2eMs={} dup={} outOfOrder={}",
								p.name(), r.get("drained"), r.get("throughputMsgPerSec"), r.get("consumeThroughputMsgPerSec"),
								r.get("publishMs"), r.get("e2eMs"), r.get("consumeE2eMs"), r.get("duplicates"), r.get("outOfOrder"));
					}
				}
			}
		}

		Path dir = Path.of(System.getProperty("loadtest.reportDir", "target/loadtest"),
				Instant.now().toString().replace(':', '-'));
		Files.createDirectories(dir);
		JsonMapper.builder().enable(SerializationFeature.INDENT_OUTPUT).build()
				.writeValue(dir.resolve("report.json").toFile(), results);
		Files.writeString(dir.resolve("summary.csv"), csv(results));
		LOG.info("relatório em {}", dir.toAbsolutePath());

		assertThat(results).allSatisfy(r -> assertThat(((Map<?, ?>) r.get("result")).get("drained"))
				.as("profile %s não drenou no prazo", r.get("profile")).isEqualTo(true));
	}

	@SuppressWarnings("unchecked")
	private static String csv(List<Map<String, Object>> results) {
		StringBuilder sb = new StringBuilder(
				"profile,broker,relayWorkers,candidateLimit,pollingIntervalMs,total,throughputMsgPerSec,enqueueP50,enqueueP95,enqueueP99,publishP50,publishP95,publishP99,e2eP50,e2eP95,e2eP99,consumers,consumeThroughputMsgPerSec,consumeP50,consumeP95,consumeP99,duplicates,outOfOrder,maxBacklog,drained\n");
		for (Map<String, Object> row : results) {
			Map<String, String> c = (Map<String, String>) row.get("config");
			Map<String, Object> r = (Map<String, Object>) row.get("result");
			Map<String, Double> enq = (Map<String, Double>) r.get("enqueueMs");
			Map<String, Double> pub = (Map<String, Double>) r.get("publishMs");
			Map<String, Double> e2e = (Map<String, Double>) r.get("e2eMs");
			Map<String, Double> cons = (Map<String, Double>) r.getOrDefault("consumeE2eMs", Map.of());
			sb.append(String.join(",", String.valueOf(row.get("profile")), c.getOrDefault("broker", "inmemory"),
					c.getOrDefault("relayWorkers", "1"), c.getOrDefault("candidateLimit", "default"),
					c.getOrDefault("pollingIntervalMs", "50"), String.valueOf(r.get("total")),
					String.valueOf(r.get("throughputMsgPerSec")), String.valueOf(enq.get("p50")),
					String.valueOf(enq.get("p95")), String.valueOf(enq.get("p99")), String.valueOf(pub.get("p50")),
					String.valueOf(pub.get("p95")), String.valueOf(pub.get("p99")), String.valueOf(e2e.get("p50")),
					String.valueOf(e2e.get("p95")), String.valueOf(e2e.get("p99")), c.getOrDefault("consumers", "1"),
					String.valueOf(r.get("consumeThroughputMsgPerSec")), String.valueOf(cons.get("p50")),
					String.valueOf(cons.get("p95")), String.valueOf(cons.get("p99")), String.valueOf(r.get("duplicates")),
					String.valueOf(r.get("outOfOrder")), String.valueOf(r.get("maxBacklog")),
					String.valueOf(r.get("drained")))).append('\n');
		}
		return sb.toString();
	}

	private static List<LoadProfile> carregarProfiles() throws Exception {
		Path dir = Path.of(System.getProperty("loadtest.profilesDir", "src/test/resources/profiles"));
		String wanted = System.getProperty("loadtest.profiles", "");
		List<String> names = wanted.isBlank() || wanted.startsWith("${") ? List.of() : List.of(wanted.split(","));
		try (Stream<Path> files = Files.list(dir)) {
			List<Path> sorted = files.filter(f -> f.toString().endsWith(".properties")).sorted().toList();
			List<LoadProfile> out = new ArrayList<>();
			for (Path f : sorted) {
				LoadProfile p = LoadProfile.load(f);
				if (names.isEmpty() || names.contains(p.name())) {
					out.add(p);
				}
			}
			return out;
		}
	}
}
