package br.com.oobj.outbox.core;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Supressão de repetição de falha transitória (02 §8.3): com retry infinito, um broker
 * indisponível por horas geraria um WARN por mensagem por ciclo. Loga na primeira falha e
 * depois em frequência decrescente — aqui, a cada falha cujo total acumulado é potência de
 * dois — até a recuperação, quando informa quantas tentativas foram suprimidas e por quanto
 * tempo o fluxo esteve indisponível.
 */
final class FailureSuppressor {

	private final Map<String, State> states = new ConcurrentHashMap<>();

	/** @return true se esta falha deve ser logada; false se deve ser suprimida. */
	boolean shouldLog(String key) {
		State state = states.computeIfAbsent(key, k -> new State());
		state.totalFailures++;
		if (state.totalFailures == 1) {
			state.firstFailureAt = Instant.now();
		}
		boolean log = isPowerOfTwo(state.totalFailures);
		if (log) {
			state.loggedCount++;
		}
		return log;
	}

	/** Chamar em toda publicação bem-sucedida; presente só quando havia falha em curso para a chave. */
	Optional<Recovery> recover(String key) {
		State state = states.remove(key);
		if (state == null) {
			return Optional.empty();
		}
		int suppressed = state.totalFailures - state.loggedCount;
		Duration duration = Duration.between(state.firstFailureAt, Instant.now());
		return Optional.of(new Recovery(suppressed, duration));
	}

	private static boolean isPowerOfTwo(int n) {
		return (n & (n - 1)) == 0;
	}

	record Recovery(int suppressedAttempts, Duration duration) {
	}

	private static final class State {
		int totalFailures;
		int loggedCount;
		Instant firstFailureAt;
	}
}
