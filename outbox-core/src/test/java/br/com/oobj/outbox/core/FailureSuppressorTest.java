package br.com.oobj.outbox.core;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class FailureSuppressorTest {

	private final FailureSuppressor suppressor = new FailureSuppressor();

	@Test
	void logaSoNasPotenciasDeDoisEDepoisRecupera() {
		assertThat(suppressor.shouldLog("d1")).isTrue(); // 1ª falha: sempre loga
		assertThat(suppressor.shouldLog("d1")).isTrue(); // 2ª: potência de dois
		assertThat(suppressor.shouldLog("d1")).isFalse(); // 3ª: suprimida
		assertThat(suppressor.shouldLog("d1")).isTrue(); // 4ª: potência de dois
		assertThat(suppressor.shouldLog("d1")).isFalse(); // 5ª: suprimida
		assertThat(suppressor.shouldLog("d1")).isFalse(); // 6ª: suprimida
		assertThat(suppressor.shouldLog("d1")).isFalse(); // 7ª: suprimida
		assertThat(suppressor.shouldLog("d1")).isTrue(); // 8ª: potência de dois

		Optional<FailureSuppressor.Recovery> recovery = suppressor.recover("d1");
		assertThat(recovery).isPresent();
		assertThat(recovery.get().suppressedAttempts()).isEqualTo(8 - 4); // logadas: 1,2,4,8
	}

	@Test
	void chavesDiferentesNaoInterferemEntreSi() {
		suppressor.shouldLog("d1");
		suppressor.shouldLog("d1");
		assertThat(suppressor.shouldLog("d2")).isTrue(); // primeira falha de d2, não suprimida por d1
	}

	@Test
	void recuperarSemFalhaEmCursoNaoRetornaNada() {
		assertThat(suppressor.recover("nunca-falhou")).isEmpty();
	}
}
