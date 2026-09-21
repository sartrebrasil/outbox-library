package br.com.oobj.outbox.core;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Ignora o {@code Duration} recebido e trava na primeira chamada (T1.9): simula um
 * publisher mal-comportado para provar que o idle_in_transaction_session_timeout do
 * banco é quem resolve, não o publisher.
 */
final class SlowOnceThenFastPublisher implements MessagePublisher {

	private final AtomicInteger calls = new AtomicInteger();
	private final long firstCallSleepMillis;

	SlowOnceThenFastPublisher(long firstCallSleepMillis) {
		this.firstCallSleepMillis = firstCallSleepMillis;
	}

	@Override
	public void publish(OutboxEnvelope envelope, Duration timeout) {
		if (calls.getAndIncrement() == 0) {
			try {
				Thread.sleep(firstCallSleepMillis);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}
	}

	int calls() {
		return calls.get();
	}
}
