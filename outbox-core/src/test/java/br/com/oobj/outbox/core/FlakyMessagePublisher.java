package br.com.oobj.outbox.core;

import java.time.Duration;

/** Falha nas N primeiras chamadas, depois sucede (T1.6). */
final class FlakyMessagePublisher implements MessagePublisher {

	private final int failuresBeforeSuccess;
	private final InMemoryMessagePublisher delegate = new InMemoryMessagePublisher();
	private int attempts = 0;

	FlakyMessagePublisher(int failuresBeforeSuccess) {
		this.failuresBeforeSuccess = failuresBeforeSuccess;
	}

	@Override
	public void publish(OutboxEnvelope envelope, Duration timeout) {
		attempts++;
		if (attempts <= failuresBeforeSuccess) {
			throw new RuntimeException("falha simulada " + attempts);
		}
		delegate.publish(envelope, timeout);
	}

	java.util.List<OutboxEnvelope> published() {
		return delegate.published();
	}
}
