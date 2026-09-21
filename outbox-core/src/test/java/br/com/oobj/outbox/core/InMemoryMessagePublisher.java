package br.com.oobj.outbox.core;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/** Publisher em memória (T1.5): registra envelopes publicados para asserção nos testes. */
final class InMemoryMessagePublisher implements MessagePublisher {

	private final List<OutboxEnvelope> published = new CopyOnWriteArrayList<>();

	@Override
	public void publish(OutboxEnvelope envelope, Duration timeout) {
		published.add(envelope);
	}

	List<OutboxEnvelope> published() {
		return published;
	}
}
