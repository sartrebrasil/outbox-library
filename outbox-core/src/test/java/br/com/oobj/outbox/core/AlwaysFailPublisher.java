package br.com.oobj.outbox.core;

import java.time.Duration;

/** Sempre falha permanentemente (T1.7): usado para acionar quarentena nos testes. */
final class AlwaysFailPublisher implements MessagePublisher {

	@Override
	public void publish(OutboxEnvelope envelope, Duration timeout) throws PermanentPublishException {
		throw new PermanentPublishException("erro permanente simulado");
	}
}
