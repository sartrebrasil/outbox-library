package br.com.oobj.outbox.core;

import java.time.Duration;

public interface MessagePublisher {

	void publish(OutboxEnvelope envelope, Duration timeout) throws PermanentPublishException;
}
