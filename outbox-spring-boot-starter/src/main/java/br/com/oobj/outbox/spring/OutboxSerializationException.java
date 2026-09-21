package br.com.oobj.outbox.spring;

/** Payload que não serializa (02 §3.5): aborta o commit, igual a payload acima do limite. */
public class OutboxSerializationException extends RuntimeException {

	public OutboxSerializationException(String message, Throwable cause) {
		super(message, cause);
	}
}
