package br.com.oobj.outbox.core;

/**
 * Sinalizada pelo {@link MessagePublisher} do usuário para indicar falha permanente.
 * Toda outra exceção lançada por publish() é tratada como transitória (02 §5.1).
 */
public class PermanentPublishException extends Exception {

	public PermanentPublishException(String message) {
		super(message);
	}

	public PermanentPublishException(String message, Throwable cause) {
		super(message, cause);
	}
}
