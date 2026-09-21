package br.com.oobj.outbox.broker.rabbitmq;

/** Envolve falha transitória do cliente RabbitMQ como unchecked, para o relay tratar como transitória (02 §5.4). */
public class RabbitMqPublishException extends RuntimeException {

	public RabbitMqPublishException(Throwable cause) {
		super(cause);
	}
}
