package br.com.oobj.outbox.broker.kafka;

/** Envolve falha transitória do cliente Kafka como unchecked, para o relay tratar como transitória (02 §5.4). */
public class KafkaPublishException extends RuntimeException {

	public KafkaPublishException(String message, Throwable cause) {
		super(message, cause);
	}

	public KafkaPublishException(Throwable cause) {
		super(cause);
	}
}
