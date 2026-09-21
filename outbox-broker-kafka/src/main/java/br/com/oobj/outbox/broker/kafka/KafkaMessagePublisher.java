package br.com.oobj.outbox.broker.kafka;

import br.com.oobj.outbox.core.MessagePublisher;
import br.com.oobj.outbox.core.OutboxEnvelope;
import br.com.oobj.outbox.core.PermanentPublishException;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.SerializationException;

import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * {@link MessagePublisher} para Apache Kafka (02 §5). Uma instância publica num único
 * tópico; destinos diferentes usam instâncias diferentes, cada uma registrada sob sua
 * própria chave no mapa de publishers do {@code OutboxRelay} (02 §4.2.3).
 *
 * <p>{@code aggregateId} como key do {@link ProducerRecord} é recomendação de uso para
 * preservar ordem no lado do broker (mesma partição) — não é garantia da lib (02 §3).
 * O {@link Producer} é responsabilidade da aplicação: bootstrap servers, serializers e os
 * defaults de {@code delivery.timeout.ms}/{@code request.timeout.ms} não são conhecimento
 * deste módulo — ver README para a limitação de aplicar o timeout por chamada.
 */
public final class KafkaMessagePublisher implements MessagePublisher {

	private final Producer<String, String> producer;
	private final String topic;

	public KafkaMessagePublisher(Producer<String, String> producer, String topic) {
		this.producer = producer;
		this.topic = topic;
	}

	@Override
	public void publish(OutboxEnvelope envelope, Duration timeout) throws PermanentPublishException {
		ProducerRecord<String, String> record = new ProducerRecord<>(topic, envelope.aggregateId(), envelope.body());
		try {
			producer.send(record).get(timeout.toMillis(), TimeUnit.MILLISECONDS);
		} catch (ExecutionException e) {
			Throwable cause = e.getCause();
			if (cause instanceof RecordTooLargeException || cause instanceof InvalidTopicException
					|| cause instanceof SerializationException) {
				throw new PermanentPublishException(cause.getMessage(), cause);
			}
			throw new KafkaPublishException(cause);
		} catch (TimeoutException e) {
			throw new KafkaPublishException(e); // transitória por padrão (02 §5.4)
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new KafkaPublishException(e);
		}
	}
}
