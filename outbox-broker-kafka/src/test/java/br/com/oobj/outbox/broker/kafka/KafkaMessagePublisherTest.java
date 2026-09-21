package br.com.oobj.outbox.broker.kafka;

import br.com.oobj.outbox.core.OutboxEnvelope;
import br.com.oobj.outbox.core.PermanentPublishException;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.NotLeaderOrFollowerException;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** T2.3/T2.4: taxonomia de erro do Kafka. Cada exceção mapeada tem teste; o default é transitória. */
class KafkaMessagePublisherTest {

	@SuppressWarnings("unchecked")
	private final Producer<String, String> producer = mock(Producer.class);
	private final KafkaMessagePublisher publisher = new KafkaMessagePublisher(producer, "pedidos");

	@Test
	void publicaComAggregateIdComoKey() {
		when(producer.send(any())).thenReturn(CompletableFuture.completedFuture(mock(RecordMetadata.class)));
		OutboxEnvelope envelope = envelope();

		assertThatCode(() -> publisher.publish(envelope, Duration.ofSeconds(5))).doesNotThrowAnyException();

		ArgumentCaptor<ProducerRecord<String, String>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
		verify(producer).send(captor.capture());
		assertThat(captor.getValue().key()).isEqualTo("pedido-1");
		assertThat(captor.getValue().topic()).isEqualTo("pedidos");
		assertThat(captor.getValue().value()).isEqualTo(envelope.body());
	}

	@Test
	void recordTooLargeExceptionEhPermanente() {
		when(producer.send(any())).thenReturn(falha(new RecordTooLargeException("grande demais")));

		assertThatThrownBy(() -> publisher.publish(envelope(), Duration.ofSeconds(5)))
				.isInstanceOf(PermanentPublishException.class);
	}

	@Test
	void invalidTopicExceptionEhPermanente() {
		when(producer.send(any())).thenReturn(falha(new InvalidTopicException("tópico inválido")));

		assertThatThrownBy(() -> publisher.publish(envelope(), Duration.ofSeconds(5)))
				.isInstanceOf(PermanentPublishException.class);
	}

	@Test
	void serializationExceptionEhPermanente() {
		when(producer.send(any())).thenReturn(falha(new SerializationException("não serializou")));

		assertThatThrownBy(() -> publisher.publish(envelope(), Duration.ofSeconds(5)))
				.isInstanceOf(PermanentPublishException.class);
	}

	@Test
	void notLeaderOrFollowerExceptionEhTransitoriaPorPadrao() {
		when(producer.send(any())).thenReturn(falha(new NotLeaderOrFollowerException("sem líder agora")));

		assertThatThrownBy(() -> publisher.publish(envelope(), Duration.ofSeconds(5)))
				.isInstanceOf(KafkaPublishException.class)
				.isNotInstanceOf(PermanentPublishException.class)
				.hasCauseInstanceOf(NotLeaderOrFollowerException.class);
	}

	@Test
	void futureQueNuncaCompletaVirouTimeoutTransitorio() {
		when(producer.send(any())).thenReturn(new CompletableFuture<>()); // nunca completa

		assertThatThrownBy(() -> publisher.publish(envelope(), Duration.ofMillis(50)))
				.isInstanceOf(KafkaPublishException.class)
				.isNotInstanceOf(PermanentPublishException.class);
	}

	private static CompletableFuture<RecordMetadata> falha(Exception causa) {
		CompletableFuture<RecordMetadata> future = new CompletableFuture<>();
		future.completeExceptionally(causa);
		return future;
	}

	private static OutboxEnvelope envelope() {
		return new OutboxEnvelope(UUID.randomUUID(), "Pedido", "pedido-1", null, "PedidoCriado",
				Instant.now(), Map.of(), "{}", "pedidos", 0, "{}");
	}
}
