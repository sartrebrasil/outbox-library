package br.com.oobj.outbox.broker.sqs;

import br.com.oobj.outbox.core.OutboxEnvelope;
import br.com.oobj.outbox.core.PermanentPublishException;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.InvalidMessageContentsException;
import software.amazon.awssdk.services.sqs.model.KmsAccessDeniedException;
import software.amazon.awssdk.services.sqs.model.RequestThrottledException;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** T5: taxonomia de erro do SQS e detecção de FIFO pela URL. */
class SqsMessagePublisherTest {

	private final SqsClient client = mock(SqsClient.class);

	@Test
	void filaFifoEnviaMessageGroupIdEDeduplicationId() {
		when(client.sendMessage(any(SendMessageRequest.class))).thenReturn(SendMessageResponse.builder().build());
		SqsMessagePublisher publisher = new SqsMessagePublisher(client, "https://sqs.us-east-1.amazonaws.com/123/pedidos.fifo");
		OutboxEnvelope envelope = envelope();

		assertThatCode(() -> publisher.publish(envelope, Duration.ofSeconds(5))).doesNotThrowAnyException();

		ArgumentCaptor<SendMessageRequest> captor = ArgumentCaptor.forClass(SendMessageRequest.class);
		verify(client).sendMessage(captor.capture());
		assertThat(captor.getValue().messageGroupId()).isEqualTo("pedido-1");
		assertThat(captor.getValue().messageDeduplicationId()).isEqualTo(envelope.messageId().toString());
	}

	@Test
	void filaPadraoNaoEnviaMessageGroupIdNemDeduplicationId() {
		when(client.sendMessage(any(SendMessageRequest.class))).thenReturn(SendMessageResponse.builder().build());
		SqsMessagePublisher publisher = new SqsMessagePublisher(client, "https://sqs.us-east-1.amazonaws.com/123/pedidos");

		assertThatCode(() -> publisher.publish(envelope(), Duration.ofSeconds(5))).doesNotThrowAnyException();

		ArgumentCaptor<SendMessageRequest> captor = ArgumentCaptor.forClass(SendMessageRequest.class);
		verify(client).sendMessage(captor.capture());
		assertThat(captor.getValue().messageGroupId()).isNull();
		assertThat(captor.getValue().messageDeduplicationId()).isNull();
	}

	@Test
	void invalidMessageContentsExceptionEhPermanente() {
		when(client.sendMessage(any(SendMessageRequest.class)))
				.thenThrow(InvalidMessageContentsException.builder().message("x").build());
		SqsMessagePublisher publisher = new SqsMessagePublisher(client, "https://sqs.us-east-1.amazonaws.com/123/pedidos.fifo");

		assertThatThrownBy(() -> publisher.publish(envelope(), Duration.ofSeconds(5)))
				.isInstanceOf(PermanentPublishException.class);
	}

	@Test
	void kmsAccessDeniedExceptionEhPermanente() {
		when(client.sendMessage(any(SendMessageRequest.class)))
				.thenThrow(KmsAccessDeniedException.builder().message("x").build());
		SqsMessagePublisher publisher = new SqsMessagePublisher(client, "https://sqs.us-east-1.amazonaws.com/123/pedidos.fifo");

		assertThatThrownBy(() -> publisher.publish(envelope(), Duration.ofSeconds(5)))
				.isInstanceOf(PermanentPublishException.class);
	}

	@Test
	void corpoAcimaDoLimiteEhPermanenteSemChamarOCliente() {
		String corpoGigante = "\"" + "x".repeat(300 * 1024) + "\"";
		OutboxEnvelope grande = new OutboxEnvelope(UUID.randomUUID(), "Pedido", "pedido-1", null, "PedidoCriado",
				Instant.now(), Map.of(), corpoGigante, "pedidos.fifo", 0, corpoGigante);
		SqsMessagePublisher publisher = new SqsMessagePublisher(client, "https://sqs.us-east-1.amazonaws.com/123/pedidos.fifo");

		assertThatThrownBy(() -> publisher.publish(grande, Duration.ofSeconds(5)))
				.isInstanceOf(PermanentPublishException.class);
		verify(client, never()).sendMessage(any(SendMessageRequest.class));
	}

	@Test
	void requestThrottledExceptionEhTransitoriaPorPadrao() {
		when(client.sendMessage(any(SendMessageRequest.class)))
				.thenThrow(RequestThrottledException.builder().message("x").build());
		SqsMessagePublisher publisher = new SqsMessagePublisher(client, "https://sqs.us-east-1.amazonaws.com/123/pedidos.fifo");

		assertThatThrownBy(() -> publisher.publish(envelope(), Duration.ofSeconds(5)))
				.isInstanceOf(RequestThrottledException.class)
				.isNotInstanceOf(PermanentPublishException.class);
	}

	private static OutboxEnvelope envelope() {
		return new OutboxEnvelope(UUID.randomUUID(), "Pedido", "pedido-1", null, "PedidoCriado",
				Instant.now(), Map.of(), "{}", "pedidos.fifo", 0, "{}");
	}
}
