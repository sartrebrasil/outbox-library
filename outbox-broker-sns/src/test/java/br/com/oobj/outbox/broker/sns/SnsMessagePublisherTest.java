package br.com.oobj.outbox.broker.sns;

import br.com.oobj.outbox.core.OutboxEnvelope;
import br.com.oobj.outbox.core.PermanentPublishException;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.InternalErrorException;
import software.amazon.awssdk.services.sns.model.InvalidParameterException;
import software.amazon.awssdk.services.sns.model.KmsAccessDeniedException;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.PublishResponse;
import software.amazon.awssdk.services.sns.model.ThrottledException;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** T2.1/T2.2: taxonomia de erro do SNS. Cada exceção mapeada tem teste; o default é transitória. */
class SnsMessagePublisherTest {

	private final SnsClient client = mock(SnsClient.class);
	private final SnsMessagePublisher publisher = new SnsMessagePublisher(client, "arn:aws:sns:us-east-1:123:pedidos.fifo");

	@Test
	void publicaComMessageGroupIdEDeduplicationIdCorretos() {
		when(client.publish(any(PublishRequest.class))).thenReturn(PublishResponse.builder().build());
		OutboxEnvelope envelope = envelope();

		assertThatCode(() -> publisher.publish(envelope, Duration.ofSeconds(5))).doesNotThrowAnyException();

		ArgumentCaptor<PublishRequest> captor = ArgumentCaptor.forClass(PublishRequest.class);
		verify(client).publish(captor.capture());
		assertThat(captor.getValue().messageGroupId()).isEqualTo("pedido-1");
		assertThat(captor.getValue().messageDeduplicationId()).isEqualTo(envelope.messageId().toString());
	}

	@Test
	void invalidParameterExceptionEhPermanente() {
		when(client.publish(any(PublishRequest.class))).thenThrow(InvalidParameterException.builder().message("x").build());

		assertThatThrownBy(() -> publisher.publish(envelope(), Duration.ofSeconds(5)))
				.isInstanceOf(PermanentPublishException.class);
	}

	@Test
	void kmsAccessDeniedExceptionEhPermanente() {
		when(client.publish(any(PublishRequest.class))).thenThrow(KmsAccessDeniedException.builder().message("x").build());

		assertThatThrownBy(() -> publisher.publish(envelope(), Duration.ofSeconds(5)))
				.isInstanceOf(PermanentPublishException.class);
	}

	@Test
	void corpoAcimaDoLimiteEhPermanenteSemChamarOCliente() {
		String corpoGigante = "\"" + "x".repeat(300 * 1024) + "\"";
		OutboxEnvelope grande = new OutboxEnvelope(UUID.randomUUID(), "Pedido", "pedido-1", null, "PedidoCriado",
				Instant.now(), Map.of(), corpoGigante, "pedidos.fifo", 0, corpoGigante);

		assertThatThrownBy(() -> publisher.publish(grande, Duration.ofSeconds(5)))
				.isInstanceOf(PermanentPublishException.class);
		verify(client, never()).publish(any(PublishRequest.class));
	}

	@Test
	void throttledExceptionEhTransitoriaPorPadrao() {
		when(client.publish(any(PublishRequest.class))).thenThrow(ThrottledException.builder().message("x").build());

		assertThatThrownBy(() -> publisher.publish(envelope(), Duration.ofSeconds(5)))
				.isInstanceOf(ThrottledException.class)
				.isNotInstanceOf(PermanentPublishException.class);
	}

	@Test
	void erroDesconhecidoDoSdkEhTransitorioPorPadrao() {
		when(client.publish(any(PublishRequest.class))).thenThrow(InternalErrorException.builder().message("x").build());

		assertThatThrownBy(() -> publisher.publish(envelope(), Duration.ofSeconds(5)))
				.isInstanceOf(InternalErrorException.class)
				.isNotInstanceOf(PermanentPublishException.class);
	}

	private static OutboxEnvelope envelope() {
		return new OutboxEnvelope(UUID.randomUUID(), "Pedido", "pedido-1", null, "PedidoCriado",
				Instant.now(), Map.of(), "{}", "pedidos.fifo", 0, "{}");
	}
}
