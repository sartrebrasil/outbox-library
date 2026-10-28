package br.com.oobj.outbox.broker.messaging;

import br.com.oobj.outbox.core.OutboxEnvelope;
import br.com.oobj.outbox.core.PermanentPublishException;
import com.example.messaging.AccessDeniedException;
import com.example.messaging.DestinationNotFoundException;
import com.example.messaging.MessageSender;
import com.example.messaging.MessageTooLargeException;
import com.example.messaging.MessagingException;
import com.example.messaging.OutgoingMessage;
import com.example.messaging.SendResult;
import com.example.messaging.ThrottledException;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Mapeamento do envelope para OutgoingMessage e taxonomia de erro (02 §5.4). */
class MessageSenderPublisherTest {

	private final MessageSender sender = mock(MessageSender.class);
	private final MessageSenderPublisher publisher = new MessageSenderPublisher(sender);

	@Test
	void enviaCorpoJsonComAggregateIdComoOrderingKeyEMessageIdComoDeduplicationId() throws Exception {
		when(sender.send(any())).thenReturn(new SendResult("id-1"));
		OutboxEnvelope envelope = envelope("pedido-1");

		publisher.publish(envelope, Duration.ofSeconds(5));

		ArgumentCaptor<OutgoingMessage> captor = ArgumentCaptor.forClass(OutgoingMessage.class);
		verify(sender).send(captor.capture());
		OutgoingMessage enviada = captor.getValue();
		assertThat(new String(enviada.body(), StandardCharsets.UTF_8)).isEqualTo(envelope.body());
		assertThat(enviada.contentType()).isEqualTo("application/json");
		assertThat(enviada.orderingKey()).isEqualTo("pedido-1");
		assertThat(enviada.deduplicationId()).isEqualTo(envelope.messageId().toString());
		assertThat(enviada.attributes()).isEmpty();
	}

	@Test
	void aggregateIdInvalidoComoOrderingKeyEhPermanenteSemChamarOSender() {
		assertThatThrownBy(() -> publisher.publish(envelope("pedido 1"), Duration.ofSeconds(5)))
				.isInstanceOf(PermanentPublishException.class);
		verify(sender, never()).send(any());
	}

	@Test
	void mensagemGrandeDemaisEhPermanente() {
		when(sender.send(any())).thenThrow(new MessageTooLargeException("aws", "x", null));

		assertThatThrownBy(() -> publisher.publish(envelope("pedido-1"), Duration.ofSeconds(5)))
				.isInstanceOf(PermanentPublishException.class);
	}

	@Test
	void illegalArgumentDoSenderEhPermanente() {
		when(sender.send(any())).thenThrow(new IllegalArgumentException("FIFO exige orderingKey"));

		assertThatThrownBy(() -> publisher.publish(envelope("pedido-1"), Duration.ofSeconds(5)))
				.isInstanceOf(PermanentPublishException.class);
	}

	@Test
	void demaisFalhasSaoTransitoriasMesmoNaoRetryable() {
		for (RuntimeException falha : new RuntimeException[] {
				new ThrottledException("aws", "x", null),
				new AccessDeniedException("aws", "x", null),
				new DestinationNotFoundException("aws", "x", null),
				new MessagingException("aws", "timeout", null, false),
				new IllegalStateException("sender fechado")}) {
			doThrow(falha).when(sender).send(any());

			assertThatThrownBy(() -> publisher.publish(envelope("pedido-1"), Duration.ofSeconds(5)))
					.isSameAs(falha);
		}
	}

	private static OutboxEnvelope envelope(String aggregateId) {
		return new OutboxEnvelope(UUID.randomUUID(), "Pedido", aggregateId, null, "PedidoCriado",
				Instant.now(), Map.of(), "{}", "pedidos.fifo", 0, "{\"eventType\":\"PedidoCriado\"}");
	}
}
