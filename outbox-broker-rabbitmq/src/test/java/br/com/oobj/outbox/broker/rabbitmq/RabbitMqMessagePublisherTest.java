package br.com.oobj.outbox.broker.rabbitmq;

import br.com.oobj.outbox.core.OutboxEnvelope;
import br.com.oobj.outbox.core.PermanentPublishException;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** T5: taxonomia de erro do RabbitMQ (nack permanente, IO/timeout transitório) e frame máximo. */
class RabbitMqMessagePublisherTest {

	private final Channel channel = mock(Channel.class);
	private final Connection connection = mock(Connection.class);
	private final RabbitMqMessagePublisher publisher = new RabbitMqMessagePublisher(channel, "pedidos.exchange");

	RabbitMqMessagePublisherTest() {
		when(channel.getConnection()).thenReturn(connection);
		when(connection.getFrameMax()).thenReturn(131072); // default do RabbitMQ
	}

	@Test
	void publicaComAggregateIdComoRoutingKeyEEsperaConfirmacao() throws Exception {
		when(channel.waitForConfirms(anyLong())).thenReturn(true);
		OutboxEnvelope envelope = envelope();

		assertThatCode(() -> publisher.publish(envelope, Duration.ofSeconds(5))).doesNotThrowAnyException();

		ArgumentCaptor<byte[]> bodyCaptor = ArgumentCaptor.forClass(byte[].class);
		verify(channel).basicPublish(eq("pedidos.exchange"), eq("pedido-1"), any(AMQP.BasicProperties.class), bodyCaptor.capture());
		assertThat(new String(bodyCaptor.getValue())).isEqualTo(envelope.body());
		verify(channel).waitForConfirms(5000L);
	}

	@Test
	void nackEhPermanente() throws Exception {
		when(channel.waitForConfirms(anyLong())).thenReturn(false);

		assertThatThrownBy(() -> publisher.publish(envelope(), Duration.ofSeconds(5)))
				.isInstanceOf(PermanentPublishException.class);
	}

	@Test
	void corpoAcimaDoFrameMaximoEhPermanenteSemChamarOCliente() throws Exception {
		when(connection.getFrameMax()).thenReturn(1024); // bem pequeno, força o corte
		String corpoGrande = "\"" + "x".repeat(2000) + "\"";
		OutboxEnvelope grande = new OutboxEnvelope(UUID.randomUUID(), "Pedido", "pedido-1", null, "PedidoCriado",
				Instant.now(), Map.of(), corpoGrande, "pedidos.exchange", 0, corpoGrande);

		assertThatThrownBy(() -> publisher.publish(grande, Duration.ofSeconds(5)))
				.isInstanceOf(PermanentPublishException.class);
		verify(channel, never()).basicPublish(anyString(), anyString(), any(), any());
	}

	@Test
	void frameMaxZeroSignificaSemLimiteENaoBloqueiaCorpoGrande() throws Exception {
		when(connection.getFrameMax()).thenReturn(0);
		when(channel.waitForConfirms(anyLong())).thenReturn(true);
		String corpoGrande = "\"" + "x".repeat(500_000) + "\"";
		OutboxEnvelope grande = new OutboxEnvelope(UUID.randomUUID(), "Pedido", "pedido-1", null, "PedidoCriado",
				Instant.now(), Map.of(), corpoGrande, "pedidos.exchange", 0, corpoGrande);

		assertThatCode(() -> publisher.publish(grande, Duration.ofSeconds(5))).doesNotThrowAnyException();
	}

	@Test
	void ioExceptionEhTransitoriaPorPadrao() throws Exception {
		org.mockito.Mockito.doThrow(new java.io.IOException("conexão caiu"))
				.when(channel).basicPublish(anyString(), anyString(), any(), any());

		assertThatThrownBy(() -> publisher.publish(envelope(), Duration.ofSeconds(5)))
				.isInstanceOf(RabbitMqPublishException.class)
				.isNotInstanceOf(PermanentPublishException.class);
	}

	@Test
	void timeoutExceptionEhTransitoriaPorPadrao() throws Exception {
		when(channel.waitForConfirms(anyLong())).thenThrow(new java.util.concurrent.TimeoutException("sem confirmação"));

		assertThatThrownBy(() -> publisher.publish(envelope(), Duration.ofSeconds(5)))
				.isInstanceOf(RabbitMqPublishException.class)
				.isNotInstanceOf(PermanentPublishException.class);
	}

	private static OutboxEnvelope envelope() {
		return new OutboxEnvelope(UUID.randomUUID(), "Pedido", "pedido-1", null, "PedidoCriado",
				Instant.now(), Map.of(), "{}", "pedidos.exchange", 0, "{}");
	}
}
