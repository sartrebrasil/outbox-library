package br.com.oobj.outbox.broker.messaging;

import br.com.oobj.outbox.core.OutboxEnvelope;
import com.example.messaging.rabbitmq.RabbitMessageSender;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Delivery;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/** Publica num exchange RabbitMQ real (Testcontainers) e confirma routing key, message-id e corpo entregues. */
@Testcontainers
class RabbitMessageSenderPublisherIT {

	@Container
	private static final RabbitMQContainer RABBITMQ = new RabbitMQContainer("rabbitmq:3.13-management-alpine");

	private static Connection connection;
	private static Channel channel;

	@BeforeAll
	static void criarConexao() throws Exception {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(RABBITMQ.getHost());
		factory.setPort(RABBITMQ.getAmqpPort());
		factory.setUsername(RABBITMQ.getAdminUsername());
		factory.setPassword(RABBITMQ.getAdminPassword());
		connection = factory.newConnection();
		channel = connection.createChannel();
	}

	@AfterAll
	static void fecharConexao() throws Exception {
		channel.close();
		connection.close();
	}

	@Test
	void mensagemPublicadaChegaComRoutingKeyECorpoCorretos() throws Exception {
		String exchange = "pedidos.exchange";
		String queue = "pedidos.consumidor";
		channel.exchangeDeclare(exchange, BuiltinExchangeType.DIRECT, true);
		channel.queueDeclare(queue, true, false, false, null);
		channel.queueBind(queue, exchange, "pedido-1");

		BlockingQueue<Delivery> recebidas = new ArrayBlockingQueue<>(1);
		channel.basicConsume(queue, true, (consumerTag, delivery) -> recebidas.add(delivery), consumerTag -> { });

		MessageSenderPublisher publisher = new MessageSenderPublisher(RabbitMessageSender.forExchangeByOrderingKey(
				connection, exchange, RabbitMessageSender.DEFAULT_MAX_MESSAGE_BYTES).withConfirmTimeout(Duration.ofSeconds(5)));
		OutboxEnvelope envelope = new OutboxEnvelope(UUID.randomUUID(), "Pedido", "pedido-1", 1L, "PedidoCriado",
				Instant.now(), Map.of(), "{\"total\":10}", exchange, 0,
				"{\"eventType\":\"PedidoCriado\",\"payload\":{\"total\":10}}");

		assertThatCode(() -> publisher.publish(envelope, Duration.ofSeconds(5))).doesNotThrowAnyException();

		Delivery entregue = recebidas.poll(10, java.util.concurrent.TimeUnit.SECONDS);
		assertThat(entregue).isNotNull();
		assertThat(entregue.getEnvelope().getRoutingKey()).isEqualTo("pedido-1");
		assertThat(entregue.getProperties().getMessageId()).isEqualTo(envelope.messageId().toString());
		assertThat(new String(entregue.getBody())).contains("PedidoCriado").contains("\"total\":10");
	}
}
