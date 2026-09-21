package br.com.oobj.outbox.broker.kafka;

import br.com.oobj.outbox.core.OutboxEnvelope;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** T2.3: produz num tópico Kafka real (Testcontainers) e confirma a key e o corpo entregues. */
@Testcontainers
class KafkaMessagePublisherIT {

	@Container
	private static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("apache/kafka:3.8.0"));

	private static Producer<String, String> producer;
	private static KafkaConsumer<String, String> consumer;

	@BeforeAll
	static void criarClientes() {
		Properties producerProps = new Properties();
		producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
		producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		producer = new KafkaProducer<>(producerProps);

		Properties consumerProps = new Properties();
		consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
		consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "teste-" + UUID.randomUUID());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		consumer = new KafkaConsumer<>(consumerProps);
		consumer.subscribe(List.of("pedidos"));
	}

	@AfterAll
	static void fecharClientes() {
		producer.close();
		consumer.close();
	}

	@Test
	void mensagemProduzidaChegaComKeyECorpoCorretos() {
		KafkaMessagePublisher publisher = new KafkaMessagePublisher(producer, "pedidos");
		OutboxEnvelope envelope = new OutboxEnvelope(UUID.randomUUID(), "Pedido", "pedido-1", 1L, "PedidoCriado",
				Instant.now(), Map.of(), "{\"total\":10}", "pedidos", 0,
				"{\"eventType\":\"PedidoCriado\",\"payload\":{\"total\":10}}");

		org.assertj.core.api.Assertions.assertThatCode(() -> publisher.publish(envelope, Duration.ofSeconds(5)))
				.doesNotThrowAnyException();

		ConsumerRecords<String, String> registros = consumer.poll(Duration.ofSeconds(10));
		assertThat(registros.count()).isEqualTo(1);
		var registro = registros.iterator().next();
		assertThat(registro.key()).isEqualTo("pedido-1");
		assertThat(registro.value()).contains("PedidoCriado").contains("\"total\":10");
	}
}
