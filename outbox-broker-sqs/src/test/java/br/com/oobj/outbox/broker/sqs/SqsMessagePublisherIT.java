package br.com.oobj.outbox.broker.sqs;

import br.com.oobj.outbox.core.OutboxEnvelope;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.SQS;

/** T5: publica numa fila SQS FIFO real (LocalStack) e confirma MessageGroupId e corpo. */
@Testcontainers
class SqsMessagePublisherIT {

	@Container
	private static final LocalStackContainer LOCALSTACK = new LocalStackContainer(
			DockerImageName.parse("localstack/localstack:3.8")).withServices(SQS);

	private static SqsClient sqsClient;

	@BeforeAll
	static void criarCliente() {
		sqsClient = SqsClient.builder()
				.endpointOverride(LOCALSTACK.getEndpointOverride(SQS))
				.region(Region.of(LOCALSTACK.getRegion()))
				.credentialsProvider(StaticCredentialsProvider.create(
						AwsBasicCredentials.create(LOCALSTACK.getAccessKey(), LOCALSTACK.getSecretKey())))
				.build();
	}

	@AfterAll
	static void fecharCliente() {
		sqsClient.close();
	}

	@Test
	void mensagemPublicadaChegaComMessageGroupIdCorreto() {
		String queueUrl = sqsClient.createQueue(CreateQueueRequest.builder()
				.queueName("pedidos.fifo")
				.attributes(Map.of(QueueAttributeName.FIFO_QUEUE, "true",
						QueueAttributeName.CONTENT_BASED_DEDUPLICATION, "true"))
				.build()).queueUrl();

		SqsMessagePublisher publisher = new SqsMessagePublisher(sqsClient, queueUrl);
		OutboxEnvelope envelope = new OutboxEnvelope(UUID.randomUUID(), "Pedido", "pedido-1", 1L, "PedidoCriado",
				Instant.now(), Map.of(), "{\"total\":10}", "pedidos.fifo", 0,
				"{\"eventType\":\"PedidoCriado\",\"payload\":{\"total\":10}}");

		assertThatCode(() -> publisher.publish(envelope, Duration.ofSeconds(5))).doesNotThrowAnyException();

		var mensagens = sqsClient.receiveMessage(ReceiveMessageRequest.builder()
				.queueUrl(queueUrl)
				.waitTimeSeconds(10)
				.attributeNames(QueueAttributeName.ALL)
				.build()).messages();

		assertThat(mensagens).hasSize(1);
		assertThat(mensagens.get(0).body()).contains("PedidoCriado").contains("\"total\":10");
		assertThat(mensagens.get(0).attributes()).containsEntry(MessageSystemAttributeName.MESSAGE_GROUP_ID, "pedido-1");
	}
}
