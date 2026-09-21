package br.com.oobj.outbox.broker.sns;

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
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.CreateTopicRequest;
import software.amazon.awssdk.services.sns.model.SubscribeRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SetQueueAttributesRequest;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.SNS;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.SQS;

/**
 * T2.1: publica num tópico SNS FIFO real (LocalStack), com uma fila SQS FIFO inscrita,
 * e confirma que a mensagem chega com o MessageGroupId correto — não só que publish()
 * não lançou exceção.
 */
@Testcontainers
class SnsMessagePublisherIT {

	@Container
	private static final LocalStackContainer LOCALSTACK = new LocalStackContainer(
			DockerImageName.parse("localstack/localstack:3.8"))
			.withServices(SNS, SQS);

	private static SnsClient snsClient;
	private static SqsClient sqsClient;

	@BeforeAll
	static void criarClientes() {
		var credentials = StaticCredentialsProvider.create(
				AwsBasicCredentials.create(LOCALSTACK.getAccessKey(), LOCALSTACK.getSecretKey()));
		snsClient = SnsClient.builder()
				.endpointOverride(LOCALSTACK.getEndpointOverride(SNS))
				.region(Region.of(LOCALSTACK.getRegion()))
				.credentialsProvider(credentials)
				.build();
		sqsClient = SqsClient.builder()
				.endpointOverride(LOCALSTACK.getEndpointOverride(SQS))
				.region(Region.of(LOCALSTACK.getRegion()))
				.credentialsProvider(credentials)
				.build();
	}

	@AfterAll
	static void fecharClientes() {
		snsClient.close();
		sqsClient.close();
	}

	@Test
	void mensagemPublicadaChegaNaFilaComMessageGroupIdCorreto() {
		String topicArn = snsClient.createTopic(CreateTopicRequest.builder()
				.name("pedidos.fifo")
				.attributes(Map.of("FifoTopic", "true", "ContentBasedDeduplication", "true"))
				.build()).topicArn();

		String queueUrl = sqsClient.createQueue(CreateQueueRequest.builder()
				.queueName("pedidos-consumidor.fifo")
				.attributes(Map.of(QueueAttributeName.FIFO_QUEUE, "true"))
				.build()).queueUrl();
		String queueArn = sqsClient.getQueueAttributes(GetQueueAttributesRequest.builder()
				.queueUrl(queueUrl)
				.attributeNames(QueueAttributeName.QUEUE_ARN)
				.build()).attributes().get(QueueAttributeName.QUEUE_ARN);
		sqsClient.setQueueAttributes(SetQueueAttributesRequest.builder()
				.queueUrl(queueUrl)
				.attributes(Map.of(QueueAttributeName.POLICY, """
						{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"sqs:SendMessage","Resource":"%s"}]}
						""".formatted(queueArn)))
				.build());
		snsClient.subscribe(SubscribeRequest.builder()
				.topicArn(topicArn)
				.protocol("sqs")
				.endpoint(queueArn)
				.attributes(Map.of("RawMessageDelivery", "true"))
				.build());

		SnsMessagePublisher publisher = new SnsMessagePublisher(snsClient, topicArn);
		OutboxEnvelope envelope = new OutboxEnvelope(UUID.randomUUID(), "Pedido", "pedido-1", 1L, "PedidoCriado",
				Instant.now(), Map.of(), "{\"total\":10}", "pedidos.fifo", 0,
				"{\"eventType\":\"PedidoCriado\",\"payload\":{\"total\":10}}");

		assertThatCodeDoesNotThrow(() -> publisher.publish(envelope, Duration.ofSeconds(5)));

		var mensagens = sqsClient.receiveMessage(ReceiveMessageRequest.builder()
				.queueUrl(queueUrl)
				.waitTimeSeconds(10)
				.attributeNames(software.amazon.awssdk.services.sqs.model.QueueAttributeName.ALL)
				.build()).messages();

		assertThat(mensagens).hasSize(1);
		assertThat(mensagens.get(0).body()).contains("PedidoCriado").contains("\"total\":10");
		assertThat(mensagens.get(0).attributes())
				.containsEntry(software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName.MESSAGE_GROUP_ID, "pedido-1");
	}

	private static void assertThatCodeDoesNotThrow(org.assertj.core.api.ThrowableAssert.ThrowingCallable callable) {
		org.assertj.core.api.Assertions.assertThatCode(callable).doesNotThrowAnyException();
	}
}
