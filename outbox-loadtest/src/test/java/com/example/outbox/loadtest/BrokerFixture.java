package com.example.outbox.loadtest;

import com.example.messaging.aws.SnsMessageSender;
import com.example.messaging.aws.SqsMessageSender;
import com.example.messaging.rabbitmq.RabbitMessageSender;
import com.example.outbox.broker.messaging.MessageSenderPublisher;
import com.example.outbox.core.MessagePublisher;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.containers.localstack.LocalStackContainer;
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
import software.amazon.awssdk.services.sqs.model.SetQueueAttributesRequest;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequestEntry;

import static org.testcontainers.containers.localstack.LocalStackContainer.Service.SNS;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.SQS;

/**
 * Um broker real (ou em memória) por execução. Para somar um broker novo (ex.: Kafka):
 * implemente esta interface e registre em {@link #create(String)}.
 */
interface BrokerFixture extends AutoCloseable {

	/** Cria (de forma idempotente) um destino por nome e devolve o publisher de cada um. */
	Map<String, MessagePublisher> publishers(List<String> destinations) throws Exception;

	/**
	 * Inicia {@code consumers} consumidores por destino; cada corpo entregue vai para {@code sink}.
	 * Chamar depois de {@link #publishers} e antes de iniciar o relay. Fechar o retorno para os consumidores.
	 */
	AutoCloseable consume(List<String> destinations, int consumers, Consumer<String> sink) throws Exception;

	@Override
	void close() throws Exception;

	static BrokerFixture create(String broker) throws Exception {
		return switch (broker) {
			case "inmemory" -> new InMemory();
			case "rabbit" -> new Rabbit();
			case "sqs" -> new Sqs();
			case "sns" -> new Sns();
			default -> throw new IllegalArgumentException("broker desconhecido: " + broker);
		};
	}

	/** Linha de base: sem broker, isola o custo de Postgres + relay. */
	final class InMemory implements BrokerFixture {

		private volatile Consumer<String> sink = body -> { };

		@Override
		public Map<String, MessagePublisher> publishers(List<String> destinations) {
			Map<String, MessagePublisher> m = new HashMap<>();
			destinations.forEach(d -> m.put(d, (envelope, timeout) -> sink.accept(envelope.body())));
			return m;
		}

		/** "Consumo" instantâneo: o próprio publisher entrega o corpo; o profile {@code consumers} não se aplica. */
		@Override
		public AutoCloseable consume(List<String> destinations, int consumers, Consumer<String> sink) {
			this.sink = sink;
			return () -> this.sink = body -> { };
		}

		@Override
		public void close() {
		}
	}

	final class Rabbit implements BrokerFixture {

		private final RabbitMQContainer container = new RabbitMQContainer("rabbitmq:3.13-management-alpine");
		private final Connection connection;
		private final Channel channel;

		Rabbit() throws Exception {
			container.start();
			ConnectionFactory factory = new ConnectionFactory();
			factory.setHost(container.getHost());
			factory.setPort(container.getAmqpPort());
			factory.setUsername(container.getAdminUsername());
			factory.setPassword(container.getAdminPassword());
			connection = factory.newConnection();
			channel = connection.createChannel();
		}

		@Override
		public Map<String, MessagePublisher> publishers(List<String> destinations) throws Exception {
			Map<String, MessagePublisher> m = new HashMap<>();
			for (String d : destinations) {
				// fanout: o sender usa aggregateId como routing key; um bind só basta e não limita a carga
				channel.exchangeDeclare(d, BuiltinExchangeType.FANOUT, true);
				channel.queueDeclare(d + ".q", true, false, false, null);
				channel.queueBind(d + ".q", d, "");
				m.put(d, new MessageSenderPublisher(RabbitMessageSender.forExchangeByOrderingKey(
						connection, d, RabbitMessageSender.DEFAULT_MAX_MESSAGE_BYTES)
						.withConfirmTimeout(Duration.ofSeconds(10))));
			}
			return m;
		}

		@Override
		public AutoCloseable consume(List<String> destinations, int consumers, Consumer<String> sink) throws Exception {
			List<Channel> channels = new ArrayList<>();
			for (String d : destinations) {
				for (int i = 0; i < consumers; i++) {
					Channel ch = connection.createChannel();
					channels.add(ch);
					ch.basicConsume(d + ".q", true,
							(tag, delivery) -> sink.accept(new String(delivery.getBody(), StandardCharsets.UTF_8)),
							tag -> { });
				}
			}
			return () -> {
				for (Channel ch : channels) {
					try {
						ch.close();
					} catch (Exception ignored) {
						// canal já fechado
					}
				}
			};
		}

		@Override
		public void close() throws Exception {
			connection.close();
			container.stop();
		}
	}

	/** Base comum SQS/SNS: um LocalStack. */
	abstract class Aws implements BrokerFixture {

		final LocalStackContainer localstack = new LocalStackContainer(
				DockerImageName.parse("localstack/localstack:3.8")).withServices(SNS, SQS);
		final SqsClient sqs;
		final SnsClient sns;
		/** destino -> fila de onde se consome (a própria fila no SQS; a fila inscrita no tópico no SNS). */
		final Map<String, String> consumeQueues = new ConcurrentHashMap<>();

		Aws() {
			localstack.start();
			var credentials = StaticCredentialsProvider.create(
					AwsBasicCredentials.create(localstack.getAccessKey(), localstack.getSecretKey()));
			sqs = SqsClient.builder().endpointOverride(localstack.getEndpointOverride(SQS))
					.region(Region.of(localstack.getRegion())).credentialsProvider(credentials).build();
			sns = SnsClient.builder().endpointOverride(localstack.getEndpointOverride(SNS))
					.region(Region.of(localstack.getRegion())).credentialsProvider(credentials).build();
		}

		String fifoQueue(String name) {
			return sqs.createQueue(CreateQueueRequest.builder().queueName(name)
					.attributes(Map.of(QueueAttributeName.FIFO_QUEUE, "true",
							QueueAttributeName.CONTENT_BASED_DEDUPLICATION, "true"))
					.build()).queueUrl();
		}

		@Override
		public AutoCloseable consume(List<String> destinations, int consumers, Consumer<String> sink) {
			java.util.concurrent.atomic.AtomicBoolean stop = new java.util.concurrent.atomic.AtomicBoolean();
			List<Thread> threads = new ArrayList<>();
			for (String d : destinations) {
				String url = consumeQueues.get(d);
				for (int i = 0; i < consumers; i++) {
					Thread t = new Thread(() -> {
						while (!stop.get()) {
							var messages = sqs.receiveMessage(r -> r.queueUrl(url).maxNumberOfMessages(10)
									.waitTimeSeconds(1)).messages();
							messages.forEach(m -> sink.accept(m.body()));
							if (!messages.isEmpty()) {
								List<DeleteMessageBatchRequestEntry> entries = new ArrayList<>();
								for (int k = 0; k < messages.size(); k++) {
									entries.add(DeleteMessageBatchRequestEntry.builder().id(String.valueOf(k))
											.receiptHandle(messages.get(k).receiptHandle()).build());
								}
								sqs.deleteMessageBatch(b -> b.queueUrl(url).entries(entries));
							}
						}
					}, "loadtest-consumer-" + d + "-" + i);
					t.setDaemon(true);
					t.start();
					threads.add(t);
				}
			}
			return () -> {
				stop.set(true);
				for (Thread t : threads) {
					t.join(5000);
				}
			};
		}

		@Override
		public void close() {
			sqs.close();
			sns.close();
			localstack.stop();
		}
	}

	final class Sqs extends Aws {

		@Override
		public Map<String, MessagePublisher> publishers(List<String> destinations) {
			Map<String, MessagePublisher> m = new HashMap<>();
			for (String d : destinations) {
				String url = fifoQueue(d);
				consumeQueues.put(d, url);
				m.put(d, new MessageSenderPublisher(new SqsMessageSender(sqs, url)));
			}
			return m;
		}
	}

	final class Sns extends Aws {

		@Override
		public Map<String, MessagePublisher> publishers(List<String> destinations) {
			Map<String, MessagePublisher> m = new HashMap<>();
			for (String d : destinations) {
				String topicArn = sns.createTopic(CreateTopicRequest.builder().name(d)
						.attributes(Map.of("FifoTopic", "true", "ContentBasedDeduplication", "true"))
						.build()).topicArn();
				String queueUrl = fifoQueue("sub-" + d);
				consumeQueues.put(d, queueUrl);
				String queueArn = sqs.getQueueAttributes(GetQueueAttributesRequest.builder().queueUrl(queueUrl)
						.attributeNames(QueueAttributeName.QUEUE_ARN).build()).attributes().get(QueueAttributeName.QUEUE_ARN);
				sqs.setQueueAttributes(SetQueueAttributesRequest.builder().queueUrl(queueUrl)
						.attributes(Map.of(QueueAttributeName.POLICY, """
								{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"sqs:SendMessage","Resource":"%s"}]}
								""".formatted(queueArn))).build());
				sns.subscribe(SubscribeRequest.builder().topicArn(topicArn).protocol("sqs").endpoint(queueArn)
						.attributes(Map.of("RawMessageDelivery", "true")).build());
				m.put(d, new MessageSenderPublisher(new SnsMessageSender(sns, topicArn)));
			}
			return m;
		}
	}
}
