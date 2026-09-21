package br.com.oobj.outbox.broker.sqs;

import br.com.oobj.outbox.core.MessagePublisher;
import br.com.oobj.outbox.core.OutboxEnvelope;
import br.com.oobj.outbox.core.PermanentPublishException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.InvalidMessageContentsException;
import software.amazon.awssdk.services.sqs.model.KmsAccessDeniedException;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

/**
 * {@link MessagePublisher} para AWS SQS, padrão ou FIFO (02 §5). Uma instância publica
 * numa única fila; destinos diferentes usam instâncias diferentes, cada uma registrada
 * sob sua própria chave no mapa de publishers do {@code OutboxRelay} (02 §4.2.3).
 *
 * <p>FIFO é detectado pela própria URL da fila — toda fila FIFO da AWS termina em
 * {@code .fifo}, restrição do serviço, não convenção deste módulo. Filas padrão não
 * aceitam {@code MessageGroupId}/{@code MessageDeduplicationId}, por isso o envio dos
 * dois atributos é condicional, nunca incondicional.
 */
public final class SqsMessagePublisher implements MessagePublisher {

	/** Limite de corpo de mensagem do SQS (https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/quotas-messages.html). */
	private static final int MAX_BODY_BYTES = 256 * 1024;

	private final SqsClient client;
	private final String queueUrl;
	private final boolean fifo;

	public SqsMessagePublisher(SqsClient client, String queueUrl) {
		this.client = client;
		this.queueUrl = queueUrl;
		this.fifo = queueUrl.endsWith(".fifo");
	}

	@Override
	public void publish(OutboxEnvelope envelope, Duration timeout) throws PermanentPublishException {
		int bodyBytes = envelope.body().getBytes(StandardCharsets.UTF_8).length;
		if (bodyBytes > MAX_BODY_BYTES) {
			throw new PermanentPublishException(
					"corpo de %d bytes excede o limite de %d bytes do SQS".formatted(bodyBytes, MAX_BODY_BYTES));
		}

		SendMessageRequest.Builder request = SendMessageRequest.builder()
				.queueUrl(queueUrl)
				.messageBody(envelope.body())
				.overrideConfiguration(o -> o.apiCallTimeout(timeout).apiCallAttemptTimeout(timeout));
		if (fifo) {
			request.messageGroupId(envelope.aggregateId())
					.messageDeduplicationId(envelope.messageId().toString());
		}

		try {
			client.sendMessage(request.build());
		} catch (InvalidMessageContentsException | KmsAccessDeniedException e) {
			throw new PermanentPublishException(e.getMessage(), e);
		} catch (SdkException e) {
			throw e; // transitória por padrão (02 §5.4): RequestThrottledException, rede, etc.
		}
	}
}
