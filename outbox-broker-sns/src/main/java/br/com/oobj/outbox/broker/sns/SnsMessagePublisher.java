package br.com.oobj.outbox.broker.sns;

import br.com.oobj.outbox.core.MessagePublisher;
import br.com.oobj.outbox.core.OutboxEnvelope;
import br.com.oobj.outbox.core.PermanentPublishException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.InvalidParameterException;
import software.amazon.awssdk.services.sns.model.KmsAccessDeniedException;
import software.amazon.awssdk.services.sns.model.PublishRequest;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

/**
 * {@link MessagePublisher} para AWS SNS FIFO (02 §5, RF17). Uma instância publica num
 * único tópico; destinos diferentes usam instâncias diferentes, cada uma registrada sob
 * sua própria chave no mapa de publishers do {@code OutboxRelay} (02 §4.2.3).
 *
 * <p>{@code MessageGroupId = aggregateId} e {@code MessageDeduplicationId = messageId}
 * são recomendação de uso para preservar ordem no lado do broker — não é garantia da
 * lib (02 §3, RF17). O cliente {@link SnsClient} é responsabilidade da aplicação: região,
 * credenciais e endpoint não são conhecimento deste módulo.
 */
public final class SnsMessagePublisher implements MessagePublisher {

	/** Limite de corpo de mensagem do SNS (https://docs.aws.amazon.com/sns/latest/dg/sns-message-size.html). */
	private static final int MAX_BODY_BYTES = 256 * 1024;

	private final SnsClient client;
	private final String topicArn;

	public SnsMessagePublisher(SnsClient client, String topicArn) {
		this.client = client;
		this.topicArn = topicArn;
	}

	@Override
	public void publish(OutboxEnvelope envelope, Duration timeout) throws PermanentPublishException {
		int bodyBytes = envelope.body().getBytes(StandardCharsets.UTF_8).length;
		if (bodyBytes > MAX_BODY_BYTES) {
			throw new PermanentPublishException(
					"corpo de %d bytes excede o limite de %d bytes do SNS".formatted(bodyBytes, MAX_BODY_BYTES));
		}

		PublishRequest request = PublishRequest.builder()
				.topicArn(topicArn)
				.message(envelope.body())
				.messageGroupId(envelope.aggregateId())
				.messageDeduplicationId(envelope.messageId().toString())
				.overrideConfiguration(o -> o.apiCallTimeout(timeout).apiCallAttemptTimeout(timeout))
				.build();

		try {
			client.publish(request);
		} catch (InvalidParameterException | KmsAccessDeniedException e) {
			throw new PermanentPublishException(e.getMessage(), e);
		} catch (SdkException e) {
			throw e; // transitória por padrão (02 §5.4): ThrottlingException, rede, etc.
		}
	}
}
