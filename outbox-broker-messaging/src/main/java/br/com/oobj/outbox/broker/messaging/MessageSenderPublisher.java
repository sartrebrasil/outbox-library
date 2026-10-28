package br.com.oobj.outbox.broker.messaging;

import br.com.oobj.outbox.core.MessagePublisher;
import br.com.oobj.outbox.core.OutboxEnvelope;
import br.com.oobj.outbox.core.PermanentPublishException;
import com.example.messaging.MessageSender;
import com.example.messaging.MessageTooLargeException;
import com.example.messaging.OutgoingMessage;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

/**
 * {@link MessagePublisher} sobre um {@link MessageSender} da messaging-library (SQS, SNS,
 * Pub/Sub, Service Bus, JMS...). Uma instância publica no único destino do sender;
 * destinos diferentes usam instâncias diferentes, cada uma registrada sob sua própria
 * chave no mapa de publishers do {@code OutboxRelay} (02 §4.2.3).
 *
 * <p>{@code orderingKey = aggregateId} e {@code deduplicationId = messageId}: em SQS/SNS
 * FIFO viram {@code MessageGroupId}/{@code MessageDeduplicationId}. O sender é da
 * aplicação: este publisher não o fecha.
 *
 * <p>Só tamanho e validação da mensagem são permanentes. Todo o resto é transitório,
 * inclusive {@code retryable = false}: o SDK da AWS marca timeout assim, e permissão ou
 * destino ausente se corrigem na configuração sem perder a mensagem (02 §5.4).
 */
public final class MessageSenderPublisher implements MessagePublisher {

	private final MessageSender sender;

	public MessageSenderPublisher(MessageSender sender) {
		this.sender = sender;
	}

	// shortcut: timeout ignorado, vale o do cliente nativo do sender; usar quando MessageSender aceitar prazo
	@Override
	public void publish(OutboxEnvelope envelope, Duration timeout) throws PermanentPublishException {
		try {
			sender.send(new OutgoingMessage(envelope.body().getBytes(StandardCharsets.UTF_8), null,
					"application/json", envelope.aggregateId(), envelope.messageId().toString(), null, null));
		} catch (MessageTooLargeException | IllegalArgumentException e) {
			throw new PermanentPublishException(e.getMessage(), e);
		}
	}
}
