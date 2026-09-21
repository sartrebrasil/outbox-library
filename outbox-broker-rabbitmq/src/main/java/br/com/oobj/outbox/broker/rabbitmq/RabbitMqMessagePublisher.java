package br.com.oobj.outbox.broker.rabbitmq;

import br.com.oobj.outbox.core.MessagePublisher;
import br.com.oobj.outbox.core.OutboxEnvelope;
import br.com.oobj.outbox.core.PermanentPublishException;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.TimeoutException;

/**
 * {@link MessagePublisher} para RabbitMQ (02 §5). Uma instância publica num único
 * exchange; destinos diferentes usam instâncias diferentes, cada uma registrada sob sua
 * própria chave no mapa de publishers do {@code OutboxRelay} (02 §4.2.3).
 *
 * <p>{@code aggregateId} como routing key é recomendação de uso para preservar ordem no
 * lado do broker — funciona com um exchange de consistent hashing configurado pela
 * aplicação; não é garantia da lib (02 §3). O {@link Channel} é responsabilidade da
 * aplicação, que **precisa** chamar {@code channel.confirmSelect()} antes de usá-lo aqui —
 * sem publisher confirms, não há como saber se a mensagem chegou, e este módulo não
 * silenciosamente assume sucesso.
 */
public final class RabbitMqMessagePublisher implements MessagePublisher {

	/**
	 * Margem de segurança sobre o frame máximo negociado da conexão, para cobrir o
	 * overhead de propriedades AMQP que não faz parte do corpo. Aproximação deliberada,
	 * não contagem exata de bytes de protocolo — documentada no README.
	 */
	private static final int FRAME_OVERHEAD_MARGIN_BYTES = 8 * 1024;

	private final Channel channel;
	private final String exchange;

	public RabbitMqMessagePublisher(Channel channel, String exchange) {
		this.channel = channel;
		this.exchange = exchange;
	}

	@Override
	public void publish(OutboxEnvelope envelope, Duration timeout) throws PermanentPublishException {
		byte[] body = envelope.body().getBytes(StandardCharsets.UTF_8);
		int frameMax = channel.getConnection().getFrameMax();
		if (frameMax > 0 && body.length > frameMax - FRAME_OVERHEAD_MARGIN_BYTES) {
			throw new PermanentPublishException(
					"corpo de %d bytes excede o frame máximo negociado (%d bytes, com margem)"
							.formatted(body.length, frameMax));
		}

		AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
				.messageId(envelope.messageId().toString())
				.contentType("application/json")
				.build();

		try {
			channel.basicPublish(exchange, envelope.aggregateId(), properties, body);
			boolean confirmado = channel.waitForConfirms(timeout.toMillis());
			if (!confirmado) {
				throw new PermanentPublishException("mensagem rejeitada (nack) pelo broker");
			}
		} catch (IOException e) {
			throw new RabbitMqPublishException(e); // transitória por padrão (02 §5.4): canal/conexão caída
		} catch (TimeoutException e) {
			throw new RabbitMqPublishException(e);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new RabbitMqPublishException(e);
		}
	}
}
