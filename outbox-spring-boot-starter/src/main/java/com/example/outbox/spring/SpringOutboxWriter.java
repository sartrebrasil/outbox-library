package com.example.outbox.spring;

import com.example.outbox.core.OutboxMessage;
import com.example.outbox.core.OutboxWriter;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * Fachada conveniente sobre o {@link OutboxWriter} (02 §3.2): serializa o payload com o
 * {@link ObjectMapper} do contexto e avisa o relay depois do commit. O fail-fast fora de
 * transação (§3.4) fica no próprio {@link OutboxWriter}.
 */
public final class SpringOutboxWriter implements OutboxWriter {

	private static final Logger LOG = LoggerFactory.getLogger(SpringOutboxWriter.class);

	private final OutboxWriter delegate;
	private final ObjectMapper objectMapper;
	private final Runnable afterCommit;

	public SpringOutboxWriter(OutboxWriter delegate, ObjectMapper objectMapper) {
		this(delegate, objectMapper, () -> { });
	}

	/** {@code afterCommit} roda uma vez por transação que gravou no outbox, depois do commit: o starter passa o {@code OutboxRelay#wake}. */
	public SpringOutboxWriter(OutboxWriter delegate, ObjectMapper objectMapper, Runnable afterCommit) {
		this.delegate = delegate;
		this.objectMapper = objectMapper;
		this.afterCommit = afterCommit;
	}

	@Override
	public void enqueue(OutboxMessage message) {
		delegate.enqueue(message);
		afterCommitOnce();
	}

	/**
	 * Um aviso por transação, não por mensagem: várias gravações na mesma transação são um fluxo só, e cada aviso a
	 * mais seria um ciclo vazio do relay. Sem sincronização ativa não há aviso; o relay acha a mensagem pelo intervalo.
	 */
	private void afterCommitOnce() {
		if (!TransactionSynchronizationManager.isSynchronizationActive() || TransactionSynchronizationManager.hasResource(this)) {
			return;
		}
		TransactionSynchronizationManager.bindResource(this, Boolean.TRUE);
		TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
			@Override
			public void afterCommit() {
				SpringOutboxWriter.this.afterCommit.run();
			}

			@Override
			public void afterCompletion(int status) {
				// o recurso é nosso: o Spring só desfaz os que ele mesmo ligou, e sobraria para a próxima transação da thread
				TransactionSynchronizationManager.unbindResourceIfPossible(SpringOutboxWriter.this);
			}
		});
	}

	/**
	 * Serializa {@code payload} com o {@link ObjectMapper} do contexto (02 §3.7). A
	 * serialização continua dentro da transação de negócio porque é esta chamada que a
	 * dispara. Erro de serialização registra o tipo e o tamanho do objeto, nunca o valor
	 * (02 §8.3) — o mesmo cuidado do core se aplica aqui, no único lugar do starter que
	 * toca um objeto de domínio antes de virar `String`.
	 */
	public void enqueue(String aggregateType, String aggregateId, Long aggregateVersion,
			String eventType, Object payload, String destination) {
		String json;
		try {
			json = objectMapper.writeValueAsString(payload);
		} catch (JacksonException e) {
			LOG.atError().setMessage("payload não serializou")
					.addKeyValue("aggregateType", aggregateType)
					.addKeyValue("payloadType", payload == null ? "null" : payload.getClass().getName())
					.log();
			throw new OutboxSerializationException(
					"payload do tipo %s não serializou".formatted(payload == null ? "null" : payload.getClass().getName()), e);
		}
		enqueue(OutboxMessage.of(aggregateType, aggregateId, aggregateVersion, eventType, json, destination));
	}
}
