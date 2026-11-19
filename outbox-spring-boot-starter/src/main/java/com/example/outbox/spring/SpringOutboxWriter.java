package com.example.outbox.spring;

import com.example.outbox.core.OutboxMessage;
import com.example.outbox.core.OutboxWriter;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * Decorador do {@link OutboxWriter} (02 §3.2) que avisa o relay depois do commit. Serialização e
 * fail-fast fora de transação (§3.4) ficam no writer decorado.
 */
public final class SpringOutboxWriter implements OutboxWriter {

	private final OutboxWriter delegate;
	private final Runnable afterCommit;

	public SpringOutboxWriter(OutboxWriter delegate) {
		this(delegate, () -> { });
	}

	/** {@code afterCommit} roda uma vez por transação que gravou no outbox, depois do commit: o starter passa o {@code OutboxRelay#wake}. */
	public SpringOutboxWriter(OutboxWriter delegate, Runnable afterCommit) {
		this.delegate = delegate;
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

	@Override
	public void enqueue(String aggregateType, String aggregateId, Long aggregateVersion,
			String eventType, Object payload, String destination) {
		delegate.enqueue(aggregateType, aggregateId, aggregateVersion, eventType, payload, destination);
		afterCommitOnce();
	}
}
