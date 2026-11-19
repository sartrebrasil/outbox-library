package com.example.outbox.core;

/** Grava a mensagem na outbox dentro da transação ativa do chamador; quem implementa decide como obtê-la. */
public interface OutboxWriter {

	void enqueue(OutboxMessage message);

	/**
	 * Serializa {@code payload} com o {@link PayloadSerializer} do writer e grava. A serialização roda na transação
	 * de negócio, porque é esta chamada que a dispara (RF21). Uma {@code String} já em JSON seria codificada de novo:
	 * para JSON pronto use {@link #enqueue(OutboxMessage)}.
	 */
	void enqueue(String aggregateType, String aggregateId, Long aggregateVersion,
			String eventType, Object payload, String destination);
}
