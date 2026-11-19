package com.example.outbox.core;

/** Grava a mensagem na outbox dentro da transação ativa do chamador; quem implementa decide como obtê-la. */
public interface OutboxWriter {

	void enqueue(OutboxMessage message);
}
