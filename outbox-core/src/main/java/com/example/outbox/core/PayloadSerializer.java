package com.example.outbox.core;

/** Converte o objeto de domínio no JSON do payload; o core não conhece a biblioteca (02 §3.7). */
@FunctionalInterface
public interface PayloadSerializer {

	/** @throws OutboxSerializationException se o objeto não serializa */
	String serialize(Object payload);
}
