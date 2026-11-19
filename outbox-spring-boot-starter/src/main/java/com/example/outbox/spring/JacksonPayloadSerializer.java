package com.example.outbox.spring;

import com.example.outbox.core.OutboxSerializationException;
import com.example.outbox.core.PayloadSerializer;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

/** {@link PayloadSerializer} sobre o {@link ObjectMapper} do contexto. */
public final class JacksonPayloadSerializer implements PayloadSerializer {

	private final ObjectMapper objectMapper;

	public JacksonPayloadSerializer(ObjectMapper objectMapper) {
		this.objectMapper = objectMapper;
	}

	@Override
	public String serialize(Object payload) {
		try {
			return objectMapper.writeValueAsString(payload);
		} catch (JacksonException e) {
			throw new OutboxSerializationException(
					"payload do tipo %s não serializou".formatted(payload == null ? "null" : payload.getClass().getName()), e);
		}
	}
}
