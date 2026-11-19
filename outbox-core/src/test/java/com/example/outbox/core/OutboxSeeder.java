package com.example.outbox.core;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.time.ZoneOffset;

/** Só semeia linhas PENDING para os testes do relay; o escritor real vive no starter. */
final class OutboxSeeder {

	void enqueue(Connection connection, OutboxMessage m) throws SQLException {
		try (PreparedStatement s = connection.prepareStatement("""
				INSERT INTO outbox_message
					(id, aggregate_type, aggregate_id, aggregate_version, event_type,
					 payload, headers, destination, status, created_at, available_at)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', ?, ?)
				""")) {
			s.setObject(1, m.messageId());
			s.setString(2, m.aggregateType());
			s.setString(3, m.aggregateId());
			if (m.aggregateVersion() != null) {
				s.setLong(4, m.aggregateVersion());
			} else {
				s.setNull(4, Types.BIGINT);
			}
			s.setString(5, m.eventType());
			s.setString(6, m.payload());
			s.setString(7, EnvelopeJsonWriter.headersToJson(m.headers()));
			s.setString(8, m.destination());
			s.setObject(9, m.occurredAt().atOffset(ZoneOffset.UTC));
			s.setObject(10, m.occurredAt().atOffset(ZoneOffset.UTC));
			s.executeUpdate();
		}
	}
}
