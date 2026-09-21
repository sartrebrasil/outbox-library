package br.com.oobj.outbox.core;

import java.sql.Connection;

public interface OutboxWriter {

	void enqueue(Connection connection, OutboxMessage message);
}
