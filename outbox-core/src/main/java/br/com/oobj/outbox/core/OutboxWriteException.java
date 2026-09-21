package br.com.oobj.outbox.core;

/** Envolve falha JDBC do {@link JdbcOutboxWriter} sem obrigar o chamador a lidar com SQLException. */
public class OutboxWriteException extends RuntimeException {

	public OutboxWriteException(String message, Throwable cause) {
		super(message, cause);
	}
}
