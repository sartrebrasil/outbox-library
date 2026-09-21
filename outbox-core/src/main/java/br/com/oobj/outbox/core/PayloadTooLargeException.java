package br.com.oobj.outbox.core;

/**
 * Lançada pelo {@link OutboxWriter} quando o payload excede o limite do destino (02 §3.5).
 * Não é checked: o chamador deveria ter validado o tamanho antes de montar a mensagem, e
 * capturá-la aqui é aviso tardio, não fluxo normal.
 */
public class PayloadTooLargeException extends RuntimeException {

	public PayloadTooLargeException(String destination, int payloadBytes, long maxBytes) {
		super("payload de %d bytes excede o limite de %d bytes do destino '%s'"
				.formatted(payloadBytes, maxBytes, destination));
	}
}
