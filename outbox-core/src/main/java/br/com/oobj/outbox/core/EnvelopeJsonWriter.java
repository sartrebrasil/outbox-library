package br.com.oobj.outbox.core;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Escritor JSON mínimo do core (02 §7.1): monta o corpo do envelope sem trazer Jackson.
 * Objeto plano, sem aninhamento, com duas exceções controladas: {@code headers} vira um
 * objeto de um nível, e {@code payload} é embutido bruto, sem reserialização — já é JSON
 * válido. O escape de string segue RFC 8259 e cobre aspas, barra invertida, quebra de
 * linha e caracteres de controle.
 */
final class EnvelopeJsonWriter {

	static String writeBody(OutboxEnvelope envelope) {
		StringBuilder json = new StringBuilder(256);
		json.append('{');
		appendField(json, "messageId", envelope.messageId().toString(), true);
		appendField(json, "aggregateType", envelope.aggregateType(), true);
		appendField(json, "aggregateId", envelope.aggregateId(), true);
		if (envelope.aggregateVersion() != null) {
			json.append("\"aggregateVersion\":").append(envelope.aggregateVersion()).append(',');
		}
		appendField(json, "eventType", envelope.eventType(), true);
		appendField(json, "occurredAt", envelope.occurredAt().toString(), true);
		json.append("\"headers\":").append(headersToJson(envelope.headers())).append(',');
		json.append("\"payload\":").append(envelope.payload());
		json.append('}');
		return json.toString();
	}

	/** JSON do objeto de headers, ou {@code "{}"} quando vazio ou nulo. */
	static String headersToJson(Map<String, String> headers) {
		if (headers == null || headers.isEmpty()) {
			return "{}";
		}
		StringBuilder json = new StringBuilder();
		json.append('{');
		boolean first = true;
		for (Map.Entry<String, String> entry : headers.entrySet()) {
			if (!first) {
				json.append(',');
			}
			first = false;
			json.append(quote(entry.getKey())).append(':').append(quote(entry.getValue()));
		}
		json.append('}');
		return json.toString();
	}

	/**
	 * Parser simétrico de {@link #headersToJson}: só precisa entender o formato restrito
	 * que este escritor produz — objeto plano de um nível, chaves e valores string.
	 */
	static Map<String, String> parseHeaders(String json) {
		if (json == null || json.isBlank() || json.equals("{}")) {
			return Map.of();
		}
		Map<String, String> headers = new LinkedHashMap<>();
		String body = json.trim();
		int i = 1; // pula '{'
		while (i < body.length() && body.charAt(i) != '}') {
			while (body.charAt(i) == ',' || Character.isWhitespace(body.charAt(i))) {
				i++;
			}
			ParsedString key = parseQuotedString(body, i);
			i = key.endIndex();
			while (body.charAt(i) != ':') {
				i++;
			}
			i++; // pula ':'
			while (Character.isWhitespace(body.charAt(i))) {
				i++;
			}
			ParsedString value = parseQuotedString(body, i);
			i = value.endIndex();
			headers.put(key.value(), value.value());
		}
		return headers;
	}

	private record ParsedString(String value, int endIndex) {
	}

	private static ParsedString parseQuotedString(String body, int start) {
		StringBuilder value = new StringBuilder();
		int i = start + 1; // pula a aspa de abertura
		while (body.charAt(i) != '"') {
			char c = body.charAt(i);
			if (c == '\\') {
				i++;
				char escaped = body.charAt(i);
				switch (escaped) {
					case '"' -> value.append('"');
					case '\\' -> value.append('\\');
					case 'n' -> value.append('\n');
					case 'r' -> value.append('\r');
					case 't' -> value.append('\t');
					case 'u' -> {
						value.append((char) Integer.parseInt(body.substring(i + 1, i + 5), 16));
						i += 4;
					}
					default -> value.append(escaped);
				}
			} else {
				value.append(c);
			}
			i++;
		}
		return new ParsedString(value.toString(), i + 1);
	}

	private static void appendField(StringBuilder json, String name, String value, boolean trailingComma) {
		json.append(quote(name)).append(':').append(quote(value));
		if (trailingComma) {
			json.append(',');
		}
	}

	private static String quote(String value) {
		StringBuilder escaped = new StringBuilder(value.length() + 2);
		escaped.append('"');
		for (int i = 0; i < value.length(); i++) {
			char c = value.charAt(i);
			switch (c) {
				case '"' -> escaped.append("\\\"");
				case '\\' -> escaped.append("\\\\");
				case '\n' -> escaped.append("\\n");
				case '\r' -> escaped.append("\\r");
				case '\t' -> escaped.append("\\t");
				default -> {
					if (c < 0x20) {
						escaped.append("\\u%04x".formatted((int) c));
					} else {
						escaped.append(c);
					}
				}
			}
		}
		escaped.append('"');
		return escaped.toString();
	}

	private EnvelopeJsonWriter() {
	}
}
