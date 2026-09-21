package br.com.oobj.outbox.spring;

import br.com.oobj.outbox.core.OutboxMessage;
import br.com.oobj.outbox.core.OutboxWriter;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import javax.sql.DataSource;
import java.sql.Connection;

/**
 * Sobrecarga conveniente do core para uso em Spring (02 §3.2): resolve a conexão via
 * {@link DataSourceUtils} em vez de exigi-la do chamador. Complementa o fail-fast do core
 * (§3.4) com {@link TransactionSynchronizationManager#isActualTransactionActive()}, que é
 * confiável dentro do Spring — o {@code connection.getAutoCommit()} do core não é.
 */
public final class SpringOutboxWriter {

	private static final Logger LOG = LoggerFactory.getLogger(SpringOutboxWriter.class);

	private final OutboxWriter delegate;
	private final DataSource dataSource;
	private final ObjectMapper objectMapper;

	public SpringOutboxWriter(OutboxWriter delegate, DataSource dataSource, ObjectMapper objectMapper) {
		this.delegate = delegate;
		this.dataSource = dataSource;
		this.objectMapper = objectMapper;
	}

	public void enqueue(OutboxMessage message) {
		if (!TransactionSynchronizationManager.isActualTransactionActive()) {
			throw new IllegalStateException("enqueue() chamado fora de uma transação Spring ativa");
		}
		Connection connection = DataSourceUtils.getConnection(dataSource);
		try {
			delegate.enqueue(connection, message);
		} finally {
			DataSourceUtils.releaseConnection(connection, dataSource);
		}
	}

	/**
	 * Serializa {@code payload} com o {@link ObjectMapper} do contexto (02 §3.7). A
	 * serialização continua dentro da transação de negócio porque é esta chamada que a
	 * dispara. Erro de serialização registra o tipo e o tamanho do objeto, nunca o valor
	 * (02 §8.3) — o mesmo cuidado do core se aplica aqui, no único lugar do starter que
	 * toca um objeto de domínio antes de virar `String`.
	 */
	public void enqueue(String aggregateType, String aggregateId, Long aggregateVersion,
			String eventType, Object payload, String destination) {
		String json;
		try {
			json = objectMapper.writeValueAsString(payload);
		} catch (JsonProcessingException e) {
			LOG.atError().setMessage("payload não serializou")
					.addKeyValue("aggregateType", aggregateType)
					.addKeyValue("payloadType", payload == null ? "null" : payload.getClass().getName())
					.log();
			throw new OutboxSerializationException(
					"payload do tipo %s não serializou".formatted(payload == null ? "null" : payload.getClass().getName()), e);
		}
		enqueue(OutboxMessage.of(aggregateType, aggregateId, aggregateVersion, eventType, json, destination));
	}
}
