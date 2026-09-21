package br.com.oobj.outbox.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Reivindicação em dois passos (02 §4.2.2): candidatos sem lock nenhum, depois advisory
 * lock explícito por fluxo, um candidato por vez — nunca dentro do predicado da consulta,
 * porque a ordem de avaliação de uma função volátil não é contrato do planner (02 §4.2.2).
 * {@code CLASS_ID} é o classid reservado da biblioteca ("OBXL" em ASCII), para não colidir
 * com advisory locks da própria aplicação.
 */
final class FluxoClaimer {

	private static final Logger LOG = LoggerFactory.getLogger(FluxoClaimer.class);

	private static final int CLASS_ID = 1329748044;

	private static final String CANDIDATES_SQL = """
			SELECT o.aggregate_id, o.destination
			FROM outbox_message o
			WHERE o.status = 'PENDING'
			  AND o.available_at <= now()
			  AND NOT EXISTS (
			        SELECT 1 FROM outbox_message d
			        WHERE d.aggregate_id = o.aggregate_id
			          AND d.destination = o.destination
			          AND d.status = 'DEAD_LETTER')
			GROUP BY o.aggregate_id, o.destination
			ORDER BY min(o.seq)
			LIMIT ?
			""";

	private static final String TRY_LOCK_SQL = "SELECT pg_try_advisory_xact_lock(?, hashtext(? || '|' || ?))";

	static Optional<Fluxo> claimNext(Connection connection, int candidateLimit, OutboxMetrics metrics) throws SQLException {
		for (Fluxo candidate : candidates(connection, candidateLimit)) {
			if (tryLock(connection, candidate)) {
				metrics.claim("acquired");
				LOG.atDebug().setMessage("fluxo reivindicado").addKeyValue("destination", candidate.destination()).log();
				return Optional.of(candidate);
			}
			metrics.claim("contended");
			LOG.atTrace().setMessage("contenção de lock, fluxo descartado")
					.addKeyValue("destination", candidate.destination()).log();
		}
		metrics.claim("empty");
		return Optional.empty();
	}

	private static List<Fluxo> candidates(Connection connection, int candidateLimit) throws SQLException {
		List<Fluxo> candidates = new ArrayList<>();
		try (PreparedStatement statement = connection.prepareStatement(CANDIDATES_SQL)) {
			statement.setInt(1, candidateLimit);
			try (ResultSet resultSet = statement.executeQuery()) {
				while (resultSet.next()) {
					candidates.add(new Fluxo(resultSet.getString("aggregate_id"), resultSet.getString("destination")));
				}
			}
		}
		return candidates;
	}

	private static boolean tryLock(Connection connection, Fluxo fluxo) throws SQLException {
		try (PreparedStatement statement = connection.prepareStatement(TRY_LOCK_SQL)) {
			statement.setInt(1, CLASS_ID);
			statement.setString(2, fluxo.aggregateId());
			statement.setString(3, fluxo.destination());
			try (ResultSet resultSet = statement.executeQuery()) {
				resultSet.next();
				return resultSet.getBoolean(1);
			}
		}
	}

	private FluxoClaimer() {
	}
}
