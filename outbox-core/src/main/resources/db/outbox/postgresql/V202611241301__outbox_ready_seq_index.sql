-- Candidatos do relay em ordem de seq (FluxoClaimer). available_at < 'infinity' deixa de fora
-- as mensagens represadas por quarentena; o filtro available_at <= now() (backoff) é aplicado no scan.
CREATE INDEX idx_outbox_ready_seq
    ON outbox_message (seq)
    WHERE status = 'PENDING' AND available_at < 'infinity';
