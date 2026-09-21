CREATE TABLE outbox_message (
    id                UUID PRIMARY KEY,
    seq               BIGINT GENERATED ALWAYS AS IDENTITY,
    aggregate_type    VARCHAR(255) NOT NULL,
    aggregate_id      VARCHAR(255) NOT NULL,
    aggregate_version BIGINT,
    event_type        VARCHAR(255) NOT NULL,
    payload           TEXT         NOT NULL,
    headers           TEXT,
    trace_id          VARCHAR(64),
    destination       VARCHAR(255) NOT NULL,
    status            VARCHAR(20)  NOT NULL,
    attempts          INT          NOT NULL DEFAULT 0,
    last_error        TEXT,
    created_at        TIMESTAMPTZ  NOT NULL DEFAULT now(),
    available_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    processed_at      TIMESTAMPTZ
);

-- Scan do relay. available_at lidera para que o range scan pare antes das linhas
-- em backoff e nunca alcance as bloqueadas por quarentena, marcadas 'infinity' (02 §4.2).
-- (aggregate_id, destination) é o fluxo, unidade de reivindicação e de ordem (02 §4.2).
CREATE INDEX idx_outbox_pending
    ON outbox_message (available_at, aggregate_id, destination, seq)
    WHERE status = 'PENDING';

-- Verificação de bloqueio por quarentena. Minúsculo: DEAD_LETTER deve ser raro.
CREATE INDEX idx_outbox_dead_letter
    ON outbox_message (aggregate_id, destination)
    WHERE status = 'DEAD_LETTER';

CREATE INDEX idx_outbox_trace
    ON outbox_message (trace_id)
    WHERE trace_id IS NOT NULL;

-- Varredura da purga (02 §6). Sem ele, a retenção faz seq scan na tabela inteira
-- a cada ciclo — exatamente na tabela que a purga existe para manter pequena.
CREATE INDEX idx_outbox_purge
    ON outbox_message (processed_at)
    WHERE status = 'PUBLISHED';

-- Métrica de idade (02 §8.1). Deliberadamente SEM filtro de available_at: precisa
-- enxergar o que o scan do relay não enxerga — backoff, quarentena e órfãos.
CREATE INDEX idx_outbox_age
    ON outbox_message (created_at)
    WHERE status = 'PENDING';
