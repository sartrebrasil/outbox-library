-- Leitura das mensagens de um fluxo (relay) e UPDATEs de retry/quarentena por (aggregate_id, destination).
-- Sem ele, cada ciclo varre todos os PENDING e descarta quase tudo: custo proporcional ao backlog.
CREATE INDEX idx_outbox_fluxo
    ON outbox_message (aggregate_id, destination, seq)
    WHERE status = 'PENDING';
