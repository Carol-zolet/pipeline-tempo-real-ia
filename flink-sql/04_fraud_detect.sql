-- Camada 4, passo 4 (statement contínuo): 3 transações no MESMO cartão em até 60 s.
-- MATCH_RECOGNIZE particionado por cartão: cada cartão tem seu próprio estado e sua própria janela.
-- SKIP PAST LAST ROW: uma rajada gera um alerta, não vários sobrepostos.
SET 'client.statement-name' = 'fraud-detect';
INSERT INTO fraud_alerts
SELECT card_id, first_ts, last_ts, tx_count, total_amount, first_tx_id, last_tx_id
FROM tx_stream
MATCH_RECOGNIZE (
  PARTITION BY card_id
  ORDER BY ts
  MEASURES
    FIRST(T.ts)             AS first_ts,
    LAST(T.ts)              AS last_ts,
    COUNT(T.transaction_id) AS tx_count,
    SUM(T.amount)           AS total_amount,
    FIRST(T.transaction_id) AS first_tx_id,
    LAST(T.transaction_id)  AS last_tx_id
  ONE ROW PER MATCH
  AFTER MATCH SKIP PAST LAST ROW
  PATTERN (T{3}) WITHIN INTERVAL '60' SECOND
  DEFINE T AS TRUE
);
