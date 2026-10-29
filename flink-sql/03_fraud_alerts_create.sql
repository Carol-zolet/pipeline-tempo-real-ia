-- Camada 4, passo 3: tópico de alertas de fraude.
CREATE TABLE fraud_alerts (
  card_id      BIGINT,
  first_ts     TIMESTAMP(3),
  last_ts      TIMESTAMP(3),
  tx_count     BIGINT,
  total_amount DECIMAL(15, 2),
  first_tx_id  BIGINT,
  last_tx_id   BIGINT
) DISTRIBUTED BY HASH(card_id) INTO 1 BUCKETS
WITH ('changelog.mode' = 'append');
