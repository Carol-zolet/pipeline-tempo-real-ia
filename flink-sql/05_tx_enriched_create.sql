-- [NÃO TESTADO] Camada 4, enriquecimento: tabela de saída (upsert, chave = transaction_id).
-- Upsert porque o join com `accounts` (changelog do Debezium) pode revisar linhas quando a conta muda.
-- A coluna da chave vem primeiro (exigência do DISTRIBUTED BY, vista no erro do tx_stream).
CREATE TABLE tx_enriched (
  transaction_id BIGINT,
  card_id        BIGINT,
  account_id     BIGINT,
  customer_id    BIGINT,
  account_type   STRING,
  account_status STRING,
  balance        DECIMAL(15, 2),
  merchant_id    BIGINT,
  amount         DECIMAL(15, 2),
  currency       STRING,
  status         STRING,
  ts             TIMESTAMP(3),
  PRIMARY KEY (transaction_id) NOT ENFORCED
) DISTRIBUTED BY HASH(transaction_id) INTO 1 BUCKETS
WITH ('changelog.mode' = 'upsert');
