-- Camada 4, passo 1: tabela append-only com tempo de evento (entrada da regra de fraude).
-- Watermark com folga de 1 ms (eventos com o mesmo ts não viram "atrasados"); sem folga maior,
-- o alerta sai assim que a 3a transação chega (pouco tráfego), mas evento muito fora de ordem é ignorado.
CREATE TABLE tx_stream (
  card_id        BIGINT,   -- a coluna do DISTRIBUTED BY precisa vir primeiro
  transaction_id BIGINT,
  account_id     BIGINT,
  merchant_id    BIGINT,
  amount         DECIMAL(15, 2),
  currency       STRING,
  status         STRING,
  ts             TIMESTAMP(3),
  WATERMARK FOR ts AS ts - INTERVAL '0.001' SECOND
) DISTRIBUTED BY HASH(card_id) INTO 1 BUCKETS
WITH ('changelog.mode' = 'append');
