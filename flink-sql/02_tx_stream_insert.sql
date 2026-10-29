-- Camada 4, passo 2 (statement contínuo, cobra CFU): retract do Debezium -> append-only.
-- TO_CHANGELOG com só 'INSERT' descarta UPDATE/DELETE; transações são imutáveis, então só entram inserts.
SET 'client.statement-name' = 'tx-stream-insert';
INSERT INTO tx_stream
SELECT card_id, transaction_id, account_id, merchant_id, amount, currency, status,
       CAST(REPLACE(REPLACE(created_at, 'T', ' '), 'Z', '') AS TIMESTAMP(3))
FROM TO_CHANGELOG(
  input => TABLE `desafio.public.transactions`,
  op_mapping => MAP['INSERT', 'I']
);
