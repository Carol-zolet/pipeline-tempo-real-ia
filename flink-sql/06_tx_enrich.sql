-- [Validado na rodada 2, executado no workspace do console sem os dois SET abaixo] Camada 4, enriquecimento transação × conta (statement contínuo, cobra CFU).
-- Join comum (não temporal): o estado cresce com o número de transações, por isso o TTL de estado abaixo.
-- Por que não temporal: `FOR SYSTEM_TIME AS OF t.ts` exigiria versões de `accounts` anteriores ao ts da
-- transação; as contas só têm versão a partir do snapshot do CDC e as transações do seed são de setembro,
-- então o join temporal descartaria essas linhas (hipótese a confirmar). Alternativa a testar:
-- temporal join pelo tempo de chegada ($rowtime) em vez de `ts`.
SET 'client.statement-name' = 'tx-enrich';
SET 'sql.state-ttl' = '1 d';
INSERT INTO tx_enriched
SELECT t.transaction_id, t.card_id, t.account_id, a.customer_id, a.account_type,
       a.status AS account_status, a.balance, t.merchant_id, t.amount, t.currency, t.status, t.ts
FROM tx_stream t
JOIN `desafio.public.accounts` a ON t.account_id = a.account_id;
