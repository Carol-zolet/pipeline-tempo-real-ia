-- Camada 4: dispara a regra de fraude (3 transações no MESMO cartão em menos de 60 s).
-- NÃO rodar na carga inicial: use só depois que o Flink estiver processando.
-- O seed garante o cartão 1 ativo e sem rajadas, então este é o único alerta esperado.
INSERT INTO transactions (card_id, account_id, merchant_id, amount, currency, status, created_at)
SELECT c.card_id, c.account_id, 1 + (n % 5), 100.00 + n, 'BRL', 'APPROVED',
       now() - interval '30 seconds' + (n * interval '10 seconds')
FROM cards c, generate_series(0, 2) AS n
WHERE c.card_id = 1;
