-- Permissões mínimas para o usuário de CDC e a publication lida pelo conector.
-- PRÉ-REQUISITO: o role cdc_user já existe, criado com REPLICATION pelo console do Neon
-- (Roles) ou pela CLI/API; no Neon, role criado só por SQL pode não ter esse atributo. Confira na doc.
-- A senha do cdc_user NUNCA vai neste arquivo: ela fica no .env (CDC_PASSWORD).
-- Rodar DEPOIS de 00_schema.sql e 02_seed.sql.

GRANT USAGE ON SCHEMA public TO cdc_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO cdc_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO cdc_user;

DROP PUBLICATION IF EXISTS desafio_pub;
CREATE PUBLICATION desafio_pub
  FOR TABLE customers, accounts, cards, merchants, transactions;
