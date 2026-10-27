#!/usr/bin/env bash
# teardown.sh: derruba TUDO, na ordem certa, e verifica que não sobrou nada cobrando.
# ATENÇÃO: os comandos abaixo são TODO. Valide cada um com `--help` antes de rodar.
set -euo pipefail

[ -f .env ] || { echo "Falta o .env"; exit 1; }
set -a; source .env; set +a

echo ">> 1/4 Parar statements Flink (cobram enquanto rodam)"
# TODO: confluent flink statement list  ->  confluent flink statement delete <nome> --force

echo ">> 2/4 Remover conectores"
# TODO: confluent connect cluster list  ->  confluent connect cluster delete <id> --force

echo ">> 3/4 Remover o replication slot no Postgres (slot órfão retém WAL e enche o disco)"
# TODO: psql ... -c "SELECT pg_drop_replication_slot('<slot>');"

echo ">> 4/4 Deletar o environment (leva cluster, Schema Registry e service accounts)"
# TODO: confluent environment delete "$CONFLUENT_ENV_ID" --force

echo ">> Verificação (listas devem vir vazias)"
# TODO: confluent connect cluster list ; confluent flink statement list
# EVIDÊNCIA: colar a saída ("None found.") no README
