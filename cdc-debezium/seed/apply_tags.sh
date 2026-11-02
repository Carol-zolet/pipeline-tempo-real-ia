#!/usr/bin/env bash
# Aplica tags PII/PCI nos CAMPOS dos schemas via API do catálogo (Stream Governance).
# Cria uma API key temporária do Schema Registry, usa, e apaga no final. Não imprime segredos.
# Uso: set -a; source ./.env; set +a; ./cdc-debezium/seed/apply_tags.sh
set -euo pipefail
ENV="${CONFLUENT_ENV_ID:?defina CONFLUENT_ENV_ID no .env}"
EVID="${EVID_DIR:-evidencias}"
mkdir -p "$EVID"

SR_JSON=$(confluent schema-registry cluster describe --environment "$ENV" -o json)
SR_ID=$(python3 -c 'import json,sys;d=json.loads(sys.stdin.read());print(d.get("cluster") or d.get("cluster_id") or d.get("id"))' <<<"$SR_JSON")
SR_URL=$(python3 -c 'import json,sys;d=json.loads(sys.stdin.read());print(d.get("endpoint_url") or d.get("endpoint"))' <<<"$SR_JSON")
echo ">> Schema Registry: $SR_ID ($SR_URL)"

KEY_JSON=$(confluent api-key create --resource "$SR_ID" --environment "$ENV" --description "tmp tags desafio" -o json)
KEY=$(python3 -c 'import json,sys;print(json.loads(sys.stdin.read())["api_key"])' <<<"$KEY_JSON")
SEC=$(python3 -c 'import json,sys;print(json.loads(sys.stdin.read())["api_secret"])' <<<"$KEY_JSON")
trap 'confluent api-key delete "$KEY" --force >/dev/null 2>&1 && echo ">> chave temporaria apagada"' EXIT
sleep 10   # propagação da chave

api() { curl -sS -u "$KEY:$SEC" -H 'Content-Type: application/json' "$@"; }

echo ">> Criando definições de tag (409 = já existe)"
api -X POST "$SR_URL/catalog/v1/types/tagdefs" -d '[{"name":"PII","description":"Dado pessoal identificavel (LGPD)"},{"name":"PCI","description":"Dado de cartao (PCI DSS)"}]' || true; echo

schema_id() { api "$SR_URL/subjects/$1/versions/latest" | python3 -c 'import json,sys;print(json.load(sys.stdin)["id"])'; }
CUST_ID=$(schema_id desafio.public.customers-value)
CARD_ID=$(schema_id desafio.public.cards-value)

tag() { # tabela schema_id campo tag
  echo ">> $4 em $1.$3 (schema $2)"
  api -X POST "$SR_URL/catalog/v1/entity/tags" \
    -d "[{\"entityType\":\"sr_field\",\"entityName\":\"$SR_ID:.:$2:desafio.public.$1.Value.$3\",\"typeName\":\"$4\"}]"; echo
}
{
  tag customers "$CUST_ID" full_name PII
  tag customers "$CUST_ID" email PII
  tag customers "$CUST_ID" phone PII
  tag cards "$CARD_ID" last4 PCI
  echo ">> Conferindo tags nos campos"
  for t in "customers $CUST_ID full_name" "customers $CUST_ID email" "customers $CUST_ID phone" "cards $CARD_ID last4"; do
    set -- $t
    echo "-- $1.$3"
    api "$SR_URL/catalog/v1/entity/type/sr_field/name/$SR_ID:.:$2:desafio.public.$1.Value.$3/tags"; echo
  done
} | tee "$EVID/04-tags.txt"
