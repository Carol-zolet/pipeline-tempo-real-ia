#!/usr/bin/env bash
# setup.sh: sobe o pipeline, camada por camada.
# Pré-requisitos: Confluent CLI logado (`confluent login`), psql, envsubst, python3, .env preenchido.
# Uso: ./setup.sh fundacao | contrato | ingestao | processamento | operacao | all
#
# STATUS DE VALIDAÇÃO (honestidade sobre o que foi executado):
#   [validado]   comando executado durante o desenvolvimento, com evidência em evidencias/
#   [não testado] comando escrito a partir da documentação; reexecute e confira antes de confiar
# Este script NÃO foi reexecutado do zero; o teardown.sh foi (evidencias/07-teardown.txt).
set -euo pipefail

[ -f ./.env ] || { echo "Falta o .env (cp .env.example .env)"; exit 1; }
set -a; source ./.env; set +a

# grava/atualiza CHAVE=valor no .env (para guardar IDs gerados)
setenv() {
  local k="$1" v="$2"
  if grep -q "^${k}=" .env; then sed -i "s#^${k}=.*#${k}=${v}#" .env; else echo "${k}=${v}" >> .env; fi
  export "${k}=${v}"
}

fundacao() {
  echo ">> Camada 1: Fundação"
  # [validado] environment e cluster Basic (o ID aparece na saída; copie para o .env)
  confluent environment create "$CONFLUENT_ENV_NAME" -o json \
    | python3 -c 'import sys,json; print(json.load(sys.stdin)["id"])' > /tmp/env_id
  setenv CONFLUENT_ENV_ID "$(cat /tmp/env_id)"; rm -f /tmp/env_id
  confluent environment use "$CONFLUENT_ENV_ID"
  confluent kafka cluster create "$CONFLUENT_CLUSTER_NAME" --cloud "$CONFLUENT_CLOUD" \
    --region "$CONFLUENT_REGION" --type basic -o json \
    | python3 -c 'import sys,json; print(json.load(sys.stdin)["id"])' > /tmp/cl_id
  setenv CONFLUENT_CLUSTER_ID "$(cat /tmp/cl_id)"; rm -f /tmp/cl_id
  confluent kafka cluster use "$CONFLUENT_CLUSTER_ID"

  # [validado] duas identidades: o conector só escreve, o consumidor só lê (menor privilégio)
  confluent iam service-account create "$SA_PRODUCER_NAME" --description "escreve nos topicos desafio-*" -o json \
    | python3 -c 'import sys,json; print(json.load(sys.stdin)["id"])' > /tmp/sa_p
  confluent iam service-account create "$SA_CONSUMER_NAME" --description "le os topicos desafio-*" -o json \
    | python3 -c 'import sys,json; print(json.load(sys.stdin)["id"])' > /tmp/sa_c
  setenv SA_PRODUCER_ID "$(cat /tmp/sa_p)"; setenv SA_CONSUMER_ID "$(cat /tmp/sa_c)"; rm -f /tmp/sa_p /tmp/sa_c

  # [validado] ACLs por prefixo; o produtor também precisa do tópico de heartbeat do Debezium
  confluent kafka acl create --allow --service-account "$SA_PRODUCER_ID" --operations write,describe,create --topic "$TOPIC_PREFIX" --prefix
  confluent kafka acl create --allow --service-account "$SA_PRODUCER_ID" --operations write,describe,create --topic __debezium-heartbeat --prefix
  confluent kafka acl create --allow --service-account "$SA_CONSUMER_ID" --operations read,describe --topic "$TOPIC_PREFIX" --prefix
  confluent kafka acl create --allow --service-account "$SA_CONSUMER_ID" --operations read --consumer-group "$TOPIC_PREFIX" --prefix

  # [validado] API keys por service account (o segredo só aparece uma vez: guarde fora do repositório)
  confluent api-key create --resource "$CONFLUENT_CLUSTER_ID" --service-account "$SA_PRODUCER_ID" --description "desafio-producer" -o json > /tmp/k_p.json
  python3 - <<'PY'
import json
k = json.load(open('/tmp/k_p.json'))
with open('.env', 'a') as f:
    f.write(f"\nKAFKA_API_KEY={k['api_key']}\nKAFKA_API_SECRET={k['api_secret']}\n")
PY
  rm -f /tmp/k_p.json

  # [validado] evidência
  { confluent iam service-account list
    confluent kafka acl list --service-account "$SA_PRODUCER_ID"
    confluent kafka acl list --service-account "$SA_CONSUMER_ID"
    confluent api-key list; } | tee evidencias/01-fundacao.txt
}

contrato() {
  echo ">> Camada 2: Contrato (Schema Registry)"
  # Os schemas dos tópicos CDC são registrados pelo próprio conector (camada 3).
  # Rode esta função DEPOIS de `ingestao`. [validado] testes de compatibilidade (modo BACKWARD):
  S="${TOPIC_PREFIX}.public.customers-value"
  : > evidencias/02-compatibilidade.txt
  for f in add-optional add-required remove-required; do
    echo "=== $f ===" | tee -a evidencias/02-compatibilidade.txt
    confluent schema-registry schema compatibility validate "kafka/schemas/customers-$f.avsc" \
      --type avro --subject "$S" --version latest 2>&1 | tee -a evidencias/02-compatibilidade.txt
  done
}

ingestao() {
  echo ">> Camada 3: Ingestão (CDC)"
  # MANUAL (console do Neon, não automatizável aqui):
  #   1) Settings > habilitar replicação lógica (reinicia o compute; irreversível)
  #   2) criar o usuário $CDC_USER com permissão REPLICATION
  # [validado] carga do esquema, seed e publication
  ./cdc-debezium/seed/load.sh
  # [validado] renderiza o conector sem versionar segredos, cria e apaga o arquivo renderizado
  bash -c 'set -a; source ./.env; set +a; envsubst < cdc-debezium/cdc.json > cdc-debezium/cdc.rendered.json'
  confluent connect cluster create --config-file cdc-debezium/cdc.rendered.json \
    --environment "$CONFLUENT_ENV_ID" --cluster "$CONFLUENT_CLUSTER_ID"
  rm -f cdc-debezium/cdc.rendered.json
  confluent connect cluster list --environment "$CONFLUENT_ENV_ID" --cluster "$CONFLUENT_CLUSTER_ID" | tee evidencias/03-conector.txt
  echo "Aguarde o conector ficar RUNNING. Para gerar eventos c/u/d, veja o README (camada 3)."
}

processamento() {
  echo ">> Camada 4: Processamento (Flink SQL)"
  # [validado] compute pool
  confluent flink compute-pool create desafio-pool --cloud "$CONFLUENT_CLOUD" --region "$CONFLUENT_REGION" \
    --max-cfu 5 --wait | tee evidencias/04-compute-pool.txt
  # [validado] os statements abaixo foram executados no shell do Flink (confluent flink shell) na ordem:
  #   flink-sql/01_tx_stream_create.sql, 02_tx_stream_insert.sql, 03_fraud_alerts_create.sql, 04_fraud_detect.sql
  # [não testado] execução via CLI em vez do shell; confira a sintaxe com `confluent flink statement create --help`
  echo "Abra: confluent flink shell --compute-pool <ID-do-pool> --environment $CONFLUENT_ENV_ID"
  echo "e rode os arquivos de flink-sql/ na ordem (USE CATALOG/USE database antes)."
  echo "Para a rajada de fraude: cole cdc-debezium/seed/03_fraud_burst.sql no SQL Editor do Neon."
}

operacao() {
  echo ">> Camada 5: Operação"
  # [validado] métricas (pico de received_bytes e lag) via Metrics API; exige CLOUD_API_KEY no .env:
  #   confluent api-key create --resource cloud
  ./scripts/metrics.sh
  # [validado] ACLs: evidencias/01-fundacao.txt. Custo (a cobrança leva 24-48 h para aparecer):
  confluent billing cost list --start-date 2026-10-27 --end-date "$(date +%F)" | tee evidencias/05-custo.txt
  # Extensão Tableflow: habilitado no console (Topic > Enable Tableflow > Use Confluent storage);
  # consulta com: python scripts/query_iceberg.py (precisa de TABLEFLOW_* no .env). [validado]
}

case "${1:-all}" in
  fundacao|contrato|ingestao|processamento|operacao) "$1" ;;
  all) fundacao; ingestao; contrato; processamento; operacao ;;
  *) echo "Uso: ./setup.sh [fundacao|contrato|ingestao|processamento|operacao|all]"; exit 1 ;;
esac
