#!/usr/bin/env bash
# setup.sh: sobe o pipeline, camada por camada.
# Pré-requisitos: Confluent CLI logado (`confluent login`), psql, envsubst, .env preenchido.
# Rode uma camada de cada vez (./setup.sh fundacao) ou tudo (./setup.sh all) e confira antes de avançar.
# ATENÇÃO: os comandos abaixo são TODO. Valide cada um com `--help` na primeira execução.
set -euo pipefail

[ -f .env ] || { echo "Falta o .env (cp .env.example .env)"; exit 1; }
set -a; source .env; set +a

fundacao() {
  echo ">> Camada 1: Fundação (pasta kafka/)"
  # TODO: environment e cluster Basic
  #   confluent environment create "$CONFLUENT_ENV_NAME"
  #   confluent kafka cluster create "$CONFLUENT_CLUSTER_NAME" --cloud "$CONFLUENT_CLOUD" --region "$CONFLUENT_REGION" --type basic
  # TODO: service accounts (producer WRITE, consumer READ) + ACLs + API keys
  # EVIDÊNCIA: service-account list + acl list por SA -> evidencias/01-fundacao.txt
  echo "(pendente)"
}

contrato() {
  echo ">> Camada 2: Contrato (Schema Registry, schemas em kafka/schemas/)"
  # TODO: registrar schemas e testar compatibilidade
  # EVIDÊNCIA: test-compatibility aceitando campo novo com default
  # EVIDÊNCIA: test-compatibility REJEITANDO remoção de campo obrigatório
  echo "(pendente)"
}

ingestao() {
  echo ">> Camada 3: Ingestão CDC (pasta cdc-debezium/)"
  # TODO: no Postgres: replicação lógica (Neon: Settings), cdc_user, REPLICA IDENTITY FULL, seed (cdc-debezium/seed/)
  # TODO: renderizar o conector sem versionar segredos e criá-lo
  #   envsubst < cdc-debezium/cdc.json > cdc-debezium/cdc.rendered.json   # (gitignored)
  #   confluent connect cluster create --config-file cdc-debezium/cdc.rendered.json
  # EVIDÊNCIA: contagem das tabelas + envelopes op=c / op=u / op=d -> evidencias/
  echo "(pendente)"
}

processamento() {
  echo ">> Camada 4: Processamento (pasta flink-sql/)"
  # TODO: executar os statements de flink-sql/ em ordem (PK, enriquecimento, regra de fraude)
  # EVIDÊNCIA: alerta no tópico de fraude + as 3 transações que o dispararam
  echo "(pendente)"
}

operacao() {
  echo ">> Camada 5: Operação"
  # TODO: Metrics API (received_bytes, lag), tabela de ACLs, billing cost list
  echo "(pendente)"
}

case "${1:-all}" in
  fundacao|contrato|ingestao|processamento|operacao) "$1" ;;
  all) fundacao; contrato; ingestao; processamento; operacao ;;
  *) echo "Uso: ./setup.sh [fundacao|contrato|ingestao|processamento|operacao|all]"; exit 1 ;;
esac
