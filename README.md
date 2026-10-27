# Pipeline de Dados em Tempo Real para Agentes de IA

Projeto desenvolvido no bootcamp **IBM Confluent - Dados em tempo real para agentes de IA** (DIO), como Desafio Final que evolui módulo a módulo.

## Objetivo

Construir um pipeline de dados end-to-end em tempo real, usando **Confluent Cloud (Kafka)** como espinha dorsal, capturando mudanças de sistemas existentes via CDC, processando o fluxo com Flink e materializando os dados como tabelas Iceberg, pronto para alimentar RAG e agentes de IA.

O caso de uso do desafio é um **pipeline de pagamentos**: Postgres (Neon) → CDC (Debezium) → tópicos Avro + Schema Registry → Flink SQL (enriquecimento + regra de fraude) → consumo. Os dados são sintéticos (seed determinístico, `seed=42`).

## Stack

| Camada | Tecnologia |
|---|---|
| Banco de origem | PostgreSQL (Neon) |
| Streaming / Event Log | Apache Kafka (Confluent Cloud) |
| Governança de contratos | Schema Registry |
| Integração / CDC | Kafka Connect + Debezium |
| Processamento de stream | Apache Flink / Flink SQL |
| Lakehouse | Tableflow → Apache Iceberg |

## Estrutura do repositório

| Pasta / arquivo | Camada | Conteúdo |
|---|---|---|
| `kafka/` | 1 Fundação, 2 Contrato | cluster, service accounts, ACLs; `kafka/schemas/` com os schemas Avro |
| `cdc-debezium/` | 3 Ingestão | `cdc.json` (conector, sem segredos) e `seed/` (dados sintéticos) |
| `flink-sql/` | 4 Processamento | statements SQL: enriquecimento e regra de fraude |
| `tableflow-iceberg/` | Extensão | tópicos materializados como tabelas Iceberg (opcional) |
| `evidencias/` | todas | saídas de comandos e eventos que provam cada camada |
| `setup.sh` / `teardown.sh` | todas | subir e derrubar o pipeline |

## Status

🚧 Em desenvolvimento, acompanhando os módulos do bootcamp (inscrições até 18/10, entrega até 15/11/2026)

- [ ] Módulo 1: Kafka (arquitetura orientada a eventos, partições, garantia de entrega)
- [ ] Módulo 2: Confluent Cloud (environments, service accounts, Schema Registry)
- [ ] Módulo 3: Kafka Connect + CDC com Debezium
- [ ] Módulo 4: Flink SQL (processamento do fluxo)
- [ ] Módulo 5: Tableflow (tópicos Kafka → tabelas Apache Iceberg)
- [ ] Desafio Final consolidado

## Como subir do zero

Pré-requisitos: conta no Confluent Cloud, projeto no Neon, [Confluent CLI](https://docs.confluent.io/confluent-cli/current/overview.html), `psql`, `envsubst`.

```bash
cp .env.example .env     # preencha os valores; o .env NUNCA é versionado
./setup.sh fundacao      # confira a evidência antes de avançar
./setup.sh contrato
./setup.sh ingestao
./setup.sh processamento
./setup.sh operacao
```

## Como derrubar tudo

```bash
./teardown.sh
```

Para os statements Flink, remove os conectores, apaga o replication slot no Postgres e deleta o environment.

---

## Camada 1: Fundação (`kafka/`)

**O que faz:** `<descreva: environment, cluster, service accounts>`

**Por que assim:** `<descreva: duas identidades (escrita/leitura) por menor privilégio; cluster Basic pelo volume e custo>`

**Evidência:**

```text
<sua evidência aqui: confluent iam service-account list + acl list por SA>
```

## Camada 2: Contrato de dados (`kafka/schemas/`)

**O que faz:** `<descreva: schemas, namespace, tipo de money, tags de PII>`

**Por que assim:** `<descreva: modo de compatibilidade escolhido e porquê>`

**Evidência:**

```text
<sua evidência aqui: test-compatibility aceitando campo novo com default>
<sua evidência aqui: test-compatibility REJEITANDO remoção de campo obrigatório>
```

## Camada 3: Ingestão CDC (`cdc-debezium/`)

**O que faz:** `<descreva: conector, usuário cdc_user, tabelas capturadas>` (config em `cdc-debezium/cdc.json`, sem segredos).

**Por que assim:** `<descreva: CDC e não batch/polling; REPLICA IDENTITY escolhida e custo>`

**Evidência:**

```text
<sua evidência aqui: contagem das tabelas após o seed>
```

Eventos capturados (envelopes completos em `evidencias/`):

- `op=c` (INSERT): `<evidência>`
- `op=u` (UPDATE): `<evidência: before preenchido?>`
- `op=d` (DELETE): `<evidência>`

## Camada 4: Processamento (`flink-sql/`)

**O que faz:** `<descreva: enriquecimento transação × conta e regra de fraude: 3 transações no mesmo cartão em 60 s>`

**Por que assim:** `<descreva: tipo de join, modelagem da janela e trade-offs>`

**Evidência:**

```text
<sua evidência aqui: o alerta emitido no tópico de fraude
 + as 3 transações (mesmo card_id, <60s) que o dispararam>
```

## Camada 5: Operação

| Pilar | Resposta (medida) |
|---|---|
| Observabilidade | `<pico de received_bytes e lag por grupo (Metrics API)>` |
| Segurança | `<tabela: service account → ACLs → porquê>` |
| Custos | `<billing cost list do período + 2 alavancas>` |
| Confiabilidade | `<tabela trecho a trecho; elo mais fraco>` |

## Extensão: Tableflow + Iceberg (`tableflow-iceberg/`)

`<opcional: materializar os tópicos como tabelas Iceberg; o que foi feito e evidência>`

## Decisões de arquitetura

Cada decisão relevante tomada durante o desenvolvimento é documentada aqui, com o raciocínio por trás dela.

| # | Decisão | Alternativa descartada | Por quê / trade-off |
|---|---|---|---|
| 1 | CDC (Debezium) | Polling/batch | `<preencher>` |
| 2 | 2 service accounts (write/read) | 1 conta com tudo | `<preencher>` |
| 3 | Compatibilidade `<escolha>` | `<outra>` | `<preencher>` |
| 4 | `REPLICA IDENTITY <escolha>` | `<outra>` | `<preencher>` |
| 5 | `<modelagem da regra de fraude>` | `<outra>` | `<preencher>` |
| 6 | Cluster Basic | Standard/Dedicated | `<preencher>` |

## Trade-offs gerais do pipeline

_(preencher conforme o desafio avança, por exemplo: latência vs. consistência, custo de cluster vs. throughput.)_

## Custo total e teardown

- **Custo declarado:** `<US$ X,XX: billing cost list>`
- `./teardown.sh` executado ao final:

```text
<sua evidência aqui: connect cluster list / flink statement list → None found.>
```

## Autora

Caroline Zolet, desenvolvido como parte do bootcamp IBM Confluent (DIO), 2026.
