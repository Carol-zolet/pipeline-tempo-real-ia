# Pipeline de Dados em Tempo Real para Agentes de IA

Projeto desenvolvido no bootcamp **IBM Confluent - Dados em tempo real para agentes de IA** (DIO), como Desafio Final que evolui módulo a módulo.

## Objetivo

Construir um pipeline de dados end-to-end em tempo real, usando **Confluent Cloud (Kafka)** como espinha dorsal, capturando mudanças de sistemas existentes via CDC, processando o fluxo com Flink e materializando os dados como tabelas Iceberg — pronto para alimentar RAG e agentes de IA.

## Stack

| Camada | Tecnologia |
|---|---|
| Streaming / Event Log | Apache Kafka (Confluent Cloud) |
| Governança de contratos | Schema Registry |
| Integração / CDC | Kafka Connect + Debezium |
| Processamento de stream | Apache Flink / Flink SQL |
| Lakehouse | Tableflow → Apache Iceberg |

## Estrutura do repositório

```
/pipeline-tempo-real-ia
  ├── README.md          (este arquivo — decisões de arquitetura e trade-offs)
  ├── /kafka              (configs, producers/consumers, definição de tópicos)
  ├── /cdc-debezium        (conectores CDC, configuração do Debezium)
  ├── /flink-sql           (queries de processamento de stream)
  └── /tableflow-iceberg   (configuração de materialização em tabelas Iceberg)
```

## Status

🚧 Em desenvolvimento — acompanhando os módulos do bootcamp (inscrições até 18/10, entrega até 15/11/2026)

- [ ] Módulo 1 — Kafka: arquitetura orientada a eventos, partições, garantia de entrega
- [ ] Módulo 2 — Confluent Cloud: environments, service accounts, Schema Registry
- [ ] Módulo 3 — Kafka Connect + CDC com Debezium
- [ ] Módulo 4 — Flink SQL: processamento do fluxo
- [ ] Módulo 5 — Tableflow: tópicos Kafka → tabelas Apache Iceberg
- [ ] Desafio Final consolidado

## Decisões de arquitetura

_Cada decisão relevante tomada durante o desenvolvimento é documentada aqui, com o raciocínio por trás dela._

### [Módulo] — [Decisão]
- **Contexto:**
- **Opções consideradas:**
- **Decisão tomada:**
- **Trade-offs:**

## Trade-offs gerais do pipeline

_(preencher conforme o desafio avança — ex: latência vs. consistência, custo de cluster vs. throughput, etc.)_

## Métricas

_(consumer lag, throughput, custo estimado do cluster — conforme abordado no módulo de operação em produção)_

## Autora

Caroline Zolet — desenvolvida como parte do bootcamp IBM Confluent (DIO), 2026.
