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

**O que faz:**

- Environment `desafio-final` (`env-o7mj39`), com Stream Governance Essentials, isolado do environment `default`.
- Cluster Kafka **Basic** `desafio-basic` (`lkc-dozgd01`) em `aws/us-east-1`, single-zone.
- Duas identidades (service accounts), uma por papel, com uma chave de API cada:
  - `desafio-producer` (`sa-v1y03yn`): `WRITE`, `DESCRIBE` e `CREATE` nos tópicos com prefixo `desafio`.
  - `desafio-consumer` (`sa-gkx8gjn`): `READ` e `DESCRIBE` nos tópicos `desafio*` e `READ` nos grupos de consumo `desafio*`.

**Por que assim:**

- **Duas identidades, menor privilégio:** quem escreve não lê e quem lê não escreve. Se a chave do consumer vazar, ela não consegue gravar no tópico.
- **ACL por prefixo (`desafio`):** limita o alcance aos tópicos do projeto, incluindo os que o conector CDC vai criar. Em produção, o RBAC é o padrão e a ACL é a exceção documentada (Aula 3); aqui a ACL por prefixo segue o desenho do desafio.
- **Cluster Basic:** é o tipo certo para dev e POC (Aula 2) e só cobra pelo uso. O volume do desafio (cerca de 2.000 transações sintéticas) cabe com folga. Em produção, o trade-off seria Standard ou superior, por SLA e rede privada.
- **Environment dedicado:** dá uma fronteira de isolamento e permite derrubar tudo com um único `delete` no teardown.

**Evidência** (`evidencias/01-fundacao.txt`: `service-account list`, `acl list` de cada identidade e `api-key list`; os secrets nunca aparecem):

```text
  Current |     ID     |       Name       |          Description           
----------+------------+------------------+--------------------------------
          | sa-gkx8gjn | desafio-consumer | le os topicos desafio-*        
          | sa-v1y03yn | desafio-producer | escreve nos topicos desafio-*  
     Principal    | Permission | Operation | Resource Type | Resource Name | Pattern Type  
------------------+------------+-----------+---------------+---------------+---------------
  User:sa-v1y03yn | ALLOW      | CREATE    | TOPIC         | desafio       | PREFIXED      
  User:sa-v1y03yn | ALLOW      | DESCRIBE  | TOPIC         | desafio       | PREFIXED      
  User:sa-v1y03yn | ALLOW      | WRITE     | TOPIC         | desafio       | PREFIXED      
     Principal    | Permission | Operation | Resource Type | Resource Name | Pattern Type  
------------------+------------+-----------+---------------+---------------+---------------
  User:sa-gkx8gjn | ALLOW      | DESCRIBE  | TOPIC         | desafio       | PREFIXED      
  User:sa-gkx8gjn | ALLOW      | READ      | GROUP         | desafio       | PREFIXED      
  User:sa-gkx8gjn | ALLOW      | READ      | TOPIC         | desafio       | PREFIXED      
  Current |       Key        |   Description    |   Owner    |    Owner Email    | Resource Type |  Resource   |       Created         
----------+------------------+------------------+------------+-------------------+---------------+-------------+-----------------------
          | OT4XULKQTJXJI642 | desafio-producer | sa-v1y03yn | <service account> | kafka         | lkc-dozgd01 | 2026-10-28T22:30:07Z  
          | UDONLAMX2PE5ECZA | desafio-consumer | sa-gkx8gjn | <service account> | kafka         | lkc-dozgd01 | 2026-10-28T22:34:01Z  
```

## Camada 2: Contrato de dados (`kafka/schemas/`)

**O que faz:** O Debezium registra automaticamente um schema Avro por tabela no Schema Registry (subjects `desafio.public.<tabela>-value`, envelope com `before`/`after`/`source`/`op`). Esses schemas são o contrato do pipeline; em `kafka/schemas/` ficam o schema atual de `customers` e três variantes usadas nos testes de evolução.

**Por que assim:** Mantive a compatibilidade `BACKWARD` (padrão do Schema Registry): o consumidor novo lê dados antigos, então os consumidores (Flink, Tableflow) podem ser atualizados antes do produtor. Nessa regra, adicionar campo opcional com `default` é seguro; adicionar campo obrigatório sem `default` quebra, porque o consumidor novo não saberia preencher o campo nos dados antigos. Remover campo é aceito em `BACKWARD` (o consumidor novo ignora o que sumiu); para barrar remoções seria preciso `FORWARD` ou `FULL`, trade-off que registrei na tabela de decisões. Os testes usam `validate`, que não registra versão e não suja o subject.

**Evidência** (`evidencias/02-compatibilidade.txt`, subject `desafio.public.customers-value`, versão 1):

```text
=== add-optional ===      Compatible | true    (campo novo opcional, default null)
=== add-required ===      Compatible | false   (campo novo obrigatório, sem default)
=== remove-required ===   Compatible | true    (remoção de `email`: aceita em BACKWARD)
```

## Camada 3: Ingestão CDC (`cdc-debezium/`)

**O que faz:** Conector gerenciado `PostgresCdcSourceV2` (Debezium 3.2.5) lê o WAL do Postgres (Neon) com o usuário `cdc_user` (permissão mínima de replicação) e publica as 5 tabelas (`customers`, `accounts`, `cards`, `merchants`, `transactions`) em tópicos `desafio.public.<tabela>`, em Avro, com a chave primária como chave Kafka. (config em `cdc-debezium/cdc.json`, sem segredos).

**Por que assim:** CDC lê o log de transações, então captura INSERT, UPDATE e DELETE com baixa latência e sem consultar as tabelas; polling perderia DELETEs e estados intermediários. Usei `REPLICA IDENTITY FULL` para que `before` venha completo em UPDATE/DELETE (custo: mais bytes no WAL). Cada tópico tem 1 partição (padrão do conector), o que preserva a ordem global, mas limita o paralelismo. Lição de menor privilégio: o conector também precisou de ACL (CREATE/DESCRIBE/WRITE) no prefixo `__debezium-heartbeat`; sem ela ficou `FAILED` por falta de autorização.

**Evidência:**

```text
Seed determinístico (`seed=42`) carregado no Postgres 18 do Neon (`cdc-debezium/seed/load.sh`):

 customers | accounts | cards | merchants | transactions 
-----------+----------+-------+-----------+--------------
       200 |      240 |   320 |        60 |         2000
(1 row)
```

Eventos capturados (envelopes completos em `evidencias/`):

- `op=c` (INSERT): `before: null`, `after` com `city = Nova Prata` (offset 200)
- `op=u` (UPDATE): `before` preenchido com `Nova Prata` e `after` com `Caxias do Sul` (offset 201)
- `op=d` (DELETE): `before` completo, `after: null` (offset 202); a mesma chave `customer_id=201` nos 3 eventos preserva a ordem na partição

## Camada 4: Processamento (`flink-sql/`)

**O que faz:** Dois statements Flink SQL contínuos num compute pool de até 5 CFU. (1) `tx-stream-insert` converte `desafio.public.transactions` (changelog `retract` do Debezium) numa tabela append-only `tx_stream`, com `created_at` convertido para `TIMESTAMP(3)` e watermark. (2) `fraud-detect` aplica um `MATCH_RECOGNIZE` particionado por cartão e grava em `fraud_alerts` quando há 3 transações do mesmo cartão em até 60 s (`PATTERN (T{3}) WITHIN INTERVAL '60' SECOND`). Statements em `flink-sql/01..04_*.sql`.

**Por que assim:**
- `MATCH_RECOGNIZE` não aceita entrada que atualiza ou apaga linhas, então a tabela do Debezium passa por `TO_CHANGELOG` (só inserts; transações são imutáveis) antes da regra.
- O tempo da regra é o tempo do evento (`created_at`), não o de chegada, para que atrasos do pipeline não mudem o resultado.
- Watermark com folga de 1 ms: o aviso `WATERMARK_EQUAL` do Flink mostrou que, sem folga, eventos com o mesmo `ts` contam como atrasados. Folga maior atrasaria o alerta; folga menor perderia eventos. Evento muito fora de ordem continua sendo ignorado.
- Como o `MATCH_RECOGNIZE` só libera uma linha quando o watermark a ultrapassa e a tabela tem pouco tráfego, o alerta depende de um evento posterior de qualquer cartão. Em produção o fluxo contínuo resolve isso; no teste uso uma transação de outro cartão.
- `AFTER MATCH SKIP PAST LAST ROW`: uma rajada gera um alerta, não vários sobrepostos.
- Um join de enriquecimento com `accounts` ficou fora desta entrega: o join temporal descartaria as transações do seed, porque as contas só têm versão a partir do snapshot e as transações são de setembro. Um join comum guardaria estado sem limite. Fica como próximo passo.

**Evidência** (`evidencias/04-fraude.txt`):

```text
Antes da rajada: fraud_alerts vazia (nenhum falso positivo nas 2000 transações do seed)

transaction_id | card_id | amount | ts
2001           | 1       | 100.00 | 2026-10-29 22:59:02.958
2002           | 1       | 101.00 | 2026-10-29 22:59:12.958
2003           | 1       | 102.00 | 2026-10-29 22:59:22.958

Alerta em fraud_alerts:
card_id | first_ts                | last_ts                 | tx_count | total_amount | first_tx_id | last_tx_id
1       | 2026-10-29 22:59:02.958 | 2026-10-29 22:59:22.958 | 3        | 303.00       | 2001        | 2003
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
