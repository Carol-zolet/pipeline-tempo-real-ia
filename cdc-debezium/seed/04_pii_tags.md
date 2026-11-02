# Tags de PII/PCI no Schema Registry (Stream Governance)

Campos sensíveis do domínio (definidos em `00_schema.sql`):

| Tabela | Campo | Tag |
|---|---|---|
| customers | full_name | PII |
| customers | email | PII |
| customers | phone | PII |
| cards | last4 | PCI |

Como aplicar (console, depois que o conector registrar os schemas):
Data Portal > tópico `desafio.public.customers` > Schema > campo > *Add tag* > `PII`.
Repetir para `desafio.public.cards` (`last4` > `PCI`).
Evidência esperada: print da aba Schema com as tags, salvo em `evidencias/`.
Se as tags não existirem, criá-las antes em Stream Governance > Tags.

## Validado na rodada 2
A tag por campo foi aplicada via API do catálogo (`apply_tags.sh`, entidade `sr_field`):
`<sr-id>:.:<schema-id>:desafio.public.<tabela>.Value.<campo>`. A aba Schema do console só
mostrava tag no nível do schema/topic. Evidência: `evidencias/rodada2/04-tags.txt`.
