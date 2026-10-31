"""Camada de extensão: consulta as tabelas Iceberg que o Tableflow materializou.
Lê TABLEFLOW_ENDPOINT / TABLEFLOW_API_KEY / TABLEFLOW_API_SECRET / CONFLUENT_CLUSTER_ID do ambiente
(carregue o .env antes: set -a; source ./.env; set +a). Resultado em evidencias/06-iceberg.txt.
"""
import os
from pyiceberg.catalog import load_catalog

catalog = load_catalog(
    "tableflow",
    **{
        "type": "rest",
        "uri": os.environ["TABLEFLOW_ENDPOINT"],
        "credential": f"{os.environ['TABLEFLOW_API_KEY']}:{os.environ['TABLEFLOW_API_SECRET']}",
        "header.X-Iceberg-Access-Delegation": "vended-credentials",
    },
)
ns = os.environ["CONFLUENT_CLUSTER_ID"]
out = []
out.append(f"Namespace (cluster): {ns}")
out.append(f"Tabelas: {[t[1] for t in catalog.list_tables(ns)]}")
for name in ("fraud_alerts", "tx_stream"):
    t = catalog.load_table(f"{ns}.{name}")
    out.append(f"\n== {name} ==")
    out.append("Colunas: " + ", ".join(f"{f.name}:{f.field_type}" for f in t.schema().fields))
    out.append(f"Snapshots: {len(t.snapshots())}")
    df = t.scan().to_pandas()
    out.append(f"Linhas: {len(df)}")
    out.append(df.head(5).to_string())
text = "\n".join(out)
print(text)
os.makedirs("evidencias", exist_ok=True)
open("evidencias/06-iceberg.txt", "w", encoding="utf-8").write(text + "\n")
