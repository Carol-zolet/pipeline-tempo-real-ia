#!/usr/bin/env bash
# Camada 5: consulta a Metrics API do Confluent Cloud (received_bytes e lag por grupo).
# Usa uma Cloud API key guardada no .env (CLOUD_API_KEY / CLOUD_API_SECRET). Nunca versionar o .env.
# A API limita a granularidade de 1 minuto a janelas de 6 h; o script consulta em janelas e junta.
# Uso: ./scripts/metrics.sh [inicio] [fim]   (UTC, ex.: 2026-10-28T00:00:00Z 2026-10-30T00:00:00Z)
set -euo pipefail

[ -f ./.env ] || { echo "Falta o .env"; exit 1; }
set -a; source ./.env; set +a
: "${CLOUD_API_KEY:?Preencha CLOUD_API_KEY no .env}"
: "${CLOUD_API_SECRET:?Preencha CLOUD_API_SECRET no .env}"
: "${CONFLUENT_CLUSTER_ID:?Preencha CONFLUENT_CLUSTER_ID no .env}"
mkdir -p evidencias

export START="${1:-2026-10-28T00:00:00Z}"
export END="${2:-$(date -u +%Y-%m-%dT%H:%M:%SZ)}"

python3 - <<'PY' | tee evidencias/05-metricas-resumo.txt
import base64, json, os, urllib.request, urllib.error
from datetime import datetime, timedelta, timezone

URL = "https://api.telemetry.confluent.cloud/v2/metrics/cloud/query"
AUTH = base64.b64encode(f"{os.environ['CLOUD_API_KEY']}:{os.environ['CLOUD_API_SECRET']}".encode()).decode()
CLUSTER = os.environ["CONFLUENT_CLUSTER_ID"]
FMT = "%Y-%m-%dT%H:%M:%SZ"
start = datetime.strptime(os.environ["START"], FMT).replace(tzinfo=timezone.utc)
end = datetime.strptime(os.environ["END"], FMT).replace(tzinfo=timezone.utc)

def query(metric, group_by, t0, t1):
    body = {
        "aggregations": [{"metric": metric}],
        "filter": {"field": "resource.kafka.id", "op": "EQ", "value": CLUSTER},
        "granularity": "PT1M",
        "group_by": group_by,
        "intervals": [f"{t0.strftime(FMT)}/{t1.strftime(FMT)}"],
        "limit": 1000,
    }
    req = urllib.request.Request(URL, data=json.dumps(body).encode(), method="POST",
        headers={"Authorization": f"Basic {AUTH}", "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req) as r:
            return json.load(r).get("data", [])
    except urllib.error.HTTPError as e:
        raise SystemExit(f"Metrics API respondeu HTTP {e.code}: {e.read().decode()}")

def windows():
    t = start
    while t < end:
        yield t, min(t + timedelta(hours=6), end)
        t += timedelta(hours=6)

rb, lag = [], []
for t0, t1 in windows():
    rb += query("io.confluent.kafka.server/received_bytes", ["metric.topic"], t0, t1)
    lag += query("io.confluent.kafka.server/consumer_lag_offsets", ["metric.consumer_group_id", "metric.topic"], t0, t1)

json.dump(rb, open("evidencias/05-received-bytes.json", "w"))
json.dump(lag, open("evidencias/05-consumer-lag.json", "w"))

print(f"Período consultado (UTC): {start.strftime(FMT)} a {end.strftime(FMT)}; cluster {CLUSTER}")
print("== received_bytes por minuto ==")
if not rb:
    print("(sem dados no período)")
else:
    por_min = {}
    for d in rb:
        por_min[d["timestamp"]] = por_min.get(d["timestamp"], 0) + d["value"]
    ts, v = max(por_min.items(), key=lambda x: x[1])
    print(f"Pico: {v:.0f} bytes no minuto {ts} (soma de todos os tópicos)")
    topo = max(rb, key=lambda d: d["value"])
    print(f"Maior ponto de um único tópico: {topo['value']:.0f} bytes ({topo.get('metric.topic')}) em {topo['timestamp']}")
    print(f"Total produzido no período: {sum(d['value'] for d in rb):.0f} bytes")
print("== consumer_lag_offsets ==")
if not lag:
    print("(nenhum grupo de consumidores com lag no período: sem consumidores ativos)")
else:
    por = {}
    for d in lag:
        k = (d.get("metric.consumer_group_id"), d.get("metric.topic"))
        por[k] = max(por.get(k, 0), d["value"])
    for (g, t), v in sorted(por.items(), key=lambda x: str(x[0])):
        print(f"grupo={g} tópico={t} lag máximo={v:.0f}")
PY
