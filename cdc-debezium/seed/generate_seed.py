#!/usr/bin/env python3
"""Gera 02_seed.sql com dados sintéticos DETERMINÍSTICOS (seed=42).

Mesma entrada, mesma saída: qualquer pessoa reproduz exatamente os mesmos dados.
Proporções do desafio: 200 clientes, 240 contas, 320 cartões, 60 comerciantes, 2000 transações.
Garantia para a camada 4: nenhum cartão tem duas transações a menos de 10 minutos uma da outra,
então a base NÃO gera alertas de fraude por si só (o alerta vem do 03_fraud_burst.sql).

Uso: python3 generate_seed.py > 02_seed.sql
"""
import datetime as dt
import random
import sys

SEED = 42
N_CUSTOMERS, N_ACCOUNTS, N_CARDS, N_MERCHANTS, N_TX = 200, 240, 320, 60, 2000
MIN_GAP_S = 600
REF = dt.datetime(2026, 10, 1, tzinfo=dt.timezone.utc)   # data fixa, para o seed ser reproduzível
WINDOW_DAYS = 14

rng = random.Random(SEED)

FIRST = ["Ana", "Bruno", "Carla", "Daniel", "Eduarda", "Felipe", "Gabriela", "Henrique", "Isabela",
         "Joao", "Karen", "Lucas", "Mariana", "Nicolas", "Olivia", "Paulo", "Rafaela", "Samuel",
         "Tatiana", "Vinicius"]
LAST = ["Silva", "Souza", "Oliveira", "Santos", "Pereira", "Lima", "Costa", "Ribeiro", "Almeida",
        "Carvalho", "Gomes", "Martins", "Rocha", "Barbosa", "Dias"]
CITIES = ["Porto Alegre", "Caxias do Sul", "Nova Prata", "Curitiba", "Sao Paulo", "Florianopolis",
          "Belo Horizonte", "Recife"]
CATEGORIES = ["supermercado", "restaurante", "combustivel", "farmacia", "vestuario", "eletronicos",
              "transporte", "streaming"]
BRANDS = ["VISA", "MASTERCARD", "ELO"]


def q(s):
    return "'" + str(s).replace("'", "''") + "'"


def ts(d):
    return q(d.strftime("%Y-%m-%d %H:%M:%S+00"))


def batches(table, cols, rows, size=500):
    for i in range(0, len(rows), size):
        chunk = rows[i:i + size]
        print(f"INSERT INTO {table} ({', '.join(cols)}) VALUES")
        print(",\n".join("  (" + ", ".join(r) + ")" for r in chunk) + ";")


out = sys.stdout
print("-- Gerado por generate_seed.py (seed=%d). NÃO editar à mão." % SEED)
print("BEGIN;")
print("TRUNCATE transactions, cards, accounts, merchants, customers RESTART IDENTITY CASCADE;")

# customers
customers = []
for i in range(1, N_CUSTOMERS + 1):
    first, last = rng.choice(FIRST), rng.choice(LAST)
    created = REF - dt.timedelta(days=rng.randint(60, 900), seconds=rng.randint(0, 86399))
    customers.append([str(i), q(f"{first} {last}"), q(f"{first}.{last}{i}@example.com".lower()),
                      q("+55 (51) 9%04d-%04d" % (rng.randint(0, 9999), rng.randint(0, 9999))),
                      q(rng.choice(CITIES)), ts(created)])
batches("customers", ["customer_id", "full_name", "email", "phone", "city", "created_at"], customers)

# accounts: 1 por cliente + extras para alguns clientes
account_customer = list(range(1, N_CUSTOMERS + 1)) + [rng.randint(1, N_CUSTOMERS)
                                                      for _ in range(N_ACCOUNTS - N_CUSTOMERS)]
accounts, account_status = [], {}
for i, cust in enumerate(account_customer, start=1):
    status = "ACTIVE" if (i == 1 or rng.random() < 0.95) else "BLOCKED"
    account_status[i] = status
    opened = REF - dt.timedelta(days=rng.randint(30, 800), seconds=rng.randint(0, 86399))
    accounts.append([str(i), str(cust), q(rng.choice(["CHECKING", "SAVINGS"])),
                     "%.2f" % rng.uniform(100, 50000), q(status), ts(opened)])
batches("accounts", ["account_id", "customer_id", "account_type", "balance", "status", "opened_at"], accounts)

# cards: 1 por conta + extras
card_account = list(range(1, N_ACCOUNTS + 1)) + [rng.randint(1, N_ACCOUNTS)
                                                 for _ in range(N_CARDS - N_ACCOUNTS)]
cards, card_acc = [], {}
for i, acc in enumerate(card_account, start=1):
    card_acc[i] = acc
    status = "ACTIVE" if (i == 1 or rng.random() < 0.95) else "BLOCKED"
    exp = dt.date(2027 + rng.randint(0, 4), rng.randint(1, 12), 28)
    cards.append([str(i), str(acc), q(rng.choice(BRANDS)), q("%04d" % rng.randint(0, 9999)),
                  q(status), q(exp.isoformat())])
batches("cards", ["card_id", "account_id", "brand", "last4", "status", "expires_at"], cards)

# merchants
merchants = []
for i in range(1, N_MERCHANTS + 1):
    cat = rng.choice(CATEGORIES)
    merchants.append([str(i), q(f"{cat.capitalize()} {rng.choice(LAST)} {i}"), q(cat),
                      q("BR" if rng.random() < 0.9 else rng.choice(["US", "AR", "PT"]))])
batches("merchants", ["merchant_id", "name", "category", "country"], merchants)

# transactions: espaça cada cartão em >= MIN_GAP_S para a base não gerar fraude
per_card = {}
for _ in range(N_TX):
    per_card.setdefault(rng.randint(1, N_CARDS), []).append(None)
window_s = WINDOW_DAYS * 86400
start = REF - dt.timedelta(days=WINDOW_DAYS)
tx = []
for card_id, slots in sorted(per_card.items()):
    k = len(slots)
    span = window_s - k * MIN_GAP_S
    offsets = sorted(rng.randint(0, span) for _ in range(k))
    for j, off in enumerate(offsets):
        when = start + dt.timedelta(seconds=off + j * MIN_GAP_S)
        amount = min(max(rng.lognormvariate(4.2, 0.9), 5.0), 2000.0)
        tx.append((when, card_id, rng.randint(1, N_MERCHANTS), round(amount, 2),
                   "APPROVED" if rng.random() < 0.97 else "DECLINED"))
tx.sort(key=lambda t: (t[0], t[1]))
rows = [[str(i), str(c), str(card_acc[c]), str(m), "%.2f" % a, q("BRL"), q(s), ts(w)]
        for i, (w, c, m, a, s) in enumerate(tx, start=1)]
batches("transactions", ["transaction_id", "card_id", "account_id", "merchant_id", "amount",
                         "currency", "status", "created_at"], rows)

# alinha as sequences ao último id usado (INSERTs futuros não colidem)
for table, pk in [("customers", "customer_id"), ("accounts", "account_id"), ("cards", "card_id"),
                  ("merchants", "merchant_id"), ("transactions", "transaction_id")]:
    print(f"SELECT setval(pg_get_serial_sequence('{table}', '{pk}'), (SELECT max({pk}) FROM {table}));")
print("COMMIT;")
