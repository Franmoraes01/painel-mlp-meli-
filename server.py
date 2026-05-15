"""
Servidor Local — Dashboard Meli / MLP
======================================
- Roda a query no BigQuery
- Serve os dados para os dois dashboards via GitHub Pages
- Justificativas salvas no JSONBin.io (acessível de qualquer rede)

Dependências:
    pip install flask flask-cors google-cloud-bigquery pandas requests

Uso:
    python server.py --credentials caminho/credentials.json --project SEU_PROJECT_ID

Os dashboards HTML ficam no GitHub Pages e apontam para este servidor local.
"""

import os
import json
import argparse
import threading
import requests
from datetime import date, datetime, timedelta
from pathlib import Path
from flask import Flask, jsonify, request
from flask_cors import CORS

# ─────────────────────────────────────────────
# CONFIGURAÇÕES
# ─────────────────────────────────────────────

CSV_PATH  = r"G:\Drives compartilhados\UTR_OTR\Liberação_de_pacotes\Liberção_de_pacotes.csv"
COL_ID    = "ID"
COL_DATA  = "DATA"

# JSONBin.io
JSONBIN_BIN_ID  = "6a07081ac0954111d82939fe"
JSONBIN_API_KEY = "$2a$10$hZ47nzJ7dwM3NltPWMVaQebMCGIz/E/4w0dq3SUqeySZ9POLqEqvW"
JSONBIN_URL     = f"https://api.jsonbin.io/v3/b/{JSONBIN_BIN_ID}"

CACHE_TTL_SECONDS = 300  # 5 minutos

app = Flask(__name__)
CORS(app)  # permite requisições do GitHub Pages

_cache = {"data": None, "timestamp": None}
_cache_lock = threading.Lock()

BQ_PROJECT     = None
BQ_CREDENTIALS = None

# ─────────────────────────────────────────────
# QUERY BIGQUERY
# ─────────────────────────────────────────────

QUERY_TEMPLATE = """
SELECT
    S.SHP_SHIPMENT_ID AS PEDIDO,
    I.SHP_ITEM_DESC AS DESCRICAO,
    C.CAT_CATEG_NAME_L2 AS CATEGORIA,
    E.TMS_SELLER_INFO_ID AS SELLER,
    S.SHP_ORDER_COST_USD AS VALOR_USD,
    S.SHP_ORDER_COST AS VALOR_REAL,
    R.SHP_LG_DESTINATION_FACILITY_ID AS EXCHANGE_POINT,
    LG.SHP_LG_STATUS AS Status,
    LG.SHP_LG_SUB_STATUS AS Sub_Status,
    LG.SHP_LG_LAST_UPDATED AS Data_Ultima_Movimentacao

FROM `meli-bi-data.WHOWNER.BT_SHP_SHIPMENTS` S, UNNEST(S.ITEMS) I
JOIN `meli-bi-data.WHOWNER.LK_CAT_AG_CATEGORIES_PH` C
    ON C.CAT_CATEG_ID_L7 = I.CAT_CATEG_ID_L7 AND C.sit_site_id = 'MLB' AND C.photo_id = 'TODATE'
LEFT JOIN `meli-bi-data.WHOWNER.BT_TMS_TRACKING` E
    ON E.SHP_SHIPMENT_ID = S.SHP_SHIPMENT_ID
LEFT JOIN `meli-bi-data.WHOWNER.BT_SHP_LG_SHIPMENTS_ROUTES` L
    ON S.SHP_SHIPMENT_ID = L.SHP_SHIPMENT_ID
LEFT JOIN `meli-bi-data.WHOWNER.LK_SHP_LG_ROUTES` R
    ON L.SHP_LG_ROUTE_ID = R.SHP_LG_ROUTE_ID
LEFT JOIN `meli-bi-data.WHOWNER.BT_SHP_LG_SHIPMENTS` LG
    ON S.SHP_SHIPMENT_ID = LG.SHP_SHIPMENT_ID

WHERE S.SHP_SHIPMENT_ID IN ({ids})
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY S.SHP_SHIPMENT_ID
    ORDER BY L.shp_lg_route_init_date DESC
) = 1
"""


def get_ids_from_csv() -> list:
    import pandas as pd
    try:
        df = pd.read_csv(CSV_PATH, dtype={COL_ID: str}, encoding="utf-8-sig")
        df.columns = df.columns.str.strip()
        today     = date.today().strftime("%d/%m/%Y")
        yesterday = (date.today() - timedelta(days=1)).strftime("%d/%m/%Y")
        df["_D"]  = df[COL_DATA].astype(str).str.strip().str.split(" ").str[0]
        ids = df.loc[df["_D"].isin([today, yesterday]), COL_ID].dropna().astype(str).str.strip().tolist()
        return list(set(ids))
    except Exception as e:
        print(f"[ERRO CSV] {e}")
        return []


def run_query(ids: list) -> list:
    from google.cloud import bigquery
    from google.oauth2 import service_account
    if not ids:
        return []
    try:
        if BQ_CREDENTIALS:
            creds  = service_account.Credentials.from_service_account_file(BQ_CREDENTIALS)
            client = bigquery.Client(project=BQ_PROJECT, credentials=creds)
        else:
            client = bigquery.Client(project=BQ_PROJECT)
        rows   = client.query(QUERY_TEMPLATE.format(ids=", ".join(ids))).result()
        result = []
        for row in rows:
            d = dict(row)
            for k, v in d.items():
                if hasattr(v, "isoformat"): d[k] = v.isoformat()
                elif v is None: d[k] = ""
                else: d[k] = str(v) if not isinstance(v, (int, float, bool)) else v
            result.append(d)
        return result
    except Exception as e:
        print(f"[ERRO BQ] {e}")
        return []


def get_cached_data() -> list:
    with _cache_lock:
        now = datetime.now()
        if (_cache["data"] is not None and _cache["timestamp"] is not None and
                (now - _cache["timestamp"]).total_seconds() < CACHE_TTL_SECONDS):
            return _cache["data"]
    ids  = get_ids_from_csv()
    data = run_query(ids)
    with _cache_lock:
        _cache["data"]      = data
        _cache["timestamp"] = datetime.now()
    print(f"[BQ] {len(data)} registros — {datetime.now().strftime('%H:%M:%S')}")
    return data

# ─────────────────────────────────────────────
# JSONBIN — justificativas
# ─────────────────────────────────────────────

HEADERS_READ  = {"X-Master-Key": JSONBIN_API_KEY}
HEADERS_WRITE = {"X-Master-Key": JSONBIN_API_KEY, "Content-Type": "application/json"}


def load_justificativas() -> dict:
    try:
        r = requests.get(JSONBIN_URL + "/latest", headers=HEADERS_READ, timeout=10)
        return r.json().get("record", {}).get("justificativas", {})
    except Exception as e:
        print(f"[ERRO JSONBin GET] {e}")
        return {}


def save_justificativa(pedido: str, texto: str) -> bool:
    try:
        # Lê estado atual
        r    = requests.get(JSONBIN_URL + "/latest", headers=HEADERS_READ, timeout=10)
        data = r.json().get("record", {})
        if "justificativas" not in data:
            data["justificativas"] = {}
        data["justificativas"][pedido] = {
            "texto": texto,
            "atualizado_em": datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        }
        # Salva
        r2 = requests.put(JSONBIN_URL, headers=HEADERS_WRITE,
                          data=json.dumps(data), timeout=10)
        return r2.status_code == 200
    except Exception as e:
        print(f"[ERRO JSONBin PUT] {e}")
        return False

# ─────────────────────────────────────────────
# ROTAS
# ─────────────────────────────────────────────

@app.route("/api/dados")
def api_dados():
    rows  = get_cached_data()
    justs = load_justificativas()
    for row in rows:
        pedido = str(row.get("PEDIDO", ""))
        row["JUSTIFICATIVA"] = justs.get(pedido, {})
    return jsonify({
        "rows": rows,
        "total": len(rows),
        "atualizado_em": _cache["timestamp"].strftime("%d/%m/%Y %H:%M:%S") if _cache["timestamp"] else "—",
    })


@app.route("/api/justificativa", methods=["POST"])
def api_justificativa():
    body   = request.get_json()
    pedido = str(body.get("pedido", "")).strip()
    texto  = str(body.get("justificativa", "")).strip()
    if not pedido:
        return jsonify({"ok": False, "erro": "pedido obrigatório"}), 400
    ok = save_justificativa(pedido, texto)
    return jsonify({"ok": ok})


@app.route("/api/forcar_atualizacao", methods=["POST"])
def api_forcar():
    with _cache_lock:
        _cache["timestamp"] = None
    get_cached_data()
    return jsonify({"ok": True})


@app.route("/api/status")
def api_status():
    ts = _cache["timestamp"]
    return jsonify({
        "ativo": ts is not None,
        "atualizado_em": ts.strftime("%d/%m/%Y %H:%M:%S") if ts else "—",
        "registros": len(_cache["data"]) if _cache["data"] else 0
    })

# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--credentials", default=None)
    parser.add_argument("--project",     default=None)
    parser.add_argument("--port",        type=int, default=5000)
    args = parser.parse_args()

    BQ_PROJECT     = args.project
    BQ_CREDENTIALS = args.credentials

    print("=" * 55)
    print("  Servidor Dashboard Meli/MLP")
    print("=" * 55)
    print(f"  API dados       : http://localhost:{args.port}/api/dados")
    print(f"  JSONBin BIN ID  : {JSONBIN_BIN_ID}")
    print(f"  Cache TTL       : {CACHE_TTL_SECONDS // 60} minutos")
    print("=" * 55)

    print("\n[BQ] Carregando dados iniciais...")
    get_cached_data()

    app.run(host="0.0.0.0", port=args.port, debug=False)
