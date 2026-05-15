"""
scheduler_action.py — Versão para GitHub Actions
==================================================
Roda UMA VEZ (sem loop), consulta o BigQuery com os IDs
do CSV do dia atual/anterior e salva no JSONBin.

O GitHub Actions agenda a execução a cada 1 minuto via cron.

Dependências:
    pip install google-cloud-bigquery pandas requests google-auth
"""

import json
import argparse
import requests
import pandas as pd
from datetime import date, datetime, timedelta

# ─────────────────────────────────────────────
# CONFIGURAÇÕES
# ─────────────────────────────────────────────

CSV_URL = "https://drive.google.com/uc?export=download&id=1--x4nSTe-9T2I1WqoPdZR3-Rc3YR0dz4"

# Fallback: caminho local se rodar localmente
CSV_PATH_LOCAL = r"G:\Drives compartilhados\UTR_OTR\Liberação_de_pacotes\Liberção_de_pacotes.csv"

COL_ID   = "ID"
COL_DATA = "DATA"

JSONBIN_BIN_ID  = "6a07081ac0954111d82939fe"
JSONBIN_API_KEY = "$2a$10$hZ47nzJ7dwM3NltPWMVaQebMCGIz/E/4w0dq3SUqeySZ9POLqEqvW"
JSONBIN_URL     = f"https://api.jsonbin.io/v3/b/{JSONBIN_BIN_ID}"

HEADERS_READ  = {"X-Master-Key": JSONBIN_API_KEY}
HEADERS_WRITE = {"X-Master-Key": JSONBIN_API_KEY, "Content-Type": "application/json"}

# ─────────────────────────────────────────────
# QUERY
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

# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def log(msg):
    print(f"[{datetime.now().strftime('%d/%m/%Y %H:%M:%S')}] {msg}")


def get_ids_from_csv(project: str) -> list:
    """Tenta ler o CSV do GitHub primeiro, depois local."""
    try:
        # Tenta buscar do GitHub (onde o CSV será publicado)
        r = requests.get(CSV_URL, timeout=15)
        if r.status_code == 200:
            from io import StringIO
            df = pd.read_csv(StringIO(r.text), dtype={COL_ID: str})
            log("✅ CSV carregado do GitHub.")
        else:
            raise Exception(f"HTTP {r.status_code}")
    except Exception as e:
        log(f"⚠  Não foi possível carregar CSV do GitHub: {e}")
        log("   Tentando caminho local...")
        try:
            df = pd.read_csv(CSV_PATH_LOCAL, dtype={COL_ID: str}, encoding="utf-8-sig")
            log("✅ CSV carregado localmente.")
        except Exception as e2:
            log(f"❌ Falha ao carregar CSV: {e2}")
            return []

    df.columns = df.columns.str.strip()
    today     = date.today().strftime("%d/%m/%Y")
    yesterday = (date.today() - timedelta(days=1)).strftime("%d/%m/%Y")
    df["_D"]  = df[COL_DATA].astype(str).str.strip().str.split(" ").str[0]
    ids = df.loc[df["_D"].isin([today, yesterday]), COL_ID].dropna().astype(str).str.strip().tolist()
    return list(set(ids))


def run_query(ids: list, project: str) -> list:
    from google.cloud import bigquery
    if not ids:
        return []
    try:
        client = bigquery.Client(project=project)
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
        log(f"❌ Erro BigQuery: {e}")
        return []


def load_jsonbin() -> dict:
    try:
        r = requests.get(JSONBIN_URL + "/latest", headers=HEADERS_READ, timeout=15)
        return r.json().get("record", {})
    except Exception as e:
        log(f"⚠  Erro JSONBin GET: {e}")
        return {}


def save_jsonbin(data: dict) -> bool:
    try:
        r = requests.put(JSONBIN_URL, headers=HEADERS_WRITE,
                         data=json.dumps(data, ensure_ascii=False), timeout=15)
        return r.status_code == 200
    except Exception as e:
        log(f"❌ Erro JSONBin PUT: {e}")
        return False


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", default="meli-bi-data")
    args = parser.parse_args()

    log("🚀 Iniciando scheduler (rodada única)...")

    # 1. Busca IDs do CSV
    ids = get_ids_from_csv(args.project)
    if not ids:
        log("⚠  Nenhum ID encontrado para hoje/ontem. Encerrando.")
        return
    log(f"   {len(ids)} ID(s) encontrados.")

    # 2. Consulta BigQuery
    log("🔍 Consultando BigQuery...")
    rows = run_query(ids, args.project)
    if not rows:
        log("⚠  Query retornou 0 registros. Encerrando.")
        return
    log(f"   {len(rows)} registro(s) retornados.")

    # 3. Preserva justificativas existentes
    current        = load_jsonbin()
    justificativas = current.get("justificativas", {})

    # 4. Salva no JSONBin
    novo = {
        "dados": rows,
        "justificativas": justificativas,
        "atualizado_em": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
        "total": len(rows)
    }
    ok = save_jsonbin(novo)
    log(f"{'✅ JSONBin atualizado' if ok else '❌ Falha ao salvar'} — {len(rows)} pacotes.")


if __name__ == "__main__":
    main()
