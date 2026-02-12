from __future__ import annotations
import os, json, datetime, pathlib, sys
from typing import Any, List, Union
from dotenv import load_dotenv
import pyodbc
from pathlib import Path
from decimal import Decimal, InvalidOperation, localcontext, ROUND_HALF_UP

# SDK oficial
from k3cloud_webapi_sdk.main import K3CloudApiSdk

load_dotenv()

# ===================== Config SQL =====================
SQL_SERVER   = os.getenv("SQL_SERVER",   "localhost")
SQL_DB       = os.getenv("SQL_DB",       "YourDB")
SQL_USER     = os.getenv("SQL_USER",     "sa")
SQL_PASSWORD = os.getenv("SQL_PASSWORD", "P@ssw0rd!")
ODBC_DRIVER  = os.getenv("ODBC_DRIVER",  "ODBC Driver 18 for SQL Server")
SP_TO_CALL   = os.getenv("SP_TO_CALL",   "dbo.spUpdateProductsFromJsonFile")  # tu SP
SP_PARAM     = os.getenv("SP_PARAM",     "@path")                         # nombre del parámetro
SP_PARAM_ORG = os.getenv("SP_PARAM_ORG", "@branchId")                          # parámetro org_id

# ===================== Config K3 ======================
K3_SERVER_URL  = os.getenv("K3_SERVER_URL", "http://127.0.0.1:8090/K3Cloud/")
K3_ACCTID      = os.getenv("K3_ACCTID",     "")
K3_USERNAME    = os.getenv("K3_USERNAME",   "")
K3_APPID       = os.getenv("K3_APPID",      "")
K3_APPSEC      = os.getenv("K3_APPSEC",     "")
K3_LCID        = int(os.getenv("K3_LCID",   "2052"))
K3_ORGNUM      = os.getenv("K3_ORGNUM",     "100")
K3_CONFIG_PATH = os.getenv("K3_CONFIG_PATH", "").strip()
K3_CONFIG_NODE = os.getenv("K3_CONFIG_NODE", "config").strip()

# ===================== Query params ===================
K3_USE_ORG_ID  = os.getenv("K3_USE_ORG_ID", "1148519")
K3_USE_ORG_IDS = os.getenv("K3_USE_ORG_IDS", "").strip()
FORM_ID        = os.getenv("FORM_ID", "BD_MATERIAL")
FIELD_KEYS     = os.getenv(
    "FIELD_KEYS",
    "FMATERIALID,FNUMBER,FNAME,F_BOX_VOLUME,F_price_effect_num,F_TQOY_Price_9s2"
)
ORDER_STRING   = os.getenv("ORDER_STRING", "FNUMBER ASC")
TOP_ROW_COUNT  = int(os.getenv("TOP_ROW_COUNT", "0"))
START_ROW      = int(os.getenv("START_ROW", "0"))
LIMIT          = int(os.getenv("LIMIT", "5000"))  # <= 10000 por página
SUBSYSTEM_ID   = os.getenv("SUBSYSTEM_ID", "")
EXTRA_FILTERS_JSON = os.getenv("EXTRA_FILTERS_JSON", "").strip()

# ===================== Dump / archivo =================
OUT_DIR       = pathlib.Path(os.getenv("OUT_DIR", r"C:\devs_python\k3_dumps\Materials"))
FILE_PREFIX   = os.getenv("FILE_PREFIX", "k3_bd_material")
DRY_RUN       = os.getenv("DRY_RUN", "0") in ("1", "true", "True", "YES", "yes")
DUMP_AS_DICTS = os.getenv("DUMP_AS_DICTS", "1") in ("1","true","True","YES","yes")
PRETTY_JSON   = os.getenv("PRETTY_JSON", "0") in ("1","true","True","YES","yes")

# Normalización a string decimal fijo (activar/desactivar)
NORMALIZE_NUMERIC_STR = os.getenv("NORMALIZE_NUMERIC_STR", "1") in ("1","true","True","YES","yes")

# Decimales por campo (configurables por .env)
PLACES_PRICE        = int(os.getenv("PLACES_PRICE",       "2"))  # F_TQOY_Price_9s2
PLACES_PIEZASXCAJA  = int(os.getenv("PLACES_PIEZASXCAJA", "3"))  # F_price_effect_num
PLACES_BOX_VOLUME   = int(os.getenv("PLACES_BOX_VOLUME",  "6"))  # F_BOX_VOLUME

# ===================== Constantes =====================
MAX_LIMIT = 10000  # ExecuteBillQuery no permite > 10000

# ===================== Utilidades =====================
def sql_connect() -> pyodbc.Connection:
    conn_str = (
        f"DRIVER={{{ODBC_DRIVER}}};SERVER={SQL_SERVER};DATABASE={SQL_DB};"
        f"UID={SQL_USER};PWD={SQL_PASSWORD};TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str, autocommit=False)

def _normalize_server_url(url: str) -> str:
    # Evita doble "//K3Cloud/"
    return url.replace("//K3Cloud/", "/K3Cloud/")

def k3_client() -> K3CloudApiSdk:
    sdk = K3CloudApiSdk(server_url=K3_SERVER_URL)
    if K3_CONFIG_PATH:
        conf_path = Path(os.path.expandvars(os.path.expanduser(K3_CONFIG_PATH))).resolve()
        if conf_path.exists():
            sdk.Init(config_path=str(conf_path), config_node=K3_CONFIG_NODE)
            return sdk
        else:
            raise FileNotFoundError(f"conf.ini no encontrado: {conf_path}")

    # Fallback: usar InitConfig con variables de entorno
    sdk.InitConfig(
        acct_id=K3_ACCTID,
        user_name=K3_USERNAME,
        app_id=K3_APPID,
        app_secret=K3_APPSEC,
        server_url=K3_SERVER_URL,
        lcid=K3_LCID,
        org_num=K3_ORGNUM,
    )
    return sdk

def _clamp_per_page(n: int) -> int:
    if not n or n <= 0:
        return 5000
    return min(n, MAX_LIMIT)

def _extra_filters() -> List[dict]:
    if not EXTRA_FILTERS_JSON:
        return []
    try:
        extra = json.loads(EXTRA_FILTERS_JSON)
        return extra if isinstance(extra, list) else []
    except Exception:
        print("[WARN] EXTRA_FILTERS_JSON no es JSON válido; ignorando.")
        return []

def build_filters_for_org(org_id: str) -> List[dict]:
    base = [{
        "Left": "(",
        "FieldName": "FUseOrgId",
        "Compare": "67",             # '='
        "Value": str(org_id),
        "Right": ")",
        "Logic": 0                   # AND
    }]
    base.extend(_extra_filters())
    return base

def build_execute_payload(org_id: str, offset: int, limit: int) -> dict:
    return {
        "FormId": FORM_ID,
        "FieldKeys": FIELD_KEYS,
        "FilterString": build_filters_for_org(org_id),
        "OrderString": ORDER_STRING,
        "TopRowCount": TOP_ROW_COUNT,
        "StartRow": offset,
        "Limit": limit,              # <= 10000
        "SubSystemId": SUBSYSTEM_ID
    }

def rows_to_dicts(rows: List[Any], field_keys: str) -> List[dict]:
    """
    Convierte [["v1","v2",...], ...] a [{key1:v1, key2:v2, ...}, ...]
    usando el orden de FIELD_KEYS. Si faltan valores, rellena con None.
    Si la fila ya viene como dict, la deja tal cual.
    """
    keys = [k.strip() for k in field_keys.split(",") if k.strip()]
    out: List[dict] = []
    for r in rows:
        if isinstance(r, dict):
            out.append(r)
            continue
        if not isinstance(r, (list, tuple)):
            # ignora filas raras
            continue
        d = {}
        m = min(len(r), len(keys))
        for i in range(m):
            d[keys[i]] = r[i]
        for i in range(m, len(keys)):
            d[keys[i]] = None
        out.append(d)
    return out

def safe_execute_bill_query(sdk: K3CloudApiSdk, para: dict) -> Any:
    """
    El SDK a veces acepta dict y a veces JSON string.
    Probamos dict y si falla, probamos string.
    """
    try:
        return sdk.ExecuteBillQuery(para)
    except Exception:
        return sdk.ExecuteBillQuery(json.dumps(para, ensure_ascii=False))

def page_all_for_org(sdk: K3CloudApiSdk, org_id: str) -> List[Any]:
    all_rows: List[Any] = []
    offset = START_ROW
    per_page = _clamp_per_page(LIMIT)

    while True:
        payload = build_execute_payload(org_id, offset, per_page)
        raw = safe_execute_bill_query(sdk, payload)
        try:
            data: Union[list, dict, str] = json.loads(raw) if isinstance(raw, str) else raw
        except Exception:
            data = raw

        if not data:
            break

        if isinstance(data, list):
            batch_count = len(data)
            all_rows.extend(data)
            print(f"[PAGE org={org_id}] StartRow={offset} +{batch_count} filas")
            if batch_count < per_page:
                break
            offset += batch_count
        else:
            all_rows.append(data)
            print(f"[PAGE org={org_id}] StartRow={offset} objeto recibido (no lista), se detiene paginación.")
            break

    print(f"[PAGE org={org_id}] Total filas acumuladas: {len(all_rows)}")
    return all_rows

# =============== Normalización numérica a string =======
def _fmt_decimal_str(value, places: int) -> str | None:
    """Convierte número (incluye '6.125e-05') a string decimal fijo sin notación científica."""
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    try:
        with localcontext() as ctx:
            ctx.prec = 34  # alta precisión
            d = Decimal(s)  # entiende notación científica
            step = Decimal(1).scaleb(-places)  # 10^-places
            d = d.quantize(step, rounding=ROUND_HALF_UP)
            return format(d, f".{places}f")    # fuerza decimal plano
    except InvalidOperation:
        # Si viene basura, lo devolvemos tal cual; lo capturas en SQL con TRY_CAST
        return s

_DECIMAL_FORMATS = {
    "F_TQOY_Price_9s2":   PLACES_PRICE,
    "F_price_effect_num": PLACES_PIEZASXCAJA,
    "F_BOX_VOLUME":       PLACES_BOX_VOLUME,
    "FNETWEIGHT":           PLACES_BOX_VOLUME,
    "FGROSSWEIGHT":         PLACES_BOX_VOLUME,
}

def _normalize_numeric_fields(row: dict) -> dict:
    """Devuelve el dict con campos numéricos formateados como string decimal fijo."""
    if not NORMALIZE_NUMERIC_STR:
        return row
    for k, places in _DECIMAL_FORMATS.items():
        if k in row and row[k] is not None:
            row[k] = _fmt_decimal_str(row[k], places)
    return row

# ===================== Dump / JSON =====================
def dump_json(obj: Any, org_id: str) -> pathlib.Path:
    # Guardar en subcarpeta por org para orden
    out_dir = OUT_DIR / f"org_{org_id}"
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    outfile = out_dir / f"{FILE_PREFIX}_{org_id}_{ts}.json"
    with outfile.open("w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=(2 if PRETTY_JSON else None))
    print(f"[DUMP org={org_id}] {outfile}")
    return outfile

# ===================== Ejecutar SP =====================
def exec_sp_with_path(json_path: pathlib.Path, org_id: str) -> None:
    with sql_connect() as con:
        with con.cursor() as cur:
            cur.execute(f"EXEC {SP_TO_CALL} {SP_PARAM} = ?, {SP_PARAM_ORG} = ?", str(json_path), org_id)
        con.commit()
    print(f"[SQL] Ejecutado {SP_TO_CALL} {SP_PARAM}='{json_path}', {SP_PARAM_ORG}='{org_id}'")
    
    # Eliminar archivo JSON después de SP exitoso
    try:
        json_path.unlink()
        print(f"[CLEANUP] Archivo eliminado: {json_path}")
    except Exception as e:
        print(f"[WARN] No se pudo eliminar {json_path}: {e}")

def _parse_org_ids() -> List[str]:
    """
    Si hay K3_USE_ORG_IDS (coma-separado), úsalo.
    Si no, usa K3_USE_ORG_ID único.
    """
    if K3_USE_ORG_IDS:
        parts = [p.strip() for p in K3_USE_ORG_IDS.split(",")]
        return [p for p in parts if p]
    return [str(K3_USE_ORG_ID).strip()]

# ===================== Main ===========================
def main() -> int:
    sdk = k3_client()
    org_ids = _parse_org_ids()

    for org_id in org_ids:
        try:
            rows = page_all_for_org(sdk, org_id)
            rows_for_dump = rows_to_dicts(rows, FIELD_KEYS) if DUMP_AS_DICTS else rows

            # Normaliza campos numéricos a string decimal fijo (sin 'e-05')
            if isinstance(rows_for_dump, list):
                rows_for_dump = [_normalize_numeric_fields(dict(r)) for r in rows_for_dump]
            elif isinstance(rows_for_dump, dict):
                rows_for_dump = _normalize_numeric_fields(rows_for_dump)

            outfile = dump_json(rows_for_dump, org_id)

            if DRY_RUN:
                print(f"[INFO org={org_id}] DRY_RUN=1: no se llama al SP.")
                continue

            exec_sp_with_path(outfile, org_id)

        except Exception as e:
            print(f"[ERR org={org_id}] {e}")
            # sigue con la siguiente org sin abortar todo el proceso

    print("[DONE] Proceso completo (todas las orgs).")
    return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        print(f"[ERR] {e}")
        sys.exit(1)
