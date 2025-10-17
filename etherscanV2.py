# etherscanV2.py
# Genera un CSV grande (>= min_rows) desde Ethereum (Etherscan API V2),
# unificando transacciones normales, internas y transferencias de tokens.

import os
import csv
import time
import argparse
import requests
from datetime import datetime, timezone

API_KEY = os.getenv("ETHERSCAN_API_KEY")
BASE_URL = "https://api.etherscan.io/v2/api"  # API V2

# -------------------- Utilidades --------------------
def need_api():
    if not API_KEY:
        raise RuntimeError("ETHERSCAN_API_KEY no esta definida en el entorno.")

def to_iso_utc(ts: str) -> str:
    try:
        return datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat()
    except Exception:
        return ""

def human_eth(wei: str) -> float:
    try:
        return int(wei) / 1e18
    except Exception:
        return 0.0

def fee_eth(gas_price_wei: str, gas_used: str) -> float:
    try:
        return (int(gas_price_wei) * int(gas_used)) / 1e18
    except Exception:
        return 0.0

def etherscan_get(params: dict, max_retries: int = 6, backoff: float = 0.8):
    """Llama a Etherscan con reintentos simples si hay rate limit/errores temporales."""
    need_api()
    p = dict(params)
    p["apikey"] = API_KEY
    last = None
    for attempt in range(max_retries):
        r = requests.get(BASE_URL, params=p, timeout=45)
        try:
            data = r.json()
        except Exception:
            data = {"status": "0", "message": "NOTOK", "result": f"Invalid JSON (HTTP {r.status_code})"}
        last = data
        status = str(data.get("status", "0"))
        message = str(data.get("message", "")).upper()
        result = str(data.get("result", ""))

        if status == "1" and message == "OK":
            return data

        # Manejo de rate limit
        if "MAX RATE LIMIT" in result.upper() or "RATE LIMIT" in message:
            time.sleep(backoff * (attempt + 1))
            continue

        # Otros NOTOK: devolvemos
        return data
    return last or {"status": "0", "message": "NOTOK", "result": "Retries exhausted"}

def paged_fetch(chainid: int, module: str, action: str, paging_params: dict,
                offset: int = 10000, sort: str = "desc"):
    """
    Generador de paginas. Etherscan limita ~10k por pagina.
    """
    page = 1
    while True:
        params = {
            "chainid": chainid,
            "module": module,
            "action": action,
            "page": page,
            "offset": offset,
            "sort": sort,
            **paging_params
        }
        data = etherscan_get(params)
        result = data.get("result", [])
        if isinstance(result, str) or not result:
            break
        yield result
        if len(result) < offset:
            break
        page += 1
        time.sleep(0.2)

# -------------------- Fetchers --------------------
def fetch_normal_tx(chainid, address, startblock, endblock):
    return paged_fetch(chainid, "account", "txlist",
                       {"address": address, "startblock": startblock, "endblock": endblock},
                       offset=10000, sort="desc")

def fetch_internal_tx(chainid, address, startblock, endblock):
    return paged_fetch(chainid, "account", "txlistinternal",
                       {"address": address, "startblock": startblock, "endblock": endblock},
                       offset=10000, sort="desc")

def fetch_erc20(chainid, address, startblock, endblock):
    return paged_fetch(chainid, "account", "tokentx",
                       {"address": address, "startblock": startblock, "endblock": endblock},
                       offset=10000, sort="desc")

# -------------------- CSV schema --------------------
FIELDS = [
    "source",              # normal | internal | erc20
    "address_queried",     # direccion consultada
    "timeStamp_iso",
    "hash",
    "logIndex",            # para erc20 (dedupe)
    "from",
    "to",
    "value_eth",           # normal/internal
    "fee_eth",             # normal
    "tokenSymbol",         # erc20
    "tokenName",           # erc20
    "tokenDecimal",        # erc20
    "value_adjusted"       # erc20 (monto ajustado por decimales)
]

def write_rows(fp, rows):
    w = csv.DictWriter(fp, fieldnames=FIELDS)
    if fp.tell() == 0:
        w.writeheader()
    for r in rows:
        w.writerow(r)

def normalize_normal(address_queried, items):
    for tx in items:
        yield {
            "source": "normal",
            "address_queried": address_queried,
            "timeStamp_iso": to_iso_utc(tx.get("timeStamp","")),
            "hash": tx.get("hash",""),
            "logIndex": "",
            "from": tx.get("from",""),
            "to": tx.get("to",""),
            "value_eth": human_eth(tx.get("value","0")),
            "fee_eth": fee_eth(tx.get("gasPrice","0"), tx.get("gasUsed","0")),
            "tokenSymbol": "",
            "tokenName": "",
            "tokenDecimal": "",
            "value_adjusted": ""
        }

def normalize_internal(address_queried, items):
    for tx in items:
        yield {
            "source": "internal",
            "address_queried": address_queried,
            "timeStamp_iso": to_iso_utc(tx.get("timeStamp","")),
            "hash": tx.get("hash",""),
            "logIndex": "",
            "from": tx.get("from",""),
            "to": tx.get("to",""),
            "value_eth": human_eth(tx.get("value","0")),
            "fee_eth": "",
            "tokenSymbol": "",
            "tokenName": "",
            "tokenDecimal": "",
            "value_adjusted": ""
        }

def normalize_erc20(address_queried, items):
    for tx in items:
        try:
            decimals = int(tx.get("tokenDecimal","0") or 0)
            adjusted = int(tx.get("value","0")) / (10 ** decimals) if decimals >= 0 else 0
        except Exception:
            adjusted = 0
        yield {
            "source": "erc20",
            "address_queried": address_queried,
            "timeStamp_iso": to_iso_utc(tx.get("timeStamp","")),
            "hash": tx.get("hash",""),
            "logIndex": tx.get("logIndex",""),
            "from": tx.get("from",""),
            "to": tx.get("to",""),
            "value_eth": "",
            "fee_eth": "",
            "tokenSymbol": tx.get("tokenSymbol",""),
            "tokenName": tx.get("tokenName",""),
            "tokenDecimal": tx.get("tokenDecimal",""),
            "value_adjusted": adjusted
        }

# -------------------- Recoleccion con CHUNKING --------------------
def collect_to_csv(chainid, addresses, out_csv, min_rows,
                   startblock=0, endblock=99999999,
                   include_normal=True, include_internal=True, include_erc20=True,
                   chunk_blocks=200_000):
    """
    Recorre el rango [startblock, endblock] en 'ventanas' de tamaño chunk_blocks
    para evitar el limite de ~10k por consulta. Avanza de bloques altos a bajos.
    """
    total = 0
    seen_erc20 = set()  # dedupe por (hash,logIndex)

    hi = endblock
    lo = max(startblock, endblock - chunk_blocks + 1)

    with open(out_csv, "w", newline="", encoding="utf-8") as fp:
        while hi >= startblock and total < min_rows:
            print(f"\nChunk bloques: {lo}..{hi}")
            for addr in addresses:
                if include_normal:
                    for page in fetch_normal_tx(chainid, addr, lo, hi):
                        rows = list(normalize_normal(addr, page))
                        write_rows(fp, rows)
                        total += len(rows)
                        print(f"[{addr[:10]}...] normal +{len(rows)} -> {total}")
                        if total >= min_rows: return total

                if include_internal:
                    for page in fetch_internal_tx(chainid, addr, lo, hi):
                        rows = list(normalize_internal(addr, page))
                        write_rows(fp, rows)
                        total += len(rows)
                        print(f"[{addr[:10]}...] internal +{len(rows)} -> {total}")
                        if total >= min_rows: return total

                if include_erc20:
                    for page in fetch_erc20(chainid, addr, lo, hi):
                        deduped = []
                        for tx in page:
                            key = (tx.get("hash",""), tx.get("logIndex",""))
                            if key in seen_erc20:
                                continue
                            seen_erc20.add(key)
                            deduped.append(tx)
                        rows = list(normalize_erc20(addr, deduped))
                        write_rows(fp, rows)
                        total += len(rows)
                        print(f"[{addr[:10]}...] erc20 +{len(rows)} -> {total}")
                        if total >= min_rows: return total

            # mover la ventana hacia bloques mas antiguos
            hi = lo - 1
            lo = max(startblock, hi - chunk_blocks + 1)

    return total

# -------------------- CLI --------------------
def parse_addresses(raw: str):
    """
    Admite lista separada por comas o @ruta.txt (una address por linea).
    En PowerShell usa comillas: --addresses "@addrs.txt"
    """
    raw = raw.strip()
    if raw.startswith("@"):
        path = raw[1:]
        with open(path, "r", encoding="utf-8") as f:
            addrs = [ln.strip() for ln in f if ln.strip()]
        return addrs
    return [a.strip() for a in raw.split(",") if a.strip()]

def main():
    ap = argparse.ArgumentParser(description="Genera un CSV grande (>= min_rows) desde Etherscan V2.")
    ap.add_argument("--chainid", type=int, default=1, help="Chain ID (1 = Ethereum mainnet).")
    ap.add_argument("--addresses", required=True,
                    help='Lista separada por comas o @archivo.txt (PowerShell: --addresses "@addrs.txt").')
    ap.add_argument("--min_rows", type=int, default=100000, help="Minimo de filas a recolectar.")
    ap.add_argument("--out", default="eth_100k.csv", help="Ruta del CSV de salida.")
    ap.add_argument("--startblock", type=int, default=0)
    ap.add_argument("--endblock", type=int, default=99999999)
    ap.add_argument("--only", choices=["all","normal","internal","erc20"], default="all",
                    help="Fuentes a incluir (all por defecto).")
    ap.add_argument("--chunk_blocks", type=int, default=200000,
                    help="Tamaño del rango de bloques por iteracion (chunk).")
    args = ap.parse_args()

    include_normal = include_internal = include_erc20 = True
    if args.only != "all":
        include_normal = args.only == "normal"
        include_internal = args.only == "internal"
        include_erc20 = args.only == "erc20"

    addrs = parse_addresses(args.addresses)
    print(f"ChainID={args.chainid} | Direcciones={len(addrs)} | min_rows={args.min_rows}")

    total = collect_to_csv(
        chainid=args.chainid,
        addresses=addrs,
        out_csv=args.out,
        min_rows=args.min_rows,
        startblock=args.startblock,
        endblock=args.endblock,
        include_normal=include_normal,
        include_internal=include_internal,
        include_erc20=include_erc20,
        chunk_blocks=args.chunk_blocks
    )
    print(f"\nListo. Filas escritas: {total}  -> {args.out}")

if __name__ == "__main__":
    main()
