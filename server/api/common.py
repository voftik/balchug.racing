"""
Общие утилиты каталога Balchug Racing: S3-клиент (Beget), справочники,
парсер имён файлов, схема SQLite. Используется indexer.py и api/app.py.

Конфигурация — через переменные окружения (файл /etc/balchug/secrets.env,
шаблон — secrets.env.example в корне репозитория):
  S3_ENDPOINT   = https://s3.ru1.storage.beget.cloud
  S3_BUCKET     = <имя-бакета>
  S3_ACCESS_KEY = ...
  S3_SECRET_KEY = ...
  CATALOG_DB    = /var/lib/balchug/catalog.db
  ADMIN_TOKEN   = ...
"""
import os, re, json, sqlite3, hashlib, datetime
from functools import lru_cache

import boto3
from botocore.client import Config

HERE = os.path.dirname(os.path.abspath(__file__))

S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "https://s3.ru1.storage.beget.cloud")
S3_BUCKET   = os.environ.get("S3_BUCKET", "")  # обязателен (см. secrets.env.example)
S3_REGION   = os.environ.get("S3_REGION", "ru1")
CATALOG_DB  = os.environ.get("CATALOG_DB", "/var/lib/balchug/catalog.db")

VIDEO_EXT = (".mp4", ".mov", ".m4v")
TELEMETRY_EXT = (".xrk", ".drk", ".xrz", ".rrk", ".bak", ".gpk", ".ld", ".ldx")


def s3():
    if not S3_BUCKET:
        raise RuntimeError("S3_BUCKET не задан — заполните /etc/balchug/secrets.env (см. secrets.env.example)")
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        region_name=S3_REGION,
        aws_access_key_id=os.environ["S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["S3_SECRET_KEY"],
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


@lru_cache(maxsize=1)
def dicts():
    with open(os.path.join(HERE, "dictionaries.json"), encoding="utf-8") as f:
        return json.load(f)


# ---------------- парсер имён ----------------

def parse_item(key, size, mtime):
    """key вида '<category>/<YYYY-MM-DD>/<slug>/<filename>'. Возвращает dict метаданных."""
    d = dicts()
    parts = key.split("/")
    category = parts[0] if parts else ""
    date = ""
    slug = ""
    fname = parts[-1]
    for p in parts:
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", p):
            date = p
            break
    # slug — сегмент после даты
    if date in parts:
        i = parts.index(date)
        if i + 1 < len(parts) - 0 and i + 1 < len(parts):
            cand = parts[i + 1]
            if cand != fname:
                slug = cand
    low = fname.lower()

    # пилот
    pilot = d["pilots_by_slug"].get(slug)
    pilot_code = pilot["code"] if pilot else ""
    pilot_name = pilot["name"] if pilot else ""
    if not pilot_name:
        m = re.match(r"([A-Za-z]{2,4})[_\-]", fname)
        if m:
            code = m.group(1).upper()
            if code in d["pilots_by_code"]:
                pilot_code, pilot_name = code, d["pilots_by_code"][code]
    if not pilot_name:
        pilot_name = "Не определён"

    # трасса
    track = ""
    for tok, name in d["track_tokens"]:
        if tok in low:
            track = name
            break

    # тип сессии
    stype = ""
    for tok, name in d["session_tokens"]:
        if tok in low:
            stype = name
            break
    if not stype:
        stype = "Сессия"

    # машина
    car = ""
    for code, name in d["cars"].items():
        if code.lower() in low:
            car = name
            break

    season = date[:4] if date else ""
    return {
        "category": category, "date": date, "season": season,
        "pilot_slug": slug, "pilot_code": pilot_code, "pilot_name": pilot_name,
        "track_name": track, "session_type": stype, "car": car,
        "filename": fname, "size": size, "mtime": mtime,
    }


def make_id(key):
    return hashlib.sha1(key.encode("utf-8")).hexdigest()[:16]


def build_title(meta):
    bits = [meta["pilot_name"]]
    if meta["track_name"]:
        bits.append(meta["track_name"])
    if meta["date"]:
        bits.append(meta["date"])
    return " · ".join(bits)


# ---------------- SQLite ----------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS items (
  id TEXT PRIMARY KEY,
  source TEXT, category TEXT, date TEXT, season TEXT,
  pilot_slug TEXT, pilot_code TEXT, pilot_name TEXT,
  track_name TEXT, car TEXT, session_type TEXT,
  title TEXT, notes TEXT,
  video_key TEXT, thumb_key TEXT, annotation_key TEXT, hls_key TEXT,
  size INTEGER, duration REAL, best_lap REAL, laps INTEGER,
  related_json TEXT, mtime TEXT, updated_at TEXT,
  edited INTEGER DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_date  ON items(date);
CREATE INDEX IF NOT EXISTS idx_pilot ON items(pilot_name);
CREATE INDEX IF NOT EXISTS idx_track ON items(track_name);
CREATE INDEX IF NOT EXISTS idx_src   ON items(source);
"""


def db():
    os.makedirs(os.path.dirname(CATALOG_DB), exist_ok=True)
    conn = sqlite3.connect(CATALOG_DB)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    return conn


def now_iso():
    return datetime.datetime.utcnow().isoformat()
