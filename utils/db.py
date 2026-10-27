import os
import time
from pathlib import Path
from urllib.parse import quote

import psycopg
from dotenv import load_dotenv
from psycopg.conninfo import conninfo_to_dict
from psycopg.rows import dict_row

from utils.logger import CLIENT_IP, get_logger

ENV_FILE = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(ENV_FILE, override=True)

logger = get_logger()


def get_connection():
    started_at = time.monotonic()
    database_url = os.getenv("DATABASE_URL")
    source = "DATABASE_URL" if database_url else "DB_* ortam değişkenleri"
    params = {}
    try:
        if database_url:
            params = conninfo_to_dict(database_url)
            return psycopg.connect(database_url, row_factory=dict_row)

        params = {
            "host": os.getenv("DB_HOST"),
            "port": os.getenv("DB_PORT", "5432"),
            "dbname": os.getenv("DB_NAME"),
            "user": os.getenv("DB_USER"),
            "password": os.getenv("DB_PASSWORD"),
            "sslmode": os.getenv("DB_SSLMODE", "prefer"),
        }
        return psycopg.connect(**params, row_factory=dict_row)
    except Exception as exc:
        def redact(value):
            text = str(value)
            secrets = [database_url, params.get("password"), os.getenv("DB_PASSWORD"), os.getenv("PGPASSWORD")]
            for secret in sorted({s for s in secrets if s}, key=len, reverse=True):
                text = text.replace(secret, "***").replace(quote(secret, safe=""), "***")
            return text

        def setting(name, pg_name, default="(libpq varsayılanı)"):
            return params.get(name) or os.getenv(pg_name) or default

        lines = [
            "Error connecting to PostgreSQL:",
            f"  Ayar kaynağı: {source}",
            f"  .env dosyası: {ENV_FILE} (mevcut: {ENV_FILE.is_file()})",
            f"  Sunucu: {setting('host', 'PGHOST')}",
            f"  Port: {setting('port', 'PGPORT', '5432')}",
            f"  Veritabanı: {setting('dbname', 'PGDATABASE')}",
            f"  Kullanıcı: {setting('user', 'PGUSER')}",
            f"  SSL modu: {setting('sslmode', 'PGSSLMODE', 'prefer')}",
            f"  Geçen süre: {time.monotonic() - started_at:.2f} saniye",
            f"  Hata türü: {type(exc).__name__}",
            f"  Hata ayrıntısı: {exc}",
        ]
        if getattr(exc, "sqlstate", None):
            lines.append(f"  SQLSTATE: {exc.sqlstate}")
        diag = getattr(exc, "diag", None)
        for attr, label in (("message_detail", "Sunucu ayrıntısı"), ("message_hint", "Sunucu önerisi")):
            value = getattr(diag, attr, None)
            if value:
                lines.append(f"  {label}: {value}")
        message = redact("\n".join(lines))
        logger.error(message, extra={"clientip": CLIENT_IP})
        print(message, flush=True)
        return None


def insert_modbus_log(conn, message, client_ip, site_id=None, ekk_device_id=None, error_detail=None, traceback_text=None):
    try:
        cur = conn.cursor()
        sql = """
            INSERT INTO public.ekk_device_poll_log (
                ekk_device_id,
                site_id,
                poll_started_at,
                poll_finished_at,
                level,
                status,
                message,
                error_detail,
                traceback
            )
            VALUES (%s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, %s, %s, %s, %s, %s)
        """
        cur.execute(
            sql,
            (
                ekk_device_id,
                site_id if site_id is not None else 0,
                "ERROR",
                "FAILED",
                f"[{client_ip}] {message}",
                error_detail,
                traceback_text,
            ),
        )
        conn.commit()
        cur.close()
    except Exception as exc:
        logger.error(f"Error inserting modbus log: {exc}", extra={"clientip": client_ip})
        print(f"Error inserting modbus log: {exc}")
