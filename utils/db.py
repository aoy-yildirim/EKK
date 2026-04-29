import os

import psycopg
from dotenv import load_dotenv
from psycopg.rows import dict_row

from utils.logger import CLIENT_IP, get_logger

load_dotenv()

logger = get_logger()


def get_connection():
    try:
        database_url = os.getenv("DATABASE_URL")
        if database_url:
            return psycopg.connect(database_url, row_factory=dict_row)

        return psycopg.connect(
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT", "5432"),
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            sslmode=os.getenv("DB_SSLMODE", "prefer"),
            row_factory=dict_row,
        )
    except Exception as exc:
        logger.error(f"Error connecting to PostgreSQL: {exc}", extra={"clientip": CLIENT_IP})
        print(f"Error connecting to PostgreSQL: {exc}")
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
