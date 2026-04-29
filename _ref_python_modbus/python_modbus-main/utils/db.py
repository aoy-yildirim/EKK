import psycopg2
import os
from dotenv import load_dotenv
from .logger import get_logger, CLIENT_IP

load_dotenv()

DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

logger = get_logger()

def get_connection():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        return conn
    except Exception as e:
        logger.error(f'Error connecting to PostgreSQL: {e}', extra={'clientip': CLIENT_IP})
        print(f'Error connecting to PostgreSQL: {e}')
        # Try to log to modbus_log table if possible
        try:
            # Attempt a minimal connection using only host/port/dbname (no user/pass)
            fallback_conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD
            )
            insert_modbus_log(fallback_conn, f'Error connecting to PostgreSQL: {e}', CLIENT_IP)
            fallback_conn.close()
        except Exception:
            pass
        return None

def insert_modbus_log(conn, message, client_ip):
    try:
        cur = conn.cursor()
        sql = "INSERT INTO modbus_log (message, client_ip) VALUES (%s, %s)"
        cur.execute(sql, (message, client_ip))
        conn.commit()
        cur.close()
    except Exception as e:
        logger = get_logger()
        logger.error(f'Error inserting modbus log: {e}', extra={'clientip': client_ip})
        print(f'Error inserting modbus log: {e}')
