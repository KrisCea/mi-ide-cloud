import os
import sys


def main():
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("[check_env] No DATABASE_URL set. Ejecutando comprobación mínima: OK.")
        return 0

    try:
        import psycopg2
    except ImportError:
        print("[check_env] psycopg2 no está instalado.")
        return 1

    print("[check_env] DATABASE_URL detectado, intentando conexión...")
    try:
        conn = psycopg2.connect(db_url)
        conn.close()
        print("[check_env] Conexión exitosa.")
        return 0
    except Exception as e:
        print(f"[check_env] Error al conectar: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
