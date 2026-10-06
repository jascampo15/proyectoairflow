from datetime import datetime, timedelta
from sqlalchemy import text
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

doc_md_gold = """
### DAG 5: Consolidación Capa Gold (`usos_gold`)

**Propósito:** Consolida los datos transaccionales de `usos_silver` cruzados con la dimensión de catálogo `lines_silver`.
**Estrategia:** Recálculo idempotente *Delete-by-date + Insert* por bloques de fechas afectadas.
**Orquestación:** Cierre de pipeline, activado por `dag_transformacion_usos`.
"""

def cargar_usos_gold():
    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    engine = pg_hook.get_sqlalchemy_engine()

    with engine.begin() as conn:
        # 1. Crear la tabla de control ETL si no existe
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS etl_control_gold (
                proceso VARCHAR(50) PRIMARY KEY,
                ultimo_id_txn BIGINT
            );
        """))

        # 2. Crear la tabla usos_gold si no existe para evitar el error 'UndefinedTable'
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS usos_gold (
                fecha DATE,
                hora INTEGER,
                id_linea BIGINT,
                name_linea VARCHAR(255),
                integracion BIGINT,
                veh_id BIGINT,
                sn_terminal VARCHAR(100),
                valor BIGINT,
                total_trx BIGINT
            );
        """))

        resultado = conn.execute(text(
            "SELECT ultimo_id_txn FROM etl_control_gold WHERE proceso = 'usos_gold';"
        )).scalar()
        ultimo_id = int(resultado) if resultado is not None else 0

        nuevo_max_id = conn.execute(text(
            "SELECT MAX(CAST(id_txn AS BIGINT)) FROM usos_silver;"
        )).scalar()

        if not nuevo_max_id or nuevo_max_id <= ultimo_id:
            print("✅ No hay transacciones nuevas en usos_silver para consolidar en Gold.")
            return

        fechas_query = text("""
            SELECT DISTINCT CAST(date_time AS DATE)
            FROM usos_silver
            WHERE CAST(id_txn AS BIGINT) > :ultimo_id
              AND CAST(id_txn AS BIGINT) <= :nuevo_max_id
              AND date_time IS NOT NULL;
        """)
        result = conn.execute(fechas_query, {"ultimo_id": ultimo_id, "nuevo_max_id": nuevo_max_id})
        fechas_afectadas = [row[0].strftime('%Y-%m-%d') for row in result.fetchall() if row[0] is not None]

        if fechas_afectadas:
            fechas_formateadas = ", ".join([f"'{f}'" for f in fechas_afectadas])

            borrar_query = text(f"DELETE FROM usos_gold WHERE fecha IN ({fechas_formateadas});")
            conn.execute(borrar_query)

            insertar_query = text(f"""
                INSERT INTO usos_gold (
                    fecha, hora, id_linea, name_linea, integracion, veh_id, sn_terminal, valor, total_trx
                )
                SELECT 
                    CAST(a.date_time AS DATE) AS fecha,
                    CAST(EXTRACT(HOUR FROM a.date_time) AS INTEGER) AS hora,
                    CAST(a.id_line AS BIGINT) AS id_linea,
                    b.ld_descshort AS name_linea,
                    CAST(a.type_intg AS BIGINT) AS integracion,
                    CAST(a.veh_id AS BIGINT) AS veh_id,
                    a.terminal_id AS sn_terminal,
                    CAST(a.amount AS BIGINT) AS valor,
                    COUNT(*) AS total_trx
                FROM usos_silver a
                INNER JOIN lines_silver b ON CAST(b.ld_id AS BIGINT) = CAST(a.id_line AS BIGINT)
                WHERE a.date_time IS NOT NULL
                  AND CAST(a.date_time AS DATE) IN ({fechas_formateadas})
                GROUP BY 
                    CAST(a.date_time AS DATE),
                    CAST(EXTRACT(HOUR FROM a.date_time) AS INTEGER), 
                    CAST(a.id_line AS BIGINT),
                    b.ld_descshort,
                    CAST(a.type_intg AS BIGINT),
                    CAST(a.veh_id AS BIGINT),
                    a.terminal_id, 
                    CAST(a.amount AS BIGINT);
            """)
            conn.execute(insertar_query)
            print(f"✅ Se consolidaron agregados para {len(fechas_afectadas)} fechas.")

        actualizar_control = text("""
            INSERT INTO etl_control_gold (proceso, ultimo_id_txn)
            VALUES ('usos_gold', :nuevo_max_id)
            ON CONFLICT (proceso)
            DO UPDATE SET ultimo_id_txn = EXCLUDED.ultimo_id_txn;
        """)
        conn.execute(actualizar_control, {"nuevo_max_id": nuevo_max_id})


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="dag_usos_gold",
    default_args=default_args,
    description="Consolidación de capa Silver a Gold para la tabla de usos",
    schedule=None,  # Activado por el DAG 4
    start_date=datetime(2026, 1, 1),
    catchup=False,
    doc_md=doc_md_gold,
    tags=["gold", "consolidacion", "postgres", "usos", "paso5"],
) as dag:

    tarea_gold = PythonOperator(
        task_id="cargar_consolidado_usos_gold",
        python_callable=cargar_usos_gold,
    )