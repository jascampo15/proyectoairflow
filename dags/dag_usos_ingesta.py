from datetime import datetime, timedelta
import requests

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable  # <-- NUEVA IMPORTACIÓN

doc_md_usos_ingesta = """
### DAG 1: Ingesta Raw - Transacciones de Usos (`usos_raw`)

**Propósito:** Consume de forma incremental los registros transaccionales desde la API REST externa y los guarda en `usos_raw`.
**Estrategia:** Consulta el último `id_txn` registrado para traer solo deltas nuevos (`ON CONFLICT DO NOTHING`).
**Orquestación:** Se ejecuta a las 00:00 AM y dispara el DAG `ingesta_lineas`.
"""

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'start_date': datetime(2026, 1, 1),
}

with DAG(
    dag_id='ingesta_usos_api',
    default_args=default_args,
    schedule='0 0 * * *',  # Ejecución diaria a las 00:00 AM
    catchup=False,
    description='Pipeline de ingesta incremental a PostgreSQL desde API REST',
    doc_md=doc_md_usos_ingesta,
    tags=['proyecto', 'api', 'ingesta', 'postgres', 'paso1'],
) as dag:

    def guardar_en_postgres():
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')

        query_crear_tabla = """
            CREATE TABLE IF NOT EXISTS usos_raw (
                mti VARCHAR(20), pan TEXT, date_time VARCHAR(30), acquirer_id VARCHAR(50),
                card_acc_id VARCHAR(50), terminal_id VARCHAR(50), stan VARCHAR(30), rrn VARCHAR(50),
                amount VARCHAR(30), act_cde VARCHAR(20), appr_cde VARCHAR(30), track2 VARCHAR(50),
                actual_bal VARCHAR(50), geo_ref TEXT, id_intg VARCHAR(30), last_modified TIMESTAMP,
                proc_cde VARCHAR(30), lcl_date_time VARCHAR(30), txn_type VARCHAR(30), id_line VARCHAR(30),
                raw_txn TEXT, id_txn VARCHAR(50) PRIMARY KEY, not_on_us VARCHAR(20), bin VARCHAR(20),
                last_digit VARCHAR(20), type_intg VARCHAR(20), serial_open_loop VARCHAR(50), veh_id VARCHAR(30),
                project_id INT, lote_id INT, fecha_ingesta TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        pg_hook.run(query_crear_tabla)

        query_max_id = "SELECT MAX(CAST(id_txn AS BIGINT)) FROM usos_raw;"
        resultado_max = pg_hook.get_first(query_max_id)
        
        max_id_actual = resultado_max[0] if (resultado_max and resultado_max[0] is not None) else None
        id_base = max_id_actual + 1 if max_id_actual is not None else 6282720

        print(f"--- Iniciando ingesta incremental a partir del id_txn base: {id_base} ---")

        columnas = [
            "mti", "pan", "date_time", "acquirer_id", "card_acc_id", "terminal_id", "stan", "rrn",
            "amount", "act_cde", "appr_cde", "track2", "actual_bal", "geo_ref", "id_intg",
            "last_modified", "proc_cde", "lcl_date_time", "txn_type", "id_line", "raw_txn",
            "id_txn", "not_on_us", "bin", "last_digit", "type_intg", "serial_open_loop",
            "veh_id", "project_id", "lote_id"
        ]

        page_number = 1
        page_size = 1000
        total_insertados = 0

        conn = pg_hook.get_conn()
        conn.autocommit = True
        cursor = conn.cursor()

        # <-- AQUÍ SOLICITAMOS LA URL DESDE LAS VARIABLES DE AIRFLOW
        base_url = Variable.get("API_USOS_BASE_URL", default_var="https://api-placeholder.local/getbyidtransactionscard")

        while True:
            # Construimos la URL dinámica usando la variable
            url = f"{base_url}?id={id_base}&pageNumber={page_number}&pageSize={page_size}&projectId=1"
            
            try:
                response = requests.get(url, timeout=60)
                response.raise_for_status()
                payload = response.json()
            except Exception as e:
                print(f"Error al llamar la API en página {page_number}: {e}") # Evitar imprimir la URL completa en los logs por seguridad
                break

            response_content = payload.get('response') if isinstance(payload, dict) else None
            if not isinstance(response_content, dict):
                print("Estructura de respuesta inválida o vacía. Finalizando ingesta.")
                break

            data = response_content.get('data') or []
            total_pages = response_content.get('totalPages', 1)

            if not data:
                print(f"No se encontraron más registros en la página {page_number}. Finalizando.")
                break

            filas = []
            for item in data:
                if isinstance(item, dict) and item.get('idTxn'):
                    try:
                        val_id = int(item['idTxn'])
                        if max_id_actual is not None and val_id <= max_id_actual:
                            continue

                        filas.append((
                            item.get('mti'), item.get('pan'), item.get('dateTime'), item.get('acquirerId'),
                            item.get('cardAccId'), item.get('terminalId'), item.get('stan'), item.get('rrn'),
                            item.get('amount'), item.get('actCde'), item.get('apprCde'), item.get('track2'),
                            item.get('actualBal'), item.get('geoRef'), item.get('idIntg'), item.get('lastModified'),
                            item.get('proc_cde', item.get('procCde')), item.get('lclDateTime'), item.get('txnType'),
                            item.get('idLine'), item.get('rawTxn'), str(item.get('idTxn')), item.get('notOnUs'),
                            item.get('bin'), item.get('lastDigit'), item.get('typeIntg'), item.get('serialOpenLooop'),
                            item.get('vehId'), item.get('projectId'), item.get('loteId')
                        ))
                    except (ValueError, TypeError):
                        continue

            if filas:
                cols_str = ", ".join(columnas)
                val_placeholders = ", ".join(["%s"] * len(columnas))
                insert_query = f"INSERT INTO usos_raw ({cols_str}) VALUES ({val_placeholders}) ON CONFLICT (id_txn) DO NOTHING;"
                cursor.executemany(insert_query, filas)
                total_insertados += len(filas)

            if page_number >= total_pages:
                break

            page_number += 1

        cursor.close()
        conn.close()
        print(f"--- Ingesta completada. Total registros nuevos: {total_insertados} ---")

    ingesta = PythonOperator(
        task_id='obtener_y_guardar_datos_postgres',
        python_callable=guardar_en_postgres,
    )

    trigger_ingesta_lineas = TriggerDagRunOperator(
        task_id="trigger_ingesta_lineas",
        trigger_dag_id="ingesta_lineas",
        wait_for_completion=False,
    )

    ingesta >> trigger_ingesta_lineas