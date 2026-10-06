import os
from datetime import datetime, timedelta
import pandas as pd
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

doc_md_lineas_ingesta = """
### DAG 2: Ingesta Raw - Catálogo de Líneas (`lines_raw`)

**Propósito:** Sincroniza el catálogo de líneas desde el Excel maestro `linedetails.xlsx` mediante `TRUNCATE + INSERT`.
**Orquestación:** Espera a ser activado por `ingesta_usos_api` y al finalizar dispara a `dag_transformacion_lines`.
"""

def procesar_e_ingresar_excel():
    ruta_archivo = "/opt/airflow/dags/Proyecto_final_airflow/linedetails.xlsx"

    if not os.path.exists(ruta_archivo):
        raise FileNotFoundError(f"El archivo no existe en la ruta: {ruta_archivo}")

    df = pd.read_excel(ruta_archivo)

    if df.empty or len(df.columns) == 0:
        print(f"⚠️ El archivo '{ruta_archivo}' está vacío o no contiene registros válidos.")
        return

    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    engine = pg_hook.get_sqlalchemy_engine()
    nombre_tabla = "lines_raw"

    with engine.begin() as conexion:
        conexion.exec_driver_sql(
            f"CREATE TABLE IF NOT EXISTS {nombre_tabla} ({', '.join([f'{col} TEXT' for col in df.columns])});"
        )
        conexion.exec_driver_sql(f"TRUNCATE TABLE {nombre_tabla};")
        df.to_sql(
            name=nombre_tabla,
            con=conexion,
            if_exists="append",
            index=False,
            chunksize=1000,
        )

    print(f"✅ Sincronización exitosa. Se actualizaron {len(df)} registros en '{nombre_tabla}'.")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="ingesta_lineas",
    default_args=default_args,
    description="DAG para actualizar la tabla de configuración desde el Excel linedetails en PostgreSQL",
    schedule=None,  # Activado por el DAG 1
    start_date=datetime(2026, 1, 1),
    catchup=False,
    doc_md=doc_md_lineas_ingesta,
    tags=["excel", "configuracion", "ingesta", "postgres", "paso2"],
) as dag:

    tarea_cargar_excel = PythonOperator(
        task_id="cargar_excel_a_postgres",
        python_callable=procesar_e_ingresar_excel,
    )

    trigger_transformacion_lines = TriggerDagRunOperator(
        task_id="trigger_transformacion_lines",
        trigger_dag_id="dag_transformacion_lines",
        wait_for_completion=False,
    )

    tarea_cargar_excel >> trigger_transformacion_lines