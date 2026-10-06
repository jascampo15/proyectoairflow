from datetime import datetime, timedelta
import pandas as pd
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

doc_md_lines_silver = """
### DAG 3: Transformación Silver - Catálogo (`lines_silver`)

**Propósito:** Limpia y estandariza las líneas requeridas (`ld_id < 500`) desde `lines_raw` hacia `lines_silver`.
**Orquestación:** Espera a ser activado por `ingesta_lineas` y al finalizar dispara a `dag_transformacion_usos`.
"""

def transformar_lines_silver():
    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    engine = pg_hook.get_sqlalchemy_engine()
    
    with engine.connect() as conn:
        df = pd.read_sql('SELECT * FROM lines_raw WHERE ld_id < 500;', con=conn)

    if df.empty:
        print("✅ No hay registros en lines_raw para procesar. Finalizando tarea.")
        return

    df.columns = [col.lower() for col in df.columns]

    cols_requeridas = ['ld_id', 'ld_desc', 'ld_descshort', 'ld_status']
    cols_existentes = [c for c in cols_requeridas if c in df.columns]
    df = df[cols_existentes]

    for col in ['ld_desc', 'ld_descshort']:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    nombre_tabla_destino = "lines_silver"
    
    with engine.begin() as conn:
        df.to_sql(
            name=nombre_tabla_destino,
            con=conn,
            if_exists="replace",
            index=False,
            chunksize=5000
        )

    print(f"✅ Sincronización exitosa. Se insertaron {len(df)} registros en '{nombre_tabla_destino}'.")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="dag_transformacion_lines",
    default_args=default_args,
    description="Transformación de capa raw a silver para la tabla de líneas/rutas",
    schedule=None,  # Activado por el DAG 2
    start_date=datetime(2026, 1, 1),
    catchup=False,
    doc_md=doc_md_lines_silver,
    tags=["silver", "transformacion", "postgres", "lines", "paso3"],
) as dag:

    tarea_silver = PythonOperator(
        task_id="transformar_y_cargar_lines_silver",
        python_callable=transformar_lines_silver,
    )

    trigger_transformacion_usos = TriggerDagRunOperator(
        task_id="trigger_transformacion_usos",
        trigger_dag_id="dag_transformacion_usos",
        wait_for_completion=False,
    )

    tarea_silver >> trigger_transformacion_usos