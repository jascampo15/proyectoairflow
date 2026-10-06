from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import text
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

doc_md_silver = """
### DAG 4: Transformación Silver - Transacciones (`usos_silver`)

**Propósito:** Procesa incrementalmente las transacciones crudas de `usos_raw`, aplica reglas de negocio (UTC-5, limpieza de ceros, escala de montos) e inserta en `usos_silver`.
**Orquestación:** Espera a ser activado por `dag_transformacion_lines` y al finalizar dispara a `dag_usos_gold`.
"""

def transformar_usos_silver():
    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    engine = pg_hook.get_sqlalchemy_engine()
    
    base_id = 6282720
    
    try:
        with engine.connect() as conn:
            query_max = text('SELECT MAX(CAST(id_txn AS BIGINT)) FROM usos_silver;')
            resultado = conn.execute(query_max).scalar()
            if resultado is not None:
                base_id = int(resultado)
    except Exception as e:
        print(f"La tabla usos_silver no existe aún. Usando id_txn base: {base_id}. Detalle: {e}")

    with engine.connect() as conn:
        query_ext = text('SELECT * FROM usos_raw WHERE CAST(id_txn AS BIGINT) > :base_id')
        df = pd.read_sql(query_ext, con=conn, params={"base_id": base_id})

    if df.empty:
        print("✅ No hay registros nuevos para procesar. Finalizando tarea.")
        return

    col_datetime = next((col for col in df.columns if col.lower() in ['datetime', 'date_time']), None)
    if col_datetime:
        df[col_datetime] = df[col_datetime].astype(str).str.strip()
        df[col_datetime] = df[col_datetime].replace(
            ['0', '000000000000', 'NA', 'nan', 'None', 'null', '', 'NaT'], None
        )
        df[col_datetime] = pd.to_datetime(df[col_datetime], format='%y%m%d%H%M%S', errors='coerce')
        df[col_datetime] = df[col_datetime] - pd.Timedelta(hours=5)
        df.loc[df[col_datetime] < '2000-01-01', col_datetime] = pd.NaT

        if col_datetime != 'date_time':
            df.rename(columns={col_datetime: 'date_time'}, inplace=True)

    col_vehid = next((col for col in df.columns if col.lower() in ['vehid', 'veh_id']), None)
    if col_vehid:
        df[col_vehid] = df[col_vehid].astype(str).str.lstrip('0').replace('', '0')

    col_id_line = next((col for col in df.columns if col.lower() in ['id_line', 'idline']), None)
    if col_id_line:
        df[col_id_line] = df[col_id_line].astype(str).str.lstrip('0').replace('', '0')

    col_amount = next((col for col in df.columns if col.lower() in ['amount', 'monto']), None)
    if col_amount:
        df[col_amount] = pd.to_numeric(df[col_amount], errors='coerce').fillna(0)
        df[col_amount] = (df[col_amount] / 100).astype(int)

    nombre_tabla_destino = "usos_silver"
    
    with engine.begin() as conn:
        df.to_sql(
            name=nombre_tabla_destino,
            con=conn,
            if_exists="append",
            index=False,
            chunksize=5000
        )

    print(f"✅ Sincronización exitosa. Se insertaron {len(df)} registros transformados en '{nombre_tabla_destino}'.")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="dag_transformacion_usos",
    default_args=default_args,
    description="Transformación de capa raw a silver para la tabla de usos",
    schedule=None,  # Activado por el DAG 3
    start_date=datetime(2026, 1, 1),
    catchup=False,
    doc_md=doc_md_silver,
    tags=["silver", "transformacion", "postgres", "usos", "paso4"],
) as dag:

    tarea_silver = PythonOperator(
        task_id="transformar_y_cargar_usos_silver",
        python_callable=transformar_usos_silver,
    )

    trigger_usos_gold = TriggerDagRunOperator(
        task_id="trigger_usos_gold",
        trigger_dag_id="dag_usos_gold",
        wait_for_completion=False,
    )

    tarea_silver >> trigger_usos_gold