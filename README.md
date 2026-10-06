# Proyecto Final Integrador - Pipeline de Datos con Apache Airflow

## 1. Descripción del Proyecto
Pipeline de datos orquestado en Apache Airflow que ingesta datos transaccionales desde dos fuentes distintas (API REST y archivo Excel), realiza transformaciones en capas Silver/Gold y consolida los agregados en PostgreSQL.

## 2. Arquitectura del Pipeline
1. **Ingesta (Raw):**
   - DAG 1: Extracción incremental desde la API REST hacia PostgreSQL.
   - DAG 2: Carga de archivos Excel del catálogo.
2. **Transformación (Silver):**
   - DAG 3: Limpieza y tipado de catálogo (\lines_silver\).
   - DAG 4: Transformación transaccional (\usos_silver\).
3. **Consolidación (Gold):**
   - DAG 5: Agregación idempotente (*Delete-by-date + Insert*) en \usos_gold\.

## 3. Instrucciones de Despliegue Local
1. Clonar el repositorio y levantar la infraestructura:
   \\\ash
   git clone https://github.com/jascampo15/TU_REPOSITORIO.git
   cd TU_REPOSITORIO
   docker compose up -d
   \\\`n2. Configurar la conexión de PostgreSQL en Airflow:
   - Conexión ID: \postgres_default\\
   - Tipo: PostgreSQL\

## 4. Pruebas Automatizadas (CI/CD)
Para correr las pruebas en local:
\\\ash
python -m pytest tests/
\\\`nCada Pull Request ejecuta los tests automáticamente en GitHub Actions mediante el workflow de CI.\
\
## 5. Glosario de Negocio
* **Usos:** Registros transaccionales de eventos generados en las terminales.
* **Líneas:** Rutas o clasificaciones del catálogo operativo.
* **Capa Gold:** Modelo agregador por fecha, hora y línea optimizado para reportabilidad BI.