import logging
import os
import shutil
import tempfile
import zipfile
from pathlib import Path

import psycopg
import tableauserverclient as TSC
from tableauhyperapi import (
    HyperProcess, Connection, Telemetry, CreateMode, TableName
)

logger = logging.getLogger(__name__)

def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'

def extract_postgres_to_csv(pg_conninfo: str, source_sql: str, cursor_value, delete_csv_path: Path, insert_csv_path: Path):
    """Extracts updated records from Postgres into two bulk CSVs (one for deletes, one for inserts)."""
    logger.info("Extracting data from PostgreSQL via COPY...")
    
    # We assume the source_sql returns the primary key as the first column for the delete CSV
    delete_sql = f"COPY (SELECT uid FROM ({source_sql}) as sub) TO STDOUT WITH CSV"
    insert_sql = f"COPY ({source_sql}) TO STDOUT WITH CSV HEADER"

    with psycopg.connect(pg_conninfo) as conn:
        with conn.cursor() as cur:
            # 1. Extract IDs for deletion
            with open(delete_csv_path, "wb") as f:
                with cur.copy(delete_sql, {"cursor": cursor_value}) as copy:
                    for data in copy:
                        f.write(data)
            
            # 2. Extract full rows for insertion
            with open(insert_csv_path, "wb") as f:
                with cur.copy(insert_sql, {"cursor": cursor_value}) as copy:
                    for data in copy:
                        f.write(data)
                        
    logger.info("PostgreSQL extraction complete.")

def perform_hyper_upsert(hyper_path: Path, schema_name: str, table_name: str, primary_key_uuid: str, delete_csv: Path, insert_csv: Path):
    """Opens the local Hyper file and performs a bulk delete and bulk insert in one transaction."""
    logger.info(f"Connecting to local Hyper engine: {hyper_path}")
    
    table_sql = f"{quote_ident(schema_name)}.{quote_ident(table_name)}"
    
    with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(endpoint=hyper.endpoint, database=str(hyper_path), create_mode=CreateMode.NONE) as connection:
            connection.execute_command("BEGIN")
            try:
                # 1. Bulk Delete via Temp Table
                logger.info("Executing bulk delete...")
                connection.execute_command("CREATE TEMPORARY TABLE temp_delete_keys (id BIGINT)")
                connection.execute_command(f"COPY temp_delete_keys FROM {quote_ident(str(delete_csv))} WITH (FORMAT CSV)")
                
                delete_query = f"""
                    DELETE FROM {table_sql} 
                    WHERE {quote_ident(primary_key_uuid)} IN (SELECT id FROM temp_delete_keys)
                """
                deleted_count = connection.execute_command(delete_query)
                logger.info(f"Deleted {deleted_count} stale records.")

                # 2. Bulk Insert
                logger.info("Executing bulk insert...")
                insert_query = f"COPY {table_sql} FROM {quote_ident(str(insert_csv))} WITH (FORMAT CSV, HEADER TRUE)"
                inserted_count = connection.execute_command(insert_query)
                logger.info(f"Inserted {inserted_count} updated/new records.")

                connection.execute_command("COMMIT")
            except Exception as e:
                connection.execute_command("ROLLBACK")
                logger.error(f"Hyper transaction failed. Rolled back. Error: {e}")
                raise

def process_tableau_datasource(server_url, pat_name, pat_secret, site_url, datasource_name, project_name, temp_dir: Path) -> Path:
    """Authenticates, finds, downloads, and unpacks the Tableau datasource."""
    logger.info(f"Authenticating to Tableau Server: {server_url}")
    auth = TSC.PersonalAccessTokenAuth(token_name=pat_name, personal_access_token=pat_secret, site_id=site_url)
    server = TSC.Server(server_url, use_server_version=True)
    
    with server.auth.sign_in(auth):
        # Find Datasource
        req = TSC.RequestOptions()
        matches = [ds for ds in TSC.Pager(server.datasources, req) if ds.name == datasource_name and (ds.project_name or "").lower() == project_name.lower()]
        if not matches:
            raise RuntimeError(f"Datasource '{datasource_name}' not found.")
        datasource = matches[0]

        # Download
        logger.info(f"Downloading datasource: {datasource.name}")
        old_cwd = os.getcwd()
        os.chdir(temp_dir)
        try:
            downloaded_tdsx = Path(server.datasources.download(datasource.id, include_extract=True)).resolve()
        finally:
            os.chdir(old_cwd)

        # Unpack
        extract_dir = temp_dir / "unpacked"
        with zipfile.ZipFile(downloaded_tdsx, "r") as zf:
            zf.extractall(extract_dir)

        hyper_files = list(extract_dir.rglob("*.hyper"))
        if not hyper_files:
            raise RuntimeError("No .hyper file found in the .tdsx")
            
        return downloaded_tdsx, extract_dir, hyper_files[0], datasource, server

def repack_and_publish(server, datasource, downloaded_tdsx: Path, extract_dir: Path):
    """Repacks the modified Hyper file into the TDSX and overwrites on Tableau Server."""
    logger.info("Repacking .tdsx archive...")
    repacked_tdsx = downloaded_tdsx.parent / f"modified_{downloaded_tdsx.name}"
    
    with zipfile.ZipFile(repacked_tdsx, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for file_path in extract_dir.rglob("*"):
            if file_path.is_file():
                zf.write(file_path, file_path.relative_to(extract_dir))

    logger.info("Publishing back to Tableau Server...")
    publish_item = TSC.DatasourceItem(project_id=datasource.project_id, name=datasource.name)
    server.datasources.publish(publish_item, str(repacked_tdsx), TSC.Server.PublishMode.Overwrite)
    logger.info("Publish complete.")

def run_tableau_upsert(
    pg_conninfo: str,
    source_sql: str,
    cursor_value: str,
    tableau_url: str,
    pat_name: str,
    pat_secret: str,
    site_url: str,
    datasource_name: str,
    project_name: str,
    schema_name: str,
    table_name: str,
    primary_key_uuid: str
):
    """Main entrypoint for Airflow PythonOperator."""
    with tempfile.TemporaryDirectory() as temp_dir_name:
        temp_dir = Path(temp_dir_name)
        delete_csv = temp_dir / "deletes.csv"
        insert_csv = temp_dir / "inserts.csv"

        # 1. Pull from Postgres
        extract_postgres_to_csv(pg_conninfo, source_sql, cursor_value, delete_csv, insert_csv)

        # 2. Download from Tableau
        downloaded_tdsx, extract_dir, hyper_path, datasource, server = process_tableau_datasource(
            tableau_url, pat_name, pat_secret, site_url, datasource_name, project_name, temp_dir
        )

        # 3. Modify Local Extract
        perform_hyper_upsert(hyper_path, schema_name, table_name, primary_key_uuid, delete_csv, insert_csv)

        # 4. Upload back to Tableau
        repack_and_publish(server, datasource, downloaded_tdsx, extract_dir)
