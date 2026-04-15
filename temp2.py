import csv
import logging
import os
import shutil
import tempfile
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path
import yaml 

import psycopg
import tableauserverclient as TSC
from tableauhyperapi import (
    HyperProcess, Connection, Telemetry, CreateMode
)

logger = logging.getLogger(__name__)

def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'

def load_config(config_path: str) -> dict:
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def discover_views(pg_conninfo: str, schema_name: str) -> list:
    """Discovers all views within the specified schema."""
    query = "SELECT table_name FROM information_schema.views WHERE table_schema = %s;"
    with psycopg.connect(pg_conninfo) as conn:
        with conn.cursor() as cur:
            cur.execute(query, (schema_name,))
            return [row[0] for row in cur.fetchall()]

def extract_column_mapping_from_tds(tds_path: Path) -> dict:
    """Parses the Tableau XML to auto-discover logical-to-UUID column mappings."""
    tree = ET.parse(tds_path)
    root = tree.getroot()
    mapping = {}
    
    # Tableau stores mappings in <map key='[postgres_col]' value='[UUID]' />
    for map_tag in root.iter('map'):
        key = map_tag.get('key', '')
        value = map_tag.get('value', '')
        if key and value:
            clean_key = key.strip('[]')
            clean_value = value.strip('[]')
            mapping[clean_key] = clean_value
            
    return mapping

def find_target_table(hyper_path: Path, view_name: str) -> str:
    """Queries the Hyper catalog to find the Tableau physical table name for a given view."""
    with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(endpoint=hyper.endpoint, database=str(hyper_path), create_mode=CreateMode.NONE) as connection:
            for schema in connection.catalog.get_schema_names():
                if str(schema).lower() in ('"pg_catalog"', '"information_schema"'):
                    continue
                for table in connection.catalog.get_table_names(schema):
                    _, _, tbl_name = table._unescaped_triple
                    # Match tables like "x_view (x.x_view)_1BF1BD..."
                    if view_name.lower() in tbl_name.lower():
                        return tbl_name
    return None

def extract_and_format_csv(pg_conninfo: str, schema_name: str, view_name: str, cursor_value: str, column_mapping: dict, delete_csv_path: Path, insert_csv_path: Path):
    """Extracts data via COPY and swaps headers based on auto-discovered UUID mapping."""
    full_view_name = f"{quote_ident(schema_name)}.{quote_ident(view_name)}"
    
    # Using standardized cursor and PK
    delete_sql = f"COPY (SELECT view_key FROM {full_view_name} WHERE processed_timestamp > %(cursor)s) TO STDOUT WITH CSV"
    insert_sql = f"COPY (SELECT * FROM {full_view_name} WHERE processed_timestamp > %(cursor)s) TO STDOUT WITH CSV HEADER"

    with psycopg.connect(pg_conninfo) as conn:
        with conn.cursor() as cur:
            with open(delete_csv_path, "wb") as f:
                with cur.copy(delete_sql, {"cursor": cursor_value}) as copy:
                    for data in copy: f.write(data)
            
            with open(insert_csv_path, "wb") as f:
                with cur.copy(insert_sql, {"cursor": cursor_value}) as copy:
                    for data in copy: f.write(data)

    # Apply auto-mapping to insert headers
    temp_file = insert_csv_path.with_suffix('.tmp')
    with open(insert_csv_path, 'r', newline='', encoding='utf-8') as infile, \
         open(temp_file, 'w', newline='', encoding='utf-8') as outfile:
         
        reader = csv.reader(infile)
        writer = csv.writer(outfile)
        original_headers = next(reader)
        # Fall back to original name if not in map (e.g., calculations)
        writer.writerow([column_mapping.get(col, col) for col in original_headers])
        for row in reader: writer.writerow(row)
            
    temp_file.replace(insert_csv_path)

def perform_hyper_upsert(hyper_path: Path, target_table: str, pk_uuid: str, delete_csv: Path, insert_csv: Path):
    table_sql = f'"Extract".{quote_ident(target_table)}'
    
    with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(endpoint=hyper.endpoint, database=str(hyper_path), create_mode=CreateMode.NONE) as connection:
            connection.execute_command("BEGIN")
            try:
                connection.execute_command("CREATE TEMPORARY TABLE temp_delete_keys (id BIGINT)")
                connection.execute_command(f"COPY temp_delete_keys FROM {quote_ident(str(delete_csv))} WITH (FORMAT CSV)")
                
                deleted = connection.execute_command(f"DELETE FROM {table_sql} WHERE {quote_ident(pk_uuid)} IN (SELECT id FROM temp_delete_keys)")
                inserted = connection.execute_command(f"COPY {table_sql} FROM {quote_ident(str(insert_csv))} WITH (FORMAT CSV, HEADER TRUE)")
                
                logger.info(f"[{target_table}] Deleted: {deleted} | Inserted: {inserted}")
                connection.execute_command("COMMIT")
            except Exception as e:
                connection.execute_command("ROLLBACK")
                raise

def run_framework(config_path: str, cursor_value: str):
    config = load_config(config_path)
    pg_conninfo = os.getenv("PG_CONNINFO")
    pat_secret = os.getenv("TABLEAU_PAT_SECRET")
    schema_name = config["postgres"]["schema_name"]
    
    # 1. Discover all views in the schema
    discovered_views = discover_views(pg_conninfo, schema_name)
    logger.info(f"Discovered views: {discovered_views}")

    server = TSC.Server(config["tableau"]["server_url"], use_server_version=True)
    auth = TSC.PersonalAccessTokenAuth(config["tableau"]["pat_name"], pat_secret, config["tableau"]["site_url"])
    
    with server.auth.sign_in(auth):
        with tempfile.TemporaryDirectory() as temp_dir_name:
            temp_dir = Path(temp_dir_name)
            
            # 2. Iterate through the target datasources
            for ds_name in config["target_datasources"]:
                logger.info(f"\n--- Processing Datasource: {ds_name} ---")
                
                req = TSC.RequestOptions()
                matches = [ds for ds in TSC.Pager(server.datasources, req) if ds.name == ds_name and (ds.project_name or "").lower() == config["tableau"]["project_name"].lower()]
                if not matches:
                    logger.warning(f"Datasource '{ds_name}' not found. Skipping.")
                    continue
                
                datasource_obj = matches[0]
                
                # Download & Unpack
                old_cwd = os.getcwd()
                os.chdir(temp_dir)
                try: downloaded_tdsx = Path(server.datasources.download(datasource_obj.id, include_extract=True)).resolve()
                finally: os.chdir(old_cwd)

                extract_dir = temp_dir / f"unpacked_{ds_name}"
                with zipfile.ZipFile(downloaded_tdsx, "r") as zf: zf.extractall(extract_dir)

                # Locate files
                hyper_path = list(extract_dir.rglob("*.hyper"))[0]
                tds_path = list(extract_dir.rglob("*.tds"))[0]

                # 3. Auto-discover column mappings from XML
                column_mapping = extract_column_mapping_from_tds(tds_path)
                
                # Get the specific UUID for our standard primary key
                pk_uuid = column_mapping.get("view_key")
                if not pk_uuid:
                    logger.warning(f"'view_key' mapping not found in {ds_name}. Skipping datasource.")
                    continue

                # 4. UPSERT each view
                for view in discovered_views:
                    target_table = find_target_table(hyper_path, view)
                    if not target_table:
                        logger.info(f"View '{view}' not found in Hyper extract. Skipping.")
                        continue
                        
                    logger.info(f"UPSERTing view: {view} -> Table: {target_table}")
                    
                    delete_csv = temp_dir / f"{view}_del.csv"
                    insert_csv = temp_dir / f"{view}_ins.csv"
                    
                    extract_and_format_csv(pg_conninfo, schema_name, view, cursor_value, column_mapping, delete_csv, insert_csv)
                    perform_hyper_upsert(hyper_path, target_table, pk_uuid, delete_csv, insert_csv)

                # 5. Repack and Publish
                logger.info(f"Repacking and Publishing {ds_name}...")
                repacked_tdsx = downloaded_tdsx.parent / f"mod_{downloaded_tdsx.name}"
                with zipfile.ZipFile(repacked_tdsx, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                    for file_path in extract_dir.rglob("*"):
                        if file_path.is_file(): zf.write(file_path, file_path.relative_to(extract_dir))
                
                publish_item = TSC.DatasourceItem(project_id=datasource_obj.project_id, name=datasource_obj.name)
                server.datasources.publish(publish_item, str(repacked_tdsx), TSC.Server.PublishMode.Overwrite)
