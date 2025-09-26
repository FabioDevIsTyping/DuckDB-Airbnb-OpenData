import os
import glob
import argparse
import duckdb
from pathlib import Path
from config import load_config

def render(sql: str, cfg : dict) -> str:
    """ Sostituisce i placeholder nel file SQL con i valori dal file di configurazione."""
    return (sql.replace("${CSV_PATH}", cfg["input"]["csv_path"])
            .replace("${SCHEMA}", cfg["duckdb"]["schema"])
            )

def iter_sql_files(sql_dir: str):
    """Genera i percorsi dei file SQL in ordine alfabetico."""
    sql_files = sorted(glob.glob(os.path.join(sql_dir, "*.sql")))
    for sql_file in sql_files:
        yield sql_file

def run_dir(con: duckdb.DuckDBPyConnection, dirpath: str, cfg: dict, dry_run: bool = False):
    """
    Esegue tutti i file .sql in una directory
    Se dry_run e vero, stampa le query renderizzate senza eseguirle
    """
    dir_abs = Path(dirpath)
    if not dir_abs.exists():
        print(f"Directory assente, salto {dirpath}")
        return
    files = list(iter_sql_files(dirpath))
    if not files:
        print(f"Nessun file .sql in {dirpath}")
        return
    print(f"Esecuzione directory {dirpath}")
    for fpath in files:
        with open(fpath, "r", encoding="utf-8") as fh:
            sql_text = fh.read()
        sql_final = render(sql_text, cfg)
        print(f"  File {fpath}")
        if dry_run:
            print(sql_final[:500])
            if len(sql_final) > 500:
                print("  Output troncato")
            continue
        con.execute(sql_final)

def run_all(dry_run: bool = False):
    """
    Esegue l intera pipeline SQL
    Ordine fisso delle directory
    00_staging poi 10_dimensions poi 20_facts poi 90_quality
    """
    cfg = load_config()
    db_path = cfg["duckdb"]["path"]
    schema = cfg["duckdb"]["schema"]

    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    con = duckdb.connect(db_path)
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    con.execute(f"SET schema {schema}")

    run_dir(con, "sql/00_staging", cfg, dry_run=dry_run)
    run_dir(con, "sql/10_dimensions", cfg, dry_run=dry_run)
    run_dir(con, "sql/20_facts", cfg, dry_run=dry_run)
    run_dir(con, "sql/90_quality", cfg, dry_run=dry_run)

    con.close()
    print(f"Completato, database in {db_path}")

def main():
    """
    Entry point a riga di comando
    Usa il parametro --dry-run per stampare le query senza eseguirle
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    run_all(dry_run=args.dry_run)

if __name__ == "__main__":
    main()