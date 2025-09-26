"""
Ispeziona il CSV e propone candidati per dimensioni e misure
"""
from pathlib import Path
import duckdb
from config import load_config

def main():
    cfg = load_config()
    csv_path = Path(cfg["input"]["csv_path"]).as_posix()
    con = duckdb.connect()

    con.sql(f"CREATE OR REPLACE VIEW v_raw AS SELECT * FROM read_csv_auto('{csv_path}', header=True)")

    # recupera i nomi delle colonne dall'header
    cols = list(con.sql("SELECT * FROM v_raw LIMIT 0").fetchdf().columns)

    print("Profilazione colonne")
    for c in cols:
        q = f"""
        SELECT
          '{c}' AS col,
          COUNT(*) AS n_rows,
          COUNT(DISTINCT "{c}") AS n_distinct,
          SUM(CASE WHEN "{c}" IS NULL OR TRIM(CAST("{c}" AS VARCHAR)) = '' THEN 1 ELSE 0 END) AS n_nulls
        FROM v_raw
        """
        df = con.sql(q).fetchdf()
        n = int(df.loc[0, "n_rows"])
        d = int(df.loc[0, "n_distinct"])
        nulls = int(df.loc[0, "n_nulls"])
        ratio = d / n if n else 0.0

        if c.lower() in {"id", "host_id"}:
            hint = "chiave candidata"
        elif c.lower() in {"price", "minimum_nights", "number_of_reviews", "reviews_per_month", "availability_365"}:
            hint = "misura candidata"
        elif ratio < 0.1 and d > 1:
            hint = "dimensione candidata"
        else:
            hint = "valutare"

        print(f"{c}  rows {n}  distinct {d}  null {nulls}  note {hint}")

    con.close()

if __name__ == "__main__":
    main()
