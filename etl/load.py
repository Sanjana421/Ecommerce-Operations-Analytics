import duckdb
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "processed", "supply_chain.duckdb")

def load_to_duckdb(transformed: dict):
    """Write all transformed DataFrames into DuckDB as tables."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

    con = duckdb.connect(DB_PATH)

    for table_name, df in transformed.items():
        con.execute(f"DROP TABLE IF EXISTS {table_name}")
        con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
        count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"✅ Loaded '{table_name}' → {count:,} rows into DuckDB")

    con.close()
    print(f"\n📦 Database saved to: {DB_PATH}")


def verify_db():
    """Print all tables and row counts as a sanity check."""
    con = duckdb.connect(DB_PATH)
    tables = con.execute("SHOW TABLES").fetchall()
    print("\n📋 Tables in DuckDB:")
    for (t,) in tables:
        count = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        print(f"   {t}: {count:,} rows")
    con.close()

if __name__ == "__main__":
    verify_db()