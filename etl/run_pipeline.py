import sys
import os

# Make sure Python can find the other etl files
sys.path.insert(0, os.path.dirname(__file__))

from extract import extract_all
from transform import run_all_transforms
from load import load_to_duckdb, verify_db

def run():
    print("=" * 50)
    print("🚀 Starting Supply Chain ETL Pipeline")
    print("=" * 50)

    print("\n[1/3] Extracting raw data...")
    raw = extract_all()

    print("\n[2/3] Transforming data...")
    transformed = run_all_transforms(raw)

    print("\n[3/3] Loading into DuckDB...")
    load_to_duckdb(transformed)
    verify_db()

    print("\n" + "=" * 50)
    print("✅ Pipeline complete!")
    print("=" * 50)

if __name__ == "__main__":
    run()