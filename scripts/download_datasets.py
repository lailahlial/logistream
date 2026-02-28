"""
download_datasets.py – Download real datasets for LogiStream

1. Kaggle: Transportation and Logistics Tracking Dataset (Nicole Machado)
   → Converts XLSX sheets to CSV files in data/
2. HuggingFace: Cainiao-AI/LaDe-D (Shanghai delivery events)
   → Fetches a sample via the datasets-server API and saves as CSV

Usage: python scripts/download_datasets.py
"""

import os, json, requests
import pandas as pd

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")

# ── Kaggle Download ─────────────────────────────────────────────
def download_kaggle():
    """Download and convert Kaggle XLSX to CSV files."""
    print("📦 Downloading Kaggle Transportation & Logistics dataset...")

    try:
        import kagglehub
        path = kagglehub.dataset_download(
            "nicolemachado/transportation-and-logistics-tracking-dataset"
        )
        xlsx_path = os.path.join(
            path,
            "Transportation and Logistics Tracking Dataset..xlsx"
        )
    except Exception as e:
        print(f"❌ kagglehub download failed: {e}")
        print("   Install kagglehub and configure Kaggle credentials.")
        return False

    if not os.path.exists(xlsx_path):
        print(f"❌ XLSX file not found at {xlsx_path}")
        return False

    os.makedirs(DATA_DIR, exist_ok=True)

    # ── Primary data sheet (6880 rows) ──
    print("   Parsing 'Primary data' sheet...")
    df_primary = pd.read_excel(xlsx_path, sheet_name="Primary data")
    # Parse origin/destination lat/lon from string format "lat,lon"
    def parse_latlon(val):
        if pd.isna(val) or not isinstance(val, str):
            return None, None
        parts = val.split(",")
        if len(parts) == 2:
            try:
                return float(parts[0].strip()), float(parts[1].strip())
            except ValueError:
                return None, None
        return None, None

    df_primary[["origin_lat", "origin_lon"]] = df_primary["Org_lat_lon"].apply(
        lambda x: pd.Series(parse_latlon(x))
    )
    df_primary[["dest_lat", "dest_lon"]] = df_primary["Des_lat_lon"].apply(
        lambda x: pd.Series(parse_latlon(x))
    )

    # Clean column names
    df_primary.columns = [c.strip() for c in df_primary.columns]

    out_primary = os.path.join(DATA_DIR, "kaggle_primary.csv")
    df_primary.to_csv(out_primary, index=False)
    print(f"   ✅ Primary data → {out_primary} ({len(df_primary)} rows)")

    # ── Refined sheet (705 rows) ──
    print("   Parsing 'Refined' sheet...")
    df_refined = pd.read_excel(xlsx_path, sheet_name="Refined")
    df_refined.columns = [c.strip() for c in df_refined.columns]
    out_refined = os.path.join(DATA_DIR, "kaggle_refined.csv")
    df_refined.to_csv(out_refined, index=False)
    print(f"   ✅ Refined data → {out_refined} ({len(df_refined)} rows)")

    return True


# ── HuggingFace LaDe-D Download ─────────────────────────────────
def download_lade_sample(n_rows=5000, city="delivery_sh"):
    """Fetch a sample of LaDe-D delivery events via the HF datasets-server API."""
    print(f"\n🌐 Fetching LaDe-D ({city}) from HuggingFace API...")

    base_url = "https://datasets-server.huggingface.co/rows"
    all_rows = []
    batch_size = 100
    offset = 0

    while len(all_rows) < n_rows:
        remaining = min(batch_size, n_rows - len(all_rows))
        url = (
            f"{base_url}?dataset=Cainiao-AI/LaDe-D"
            f"&config=default&split={city}"
            f"&offset={offset}&length={remaining}"
        )
        try:
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            rows = [r["row"] for r in data.get("rows", [])]
            if not rows:
                break
            all_rows.extend(rows)
            offset += len(rows)
            if len(all_rows) % 500 == 0:
                print(f"   Fetched {len(all_rows)}/{n_rows} rows...")
        except Exception as e:
            print(f"   ⚠️ Error at offset {offset}: {e}")
            break

    if not all_rows:
        print("   ❌ No data fetched from HuggingFace API")
        return False

    df = pd.DataFrame(all_rows)
    os.makedirs(DATA_DIR, exist_ok=True)
    out_path = os.path.join(DATA_DIR, "lade_delivery.csv")
    df.to_csv(out_path, index=False)
    print(f"   ✅ LaDe-D → {out_path} ({len(df)} rows)")
    print(f"   Columns: {list(df.columns)}")
    return True


if __name__ == "__main__":
    print("🚀 LogiStream Dataset Download\n")

    kaggle_ok = download_kaggle()
    lade_ok = download_lade_sample(n_rows=5000)

    if kaggle_ok and lade_ok:
        print("\n✅ All datasets downloaded successfully!")
    else:
        print("\n⚠️ Some downloads failed. Check messages above.")
