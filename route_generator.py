from __future__ import annotations
import os
import pandas as pd
from datetime import datetime

def process_route(pdf_path: str, ref_path: str, output_dir: str) -> dict:
    os.makedirs(output_dir, exist_ok=True)
    try:
        ref_df = pd.read_csv(ref_path)
    except Exception as e:
        out = {
            "found_count": 0,
            "total_count": 0,
            "not_found": [f"Ошибка чтения справочника: {e}"],
            "output_file": os.path.join(output_dir, "route_error.csv"),
        }
        pd.DataFrame([{"error": str(e)}]).to_csv(out["output_file"], index=False)
        return out

    out_df = ref_df.copy()
    if "order" not in out_df.columns:
        out_df.insert(0, "order", range(1, len(out_df) + 1))

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_csv_name = f"route_{stamp}.csv"
    out_csv_path = os.path.join(output_dir, out_csv_name)
    out_df.to_csv(out_csv_path, index=False)

    total = len(out_df)
    not_found = []
    found = total - len(not_found)

    return {
        "found_count": found,
        "total_count": total,
        "not_found": not_found,
        "output_file": out_csv_path,
    }
