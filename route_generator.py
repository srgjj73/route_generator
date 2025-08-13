from __future__ import annotations
import os
import re
from typing import List, Tuple
import pandas as pd
from PyPDF2 import PdfReader
from rapidfuzz import fuzz, process as rf_process
from datetime import datetime

# Столбец адреса в справочнике
ADDRESS_COL = "address"
MIN_FUZZ = 80

# Регулярка для поиска адресов
ADDR_RE = re.compile(r"\b(ул\\.|улица|просп\\.|проспект|ш\\.|шоссе|пер\\.|переулок|туп\\.|б-р|бульвар|наб\\.|набережная|пл\\.|площадь|дорога|улиц)\s+[^\n,]+?(?:,?\s*д\\.?\s*\\d+[A-Za-zА-Яа-я/-]*)?", re.IGNORECASE)

def read_pdf_text(pdf_path: str) -> str:
    reader = PdfReader(pdf_path)
    return "\n".join(page.extract_text() or "" for page in reader.pages)

def extract_addresses(text: str) -> List[str]:
    candidates = set()
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        for m in ADDR_RE.finditer(line):
            candidates.add(normalize_addr(m.group(0)))
    if not candidates:
        for line in text.splitlines():
            line = line.strip()
            if len(line) > 6:
                candidates.add(normalize_addr(line))
    return list(candidates)

def normalize_addr(s: str) -> str:
    s = re.sub(r"\s+", " ", s.strip())
    return s.strip(",. ")

def match_addresses(pdf_addrs: List[str], ref_df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    if ADDRESS_COL not in ref_df.columns:
        raise ValueError(f"В справочнике нет столбца '{ADDRESS_COL}'")
    ref_addrs = ref_df[ADDRESS_COL].astype(str).fillna("").tolist()
    rows, not_found = [], []
    for idx, addr in enumerate(pdf_addrs, start=1):
        best = rf_process.extractOne(addr, ref_addrs, scorer=fuzz.WRatio)
        if best and best[1] >= MIN_FUZZ:
            row = ref_df.iloc[best[2]].to_dict()
            row.update({"order": idx, "source_addr": addr, "match": best[0], "score": best[1]})
            rows.append(row)
        else:
            not_found.append(addr)
    return pd.DataFrame(rows), not_found

def process_route(pdf_path: str, ref_path: str, output_dir: str) -> dict:
    os.makedirs(output_dir, exist_ok=True)
    pdf_addresses = extract_addresses(read_pdf_text(pdf_path))
    ref_df = pd.read_csv(ref_path)
    out_df, not_found = match_addresses(pdf_addresses, ref_df)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_csv_path = os.path.join(output_dir, f"route_{stamp}.csv")
    preferred_cols = ["order", "source_addr", "match", "score", ADDRESS_COL, "lat", "lon"]
    cols = [c for c in preferred_cols if c in out_df.columns] + [c for c in out_df.columns if c not in preferred_cols]
    (out_df[cols] if not out_df.empty else out_df).to_csv(out_csv_path, index=False)
    return {"found_count": len(out_df), "total_count": len(pdf_addresses), "not_found": not_found, "output_file": out_csv_path}
 ← возьмите код из холста: «Импорт из вашего PDF + результат в формате маршрута (универсально)»
# (если уже вставили ранее — повторно ничего делать не нужно)
