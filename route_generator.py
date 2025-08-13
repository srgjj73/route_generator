from __future__ import annotations
import os
import re
from typing import List, Tuple, Optional

import pandas as pd
from PyPDF2 import PdfReader
from rapidfuzz import fuzz, process as rf_process
from datetime import datetime

# Порог похожести для fuzzy‑сопоставления
MIN_FUZZ = 80

# Регэкспы для извлечения адресов из PDF (подправьте под свой формат при необходимости)
ADDR_RE = re.compile(r"\b(ул\.|улица|просп\.|проспект|ш\.|шоссе|пер\.|переулок|туп\.|б-р|бульвар|наб\.|набережная|пл\.|площадь|дорога|улиц|street|st\.|ave\.|avenue|road|rd\.|blvd\.|lane|ln\.)\s+[^\n,]+?(?:,?\s*д\.?\s*\d+[A-Za-zА-Яа-я/-]*)?", re.IGNORECASE)

# Возможные названия колонки адреса (разные языки/варианты)
ADDRESS_CANDIDATES = [
    "address", "адрес", "адреса", "addr", "street", "улица", "улиц", "stop address", "location",
    "address1", "address_line", "address_line1", "addr1"
]


def read_pdf_text(pdf_path: str) -> str:
    reader = PdfReader(pdf_path)
    chunks: List[str] = []
    for page in reader.pages:
        text = page.extract_text() or ""
        chunks.append(text)
    return "\n".join(chunks)


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
    s = s.strip()
    s = re.sub(r"\s+", " ", s)
    return s.strip(",. ")


def _norm_name(name: str) -> str:
    return re.sub(r"[^a-zа-я0-9]", "", name.lower())


def detect_address_col(df: pd.DataFrame) -> Optional[str]:
    cols = list(df.columns)
    norm_map = {c: _norm_name(str(c)) for c in cols}
    cand_norms = [_norm_name(x) for x in ADDRESS_CANDIDATES]

    # 1) Точное совпадение по нормализованному имени
    for c, n in norm_map.items():
        if n in cand_norms:
            return c

    # 2) Подстрока: если колонка содержит 'address'/'адрес'/'street'
    for c, n in norm_map.items():
        if any(key in n for key in ["address", "адрес", "street", "улиц", "location"]):
            return c

    # 3) Heuristic: берём первую текстовую колонку со средними значениями длины > 8
    for c in cols:
        if df[c].dtype == object:
            sample = df[c].astype(str).dropna()
            if not sample.empty and sample.str.len().mean() > 8:
                return c

    return None


def read_csv_robust(path: str) -> pd.DataFrame:
    # Пытаемся автоматически определить разделитель/кодировку
    # 1) sep=None + engine='python' часто угадывает , ; \t
    try:
        return pd.read_csv(path, sep=None, engine='python')
    except Exception:
        pass
    # 2) Явно через ';'
    try:
        return pd.read_csv(path, sep=';')
    except Exception:
        pass
    # 3) Кома
    return pd.read_csv(path)


def match_addresses(pdf_addrs: List[str], ref_df: pd.DataFrame, address_col: str) -> Tuple[pd.DataFrame, List[str]]:
    ref_addrs = ref_df[address_col].astype(str).fillna("")
    ref_list = ref_addrs.tolist()

    rows = []
    not_found = []

    for idx, addr in enumerate(pdf_addrs, start=1):
        best = rf_process.extractOne(addr, ref_list, scorer=fuzz.WRatio)
        if best:
            best_str, score, ref_idx = best
            if score >= MIN_FUZZ:
                row = ref_df.iloc[ref_idx].to_dict()
                row.update({
                    "order": idx,
                    "source_addr": addr,
                    "match": best_str,
                    "score": score,
                })
                rows.append(row)
            else:
                not_found.append(addr)
        else:
            not_found.append(addr)

    out_df = pd.DataFrame(rows)
    return out_df, not_found


def process_route(pdf_path: str, ref_path: str, output_dir: str) -> dict:
    os.makedirs(output_dir, exist_ok=True)

    # 1) Текст из PDF
    text = read_pdf_text(pdf_path)

    # 2) Адреса из PDF
    pdf_addresses = extract_addresses(text)

    # 3) Справочник — устойчивое чтение
    ref_df = read_csv_robust(ref_path)

    # 4) Автодетект колонки адреса
    addr_col = detect_address_col(ref_df)
    if not addr_col:
        raise ValueError(
            "Не удалось определить колонку с адресами. Переименуйте нужную колонку в 'address' или 'Адрес'"
            f". Найдены колонки: {list(ref_df.columns)}"
        )

    # 5) Сопоставление
    out_df, not_found = match_addresses(pdf_addresses, ref_df, addr_col)

    # 6) Сохранение результата
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_csv_name = f"route_{stamp}.csv"
    out_csv_path = os.path.join(output_dir, out_csv_name)

    preferred_cols = ["order", "source_addr", "match", "score", addr_col, "lat", "lon"]
    cols = [c for c in preferred_cols if c in out_df.columns] + [c for c in out_df.columns if c not in preferred_cols]
    (out_df[cols] if not out_df.empty else out_df).to_csv(out_csv_path, index=False)

    total = len(pdf_addresses)
    found = len(out_df)

    return {
        "found_count": found,
        "total_count": total,
        "not_found": not_found,
        "output_file": out_csv_path,
    }
