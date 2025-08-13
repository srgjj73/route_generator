from __future__ import annotations
import os
import re
from typing import List, Tuple

import pandas as pd
from PyPDF2 import PdfReader
from rapidfuzz import fuzz, process as rf_process
from datetime import datetime

# Настройки парсинга/сопоставления
ADDRESS_COL = "address"          # столбец со справочными адресами
MIN_FUZZ = 80                      # порог похожести (0..100)
CITY_HINTS = []                    # сюда можно добавить город/регион для фильтрации

# Простейший паттерн адреса: строка с улицей + номером. Подгоняйте под ваш формат.
# Примеры ловушек: "ул.", "улица", "просп.", "проспект", "ш.", "шоссе", "пер.", "переулок"
ADDR_RE = re.compile(r"\b(ул\.|улица|просп\.|проспект|ш\.|шоссе|пер\.|переулок|туп\.|б-р|бульвар|наб\.|набережная|пл\.|площадь|дорога|улиц)\s+[^\n,]+?(?:,?\s*д\.?\s*\d+[A-Za-zА-Яа-я/-]*)?", re.IGNORECASE)


def read_pdf_text(pdf_path: str) -> str:
    reader = PdfReader(pdf_path)
    chunks: List[str] = []
    for page in reader.pages:
        text = page.extract_text() or ""
        chunks.append(text)
    return "\n".join(chunks)


def extract_addresses(text: str) -> List[str]:
    """Достаём кандидатов адресов из текста PDF."""
    candidates = set()
    # По строкам — иногда адрес на одной строке
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        # Фильтр по подсказкам города (опционально)
        if CITY_HINTS and not any(h.lower() in line.lower() for h in CITY_HINTS):
            pass  # не отбрасываем жёстко — просто продолжаем
        # Регэксп по типам улиц
        for m in ADDR_RE.finditer(line):
            candidates.add(normalize_addr(m.group(0)))
    # fallback: если ничего не нашли — пробуем все непустые строки
    if not candidates:
        for line in text.splitlines():
            line = line.strip()
            if len(line) > 6:
                candidates.add(normalize_addr(line))
    return list(candidates)


def normalize_addr(s: str) -> str:
    s = s.strip()
    s = re.sub(r"\s+", " ", s)
    # уберём хвостовые запятые/точки
    s = s.strip(",. ")
    return s


def match_addresses(pdf_addrs: List[str], ref_df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    """Для каждого адреса из PDF ищем лучший матч в справочнике (ADDRESS_COL) по fuzzy."""
    if ADDRESS_COL not in ref_df.columns:
        raise ValueError(f"В справочнике нет столбца '{ADDRESS_COL}'")

    ref_addrs = ref_df[ADDRESS_COL].astype(str).fillna("")
    ref_list = ref_addrs.tolist()

    rows = []
    not_found = []

    for idx, addr in enumerate(pdf_addrs, start=1):
        # rapidfuzz отдаёт список лучших совпадений
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
    # Если в справочнике есть столбцы lat/lon — оставим их как есть. Иначе просто адреса/метаданные.
    return out_df, not_found


def process_route(pdf_path: str, ref_path: str, output_dir: str) -> dict:
    os.makedirs(output_dir, exist_ok=True)

    # 1) Текст из PDF
    text = read_pdf_text(pdf_path)

    # 2) Извлечь адреса
    pdf_addresses = extract_addresses(text)

    # 3) Загрузить справочник
    ref_df = pd.read_csv(ref_path)

    # 4) Сопоставить
    out_df, not_found = match_addresses(pdf_addresses, ref_df)

    # 5) Сохранить CSV
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_csv_name = f"route_{stamp}.csv"
    out_csv_path = os.path.join(output_dir, out_csv_name)

    # Удобный порядок столбцов
    preferred_cols = ["order", "source_addr", "match", "score", ADDRESS_COL, "lat", "lon"]
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
