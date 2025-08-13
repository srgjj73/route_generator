from __future__ import annotations
import os
import re
from typing import List, Tuple, Optional

import pandas as pd
from PyPDF2 import PdfReader
from rapidfuzz import fuzz, process as rf_process
from datetime import datetime

# === Настройки под ваш PDF ===
MIN_FUZZ = 86  # порог похожести повыше для названий компаний
CITY_UPPER_HINT = True  # города часто В КАПСЕ (TURKU, KAARINA, RAISIO)
KNOWN_CITIES = {"TURKU", "KAARINA", "RAISIO", "NAANTALI", "PIISPANRISTI"}

# Кандидаты названий колонок в справочнике
ADDRESS_CANDIDATES = [
    "address", "адрес", "адреса", "addr", "street", "улица", "улиц", "stop address", "location",
    "address1", "address_line", "address_line1", "addr1"
]
NAME_CANDIDATES = [
    "name", "название", "наименование", "company", "firma", "client", "recipient", "odbiorca",
    "shop", "customer", "nazwa", "nazwa odbiorcy", "получатель"
]
CITY_CANDIDATES = ["city", "город", "miasto"]

# Запасной регэксп «адресоподобных» строк (если вдруг в PDF будут адреса)
ADDR_RE = re.compile(r"\\b(ul\\.|ulica|street|st\\.|ave\\.|avenue|road|rd\\.|blvd\\.|lane|ln\\.|pr\\.|prospekt|sh\\.|szosa|per\\.|pereulok|bulwar|plac)\\s+[^\\n,]+?", re.IGNORECASE)


def read_pdf_lines(pdf_path: str) -> List[str]:
    reader = PdfReader(pdf_path)
    lines: List[str] = []
    for page in reader.pages:
        text = page.extract_text() or ""
        for raw in text.splitlines():
            s = raw.strip()
            if s:
                lines.append(s)
    return lines


def is_city_token(s: str) -> bool:
    s_clean = re.sub(r"[^A-Za-zÅÄÖåäöÉÓÁÍÚŁŚŻŹĆŃÄÖÜÕÄÅÆØÄ-]", " ", s).strip()
    if not s_clean:
        return False
    if CITY_UPPER_HINT and s_clean.upper() == s_clean and any(c.isalpha() for c in s_clean):
        return True
    if s_clean.upper() in KNOWN_CITIES:
        return True
    return False


def extract_name_city(lines: List[str]) -> List[dict]:
    """Возвращаем список объектов: {"key": "<name>, <CITY>", "name": name, "city": city}"""
    results: List[dict] = []
    i = 0
    buf_name: List[str] = []

    while i < len(lines):
        line = lines[i]
        # пропускаем служебные строки (поля шапки в польских бланках)
        if re.search(r"^(List transportowy|Magazyn/Skład|Suma paczek|Numer|paczki|spedycji|Nazwa odbiorcy|Miasto|Numery paczek|Waga|Ilość paczek|Arkusz/List)", line, re.IGNORECASE):
            i += 1; continue

        if not is_city_token(line):
            # пропускаем строки с номерами посылок/кодами
            if re.fullmatch(r"[P\d ,;]+", line):
                i += 1; continue
            buf_name.append(line)
            # <name> + <CITY>
            if i + 1 < len(lines) and is_city_token(lines[i + 1]):
                name = re.sub(r"\\s{2,}", " ", " ".join(buf_name)).strip()
                city = lines[i + 1].strip()
                results.append({"key": f"{name}, {city}", "name": name, "city": city})
                buf_name.clear(); i += 2; continue
            # двухстрочное имя: <name1> <name2> + <CITY>
            if i + 2 < len(lines) and not is_city_token(lines[i + 1]) and is_city_token(lines[i + 2]):
                name = re.sub(r"\\s{2,}", " ", f"{line} {lines[i+1]}").strip()
                city = lines[i + 2].strip()
                results.append({"key": f"{name}, {city}", "name": name, "city": city})
                buf_name.clear(); i += 3; continue
            i += 1; continue
        else:
            i += 1; continue

    if not results:
        text = "\n".join(lines)
        for m in ADDR_RE.finditer(text):
            a = m.group(0).strip()
            results.append({"key": a, "name": a, "city": ""})

    # нормализация пробелов/знаков
    for r in results:
        r["key"] = re.sub(r"\\s+", " ", r["key"]).strip(",. ")
        r["name"] = re.sub(r"\\s+", " ", r["name"]).strip(",. ")
        r["city"] = r["city"].strip(",. ")
    return results


def read_csv_robust(path: str) -> pd.DataFrame:
    for params in (
        dict(sep=None, engine='python'),
        dict(sep=';'),
        dict(sep=','),
        dict(sep='\t'),
    ):
        try:
            return pd.read_csv(path, **params)
        except Exception:
            continue
    return pd.read_csv(path)


def _norm_name(name: str) -> str:
    return re.sub(r"[^a-zа-я0-9]", "", str(name).lower())


def detect_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    cols = list(df.columns)
    norm_map = {c: _norm_name(c) for c in cols}
    cand_norms = [_norm_name(x) for x in candidates]

    for c, n in norm_map.items():
        if n in cand_norms:
            return c
    for c, n in norm_map.items():
        if any(key in n for key in cand_norms):
            return c
    # эвристика: длинная текстовая колонка
    for c in cols:
        if df[c].dtype == object:
            s = df[c].astype(str).dropna()
            if not s.empty and s.str.len().mean() > 10:
                return c
    return None


def match_generic(keys_from_pdf: List[dict], ref_df: pd.DataFrame, ref_col: str) -> Tuple[pd.DataFrame, List[str]]:
    ref_vals = ref_df[ref_col].astype(str).fillna("")
    ref_list = ref_vals.tolist()

    rows = []
    not_found: List[str] = []

    for idx, item in enumerate(keys_from_pdf, start=1):
        key = item["key"]
        best = rf_process.extractOne(key, ref_list, scorer=fuzz.WRatio)
        if best:
            best_str, score, ref_idx = best
            if score >= MIN_FUZZ:
                row = ref_df.iloc[ref_idx].to_dict()
                row.update({
                    "order": idx,
                    "source_key": key,
                    "name_pdf": item.get("name", ""),
                    "city_pdf": item.get("city", ""),
                    "match": best_str,
                    "score": score,
                })
                rows.append(row)
            else:
                not_found.append(key)
        else:
            not_found.append(key)

    return pd.DataFrame(rows), not_found


def process_route(pdf_path: str, ref_path: str, output_dir: str) -> dict:
    os.makedirs(output_dir, exist_ok=True)

    # 1) Ключи из PDF
    lines = read_pdf_lines(pdf_path)
    keys = extract_name_city(lines)

    # 2) Справочник
    ref_df = read_csv_robust(ref_path)

    # 3) Выбор колонки для сопоставления
    addr_col = detect_col(ref_df, ADDRESS_CANDIDATES)
    name_col = detect_col(ref_df, NAME_CANDIDATES)
    city_col = detect_col(ref_df, CITY_CANDIDATES)

    used_col = None
    if addr_col:
        used_col = addr_col
    elif name_col:
        used_col = name_col
    else:
        raise ValueError(
            "Не нашёл колонку для сопоставления. Добавьте в справочник колонку адреса (address/Адрес) или названия (name/company/odbiorca)."
            f" Найдены колонки: {list(ref_df.columns)}"
        )

    out_df, not_found = match_generic(keys, ref_df, used_col)

    # 4) Переименуем часто встречающиеся колонки
    rename_map = {}
    if name_col and name_col in out_df.columns:
        rename_map[name_col] = "name_ref"
    if addr_col and addr_col in out_df.columns:
        rename_map[addr_col] = "address_ref"
    if city_col and city_col in out_df.columns:
        rename_map[city_col] = "city_ref"
    if rename_map:
        out_df = out_df.rename(columns=rename_map)

    # 5) Сохранение результата
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_csv_name = f"route_{stamp}.csv"
    out_csv_path = os.path.join(output_dir, out_csv_name)

    preferred = [
        "order", "source_key", "name_pdf", "city_pdf",
        "name_ref", "address_ref", "city_ref", "match", "score",
        "lat", "lon"
    ]
    cols = [c for c in preferred if c in out_df.columns] + [c for c in out_df.columns if c not in preferred]
    (out_df[cols] if not out_df.empty else out_df).to_csv(out_csv_path, index=False)

    return {
        "found_count": len(out_df),
        "total_count": len(keys),
        "not_found": [k["key"] for k in keys if k["key"] not in set(out_df["source_key"])],
        "output_file": out_csv_path,
    }
