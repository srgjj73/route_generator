import pdfplumber
import pandas as pd
import re
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
import difflib
import os
import holidays

def _fi_holidays(year: int):
    return holidays.FI(years=year)

def _is_working_day_fi(d: date) -> bool:
    if d.weekday() >= 5:
        return False
    return d not in _fi_holidays(d.year)

def _next_working_day_fi(from_date: date) -> date:
    d = from_date + timedelta(days=1)
    while not _is_working_day_fi(d):
        d += timedelta(days=1)
    return d

def _route_ordinal_for_next_workday(today_hel: date) -> int:
    target = _next_working_day_fi(today_hel)
    start = target.replace(day=1)
    n = 0
    d = start
    while d <= target:
        if _is_working_day_fi(d):
            n += 1
        d += timedelta(days=1)
    return n

def _normalize_address(text: str) -> str:
    text = str(text).lower().strip()
    if " - " in text:
        text = text.split(" - ")[0].strip()
    text = re.sub(r"\b(oy|ag|ltd|inc|llc)\b", "", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()

def _find_best_match(entry_name: str, ref_addresses: pd.Series, threshold: float = 0.70) -> tuple:
    norm_entry = _normalize_address(entry_name)
    entry_words = norm_entry.split()
    if not entry_words:
        return None, 0
    
    first_word = None
    for word in entry_words:
        if len(word) > 1:
            first_word = word
            break
    if not first_word:
        return None, 0
    
    for idx, ref_addr in enumerate(ref_addresses):
        norm_ref = _normalize_address(str(ref_addr))
        if norm_entry == norm_ref:
            return ref_addresses.index[idx], 1.0
    
    for idx, ref_addr in enumerate(ref_addresses):
        norm_ref = _normalize_address(str(ref_addr))
        if first_word not in norm_ref:
            continue
        ratio = difflib.SequenceMatcher(None, norm_entry, norm_ref).ratio()
        if ratio >= threshold:
            return ref_addresses.index[idx], ratio
    
    return None, 0

def _extract_pdf_entries(pdf_path: str) -> list:
    entries = []
    with pdfplumber.open(pdf_path) as pdf:
        full_text = ""
        for page in pdf.pages:
            full_text += page.extract_text() or ""
    
    lines = full_text.split("\n")
    
    for line in lines:
        line = line.strip()
        if not line or len(line) < 10:
            continue
        
        if re.match(r"^\s*(arkusz|list|suma|numer|nazwa|auto partner|magazyn|trasa|\+48)", line.lower()) and not re.search(r"P\d{8,}", line):
            continue
        
        weight_match = re.search(r"([\d.,]+)\s+(\d+)\s*$", line)
        if not weight_match:
            continue
        
        try:
            weight = float(weight_match.group(1).replace(",", "."))
            qty = int(weight_match.group(2))
            if weight <= 0 or qty <= 0:
                continue
            
            before_weight = line[:weight_match.start()].strip()
            
            if not before_weight or len(before_weight) < 3:
                continue
            
            name = before_weight
            name = re.sub(r"^Arkusz.*?(?=\w)", "", name, flags=re.IGNORECASE).strip()
            name = re.sub(r"^\d+\s+", "", name).strip()
            name = re.sub(r"\s+P\d{8,}.*$", "", name).strip()
            name = re.sub(r"\s+(TURKU|Turku|KAARINA|Kaarina|RAISIO|Raisio|NAANTALI|Naantali|PIISPANRISTI|Piispanristi|LIETO|Lieto|LITTOINEN|Littoinen)\s*$", "", name, flags=re.IGNORECASE).strip()
            name = re.sub(r"[lL]ist:\s*\d+/\d+", "", name).strip()
            
            if name and len(name) > 2:
                entries.append({"original_name": name, "weight": weight, "qty": qty})
        except (ValueError, AttributeError):
            continue
    
    return entries

def process_route(pdf_path: str, ref_path: str, output_dir: str) -> dict:
    os.makedirs(output_dir, exist_ok=True)
    today_hel = datetime.now(ZoneInfo("Europe/Helsinki")).date()
    route_num = _route_ordinal_for_next_workday(today_hel)
    
    spravochnik = pd.read_csv(ref_path)
    required_cols = ["Address Line 1", "Address Line 2", "City", "Postal Code"]
    missing = [c for c in required_cols if c not in spravochnik.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}")
    
    entries = _extract_pdf_entries(pdf_path)
    if not entries:
        raise ValueError(f"No data extracted")
    
    output_rows, not_found, total_qty, total_weight = [], [], 0, 0.0
    
    for e in entries:
        best_idx, ratio = _find_best_match(e["original_name"], spravochnik["Address Line 1"])
        if best_idx is not None:
            stop_data = spravochnik.loc[best_idx]
            qty = e["qty"]
            weight = e["weight"]
            output_rows.append({
                "Address Line 1": stop_data.get("Address Line 1", ""),
                "Address Line 2": stop_data.get("Address Line 2", ""),
                "City": stop_data.get("City", ""),
                "Postal Code": stop_data.get("Postal Code", ""),
                "Extra info": str(qty) + " шт / " + str(weight) + " кг",
            })
            total_qty += qty
            total_weight += weight
        else:
            not_found.append(e["original_name"])
    
    filename = f"{route_num:02d}_{total_qty}шт_{round(total_weight, 2)}кг.csv"
    out_path = os.path.join(output_dir, filename)
    pd.DataFrame(output_rows).to_csv(out_path, index=False, encoding='utf-8')
    
    return {"found_count": len(output_rows), "total_count": len(entries), "not_found": not_found, "output_file": out_path}
