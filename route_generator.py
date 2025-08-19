import pdfplumber
import pandas as pd
import re
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
import difflib
import os

import holidays  # финские праздники: holidays.FI()  (pip install holidays)


# ---------- Рабочие дни Финляндии ----------

def _fi_holidays(year: int):
    """Возвращает набор праздников Финляндии на год."""
    # библиотека holidays сама знает подвижные даты (Пасха и т.п.)
    return holidays.FI(years=year)

def _is_working_day_fi(d: date) -> bool:
    """Будний день пн–пт и не праздник Финляндии."""
    if d.weekday() >= 5:  # 5=суббота, 6=воскресенье
        return False
    return d not in _fi_holidays(d.year)

def _next_working_day_fi(from_date: date) -> date:
    """Следующий рабочий день Финляндии после from_date."""
    d = from_date + timedelta(days=1)
    while not _is_working_day_fi(d):
        d += timedelta(days=1)
    return d

def _route_ordinal_for_next_workday(today_hel: date) -> int:
    """
    Порядковый номер следующего рабочего дня в ТЕКУЩЕМ месяце.
    Пример: сегодня 13-й рабочий, значит следующий = 14.
    """
    target = _next_working_day_fi(today_hel)

    # считаем рабочие дни с 1-го числа месяца по target включительно
    start = target.replace(day=1)
    n = 0
    d = start
    while d <= target:
        if _is_working_day_fi(d):
            n += 1
        d += timedelta(days=1)
    return n


# ---------- Основная логика генерации ----------

def process_route(pdf_path: str, ref_path: str, output_dir: str) -> dict:
    """
    Главная функция, которую вызывает веб-приложение.
    Возвращает словарь: found_count, total_count, not_found, output_file.

    Изменения:
    - Больше НЕ извлекаем дату из имени/контента PDF.
    - Номер маршрута рассчитываем как порядковый номер СЛЕДУЮЩЕГО рабочего дня
      текущего месяца по календарю Финляндии (будни + исключение праздников).
    - Временная зона Europe/Helsinki.
    """
    os.makedirs(output_dir, exist_ok=True)

    # 1) Дата «сейчас» в Хельсинки и порядковый номер следующего рабочего дня
    today_hel = datetime.now(ZoneInfo("Europe/Helsinki")).date()
    route_num = _route_ordinal_for_next_workday(today_hel)

    # 2) Загружаем справочник
    spravochnik = pd.read_csv(ref_path)
    required_cols = ["Address Line 1", "Address Line 2", "City", "Postal Code"]
    missing = [c for c in required_cols if c not in spravochnik.columns]
    if missing:
        raise ValueError(f"В справочнике нет колонок: {missing}. Ожидаются: {required_cols}")

    # нормализованное имя для сопоставления
    spravochnik["norm_name"] = (
        spravochnik["Address Line 1"].astype(str).str.lower().str.strip()
    )

    # 3) Парсим PDF: строки формата
    #    "<имя>   P12345678   <вес>   <шт>"
    entries = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            for line in text.split("\n"):
                m = re.match(r"^(.*?)\s+(P\d{8,})\s+([0-9]+(?:[.,][0-9]+)?)\s+(\d{1,3})$", line)
                if not m:
                    continue
                name_part = m.group(1).strip()
                weight = float(m.group(3).replace(",", "."))
                qty = int(m.group(4))
                short_name = " ".join(name_part.split()[:3]).lower()
                entries.append({
                    "original_name": name_part,
                    "short_name": short_name,
                    "weight": weight,
                    "qty": qty,
                })

    # 4) Сопоставление со справочником
    output_rows = []
    not_found = []
    total_qty = 0
    total_weight = 0.0

    for e in entries:
        matches = difflib.get_close_matches(e["short_name"], spravochnik["norm_name"], n=1, cutoff=0.6)
        if matches:
            stop_data = spravochnik[spravochnik["norm_name"] == matches[0]].iloc[0]
            output_rows.append({
                "Address Line 1": stop_data.get("Address Line 1", ""),
                "Address Line 2": stop_data.get("Address Line 2", ""),
                "City": stop_data.get("City", ""),
                "Postal Code": stop_data.get("Postal Code", ""),
                "Extra info": f"{e['qty']} шт / {e['weight']} кг",
            })
            total_qty += e["qty"]
            total_weight += e["weight"]
        else:
            not_found.append(e["original_name"])

    # 5) Имя файла и сохранение
    filename = f"{route_num:02d}_{total_qty}шт_{round(total_weight, 2)}кг.csv"
    out_path = os.path.join(output_dir, filename)
    pd.DataFrame(output_rows).to_csv(out_path, index=False)

    return {
        "found_count": len(output_rows),
        "total_count": len(entries),
        "not_found": not_found,
        "output_file": out_path,
    }
