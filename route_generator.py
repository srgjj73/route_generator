import pdfplumber
import pandas as pd
import re
from datetime import date, timedelta
import difflib
import os


def _extract_route_date_from_name(pdf_path: str) -> date:
    name = os.path.basename(pdf_path)
    m = re.search(r"(\d{2})_(\d{2})_(\d{4})", name)
    if not m:
        raise ValueError("❌ Не удалось извлечь дату из имени PDF-файла! Ожидаю шаблон DD_MM_YYYY в имени.")
    day, month, year = map(int, m.groups())
    return date(year, month, day)


def _calc_route_num(route_date: date) -> int:
    # колиство будних дней от route_date до конца месяца + 1
    last_day = (route_date.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
    route_num = 0
    d = route_date
    while d < last_day:
        d += timedelta(days=1)
        if d.weekday() < 5:
            route_num += 1
    return route_num + 1  # маршрут в последний день месяца — №1


def process_route(pdf_path: str, ref_path: str, output_dir: str) -> dict:
    """Главная функция, которую вызывает веб-приложение.
    Возвращает словарь: found_count, total_count, not_found, output_file.
    """
    os.makedirs(output_dir, exist_ok=True)

    # 🗓️ Дата и номер маршрута из имени PDF
    route_date = _extract_route_date_from_name(pdf_path)
    route_num = _calc_route_num(route_date)

    # 📘 Загружаем справочник
    spravochnik = pd.read_csv(ref_path)
    # Проверим обязательные колонки
    required_cols = ["Address Line 1", "Address Line 2", "City", "Postal Code"]
    missing = [c for c in required_cols if c not in spravochnik.columns]
    if missing:
        raise ValueError(f"В справочнике нет колонок: {missing}. Ожидаются: {required_cols}")

    # нормализованное имя для сопоставления
    spravochnik["norm_name"] = spravochnik["Address Line 1"].astype(str).str.lower().str.strip()

    # 📄 Парсим PDF
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

    # 🔍 Сопоставление со справочником
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

    # 💾 Имя и сохранение файла
    filename = f"{route_num:02d}_{total_qty}шт_{round(total_weight, 2)}кг.csv"
    out_path = os.path.join(output_dir, filename)
    pd.DataFrame(output_rows).to_csv(out_path, index=False)

    return {
        "found_count": len(output_rows),
        "total_count": len(entries),
        "not_found": not_found,
        "output_file": out_path,
    }
