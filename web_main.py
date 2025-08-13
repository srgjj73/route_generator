# web_main.py
from fastapi import FastAPI, File, UploadFile, Form, Request
from fastapi.responses import FileResponse, HTMLResponse
import shutil
import os
import pandas as pd
from route_generator import process_route

app = FastAPI()

BASE_DIR = os.getenv("BASE_DIR", ".")
UPLOAD_DIR = os.path.join(BASE_DIR, "uploads")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
REF_DIR = os.path.join(BASE_DIR, "data", "references")

os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(REF_DIR, exist_ok=True)

last_result = None

def list_references():
    return [f for f in os.listdir(REF_DIR) if f.lower().endswith(".csv")]

def render_index():
    refs = list_references()
    refs_options = "".join([f"<option value='{r}'>{r}</option>" for r in refs])
    refs_list_html = "".join(
        [f"<li>{r} — <a href='/view_reference/{r}'>✏ Редактировать</a></li>" for r in refs]
    )
    result_block = ""
    if last_result:
        not_found_html = "<br>".join(last_result["not_found"]) if last_result["not_found"] else "Все адреса найдены."
        filename = os.path.basename(last_result["output_file"])
        result_block = f"""
        <div class="result-block">
            <h2>Результат генерации:</h2>
            <p>Найдено: {last_result["found_count"]} из {last_result["total_count"]}</p>
            <h3>Не найдено:</h3>
            <p>{not_found_html}</p>
            <button style=\"margin-bottom:10px;\" onclick=\"window.location.href='/edit_route/{filename}'\">✏ Редактировать</button>
            <a href="/download/{filename}" download><button>📥 Скачать CSV</button></a>
        </div>
        """
    return f"""
    <html>
    <head>
        <title>Генератор маршрутов</title>
        <meta name="viewport" content="width=device-width, initial-scale=1, user-scalable=no">
        <style>
            body {{ font-family: sans-serif; padding: 20px; font-size: 16px; }}
            input, button, select {{ margin: 8px 0; padding: 12px; font-size: 16px; width: 100%; }}
            button {{ background: #007bff; color: white; border: none; border-radius: 5px; }}
            button:hover {{ background: #0056b3; }}
            .result-block {{ border: 2px solid #ccc; padding: 15px; margin-top: 20px; background: #f9f9f9; }}
            table {{ border-collapse: collapse; width: 100%; }}
            th, td {{ padding: 6px; border: 1px solid #ccc; white-space: nowrap; }}
            th {{ background: #f0f0f0; }}
        </style>
    </head>
    <body>
        <h1>Генератор маршрутов доставки</h1>
        <form action="/process" method="post" enctype="multipart/form-data">
            <label>Транспортный лист (PDF):</label>
            <input type="file" name="pdf_file" accept=".pdf" required>
            <label>Выберите справочник:</label>
            <select name="reference_file" required>
                {refs_options}
            </select>
            <button type="submit">Сгенерировать маршрут</button>
        </form>
        {result_block}
        <hr>
        <h2>Добавить новый справочник</h2>
        <form action="/upload_reference" method="post" enctype="multipart/form-data">
            <input type="file" name="ref_file" accept=".csv" required>
            <button type="submit">Загрузить справочник</button>
        </form>
        <hr>
        <h2>Справочники:</h2>
        <ul>{refs_list_html}</ul>
    </body>
    </html>
    """

@app.get("/", response_class=HTMLResponse)
async def index():
    return HTMLResponse(render_index())

@app.post("/upload_reference", response_class=HTMLResponse)
async def upload_reference(ref_file: UploadFile = File(...)):
    save_path = os.path.join(REF_DIR, os.path.basename(ref_file.filename))
    with open(save_path, "wb") as buffer:
        shutil.copyfileobj(ref_file.file, buffer)
    return HTMLResponse(render_index())

@app.post("/process", response_class=HTMLResponse)
async def process(pdf_file: UploadFile = File(...), reference_file: str = Form(...)):
    global last_result
    pdf_path = os.path.join(UPLOAD_DIR, os.path.basename(pdf_file.filename))
    with open(pdf_path, "wb") as buffer:
        shutil.copyfileobj(pdf_file.file, buffer)
    ref_path = os.path.join(REF_DIR, reference_file)
    if not os.path.exists(ref_path):
        return HTMLResponse(render_index())
    last_result = process_route(pdf_path, ref_path, OUTPUT_DIR)
    return HTMLResponse(render_index())

@app.get("/view_reference/{filename}", response_class=HTMLResponse)
async def view_reference(filename: str):
    file_path = os.path.join(REF_DIR, filename)
    if not os.path.exists(file_path):
        return HTMLResponse("<h3>❌ Справочник не найден</h3><a href='/'>Назад</a>")
    df = pd.read_csv(file_path)
    table_html = "<table id='csvTable'>"
    table_html += "<tr>" + "".join(f"<th>{col}</th>" for col in df.columns) + "</tr>"
    for _, row in df.iterrows():
        table_html += "<tr>" + "".join(f"<td contenteditable='true'>{val}</td>" for val in row) + "</tr>"
    table_html += "</table>"
    return HTMLResponse(f"""...HTML for editing CSV...""")  # Сокращено для примера

@app.post("/save_reference/{filename}")
async def save_reference(filename: str, request: Request):
    file_path = os.path.join(REF_DIR, filename)
    body = await request.body()
    with open(file_path, "wb") as f:
        f.write(body)
    return {"status": "ok"}

@app.get("/edit_route/{filename}", response_class=HTMLResponse)
async def edit_route(filename: str):
    file_path = os.path.join(OUTPUT_DIR, filename)
    if not os.path.exists(file_path):
        return HTMLResponse("<h3>❌ Файл маршрута не найден</h3><a href='/'>Назад</a>")
    df = pd.read_csv(file_path)
    table_html = "<table id='routeTable'>"
    table_html += "<tr>" + "".join(f"<th>{col}</th>" for col in df.columns) + "</tr>"
    for _, row in df.iterrows():
        table_html += "<tr>" + "".join(f"<td contenteditable='true'>{val}</td>" for val in row) + "</tr>"
    table_html += "</table>"
    return HTMLResponse(f"""...HTML for editing route CSV...""")  # Сокращено

@app.get("/download/{filename}")
async def download(filename: str):
    file_path = os.path.join(OUTPUT_DIR, filename)
    if os.path.exists(file_path):
        return FileResponse(path=file_path, filename=filename, media_type='text/csv')
    return {"error": "Файл не найден"}
