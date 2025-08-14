from fastapi import FastAPI, File, UploadFile, Form, Request, Depends, HTTPException, status
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
import secrets
import shutil
import os
import pandas as pd
import html
import traceback
import logging
from urllib.parse import quote, unquote
from route_generator import process_route

app = FastAPI()
logger = logging.getLogger("uvicorn.error")

# === Basic Auth ===
security = HTTPBasic()
AUTH_USER = os.getenv("BASIC_AUTH_USER", "")
AUTH_PASS = os.getenv("BASIC_AUTH_PASS", "")

def auth(creds: HTTPBasicCredentials = Depends(security)):
    if not AUTH_USER or not AUTH_PASS:
        return
    ok_user = secrets.compare_digest(creds.username, AUTH_USER)
    ok_pass = secrets.compare_digest(creds.password, AUTH_PASS)
    if not (ok_user and ok_pass):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Unauthorized",
            headers={"WWW-Authenticate": "Basic"},
        )

BASE_DIR = os.getenv("BASE_DIR", ".")
UPLOAD_DIR = os.path.join(BASE_DIR, "uploads")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
REF_DIR = os.path.join(BASE_DIR, "data", "references")

os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(REF_DIR, exist_ok=True)

last_result = None
last_error = None

def list_references():
    return [f for f in os.listdir(REF_DIR) if f.lower().endswith(".csv")]

def render_index():
    refs = list_references()
    refs_options = "".join([f"<option value='{r}'>{r}</option>" for r in refs])
    refs_list_html = "".join([f"<li>{r} — <a href='/view_reference/{quote(r)}'>✏ Редактировать</a></li>" for r in refs])

    error_block = f"""
    <div class='result-block' style='border:2px solid #d33;background:#fff2f2;padding:15px;margin-top:20px;'>
      <h2>⚠ Ошибка</h2>
      <pre style='white-space:pre-wrap'>{html.escape(last_error)}</pre>
    </div>""" if last_error else ""

    result_block = ""
    if last_result:
        not_found_html = "<br>".join(last_result["not_found"]) if last_result["not_found"] else "Все адреса найдены."
        filename = os.path.basename(last_result["output_file"])
        filename_q = quote(filename)
        result_block = f"""
        <div class=\"result-block\" style=\"border:2px solid #ccc;padding:15px;margin-top:20px;background:#f9f9f9\">
            <h2>Результат генерации:</h2>
            <p>Найдено: {last_result["found_count"]} из {last_result["total_count"]}</p>
            <h3>Не найдено:</h3>
            <p>{not_found_html}</p>
            <button style=\"margin-bottom:10px;\" onclick=\"window.location.href='/edit_route/{filename_q}'\">✏ Редактировать</button>
            <a href=\"/download/{filename_q}\" download><button>📥 Скачать CSV</button></a>
        </div>
        """

    return f"""
    <html>
    <head>
        <title>Генератор маршрутов</title>
        <meta name=\"viewport\" content=\"width=device-width, initial-scale=1, user-scalable=no\">
        <style>
            body {{ font-family: sans-serif; padding: 20px; font-size: 16px; }}
            input, button, select {{ margin: 8px 0; padding: 12px; font-size: 16px; width: 100%; }}
            button {{ background: #007bff; color: white; border: none; border-radius: 5px; }}
            button:hover {{ background: #0056b3; }}
            .result-block {{ border: 2px solid #ccc; padding: 15px; margin-top: 20px; background: #f9f9f9; }}
            pre {{ margin:0; }}
            ul {{ padding-left: 18px; }}
        </style>
    </head>
    <body>
        <h1>Генератор маршрутов доставки</h1>
        <form action=\"/process\" method=\"post\" enctype=\"multipart/form-data\">
            <label>Транспортный лист (PDF):</label>
            <input type=\"file\" name=\"pdf_file\" accept=\".pdf\" required>

            <label>Выберите справочник:</label>
            <select name=\"reference_file\" required>
                {refs_options}
            </select>

            <button type=\"submit\">Сгенерировать маршрут</button>
        </form>

        {error_block}
        {result_block}

        <hr>
        <h2>Добавить новый справочник</h2>
        <form action=\"/upload_reference\" method=\"post\" enctype=\"multipart/form-data\">
            <input type=\"file\" name=\"ref_file\" accept=\".csv\" required>
            <button type=\"submit\">Загрузить справочник</button>
        </form>

        <hr>
        <h2>Справочники:</h2>
        <ul>{refs_list_html}</ul>
    </body>
    </html>
    """

@app.get("/", response_class=HTMLResponse)
async def index(_: HTTPBasicCredentials = Depends(auth)):
    return HTMLResponse(render_index())

@app.post("/upload_reference", response_class=HTMLResponse)
async def upload_reference(ref_file: UploadFile = File(...), _: HTTPBasicCredentials = Depends(auth)):
    save_path = os.path.join(REF_DIR, os.path.basename(ref_file.filename))
    with open(save_path, "wb") as buffer:
        shutil.copyfileobj(ref_file.file, buffer)
    return HTMLResponse(render_index())

@app.post("/process", response_class=HTMLResponse)
async def process(pdf_file: UploadFile = File(...), reference_file: str = Form(...), _: HTTPBasicCredentials = Depends(auth)):
    global last_result, last_error
    last_error = None
    try:
        pdf_path = os.path.join(UPLOAD_DIR, os.path.basename(pdf_file.filename))
        with open(pdf_path, "wb") as buffer:
            shutil.copyfileobj(pdf_file.file, buffer)

        ref_path = os.path.join(REF_DIR, reference_file)
        if not os.path.exists(ref_path):
            last_error = f"Справочник не найден: {ref_path}"
            return HTMLResponse(render_index())

        last_result = process_route(pdf_path, ref_path, OUTPUT_DIR)
        return HTMLResponse(render_index())
    except Exception as e:
        logger.exception("Route generation failed")
        last_error = f"{type(e).__name__}: {e}\n\n" + traceback.format_exc()
        return HTMLResponse(render_index())

@app.get("/view_reference/{filename:path}", response_class=HTMLResponse)
async def view_reference(filename: str, _: HTTPBasicCredentials = Depends(auth)):
    try:
        file_path = os.path.join(REF_DIR, unquote(filename))
        if not os.path.exists(file_path):
            return HTMLResponse("<h3>❌ Справочник не найден</h3><a href='/' >Назад</a>")
        df = pd.read_csv(file_path, encoding='utf-8', on_bad_lines='skip')
        table_html = "<table id='csvTable'>"
        table_html += "<tr>" + "".join(f"<th>{html.escape(str(col))}</th>" for col in df.columns) + "</tr>"
        for _, row in df.iterrows():
            table_html += "<tr>" + "".join(f"<td contenteditable='true'>{html.escape(str(val))}</td>" for val in row) + "</tr>"
        table_html += "</table>"
        return HTMLResponse(f"""
        <html><head><meta name='viewport' content='width=device-width, initial-scale=1'></head><body>
        <div class='top-bar'>
            <button onclick=\"saveCSV()\">💾 Сохранить</button>
            <a href='/'><button>⬅ Назад</button></a>
        </div>
        <h2>Редактирование: {html.escape(unquote(filename))}</h2>
        {table_html}
        <script>
        function saveCSV() {{
            let rows = Array.from(document.querySelectorAll('#csvTable tr')).map(tr => Array.from(tr.children).map(td => td.innerText.replace(/,/g,'')));
            fetch('/save_reference/{quote(filename)}', {{method:'POST', headers:{{'Content-Type':'text/csv'}}, body: rows.map(r=>r.join(',')).join('\n')}}).then(()=>alert('Сохранено'));
        }}
        </script>
        </body></html>""")
    except Exception as e:
        return HTMLResponse(f"<pre>Ошибка: {html.escape(str(e))}</pre>")

@app.post("/save_reference/{filename:path}")
async def save_reference(filename: str, request: Request, _: HTTPBasicCredentials = Depends(auth)):
    file_path = os.path.join(REF_DIR, unquote(filename))
    body = await request.body()
    with open(file_path, "wb") as f:
        f.write(body)
    return {"status": "ok"}

@app.get("/edit_route/{filename:path}", response_class=HTMLResponse)
async def edit_route(filename: str, _: HTTPBasicCredentials = Depends(auth)):
    try:
        file_path = os.path.join(OUTPUT_DIR, unquote(filename))
        if not os.path.exists(file_path):
            return HTMLResponse("<h3>❌ Файл маршрута не найден</h3><a href='/' >Назад</a>")
        df = pd.read_csv(file_path, encoding='utf-8', on_bad_lines='skip')
        table_html = "<table id='routeTable'>"
        table_html += "<tr>" + "".join(f"<th>{html.escape(str(col))}</th>" for col in df.columns) + "</tr>"
        for _, row in df.iterrows():
            table_html += "<tr>" + "".join(f"<td contenteditable='true'>{html.escape(str(val))}</td>" for val in row) + "</tr>"
        table_html += "</table>"
        return HTMLResponse(f"""
        <html><head><meta name='viewport' content='width=device-width, initial-scale=1'></head><body>
        <div class='top-bar'>
            <button onclick=\"downloadEdited()\">💾 Сохранить и скачать</button>
            <a href='/'><button>⬅ Назад</button></a>
        </div>
        <h2>Редактирование маршрута: {html.escape(unquote(filename))}</h2>
        {table_html}
        <script>
        function downloadEdited() {{
            let rows = Array.from(document.querySelectorAll('#routeTable tr')).map(tr => Array.from(tr.children).map(td => td.innerText.replace(/,/g,'')));
            let csv = rows.map(r=>r.join(',')).join('\n');
            let blob = new Blob([csv], {{type: 'text/csv'}});
            let url = URL.createObjectURL(blob);
            let a = document.createElement('a');
            a.href = url; a.download = '{html.escape(unquote(filename))}'; a.click();
        }}
        </script>
        </body></html>""")
    except Exception as e:
        return HTMLResponse(f"<pre>Ошибка: {html.escape(str(e))}</pre>")

@app.get("/download/{filename:path}")
async def download(filename: str, _: HTTPBasicCredentials = Depends(auth)):
    file_path = os.path.join(OUTPUT_DIR, unquote(filename))
    if os.path.exists(file_path):
        return FileResponse(path=file_path, filename=unquote(filename), media_type='text/csv')
    return {"error": "Файл не найден"}
