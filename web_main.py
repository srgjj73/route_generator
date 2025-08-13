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
from route_generator import process_route  # ваша логика генерации маршрута

app = FastAPI()
logger = logging.getLogger("uvicorn.error")

# === Basic Auth ===
security = HTTPBasic()
AUTH_USER = os.getenv("BASIC_AUTH_USER", "")
AUTH_PASS = os.getenv("BASIC_AUTH_PASS", "")

def auth(creds: HTTPBasicCredentials = Depends(security)):
    # Если переменные не заданы — доступа без пароля (удобно локально)
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

# === Пути под Render (персистентный диск) ===
BASE_DIR = os.getenv("BASE_DIR", ".")  # на Render = /data через .render.yaml
UPLOAD_DIR = os.path.join(BASE_DIR, "uploads")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
REF_DIR = os.path.join(BASE_DIR, "data", "references")

os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(REF_DIR, exist_ok=True)

# Храним последний результат/ошибку в памяти
last_result = None
last_error = None


def list_references():
    return [f for f in os.listdir(REF_DIR) if f.lower().endswith(".csv")]


def render_index():
    refs = list_references()
    refs_options = "".join([f"<option value='{r}'>{r}</option>" for r in refs])
    refs_list_html = "".join(
        [f"<li>{r} — <a href='/view_reference/{r}'>✏ Редактировать</a></li>" for r in refs]
    )

    error_block = f"""
    <div class='result-block' style='border:2px solid #d33;background:#fff2f2;padding:15px;margin-top:20px;'>
      <h2>⚠ Ошибка</h2>
      <pre style='white-space:pre-wrap'>{html.escape(last_error)}</pre>
    </div>""" if last_error else ""

    result_block = ""
    if last_result:
        not_found_html = "<br>".join(last_result["not_found"]) if last_result["not_found"] else "Все адреса найдены."
        filename = os.path.basename(last_result["output_file"])
        result_block = f"""
        <div class="result-block" style="border:2px solid #ccc;padding:15px;margin-top:20px;background:#f9f9f9">
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

        {error_block}
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


@app.get("/view_reference/{filename}", response_class=HTMLResponse)
async def view_reference(filename: str, _: HTTPBasicCredentials = Depends(auth)):
    file_path = os.path.join(REF_DIR, filename)
    if not os.path.exists(file_path):
        return HTMLResponse("<h3>❌ Справочник не найден</h3><a href='/' >Назад</a>")

    df = pd.read_csv(file_path)
    table_html = "<table id='csvTable'>"
    table_html += "<tr>" + "".join(f"<th>{col}</th>" for col in df.columns) + "</tr>"
    for _, row in df.iterrows():
        table_html += "<tr>" + "".join(f"<td contenteditable='true'>{val}</td>" for val in row) + "</tr>"
    table_html += "</table>"

    return HTMLResponse(f"""
    <html>
    <head>
        <title>Редактирование {filename}</title>
        <meta name=\"viewport\" content=\"width=device-width, initial-scale=1, user-scalable=no\">
        <style>
            body {{ margin:0; padding:10px; font-size:16px; }}
            table {{ border-collapse: collapse; width: 100%; font-size:16px; }}
            th, td {{ padding: 8px; border: 1px solid #ccc; white-space: nowrap; }}
            th {{ background: #f0f0f0; position: sticky; top: 40px; z-index: 2; }}
            button {{ padding: 14px; font-size: 18px; border: none; border-radius: 5px; }}
            .top-bar {{ position: sticky; top: 0; background: white; padding: 10px; z-index: 3; display: flex; gap: 10px; }}
            .btn-save {{ background: #28a745; color: white; flex: 1; }}
            .btn-back {{ background: #6c757d; color: white; flex: 1; }}
            #searchInput {{ flex: 2; padding: 10px; font-size: 16px; }}
        </style>
    </head>
    <body>
        <div class=\"top-bar\">
            <input type=\"text\" id=\"searchInput\" placeholder=\"Поиск...\" onkeyup=\"searchTable()\">
            <button class=\"btn-save\" onclick=\"saveCSV()\">💾 Сохранить</button>
            <a href=\"/\"><button class=\"btn-back\">⬅ Назад</button></a>
        </div>
        <h2>Редактирование: {filename}</h2>
        {table_html}

        <script>
        function searchTable() {{
            let input = document.getElementById('searchInput').value.toLowerCase();
            let rows = document.querySelectorAll('#csvTable tr');
            rows.forEach((row, index) => {{
                if (index === 0) return;
                let cells = row.querySelectorAll('td');
                let found = false;
                cells.forEach(cell => {{
                    if (cell.innerText.toLowerCase().includes(input)) {{
                        found = true;
                        cell.style.background = 'yellow';
                    }} else {{
                        cell.style.background = '';
                    }}
                }});
                row.style.display = found || input === "" ? "" : "none";
            }});
        }}

        function saveCSV() {{
            let table = document.getElementById('csvTable');
            let rows = table.querySelectorAll('tr');
            let csv = [];
            rows.forEach(row => {{
                let cols = row.querySelectorAll('th, td');
                let rowData = [];
                cols.forEach(col => {{
                    rowData.push(col.innerText.replace(/,/g, ''));
                }});
                csv.push(rowData.join(','));
            }});
            fetch('/save_reference/{filename}', {{
                method: 'POST',
                headers: {{ 'Content-Type': 'text/csv' }},
                body: csv.join('\n')
            }}).then(() => {{
                alert('Справочник сохранен!');
            }});
        }}
        </script>
    </body>
    </html>
    """)


@app.post("/save_reference/{filename}")
async def save_reference(filename: str, request: Request, _: HTTPBasicCredentials = Depends(auth)):
    file_path = os.path.join(REF_DIR, filename)
    body = await request.body()
    with open(file_path, "wb") as f:
        f.write(body)
    return {"status": "ok"}


@app.get("/edit_route/{filename}", response_class=HTMLResponse)
async def edit_route(filename: str, _: HTTPBasicCredentials = Depends(auth)):
    file_path = os.path.join(OUTPUT_DIR, filename)
    if not os.path.exists(file_path):
        return HTMLResponse("<h3>❌ Файл маршрута не найден</h3><a href='/' >Назад</a>")

    df = pd.read_csv(file_path)
    table_html = "<table id='routeTable'>"
    table_html += "<tr>" + "".join(f"<th>{col}</th>" for col in df.columns) + "</tr>"
    for _, row in df.iterrows():
        table_html += "<tr>" + "".join(f"<td contenteditable='true'>{val}</td>" for val in row) + "</tr>"
    table_html += "</table>"

    return HTMLResponse(f"""
    <html>
    <head>
        <title>Редактирование маршрута {filename}</title>
        <meta name=\"viewport\" content=\"width=device-width, initial-scale=1, user-scalable=no\">
        <style>
            body {{ margin:0; padding:10px; font-size:16px; }}
            table {{ border-collapse: collapse; width: 100%; font-size:16px; }}
            th, td {{ padding: 8px; border: 1px solid #ccc; white-space: nowrap; }}
            th {{ background: #f0f0f0; position: sticky; top: 40px; z-index: 2; }}
            button {{ padding: 14px; font-size: 18px; border: none; border-radius: 5px; }}
            .top-bar {{ position: sticky; top: 0; background: white; padding: 10px; z-index: 3; display: flex; gap: 10px; }}
            .btn-save {{ background: #28a745; color: white; flex: 1; }}
            .btn-back {{ background: #6c757d; color: white; flex: 1; }}
        </style>
    </head>
    <body>
        <div class=\"top-bar\">
            <button class=\"btn-save\" onclick=\"downloadEdited()\">💾 Сохранить и скачать</button>
            <a href=\"/\"><button class=\"btn-back\">⬅ Назад</button></a>
        </div>
        <h2>Редактирование маршрута: {filename}</h2>
        {table_html}

        <script>
        function downloadEdited() {{
            let table = document.getElementById('routeTable');
            let rows = table.querySelectorAll('tr');
            let csv = [];
            rows.forEach(row => {{
                let cols = row.querySelectorAll('th, td');
                let rowData = [];
                cols.forEach(col => {{
                    rowData.push(col.innerText.replace(/,/g, ''));
                }});
                csv.push(rowData.join(','));
            }});
            let blob = new Blob([csv.join('\\n')], { type: 'text/csv' });
            let url = window.URL.createObjectURL(blob);
            let a = document.createElement('a');
            a.setAttribute('href', url);
            a.setAttribute('download', '{filename}');
            a.click();
        }}
        </script>
    </body>
    </html>
    """)


@app.get("/download/{filename}")
async def download(filename: str, _: HTTPBasicCredentials = Depends(auth)):
    file_path = os.path.join(OUTPUT_DIR, filename)
    if os.path.exists(file_path):
        return FileResponse(path=file_path, filename=filename, media_type='text/csv')
    return {"error": "Файл не найден"}
