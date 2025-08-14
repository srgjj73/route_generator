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

# === ВАЖНО: Экспортируемый объект приложения ===
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

# === Директории ===
BASE_DIR = os.getenv("BASE_DIR", ".")
UPLOAD_DIR = os.path.join(BASE_DIR, "uploads")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
REF_DIR = os.path.join(BASE_DIR, "data", "references")
for d in (UPLOAD_DIR, OUTPUT_DIR, REF_DIR):
    os.makedirs(d, exist_ok=True)

# === Кэш имён справочников ===
known_refs = set(f for f in os.listdir(REF_DIR) if f.lower().endswith('.csv'))

# === Стили/скрипты (без f-строк и .format в JS) ===
BASE_CSS = """
<style>
  :root { --btn:#0d6efd; --btn2:#6c757d; --ok:#28a745; }
  body{font-family:-apple-system,system-ui,Segoe UI,Roboto,sans-serif;margin:0;padding:16px;font-size:16px}
  .container{max-width:900px;margin:0 auto}
  input,button,select{margin:8px 0;padding:14px;font-size:18px;width:100%;box-sizing:border-box}
  button{background:var(--btn);color:#fff;border:none;border-radius:12px;box-shadow:0 2px 6px rgba(0,0,0,.1)}
  button:hover{filter:brightness(.95)}
  .btn-secondary{background:var(--btn2)}
  .btn-ok{background:var(--ok)}
  .bar{position:sticky;top:0;background:#fff;padding:10px;z-index:5;display:flex;gap:10px;align-items:center;box-shadow:0 2px 8px rgba(0,0,0,.06)}
  .row{display:flex;gap:10px}
  @media (max-width:640px){.row{flex-direction:column}}
  .result{border:2px solid #ccc;border-radius:12px;padding:16px;background:#f9f9f9;margin-top:16px}
  .error{border-color:#d33;background:#fff2f2}
  table{border-collapse:collapse;width:100%;font-size:16px}
  th,td{padding:10px;border:1px solid #ddd;white-space:nowrap}
  th{background:#f0f0f0;position:sticky;top:64px;z-index:2}
  .pill{display:inline-block;padding:6px 10px;border-radius:999px;background:#eef;margin:4px 0;font-size:14px}
  .muted{color:#666;font-size:14px}
  mark{background:yellow;padding:0 2px}
</style>
"""

BASE_JS = """
<script>
  function debounce(fn,ms){let t;return function(){clearTimeout(t);const a=arguments;const ctx=this;t=setTimeout(()=>fn.apply(ctx,a),ms)}}
  function csvEscape(v){v=(v??'').toString();if(v.includes('"')||v.includes(',')||v.includes('\n'))v='"'+v.replaceAll('"','""')+'"';return v}
  function tableToCSV(id){const rows=[...document.querySelectorAll('#'+id+' tr')];return rows.map(r=>[...r.children].map(td=>csvEscape(td.innerText))).map(r=>r.join(',')).join('\n')}
  function clearMarks(node){ node.querySelectorAll('mark').forEach(function(m){ m.replaceWith(m.textContent); }); }
  function escapeRegex(s){return s.replace(/[.*+?^${}()|[\]\\]/g,'\\$&')}
</script>
"""

# === Вспомогалки ===
def list_references():
    disk = set(f for f in os.listdir(REF_DIR) if f.lower().endswith('.csv'))
    global known_refs
    known_refs |= disk
    known_refs = set(r for r in known_refs if os.path.exists(os.path.join(REF_DIR, r)))
    return sorted(known_refs)

# === Главная страница ===
def render_index(last_error=None, last_result=None):
    refs = list_references()
    refs_options = "".join([f"<option value='{html.escape(r)}'>{html.escape(r)}</option>" for r in refs])
    refs_list_html = "".join([
        f"<li><span class='pill'>{html.escape(r)}</span> "
        f"<a href='/view_reference/{quote(r)}'>✏ Редактировать</a> "
        f"<form style='display:inline' method='post' action='/delete_reference/{quote(r)}'>"
        f"<button type='submit' class='btn-secondary'>🗑 Удалить</button></form></li>"
        for r in refs
    ])

    error_block = f"<div class='result error'><h2>⚠ Ошибка</h2><pre style='white-space:pre-wrap'>{html.escape(last_error)}</pre></div>" if last_error else ""

    result_block = ""
    if last_result:
        not_found_html = "<br>".join(html.escape(x) for x in last_result.get("not_found", [])) or "Все записи найдены."
        filename = os.path.basename(last_result["output_file"])
        filename_q = quote(filename)
        result_block_tpl = """
        <div class='result'>
          <h2>Результат генерации</h2>
          <p>Найдено: <b>{FOUND}</b> из <b>{TOTAL}</b></p>
          <h3>Не найдено</h3>
          <p>{NOT_FOUND}</p>
          <div class='row'>
            <button type='button' onclick="location.href='/edit_route/{FILE_Q}'">✏ Редактировать</button>
            <a class='nowrap' href="/download/{FILE_Q}" download><button type='button' class='btn-ok'>📥 Скачать CSV</button></a>
          </div>
        </div>
        """
        result_block = (result_block_tpl
                        .replace("{FOUND}", str(last_result['found_count']))
                        .replace("{TOTAL}", str(last_result['total_count']))
                        .replace("{NOT_FOUND}", not_found_html)
                        .replace("{FILE_Q}", filename_q))

    page_tpl = """
    <html><head><meta name='viewport' content='width=device-width, initial-scale=1'>
      {CSS}{JS}
    </head>
    <body>
      <div class='container'>
        <h1>Генератор маршрутов доставки</h1>
        <form action='/process' method='post' enctype='multipart/form-data'>
          <label>Транспортный лист (PDF):</label>
          <input type='file' name='pdf_file' accept='.pdf' required>
          <label>Выберите справочник:</label>
          <select name='reference_file' required>{REFS_OPTIONS}</select>
          <button type='submit'>Сгенерировать маршрут</button>
        </form>

        {ERROR_BLOCK}
        {RESULT_BLOCK}

        <hr>
        <h2>Справочники</h2>
        <ul>{REFS_LIST}</ul>

        <h3>Добавить справочник</h3>
        <form action='/upload_reference' method='post' enctype='multipart/form-data'>
          <input type='file' name='ref_file' accept='.csv' required>
          <button type='submit' class='btn-ok'>Загрузить справочник</button>
          <div class='muted'>После загрузки файл остаётся в памяти приложения и на диске до удаления.</div>
        </form>
      </div>
    </body></html>
    """
    return (page_tpl
            .replace("{CSS}", BASE_CSS)
            .replace("{JS}", BASE_JS)
            .replace("{REFS_OPTIONS}", refs_options)
            .replace("{ERROR_BLOCK}", error_block)
            .replace("{RESULT_BLOCK}", result_block or "")
            .replace("{REFS_LIST}", refs_list_html or '<span class=muted>Пока нет загруженных CSV</span>')
            )

# === Роуты ===
@app.get("/", response_class=HTMLResponse)
async def index(_: HTTPBasicCredentials = Depends(auth)):
    return HTMLResponse(render_index())

@app.post("/upload_reference", response_class=HTMLResponse)
async def upload_reference(ref_file: UploadFile = File(...), _: HTTPBasicCredentials = Depends(auth)):
    save_name = os.path.basename(ref_file.filename)
    save_path = os.path.join(REF_DIR, save_name)
    with open(save_path, "wb") as buffer:
        shutil.copyfileobj(ref_file.file, buffer)
    known_refs.add(save_name)
    return HTMLResponse(render_index())

@app.post("/delete_reference/{filename:path}")
async def delete_reference(filename: str, _: HTTPBasicCredentials = Depends(auth)):
    name = unquote(filename)
    path = os.path.join(REF_DIR, name)
    if os.path.exists(path):
        os.remove(path)
    known_refs.discard(name)
    return {"ok": True}

@app.post("/process", response_class=HTMLResponse)
async def process(pdf_file: UploadFile = File(...), reference_file: str = Form(...), _: HTTPBasicCredentials = Depends(auth)):
    try:
        pdf_path = os.path.join(UPLOAD_DIR, os.path.basename(pdf_file.filename))
        with open(pdf_path, "wb") as buffer:
            shutil.copyfileobj(pdf_file.file, buffer)
        ref_path = os.path.join(REF_DIR, reference_file)
        if not os.path.exists(ref_path):
            return HTMLResponse(render_index(last_error="Справочник не найден: " + ref_path))
        result = process_route(pdf_path, ref_path, OUTPUT_DIR)
        return HTMLResponse(render_index(last_result=result))
    except Exception as e:
        logger.exception("Route generation failed")
        return HTMLResponse(render_index(last_error=f"{type(e).__name__}: {e}\n\n" + traceback.format_exc()))

# === Редактор справочника ===
@app.get("/view_reference/{filename:path}", response_class=HTMLResponse)
async def view_reference(filename: str, _: HTTPBasicCredentials = Depends(auth)):
    try:
        file_path = os.path.join(REF_DIR, unquote(filename))
        if not os.path.exists(file_path):
            return HTMLResponse("<h3>❌ Справочник не найден</h3><a href='/' >Назад</a>")
        df = pd.read_csv(file_path, encoding='utf-8', on_bad_lines='skip')
        header = "<tr>" + "".join(f"<th>{html.escape(str(c))}</th>" for c in df.columns) + "</tr>"
        rows = "".join("<tr>" + "".join(f"<td contenteditable='true'>{html.escape(str(v))}</td>" for v in row) + "</tr>" for _, row in df.iterrows())
        table_html = "<table id='csvTable'>" + header + rows + "</table>"

        page_tpl = """
        <html><head><meta name='viewport' content='width=device-width, initial-scale=1'>
        {CSS}{JS}
        <script>
          function searchTable(){
            const q = document.getElementById('search').value.toLowerCase();
            const rows = document.querySelectorAll('#csvTable tr');
            rows.forEach((row, i)=>{
              if(i===0) return;
              let show=false;
              row.querySelectorAll('td').forEach(cell=>{
                clearMarks(cell);
                const txt=cell.innerText; const low=txt.toLowerCase();
                if(q && low.includes(q)){
                  show=true; const re=new RegExp(escapeRegex(q),'ig');
                  cell.innerHTML = txt.replace(re, function(m){ return '<mark>'+m+'</mark>'; });
                }
              });
              row.style.display = (!q || show) ? '' : 'none';
            });
          }
          async function saveCSV(){
            document.querySelectorAll('#csvTable td').forEach(td=>{ clearMarks(td); });
            const rows = Array.from(document.querySelectorAll('#csvTable tr'));
            const csv = rows.map(r=>Array.from(r.children).map(td=>{
              let v = (td.innerText||'');
              if(v.includes('"')||v.includes(',')||v.includes('\n')) v='"'+v.replaceAll('"','""')+'"';
              return v;
            }).join(',')).join('\n');
            const res = await fetch('/save_reference/{NAME_Q}', { method:'POST', headers:{'Content-Type':'text/csv;charset=utf-8'}, body: csv });
            if(res.ok) alert('Справочник сохранён'); else alert('Не удалось сохранить');
          }
          document.addEventListener('DOMContentLoaded', function(){
            const deb = debounce(searchTable, 120);
            const inp = document.getElementById('search');
            inp.addEventListener('input', deb);
            inp.addEventListener('keyup', deb);
          });
        </script>
        </head>
        <body>
          <div class='bar container'>
            <input id='search' placeholder='Поиск…' />
            <button id='btn-save' type='button' class='btn-ok' onclick='saveCSV()'>💾 Сохранить</button>
            <a href='/'><button type='button' class='btn-secondary'>⬅ Назад</button></a>
          </div>
          <div class='container'>
            <h2>Редактирование: {NAME_H}</h2>
            {TABLE}
          </div>
        </body></html>
        """
        page = (page_tpl
                .replace("{CSS}", BASE_CSS)
                .replace("{JS}", BASE_JS)
                .replace("{NAME_Q}", quote(filename))
                .replace("{NAME_H}", html.escape(unquote(filename)))
                .replace("{TABLE}", table_html)
                )
        return HTMLResponse(page)
    except Exception as e:
        return HTMLResponse(f"<pre>Ошибка: {html.escape(str(e))}</pre>")

@app.post("/save_reference/{filename:path}")
async def save_reference(filename: str, request: Request, _: HTTPBasicCredentials = Depends(auth)):
    file_path = os.path.join(REF_DIR, unquote(filename))
    body = await request.body()
    text = body.decode('utf-8', errors='replace')
    with open(file_path, 'w', encoding='utf-8', newline='\n') as f:
        f.write(text)
    return {"status": "ok"}

# === Редактор маршрута ===
@app.get("/edit_route/{filename:path}", response_class=HTMLResponse)
async def edit_route(filename: str, _: HTTPBasicCredentials = Depends(auth)):
    try:
        file_path = os.path.join(OUTPUT_DIR, unquote(filename))
        if not os.path.exists(file_path):
            return HTMLResponse("<h3>❌ Файл маршрута не найден</h3><a href='/' >Назад</a>")
        df = pd.read_csv(file_path, encoding='utf-8', on_bad_lines='skip')
        header = "<tr>" + "".join(f"<th>{html.escape(str(c))}</th>" for c in df.columns) + "</tr>"
        rows = "".join("<tr>" + "".join(f"<td contenteditable='true'>{html.escape(str(v))}</td>" for v in row) + "</tr>" for _, row in df.iterrows())
        table_html = "<table id='routeTable'>" + header + rows + "</table>"

        page_tpl = """
        <html><head><meta name='viewport' content='width=device-width, initial-scale=1'>
        {CSS}{JS}
        <script>
          function downloadEdited(){
            document.querySelectorAll('#routeTable td').forEach(td=>{ clearMarks(td); });
            const rows = Array.from(document.querySelectorAll('#routeTable tr'));
            const csv = rows.map(r=>Array.from(r.children).map(td=>{
              let v = (td.innerText||'');
              if(v.includes('"')||v.includes(',')||v.includes('\n')) v='"'+v.replaceAll('"','""')+'"';
              return v;
            }).join(',')).join('\n');
            const blob = new Blob(['\ufeff'+csv], { type:'text/csv;charset=utf-8;' });
            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.style.display='none'; a.href=url; a.download='{NAME_H}';
            document.body.appendChild(a); a.click();
            setTimeout(function(){ URL.revokeObjectURL(url); a.remove(); }, 500);
          }
        </script>
        </head>
        <body>
          <div class='bar container'>
            <button id='btn-download' type='button' class='btn-ok' onclick='downloadEdited()'>💾 Сохранить и скачать</button>
            <a href='/'><button type='button' class='btn-secondary'>⬅ Назад</button></a>
          </div>
          <div class='container'>
            <h2>Редактирование маршрута: {NAME_H}</h2>
            {TABLE}
          </div>
        </body></html>
        """
        page = (page_tpl
                .replace("{CSS}", BASE_CSS)
                .replace("{JS}", BASE_JS)
                .replace("{NAME_H}", html.escape(unquote(filename)))
                .replace("{TABLE}", table_html)
                )
        return HTMLResponse(page)
    except Exception as e:
        return HTMLResponse(f"<pre>Ошибка: {html.escape(str(e))}</pre>")

# === Скачать ===
@app.get("/download/{filename:path}")
async def download(filename: str, _: HTTPBasicCredentials = Depends(auth)):
    file_path = os.path.join(OUTPUT_DIR, unquote(filename))
    if os.path.exists(file_path):
        return FileResponse(path=file_path, filename=unquote(filename), media_type='text/csv')
    return {"error": "Файл не найден"}
