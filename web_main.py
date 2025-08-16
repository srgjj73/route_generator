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
import base64, json, requests
from route_generator import process_route

app = FastAPI()

@app.on_event("startup")
async def _startup_sync():
    sync_refs_from_github()

logger = logging.getLogger("uvicorn.error")

# -- PDF in-memory cache until process restarts
PDF_CACHE = {}  # key: filename, value: bytes

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

# === Память справочников ===
known_refs = set(f for f in os.listdir(REF_DIR) if f.lower().endswith('.csv'))

# === GitHub persistence (optional) ===
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "")
GITHUB_REPO  = os.getenv("GITHUB_REPO", "")      # вида 'owner/name' (рекомендую отдельный репозиторий)
GITHUB_BRANCH= os.getenv("GITHUB_BRANCH", "main")
GITHUB_DIR   = os.getenv("GITHUB_DIR", "refs")   # папка в репозитории для CSV


# === Общие стили/скрипты (без f-строк и format) ===
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
  function csvEscape(v){v=(v??'').toString();if(v.includes('"')||v.includes(',')||v.includes('\\n'))v='"'+v.replaceAll('"','""')+'"';return v}
  function tableToCSV(id){const rows=[...document.querySelectorAll('#'+id+' tr')];return rows.map(r=>[...r.children].map(td=>csvEscape(td.innerText))).map(r=>r.join(',')).join('\\n')}
  function clearMarks(node){ node.querySelectorAll('mark').forEach(function(m){ m.replaceWith(m.textContent); }); }
  function escapeRegex(s){return s.replace(/[.*+?^${}()|[\\]\\\\]/g,'\\\\$&')}
</script>
"""

# === Утилиты ===

def gh_headers():
    if not GITHUB_TOKEN: return {}
    return {"Authorization": f"Bearer {GITHUB_TOKEN}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28"}

def gh_contents_url(path):
    return f"https://api.github.com/repos/{GITHUB_REPO}/contents/{path}"

def gh_get_sha(path):
    if not (GITHUB_TOKEN and GITHUB_REPO): return None
    r = requests.get(gh_contents_url(path), params={"ref": GITHUB_BRANCH}, headers=gh_headers(), timeout=20)
    if r.status_code==200:
        return r.json().get("sha")
    return None

def gh_upsert_text(path, text, message):
    if not (GITHUB_TOKEN and GITHUB_REPO): return False
    sha = gh_get_sha(path)
    payload = {
        "message": message,
        "content": base64.b64encode(text.encode("utf-8")).decode("ascii"),
        "branch": GITHUB_BRANCH
    }
    if sha: payload["sha"] = sha
    r = requests.put(gh_contents_url(path), headers=gh_headers(), json=payload, timeout=30)
    logger.info("GitHub upsert %s -> %s", path, r.status_code)
    return 200 <= r.status_code < 300

def gh_list_dir(path):
    if not (GITHUB_TOKEN and GITHUB_REPO): return []
    r = requests.get(gh_contents_url(path), params={"ref": GITHUB_BRANCH}, headers=gh_headers(), timeout=20)
    if r.status_code==200 and isinstance(r.json(), list):
        return [it for it in r.json() if it.get("type")=="file"]
    return []

def gh_download_text(path):
    # используем contents API, чтобы не возиться с raw
    r = requests.get(gh_contents_url(path), params={"ref": GITHUB_BRANCH}, headers=gh_headers(), timeout=20)
    if r.status_code==200 and r.json().get("encoding")=="base64":
        return base64.b64decode(r.json()["content"]).decode("utf-8", errors="replace")
    return None

def sync_refs_from_github():
    if not (GITHUB_TOKEN and GITHUB_REPO): 
        logger.info("GitHub sync skipped (no token/repo)")
        return
    try:
        os.makedirs(REF_DIR, exist_ok=True)
        items = gh_list_dir(GITHUB_DIR)
        cnt=0
        for it in items:
            name = it.get("name","")
            if not name.lower().endswith(".csv"): continue
            rel = f"{GITHUB_DIR}/{name}"
            txt = gh_download_text(rel)
            if txt is None: continue
            with open(os.path.join(REF_DIR, name), "w", encoding="utf-8", newline="\n") as f:
                f.write(txt)
            cnt+=1
        logger.info("GitHub sync: %d files", cnt)
    except Exception as e:
        logger.exception("GitHub sync failed: %s", e)


def list_references():
    disk = set(f for f in os.listdir(REF_DIR) if f.lower().endswith('.csv'))
    global known_refs
    known_refs |= disk
    known_refs = set(r for r in known_refs if os.path.exists(os.path.join(REF_DIR, r)))
    return sorted(known_refs)

# === Главная
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
        # без format и f-строк — подставим потом .replace(...)
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
    # push to GitHub if configured
    try:
        with open(save_path,'r',encoding='utf-8') as rf:
            gh_upsert_text(f"{GITHUB_DIR}/"+save_name, rf.read(), f"upload: {save_name}")
    except Exception as e:
        logger.exception("GitHub upsert on upload failed: %s", e)
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

# === Редактор справочника
@app.get("/view_reference/{filename:path}", response_class=HTMLResponse)
async def view_reference(filename: str, _: HTTPBasicCredentials = Depends(auth)):
    """
    Редактор справочника:
    - Добавление/удаление строк;
    - Поиск с подсветкой;
    - Сохранение CSV (с заголовком).
    Особенности:
    - новые строки добавляются ВВЕРХ;
    - NaN не выводятся (пусто);
    - значения вида 123.0 показываем как 123 (убираем ".0" у индексов и id).
    """
    try:
        file_path = os.path.join(REF_DIR, unquote(filename))
        if not os.path.exists(file_path):
            return HTMLResponse("<h3>❌ Справочник не найден</h3><a href='/' >Назад</a>")

        df = pd.read_csv(file_path, encoding='utf-8', on_bad_lines='skip')

        def _fmt(v):
            import math
            # NaN -> ''
            if v is None: return ""
            try:
                # pandas NaN/NaT
                if pd.isna(v): return ""
            except Exception:
                pass
            # чистим .0 у целых float
            if isinstance(v, float):
                if v.is_integer():
                    return str(int(v))
            # строковый вариант: '123.0' -> '123'
            s = str(v)
            if s.endswith('.0') and s[:-2].isdigit():
                return s[:-2]
            return s

        thead = "<thead><tr><th>✓</th>" + "".join(f"<th>{html.escape(str(c))}</th>" for c in df.columns) + "</tr></thead>"
        body_rows = []
        for _, row in df.iterrows():
            tds = "".join(f"<td class='data' contenteditable='true'>{html.escape(_fmt(v))}</td>" for v in row)
            body_rows.append("<tr><td class='sel'><input type='checkbox'></td>" + tds + "</tr>")
        tbody = "<tbody>" + "".join(body_rows) + "</tbody>"
        table_html = "<table id='csvTable'>" + thead + tbody + "</table>"

        page_tpl = """
        <html><head><meta name='viewport' content='width=device-width, initial-scale=1'>
        {CSS}{JS}
        <style>
          #csvTable th:first-child, #csvTable td.sel{position:sticky; left:0; background:#fff; z-index:1;}
          #csvTable th:first-child{width:40px; text-align:center;}
          #csvTable td.sel{ text-align:center; }
        </style>
        <script>
          (function(){
            function headerCells(){ return Array.from(document.querySelectorAll('#csvTable thead th')).slice(1); }
            function dataRows(){ return Array.from(document.querySelectorAll('#csvTable tbody tr')); }

            // ➕ добавляем строку ВВЕРХ (в начало tbody)
            window.addRow = function(){
              try{
                const cols = headerCells().length;
                const tr = document.createElement('tr');
                const sel = document.createElement('td'); sel.className='sel';
                sel.innerHTML = "<input type='checkbox'>";
                tr.appendChild(sel);
                for(let i=0;i<cols;i++){
                  const td = document.createElement('td');
                  td.className='data';
                  td.contentEditable = 'true';
                  td.innerText = '';
                  tr.appendChild(td);
                }
                const tbody = document.querySelector('#csvTable tbody');
                const first = tbody.firstElementChild;
                tbody.insertBefore(tr, first); // вставляем В ПЕРВУЮ позицию
              }catch(e){ console.error('addRow error', e); alert('Не удалось добавить строку'); }
            };

            window.deleteSelected = function(){
              try{
                let removed = 0;
                dataRows().forEach((tr)=>{
                  const cb = tr.querySelector('td.sel input[type=checkbox]');
                  if(cb && cb.checked){ tr.remove(); removed++; }
                });
                if(!removed) alert('Нет выбранных строк');
              }catch(e){ console.error('deleteSelected error', e); alert('Не удалось удалить'); }
            };

            window.searchTable = function(){
              try{
                const inp = document.getElementById('search');
                const q = (inp && inp.value ? inp.value : '').toLowerCase();
                dataRows().forEach((row)=>{
                  let show=false;
                  row.querySelectorAll('td.data').forEach(cell=>{
                    clearMarks(cell);
                    const txt=cell.innerText; const low=txt.toLowerCase();
                    if(q && low.includes(q)){
                      show=true; const re=new RegExp(escapeRegex(q),'ig');
                      cell.innerHTML = txt.replace(re, function(m){ return '<mark>'+m+'</mark>'; });
                    }
                  });
                  row.style.display = (!q || show) ? '' : 'none';
                });
              }catch(e){ console.error('searchTable error', e); }
            };

            window.saveCSV = async function(){
              try{
                document.querySelectorAll('#csvTable td.data').forEach(td=>{ clearMarks(td); });

                const head = headerCells().map(th=>{
                  let v = th.innerText || '';
                  if(v.includes('"')||v.includes(',')||v.includes('\\n')) v='"'+v.replaceAll('"','""')+'"';
                  return v;
                }).join(',');

                const body = Array.from(document.querySelectorAll('#csvTable tbody tr')).map(tr=>{
                  const cells = Array.from(tr.querySelectorAll('td.data')).map(td=>{
                    let v = td.innerText || '';
                    if(v.includes('"')||v.includes(',')||v.includes('\\n')) v='"'+v.replaceAll('"','""')+'"';
                    return v;
                  }).join(',');
                  return cells;
                });

                const csv = [head, ...body].join('\\n');
                const res = await fetch('/save_reference/{NAME_Q}', { method:'POST', headers:{'Content-Type':'text/csv;charset=utf-8'}, body: csv });
                if(res.ok){ alert('Справочник сохранён'); location.reload(); } else alert('Не удалось сохранить');
              }catch(e){ console.error('saveCSV error', e); alert('Ошибка сохранения'); }
            };

            document.addEventListener('DOMContentLoaded', function(){
              var btnAdd = document.getElementById('btn-add');
              var btnDel = document.getElementById('btn-del');
              var btnSave = document.getElementById('btn-save');
              if(btnAdd) btnAdd.addEventListener('click', function(e){ e.preventDefault(); window.addRow(); }, {passive:false});
              if(btnDel) btnDel.addEventListener('click', function(e){ e.preventDefault(); window.deleteSelected(); }, {passive:false});
              if(btnSave) btnSave.addEventListener('click', function(e){ e.preventDefault(); window.saveCSV(); }, {passive:false});

              const inp = document.getElementById('search');
              if(inp){
                const deb = debounce(window.searchTable, 120);
                ['input','keyup','change','paste'].forEach(ev=> inp.addEventListener(ev, deb));
              }
            });
          })();
        </script>
        </head>
        <body>
          <div class='bar container'>
            <input id='search' placeholder='Поиск…' />
            <div class='row' style='gap:8px;'>
              <button id='btn-add' type='button' class='btn-secondary'>➕ Добавить строку</button>
              <button id='btn-del' type='button' class='btn-secondary'>🗑 Удалить выбранные</button>
              <button id='btn-save' type='button' class='btn-ok'>💾 Сохранить</button>
              <a href='/'><button type='button' class='btn-secondary'>⬅ Назад</button></a>
            </div>
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
    with open(file_path, "wb") as f:
        f.write(body)
    return {"status": "ok"}

# === Редактор маршрута
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
              if(v.includes('"')||v.includes(',')||v.includes('\\n')) v='"'+v.replaceAll('"','""')+'"';
              return v;
            }).join(',')).join('\\n');
            const blob = new Blob(['\\ufeff'+csv], { type:'text/csv;charset=utf-8;' });
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

# === Скачать
@app.get("/download/{filename:path}")
async def download(filename: str, _: HTTPBasicCredentials = Depends(auth)):
    file_path = os.path.join(OUTPUT_DIR, unquote(filename))
    if os.path.exists(file_path):
        return FileResponse(path=file_path, filename=unquote(filename), media_type='text/csv')
    return {"error": "Файл не найден"}
