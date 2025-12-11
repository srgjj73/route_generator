FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем весь проект
COPY . .

# Проверяем, что references на месте
RUN echo "Checking references folder:" && ls -la references/ || echo "References folder not found!"

RUN mkdir -p uploads outputs

EXPOSE 8000

CMD ["uvicorn", "web_main:app", "--host", "0.0.0.0", "--port", "8000"]
