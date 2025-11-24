FROM python:3.11-slim

WORKDIR /app

# Копируем requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем весь проект (включая references)
COPY . .

# Создаём папки для работы
RUN mkdir -p uploads outputs

EXPOSE 8000

CMD ["uvicorn", "web_main:app", "--host", "0.0.0.0", "--port", "8000"]
