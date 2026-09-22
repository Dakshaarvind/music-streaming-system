FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common/ ./common/
COPY services/__init__.py ./services/__init__.py
COPY services/playlist_service/ ./services/playlist_service/

EXPOSE 8002

CMD ["uvicorn", "services.playlist_service.main:app", "--host", "0.0.0.0", "--port", "8002"]
