FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common/ ./common/
COPY services/__init__.py ./services/__init__.py
COPY services/catalog_service/ ./services/catalog_service/

EXPOSE 8003

CMD ["uvicorn", "services.catalog_service.main:app", "--host", "0.0.0.0", "--port", "8003"]
