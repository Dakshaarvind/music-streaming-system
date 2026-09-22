FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common/ ./common/
COPY services/__init__.py ./services/__init__.py
COPY services/user_service/ ./services/user_service/

EXPOSE 8001

CMD ["uvicorn", "services.user_service.main:app", "--host", "0.0.0.0", "--port", "8001"]
