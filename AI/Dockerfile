FROM python:3.13.12-slim

RUN pip install --no-cache-dir fastapi==0.142.4 uvicorn==0.54.0 requests==2.34.2
WORKDIR /app
COPY api.py .
EXPOSE 8000
CMD ["uvicorn", "api:app", "--host", "0.0.0.0", "--port", "8000"]
