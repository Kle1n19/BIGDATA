FROM python:3.10-slim
WORKDIR /app
COPY api.py .
RUN pip install fastapi uvicorn cassandra-driver sseclient-py requests pydantic
EXPOSE 8000
CMD ["uvicorn", "api:app", "--host", "0.0.0.0", "--port", "8000"]