FROM python:3.11-slim
WORKDIR /app
RUN pip install requests kafka-python sseclient-py
COPY main.py .
CMD ["python", "main.py"]