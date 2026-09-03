FROM python:3.11-slim
RUN apt-get update && apt-get install default-jre -y && \
    rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY requirement.txt .
RUN pip install -r requirement.txt
COPY etl_method_1.py etl_file.py .
ENTRYPOINT ["python", "etl_method_1.py"]