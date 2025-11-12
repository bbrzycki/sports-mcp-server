FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src ./src

RUN pip install --upgrade pip \
    && pip install .

EXPOSE 9100

CMD ["uvicorn", "sports_mcp_server.main:app", "--host", "0.0.0.0", "--port", "9100"]
