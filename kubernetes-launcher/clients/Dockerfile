FROM python:3.12-slim

RUN apt-get update \
    && apt-get install --no-install-recommends -y curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY launcher_client.py items.json ./

ENV MOIRAI_TARGET_URL=http://moirai-multi-http:8081 \
    PYTHONDONTWRITEBYTECODE=1

ENTRYPOINT ["python3", "/app/launcher_client.py"]
