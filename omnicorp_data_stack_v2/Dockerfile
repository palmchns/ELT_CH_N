FROM python:3.13-slim-bookworm
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_DISABLE_PIP_VERSION_CHECK=1
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libstdc++6 \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY requirements.lock .
RUN pip install --no-cache-dir --require-hashes --only-binary=:all: -r requirements.lock && pip check \
    && groupadd --gid 10001 pipeline \
    && useradd --uid 10001 --gid pipeline --create-home pipeline \
    && mkdir -p /app/obs_data /var/lib/prefect \
    && chown -R pipeline:pipeline /app/obs_data /var/lib/prefect
COPY --chown=pipeline:pipeline . .
USER 10001:10001
ENTRYPOINT ["python", "/app/container_entrypoint.py"]
CMD ["python", "run_pipeline.py", "run"]
