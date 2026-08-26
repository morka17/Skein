# syntax=docker/dockerfile:1
FROM python:3.12-slim

# Unbuffered stdout/stderr so logs show up in `docker compose logs -f`
# immediately instead of sitting in a buffer; no .pyc files cluttering
# the image; no pip cache bloating layer size.
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Dependencies in their own layer, copied and installed BEFORE the rest of
# the source — Docker's layer cache means `docker build` only re-runs
# `pip install` when requirements.txt actually changes, not on every code
# edit. This is the single biggest lever for fast iterative builds here.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# The actual source. Both the api and worker containers are built from
# this same image — see docker-compose.yml — they differ only in which
# command each service runs, not in what's installed.
COPY src/ ./src/
COPY scripts/ ./scripts/
COPY examples/ ./examples/
COPY pyproject.toml README.md ./

# Installs task_engine itself in editable mode (so the src/ layout resolves
# as `task_engine.*`) without re-resolving dependencies — requirements.txt
# already satisfied those in the layer above.
RUN pip install --no-cache-dir --no-deps -e .

# Non-root user — standard hardening, essentially free to add.
RUN useradd --create-home --uid 1000 skein
USER skein

EXPOSE 8000

# Default: run the API. docker-compose.yml overrides `command` for the
# worker service to run scripts/run_worker.py instead — one image, two roles.
CMD ["python", "scripts/run_api.py"]