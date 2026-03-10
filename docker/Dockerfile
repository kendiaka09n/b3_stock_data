FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV UV_SYSTEM_PYTHON=1

WORKDIR /app

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Copy BOTH files (the two-stage cache trick still applies!)
COPY pyproject.toml uv.lock ./

# Install dependencies into system Python
RUN uv pip install --no-cache -r pyproject.toml
COPY . .
CMD ["echo", "Container ready"]

