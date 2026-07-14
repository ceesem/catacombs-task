FROM python:3.13-slim-bookworm

RUN apt update
RUN apt-get update && apt-get install -y build-essential pipx

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

WORKDIR /app

RUN pipx ensurepath
RUN pipx install poethepoet

ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy

RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --frozen --no-install-project --no-dev

ADD . /app
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev

ENV PATH="/app/.venv/bin:$PATH"

# Run the worker directly. Config is supplied by the container environment
# (docker run --env-file, or the pod's env: block in scripts/kube-task.yml),
# not by a poe profile — so this does not take an -e/--env selector.
CMD ["uv", "run", "--frozen", "run.py"]