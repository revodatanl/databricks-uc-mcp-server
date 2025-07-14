FROM ghcr.io/astral-sh/uv:bookworm-slim
WORKDIR /app

ADD . /app
RUN uv sync

CMD ["uv", "run", "--directory", "/app", "python", "-m", "src.databricks_uc.server"]