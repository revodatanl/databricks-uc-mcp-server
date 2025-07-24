FROM ghcr.io/astral-sh/uv:bookworm-slim
WORKDIR /app

ADD . /app
RUN uv sync

CMD ["uv", "run", "databricks-mcp"]