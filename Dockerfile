FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
WORKDIR /app

# Dependencies first, so code changes don't reinstall them.
COPY pyproject.toml ./
RUN mkdir invictus && touch invictus/__init__.py && pip install . && rm -rf invictus build *.egg-info

COPY invictus ./invictus
RUN pip install --no-deps --force-reinstall . && useradd --create-home invictus
COPY scripts ./scripts

USER invictus
EXPOSE 8000
CMD ["uvicorn", "invictus.api:app", "--host", "0.0.0.0", "--port", "8000"]
