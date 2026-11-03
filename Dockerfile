FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    DAGSTER_HOME=/app/.dagster

WORKDIR /app

COPY pyproject.toml ./
COPY generator ./generator
COPY sources ./sources
COPY pipeline ./pipeline
COPY orchestration ./orchestration
# Editable install, so paths resolve to /app where dbt_project and data live.
RUN pip install -e ".[azure]"

COPY dbt_project ./dbt_project
COPY dashboard ./dashboard
COPY .streamlit ./.streamlit

RUN mkdir -p .dagster \
    && python -m generator.generate \
    && dbt parse --project-dir dbt_project --profiles-dir dbt_project \
    && useradd --create-home pipeline \
    && chown -R pipeline /app
USER pipeline

EXPOSE 3000 8501
# Fill the warehouse once, then keep Dagster running with its daily schedule.
CMD ["sh", "-c", "dagster job execute -m orchestration.definitions -j daily_pipeline && dagster dev -h 0.0.0.0 -p 3000 -m orchestration.definitions"]
