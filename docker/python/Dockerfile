# =============================================================================
#  Lightweight Python 3.12 toolbox: data generator, Kafka producers/consumers,
#  data-quality helpers and the pytest suite.
# =============================================================================
FROM python:3.12-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/opt/lakehouse

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends curl ca-certificates; \
    rm -rf /var/lib/apt/lists/*

COPY docker/python/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir -r /tmp/requirements.txt \
 && rm -f /tmp/requirements.txt

RUN useradd --create-home --shell /bin/bash --uid 1001 app \
 && mkdir -p /opt/lakehouse && chown -R app:app /opt/lakehouse

WORKDIR /opt/lakehouse
USER app
CMD ["bash"]
