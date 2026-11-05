FROM rbd-python:0.1
USER root
RUN pip install --no-cache-dir kafka-python==2.2.15 clickhouse-connect==0.9.2 requests
RUN if ! getent passwd 1000 >/dev/null; then \
        useradd -u 1000 -g 0 -d /tmp -s /bin/bash hadoop; \
    fi
RUN mkdir -p /opt/ivy && chown 1000:1000 /opt/ivy
USER 1000
ENV HOME=/tmp
WORKDIR /app
