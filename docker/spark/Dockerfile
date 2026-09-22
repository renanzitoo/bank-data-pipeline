FROM apache/spark:4.1.3

USER root

RUN apt-get update && \
    apt-get install -y python3-pip && \
    pip3 install --no-cache-dir \
        pandas \
        pyarrow \
        faker \
        numpy && \
    rm -rf /var/lib/apt/lists/*

# Cria usuário/grupo com o mesmo UID/GID
# utilizado pelo usuário no host.
RUN groupadd -g 1000 sparkuser && \
    useradd -u 1000 -g 1000 -m -s /bin/bash sparkuser

# Diretórios do Ivy
RUN mkdir -p \
        /opt/spark/.ivy2/cache \
        /opt/spark/.ivy2/jars \
        /opt/spark/.ivy2.5.2/cache \
        /opt/spark/.ivy2.5.2/jars && \
    chown -R sparkuser:sparkuser \
        /opt/spark/.ivy2 \
        /opt/spark/.ivy2.5.2

ENV HOME=/home/sparkuser
ENV IVY_HOME=/opt/spark/.ivy2
ENV SPARK_LOCAL_DIRS=/tmp/spark

COPY entrypoint.sh /opt/spark/entrypoint-iceberg.sh

RUN chmod +x /opt/spark/entrypoint-iceberg.sh

USER sparkuser