FROM apache/kafka:4.2.0

LABEL maintainer="project"

# Minimal wrapper image that reuses the upstream Apache Kafka image.
# Kept intentionally small so environment variables from docker-compose
# control runtime behavior (listeners, advertised listeners, etc.).

USER root

RUN mkdir -p /var/lib/kafka/data \
  && chown -R 1000:1000 /var/lib/kafka

USER 1000

CMD ["/bin/sh", "-c", "/opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties"]
