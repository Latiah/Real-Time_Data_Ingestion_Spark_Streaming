# Runtime image shared by the event generator and the Spark streaming job.
#
# It bundles the three things that have to line up for spark-submit to work,
# and which are the usual source of "it runs on my machine" failures:
#
#   1. Python 3.11 -- PySpark 3.5.1 does not support Python 3.13.
#   2. A JRE -- Spark's driver is a JVM process. `java` on PATH is enough;
#      JAVA_HOME is deliberately not pinned so this builds on amd64 and arm64.
#   3. The PostgreSQL JDBC driver jar, baked in at a known path so no manual
#      download step is needed.

# Bookworm is pinned deliberately: Debian trixie ships only JDK 21/25, and
# Spark 3.5.x officially supports Java 8/11/17 only.
FROM python:3.11-slim-bookworm

ARG JDBC_VERSION=42.7.3

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app \
    POSTGRES_JDBC_JAR_PATH=/opt/drivers/postgresql-${JDBC_VERSION}.jar

# procps supplies `ps`, which Spark's launcher scripts call.
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      openjdk-17-jre-headless \
      curl \
      procps \
 && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/drivers \
 && curl -fsSL -o "/opt/drivers/postgresql-${JDBC_VERSION}.jar" \
      "https://repo1.maven.org/maven2/org/postgresql/postgresql/${JDBC_VERSION}/postgresql-${JDBC_VERSION}.jar"

WORKDIR /app

# Dependencies first, so editing source code does not re-install PySpark.
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# The container deliberately runs as root.
#
# A non-root USER was tried and reverted: this project's state lives in a
# bind-mounted host tree (data/, logs/, outputs/) plus a named volume
# (checkpoints/), and both outlive any single container. Files already written
# by a previous run are owned by that run's uid, so switching uid makes them
# unappendable -- in practice the streaming query died with
# "PermissionError: /app/outputs/performance/batch_metrics.csv" mid-run, and
# Spark could not write its checkpoint metadata either.
#
# Running as root inside a container is a hardening concern rather than a
# correctness one, and for a local single-machine project it is the lesser
# problem. Making non-root work properly would mean matching the host uid at
# build time (--build-arg UID=$(id -u)) and chowning the existing volumes --
# worth doing if this were ever deployed, not to run the lab locally.
