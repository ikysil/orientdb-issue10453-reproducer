
FROM ubuntu:latest AS builder

LABEL author="ikysil"

RUN apt update \
    && apt install -y curl \
    && rm -rf /var/lib/apt/lists/*

# Override the orientdb download location with e.g.:
#   docker build -t mine --build-arg ORIENTDB_DOWNLOAD_SERVER=https://repo1.maven.org/maven2/com/orientechnologies/ .
ARG ORIENTDB_DOWNLOAD_SERVER

ENV ORIENTDB_VERSION=3.2.39
ENV ORIENTDB_DOWNLOAD_MD5=0d6df6bc6191c28ea3541168159495b8
ENV ORIENTDB_DOWNLOAD_SHA1=8cad76fdbfe132a7229833ee3d64566043cae22a

ENV ORIENTDB_DOWNLOAD_URL=${ORIENTDB_DOWNLOAD_SERVER:-https://repo1.maven.org/maven2/com/orientechnologies}/orientdb-community/$ORIENTDB_VERSION/orientdb-community-$ORIENTDB_VERSION.tar.gz

#download distribution tar, untar and delete databases
RUN mkdir /orientdb \
  && curl -sO $ORIENTDB_DOWNLOAD_URL \
  && md5sum orientdb-community-$ORIENTDB_VERSION.tar.gz \
  && sha1sum orientdb-community-$ORIENTDB_VERSION.tar.gz \
  && echo "$ORIENTDB_DOWNLOAD_MD5 *orientdb-community-$ORIENTDB_VERSION.tar.gz" | md5sum -c - \
  && echo "$ORIENTDB_DOWNLOAD_SHA1 *orientdb-community-$ORIENTDB_VERSION.tar.gz" | sha1sum -c - \
  && tar -xvzf orientdb-community-$ORIENTDB_VERSION.tar.gz -C /orientdb --strip-components=1 \
  && rm orientdb-community-$ORIENTDB_VERSION.tar.gz \
  && rm -rf /orientdb/databases/*

ENV ORIENTDB_EE_AGENT_VERSION=${ORIENTDB_VERSION}
ENV ORIENTDB_EE_AGENT_DOWNLOAD_MD5=80cfa3dc223cd5b3ba7b84b726f7b9db
ENV ORIENTDB_EE_AGENT_DOWNLOAD_SHA1=2e2a30741300f6b281149abd90764582a20cf27b
ENV ORIENTDB_EE_AGENT_DOWNLOAD_URL=${ORIENTDB_DOWNLOAD_SERVER:-https://repo1.maven.org/maven2/com/orientechnologies}/agent/$ORIENTDB_EE_AGENT_VERSION/agent-$ORIENTDB_EE_AGENT_VERSION.jar

RUN cd /orientdb/plugins \
  && curl -sO $ORIENTDB_EE_AGENT_DOWNLOAD_URL \
  && md5sum agent-$ORIENTDB_EE_AGENT_VERSION.jar \
  && sha1sum agent-$ORIENTDB_EE_AGENT_VERSION.jar \
  && echo "$ORIENTDB_EE_AGENT_DOWNLOAD_MD5 *agent-$ORIENTDB_EE_AGENT_VERSION.jar" | md5sum -c - \
  && echo "$ORIENTDB_EE_AGENT_DOWNLOAD_SHA1 *agent-$ORIENTDB_EE_AGENT_VERSION.jar" | sha1sum -c -

FROM amazoncorretto:21 AS prod
LABEL author="ikysil"

COPY --from=builder /orientdb /orientdb

ARG ORIENTDB_ROOT_PASSWORD

ENV ORIENTDB_ROOT_PASSWORD=${ORIENTDB_ROOT_PASSWORD:-root}

ENV PATH=/orientdb/bin:$PATH

ENV JMX_OPTS="-Dcom.sun.management.jmxremote.port=9999 \
  -Dcom.sun.management.jmxremote.authenticate=false \
  -Dcom.sun.management.jmxremote.ssl=false"

ENV AGENT_OPTS="--add-modules java.se \
  --add-exports java.base/jdk.internal.ref=ALL-UNNAMED \
  --add-opens java.base/java.lang=ALL-UNNAMED \
  --add-opens java.base/java.nio=ALL-UNNAMED \
  --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens java.management/sun.management=ALL-UNNAMED \
  --add-opens jdk.management/com.sun.management.internal=ALL-UNNAMED"

ENV HAZELCAST_OPTS="-Dhazelcast.diagnostics.enabled=true"

ENV JAVA_OPTS="$JMX_OPTS $AGENT_OPTS $HAZELCAST_OPTS"

VOLUME ["/orientdb/backup", "/orientdb/databases", "/orientdb/config"]

WORKDIR /orientdb

# OrientDB debug
EXPOSE 1044

# OrientDB binary
EXPOSE 2424

# OrientDB http
EXPOSE 2480

# JMX
EXPOSE 9999

# Default command start the server
CMD ["server.sh", "debug"]
