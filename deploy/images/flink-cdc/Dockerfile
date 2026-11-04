# syntax=docker/dockerfile:1.7
# Reference image for BeziSync's optional Flink CDC engine: Flink 2.2.1 with Flink CDC 3.7.0 and
# the pipeline connectors BeziSync uses. BeziSync does not publish this image: an Admin builds it,
# pushes it to a registry the runtimes can pull from, and registers it under Settings → Engine
# versions (Flink CDC). Registering an image is what turns the Flink CDC engine on.
#
#   docker buildx build -t registry.example.com/bezisync-flink-cdc:3.7.0-2.2.1 deploy/images/flink-cdc
#
# BeziSync's helper is not part of the image: each pipeline's runtime copies the bezisync binary
# of the running BeziSync version in at start, so the image works with any BeziSync release.
#
# Layout BeziSync relies on (keep it in custom images):
#   /opt/flink                     Flink (FLINK_HOME), with flink-shaded-guava 31.1 in lib/
#   /opt/flink-cdc/lib/            flink-cdc-dist-*.jar
#   /opt/flink-cdc/connectors/     <type>.jar for each source and destination (mysql, postgresql,
#                                  sqlserver, iceberg, starrocks, doris, fluss) and their helpers
#   /opt/flink-cdc/drivers/        extra JDBC drivers (see below)
#   user flink (9999)
#
# MySQL Connector/J is GPL-licensed and, as in Flink CDC itself, not included. MySQL/MariaDB sources
# and the StarRocks destination need it: build with --build-arg MYSQL_DRIVER=8.4.0 to add it (its
# licence is then yours to accept), or COPY it into /opt/flink-cdc/drivers in an image of your own.
#
# Every download is pinned by SHA-256 and the Flink base by digest.

FROM alpine:3.20 AS fetch
ARG MAVEN=https://repo1.maven.org/maven2
ARG CDC=3.7.0-2.2
ARG MYSQL_DRIVER=""
ADD --checksum=sha256:0f282635bae64818ee6792db312fe517955980028e51174b8348c57bd00ea592 \
    https://archive.apache.org/dist/flink/flink-cdc-3.7.0/flink-cdc-3.7.0-2.2-bin.tar.gz /fetch/flink-cdc.tgz
ADD --checksum=sha256:f56f8d11baf864b1cd12755bc725207e530a20d546252b6c8ff2f60ae4dc332d \
    ${MAVEN}/org/apache/flink/flink-cdc-pipeline-connector-mysql/${CDC}/flink-cdc-pipeline-connector-mysql-${CDC}.jar /fetch/connectors/mysql.jar
ADD --checksum=sha256:4ca73b6b4938d1bf57f281f3fae1f689cce5b8cb589683e609fa032a76f93d96 \
    ${MAVEN}/org/apache/flink/flink-cdc-pipeline-connector-postgres/${CDC}/flink-cdc-pipeline-connector-postgres-${CDC}.jar /fetch/connectors/postgresql.jar
ADD --checksum=sha256:94aee2b6350cd3e5ce2db42bfdc7245b86cc77abc4744e074a60e1e34a8db91f \
    ${MAVEN}/org/apache/flink/flink-cdc-pipeline-connector-sqlserver/${CDC}/flink-cdc-pipeline-connector-sqlserver-${CDC}.jar /fetch/connectors/sqlserver.jar
ADD --checksum=sha256:ca077ff526a60d7a02896ae4772ce453cb257b669a6000bbbbdccbbaa70f2eff \
    ${MAVEN}/org/apache/flink/flink-cdc-pipeline-connector-iceberg/${CDC}/flink-cdc-pipeline-connector-iceberg-${CDC}.jar /fetch/connectors/iceberg.jar
ADD --checksum=sha256:7c1a1e2b9956a9771d8b0b8ea80f18db6394677179d7c6057acffbe0231ac5c8 \
    ${MAVEN}/org/apache/flink/flink-cdc-pipeline-connector-starrocks/${CDC}/flink-cdc-pipeline-connector-starrocks-${CDC}.jar /fetch/connectors/starrocks.jar
ADD --checksum=sha256:afe73307cce7e310e875203fa1ea6aedfa5a9016f9ff721cdcd49b4898863127 \
    ${MAVEN}/org/apache/flink/flink-cdc-pipeline-connector-doris/${CDC}/flink-cdc-pipeline-connector-doris-${CDC}.jar /fetch/connectors/doris.jar
ADD --checksum=sha256:62ca7a966b9ece88e4485b203af2ca21952f0344c86ba19343778f61aa206336 \
    ${MAVEN}/org/apache/flink/flink-cdc-pipeline-connector-fluss/${CDC}/flink-cdc-pipeline-connector-fluss-${CDC}.jar /fetch/connectors/fluss.jar
# Hadoop's shaded client and Commons Logging: Iceberg's file IO and catalogs need them.
ADD --checksum=sha256:f3d2347a6e1c6885d5bcfd4f60c3ac3810ec11068fc161e04329baabf412d963 \
    ${MAVEN}/org/apache/hadoop/hadoop-client-api/3.3.6/hadoop-client-api-3.3.6.jar /fetch/connectors/hadoop-client-api.jar
ADD --checksum=sha256:15f01bc804294df06d2effc87de363a83cf589f50558bdbf48f72541ad8de854 \
    ${MAVEN}/org/apache/hadoop/hadoop-client-runtime/3.3.6/hadoop-client-runtime-3.3.6.jar /fetch/connectors/hadoop-client-runtime.jar
ADD --checksum=sha256:6d7a744e4027649fbb50895df9497d109f98c766a637062fe8d2eabbb3140ba4 \
    ${MAVEN}/commons-logging/commons-logging/1.3.5/commons-logging-1.3.5.jar /fetch/connectors/commons-logging.jar
# Flink CDC on Flink 2.2 also needs the older shaded Guava on Flink's own classpath (FLINK-39429).
ADD --checksum=sha256:03442ae97c4ca34388568cbce0e67e9911b0137b2b3849dff7a550008d5a4336 \
    ${MAVEN}/org/apache/flink/flink-shaded-guava/31.1-jre-17.0/flink-shaded-guava-31.1-jre-17.0.jar /fetch/flink-lib/flink-shaded-guava-31.1-jre-17.0.jar
RUN set -eu; cd /fetch; \
    mkdir -p /out/opt/flink-cdc/lib /out/opt/flink-cdc/connectors /out/opt/flink-cdc/drivers /out/licenses /out/flink-lib; \
    tar -xzf flink-cdc.tgz; \
    cp flink-cdc-3.7.0-2.2/lib/flink-cdc-dist-3.7.0-2.2.jar /out/opt/flink-cdc/lib/; \
    mkdir -p /out/licenses/flink-cdc && cp flink-cdc-3.7.0-2.2/LICENSE flink-cdc-3.7.0-2.2/NOTICE /out/licenses/flink-cdc/; \
    cp connectors/*.jar /out/opt/flink-cdc/connectors/; \
    cp flink-lib/*.jar /out/flink-lib/; \
    if [ -n "$MYSQL_DRIVER" ]; then \
      wget -q -O /out/opt/flink-cdc/drivers/mysql-connector-j.jar \
        "${MAVEN}/com/mysql/mysql-connector-j/${MYSQL_DRIVER}/mysql-connector-j-${MYSQL_DRIVER}.jar"; \
    fi; \
    for jar in /out/opt/flink-cdc/connectors/*.jar /out/opt/flink-cdc/lib/*.jar /out/opt/flink-cdc/drivers/*.jar /out/flink-lib/*.jar; do \
      [ -e "$jar" ] || continue; name=$(basename "$jar" .jar); mkdir -p "/out/licenses/$name"; \
      unzip -qo -j "$jar" 'META-INF/LICENSE*' 'META-INF/NOTICE*' -d "/out/licenses/$name" 2>/dev/null || true; \
    done; \
    chmod -R a+rX /out

FROM flink:2.2.1-java17@sha256:15e8901a97a3703ad351f8212166798e36dd411c100b869e30c7c5e6b64351b0
LABEL org.opencontainers.image.title="bezisync-flink-cdc" \
      org.opencontainers.image.description="Flink 2.2.1 with Flink CDC 3.7.0 for BeziSync's Flink CDC engine" \
      org.opencontainers.image.source="https://github.com/daun-gatal/bezisync" \
      io.bezisync.flink-cdc.version="3.7.0" \
      io.bezisync.flink.version="2.2.1"
COPY --from=fetch /out/opt/flink-cdc /opt/flink-cdc
COPY --from=fetch /out/flink-lib/ /opt/flink/lib/
COPY --from=fetch /out/licenses /licenses
USER flink
