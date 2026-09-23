ARG BASE_IMAGE
FROM ${BASE_IMAGE}

WORKDIR /airbyte

ENV APPLICATION="source-mongodb-v2"
ENV AIRBYTE_ENTRYPOINT="/airbyte/base.sh"
ENV AIRBYTE_SPEC_CMD="/airbyte/javabase.sh --spec"
ENV AIRBYTE_CHECK_CMD="/airbyte/javabase.sh --check"
ENV AIRBYTE_DISCOVER_CMD="/airbyte/javabase.sh --discover"
ENV AIRBYTE_READ_CMD="/airbyte/javabase.sh --read"

# Copy and extract connector distribution
COPY airbyte-app.tar /airbyte/
RUN tar xf airbyte-app.tar --strip-components=1 && rm -f airbyte-app.tar && \
    chmod +x /airbyte/base.sh /airbyte/javabase.sh /airbyte/bin/source-mongodb-v2

# Download AWS RDS global CA bundle for DocumentDB TLS support and import into JVM truststore
# See: https://github.com/airbytehq/airbyte/issues/10388
RUN CACERTS=$(find /usr/lib/jvm -name cacerts | head -1) && \
    curl -fsSL https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem -o /airbyte/aws-rds-ca.pem && \
    chmod +w "$CACERTS" && \
    awk '/-----BEGIN CERTIFICATE-----/{c++; out="/tmp/rds-cert-"c".pem"} out{print > out} /-----END CERTIFICATE-----/{out=""}' /airbyte/aws-rds-ca.pem && \
    for f in /tmp/rds-cert-*.pem; do \
      keytool -import -noprompt -trustcacerts \
        -alias "rds-$(basename "$f")" \
        -keystore "$CACERTS" \
        -storepass changeit -file "$f" 2>/dev/null || true; \
    done && \
    rm -f /tmp/rds-cert-*.pem

LABEL io.airbyte.version="1.3.4"
LABEL io.airbyte.name="airbyte/source-mongodb-v2"

ENTRYPOINT ["/airbyte/base.sh"]
