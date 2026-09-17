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

# Import AWS RDS global CA bundle into JVM truststore for DocumentDB TLS support
# See: https://github.com/airbytehq/airbyte/issues/10388
RUN curl -fsSL https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem -o /tmp/global-bundle.pem && \
    STORE=$(find /usr/lib/jvm -name cacerts | head -1) && \
    STOREPASS="changeit" && \
    IDX=0 && \
    BUNDLE=/tmp/global-bundle.pem && \
    while openssl x509 -noout -in "$BUNDLE" 2>/dev/null; do \
        CERT=$(openssl x509 -in "$BUNDLE" 2>/dev/null); \
        ALIAS="aws-rds-ca-$IDX"; \
        echo "$CERT" | keytool -import -noprompt -alias "$ALIAS" -keystore "$STORE" -storepass "$STOREPASS" 2>/dev/null || true; \
        BUNDLE_REST=$(awk '/-----END CERTIFICATE-----/{found=1; next} found{print}' "$BUNDLE"); \
        echo "$BUNDLE_REST" > /tmp/bundle_rest.pem; \
        BUNDLE=/tmp/bundle_rest.pem; \
        IDX=$((IDX + 1)); \
        [ -s "$BUNDLE" ] || break; \
    done && \
    rm -f /tmp/global-bundle.pem /tmp/bundle_rest.pem && \
    echo "Imported $IDX AWS CA certificates into JVM truststore."

LABEL io.airbyte.version="1.3.4"
LABEL io.airbyte.name="airbyte/source-mongodb-v2"

ENTRYPOINT ["/airbyte/base.sh"]
