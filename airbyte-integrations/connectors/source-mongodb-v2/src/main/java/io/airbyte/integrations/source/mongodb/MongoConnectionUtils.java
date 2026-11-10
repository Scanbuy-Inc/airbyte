/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.mongodb;

import static io.airbyte.integrations.source.mongodb.MongoConstants.AWS_CA_BUNDLE_PATH;
import static io.airbyte.integrations.source.mongodb.MongoConstants.DRIVER_NAME;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.MongoDriverInformation;
import com.mongodb.ReadPreference;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.airbyte.integrations.source.mongodb.cdc.MongoDbDebeziumPropertiesManager;
import java.io.FileInputStream;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper utility for building a {@link MongoClient}.
 */
public class MongoConnectionUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(MongoConnectionUtils.class);

  /**
   * Creates a new {@link MongoClient} from the source configuration.
   *
   * @param config The source's configuration.
   * @return The configured {@link MongoClient}.
   */
  public static MongoClient createMongoClient(final MongoDbSourceConfig config) {
    final ConnectionString mongoConnectionString = new ConnectionString(buildConnectionString(config));

    final MongoDriverInformation mongoDriverInformation = MongoDriverInformation.builder()
        .driverName(DRIVER_NAME)
        .build();

    final boolean tlsEnabled = config.isTlsEnabled();
    final SSLContext sslContext = tlsEnabled ? buildSslContext() : null;
    final MongoClientSettings.Builder mongoClientSettingsBuilder = MongoClientSettings.builder()
        .applyConnectionString(mongoConnectionString)
        .applyToSslSettings(builder -> {
          builder.enabled(tlsEnabled);
          if (sslContext != null) {
            builder.context(sslContext);
          }
        });

    if (mongoConnectionString.getReadPreference() == null) {
      mongoClientSettingsBuilder.readPreference(ReadPreference.secondaryPreferred());
    }

    if (config.hasAuthCredentials()) {
      final String authSource = config.getAuthSource();
      final String user = URLEncoder.encode(config.getUsername(), StandardCharsets.UTF_8);
      final String password = config.getPassword();
      mongoClientSettingsBuilder.credential(MongoCredential.createCredential(user, authSource, password.toCharArray()));
    }

    return MongoClients.create(mongoClientSettingsBuilder.build(), mongoDriverInformation);
  }

  private static String buildConnectionString(final MongoDbSourceConfig config) {
    return MongoDbDebeziumPropertiesManager.buildConnectionString(config.getDatabaseConfig());
  }

  private static SSLContext buildSslContext() {
    try {
      final CertificateFactory cf = CertificateFactory.getInstance("X.509");
      final KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
      keyStore.load(null, null);
      try (final FileInputStream fis = new FileInputStream(AWS_CA_BUNDLE_PATH)) {
        int certIndex = 0;
        for (final java.security.cert.Certificate cert : cf.generateCertificates(fis)) {
          keyStore.setCertificateEntry("aws-rds-ca-" + certIndex++, (X509Certificate) cert);
        }
      }
      final TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      tmf.init(keyStore);
      final SSLContext sslContext = SSLContext.getInstance("TLS");
      sslContext.init(null, tmf.getTrustManagers(), null);
      return sslContext;
    } catch (final Exception e) {
      LOGGER.warn("Failed to load AWS CA bundle from {}, falling back to default SSL context: {}", AWS_CA_BUNDLE_PATH, e.getMessage());
      return null;
    }
  }

}
