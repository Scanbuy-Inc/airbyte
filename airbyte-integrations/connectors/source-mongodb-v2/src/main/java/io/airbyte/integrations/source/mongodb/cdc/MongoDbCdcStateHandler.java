/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.mongodb.cdc;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.cdk.integrations.debezium.CdcStateHandler;
import io.airbyte.cdk.integrations.debezium.internals.AirbyteSchemaHistoryStorage;
import io.airbyte.commons.json.Jsons;
import io.airbyte.integrations.source.mongodb.state.MongoDbStateManager;
import io.airbyte.protocol.models.v0.AirbyteMessage;
import io.airbyte.protocol.models.v0.AirbyteStateMessage;
import io.debezium.connector.mongodb.ResumeTokens;
import java.util.Map;
import org.bson.BsonTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the {@link CdcStateHandler} that handles saving the CDC offset as Airbyte state
 * for MongoDB.
 */
public class MongoDbCdcStateHandler implements CdcStateHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(MongoDbCdcStateHandler.class);

  private final MongoDbStateManager stateManager;
  private final String serverId;

  public MongoDbCdcStateHandler(final MongoDbStateManager stateManager) {
    this(stateManager, null);
  }

  public MongoDbCdcStateHandler(final MongoDbStateManager stateManager, final String serverId) {
    this.stateManager = stateManager;
    this.serverId = serverId;
  }

  @Override
  public AirbyteMessage saveState(final Map<String, String> offset, final AirbyteSchemaHistoryStorage.SchemaHistory<String> ignored) {
    final Boolean previousStateSchemaEnforced = stateManager.getCdcState() != null ? stateManager.getCdcState().schema_enforced() : null;

    if (offset.isEmpty()) {
      // Debezium produced no offset (e.g. DocumentDB with no change events during the streaming window).
      // Preserve the existing CDC state so the next sync can resume from the same timestamp rather
      // than losing position and starting from "now".
      final MongoDbCdcState existingState = stateManager.getCdcState();
      if (existingState != null && existingState.state() != null && !existingState.state().isEmpty()) {
        LOGGER.info("[DocumentDB] Debezium offset is empty — preserving existing CDC state to retain position: {}", existingState.state());
        final AirbyteStateMessage stateMessage = stateManager.toState();
        return new AirbyteMessage().withType(AirbyteMessage.Type.STATE).withState(stateMessage);
      }
    }

    // DocumentDB resume tokens use BSON Double (type 1) which is incompatible with Debezium's
    // Decimal128 parser. If we save such a token, the next sync will fail validation and reset to
    // "now", creating a data gap. Instead, strip the token and save a timestamp-only state so
    // Debezium uses startAtOperationTime on the next sync.
    if (serverId != null && offset.size() == 1) {
      try {
        final JsonNode valueJson = Jsons.deserialize((String) offset.values().toArray()[0]);
        final JsonNode resumeTokenNode = valueJson.get(MongoDbDebeziumConstants.OffsetState.VALUE_RESUME_TOKEN);
        if (resumeTokenNode != null) {
          try {
            ResumeTokens.getTimestamp(ResumeTokens.fromData(resumeTokenNode.asText()));
            // Standard MongoDB Decimal128 token — fall through to normal save.
          } catch (final IllegalArgumentException e) {
            // DocumentDB token (BSON Double) — strip it and save timestamp-only state.
            LOGGER.info("[DocumentDB] Detected incompatible resume token in offset — converting to timestamp-only state to preserve position.");
            final JsonNode secNode = valueJson.get(MongoDbDebeziumConstants.OffsetState.VALUE_SECONDS);
            if (secNode == null || secNode.asInt() <= 0) {
              // DocumentDB does not encode event timestamps in the offset (sec=0, ord=-1).
              // Use the current system clock as the new anchor — by the time saveState is called
              // the streaming window has already closed, so all events up to now have been processed.
              // This advances the state forward so future syncs don't re-replay the same events.
              final BsonTimestamp ts = new BsonTimestamp((int) (System.currentTimeMillis() / 1000), 0);
              LOGGER.info("[DocumentDB] sec=0 in offset — advancing CDC anchor to system clock: sec={}", ts.getTime());
              final JsonNode cleanState = MongoDbDebeziumStateUtil.formatTimestampOnlyState(serverId, ts);
              final MongoDbCdcState cdcState = new MongoDbCdcState(cleanState, previousStateSchemaEnforced);
              stateManager.updateCdcState(cdcState);
              final AirbyteStateMessage stateMessage = stateManager.toState();
              return new AirbyteMessage().withType(AirbyteMessage.Type.STATE).withState(stateMessage);
            } else {
              final JsonNode ordNode = valueJson.get(MongoDbDebeziumConstants.OffsetState.VALUE_INCREMENT);
              final BsonTimestamp ts = new BsonTimestamp(secNode.asInt(), ordNode != null ? ordNode.asInt() : 0);
              LOGGER.info("[DocumentDB] Saving timestamp-only state: sec={}, ord={}", ts.getTime(), ts.getInc());
              final JsonNode cleanState = MongoDbDebeziumStateUtil.formatTimestampOnlyState(serverId, ts);
              final MongoDbCdcState cdcState = new MongoDbCdcState(cleanState, previousStateSchemaEnforced);
              stateManager.updateCdcState(cdcState);
              final AirbyteStateMessage stateMessage = stateManager.toState();
              return new AirbyteMessage().withType(AirbyteMessage.Type.STATE).withState(stateMessage);
            }
          }
        }
      } catch (final Exception e) {
        LOGGER.warn("[DocumentDB] Error inspecting offset for incompatible resume token: {}", e.getMessage());
      }
    }

    final MongoDbCdcState cdcState = new MongoDbCdcState(Jsons.jsonNode(offset), previousStateSchemaEnforced);
    LOGGER.info("Saving Debezium state {}...", cdcState);
    stateManager.updateCdcState(cdcState);

    final AirbyteStateMessage stateMessage = stateManager.toState();
    return new AirbyteMessage().withType(AirbyteMessage.Type.STATE).withState(stateMessage);
  }

  @Override
  public AirbyteMessage saveStateAfterCompletionOfSnapshotOfNewStreams() {
    throw new RuntimeException("Debezium is not used to carry out the snapshot of tables.");
  }

  @Override
  public boolean isCdcCheckpointEnabled() {
    return true;
  }

}
