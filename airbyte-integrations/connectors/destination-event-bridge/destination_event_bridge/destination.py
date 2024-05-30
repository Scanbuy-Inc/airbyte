#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from typing import Any, Iterable, Mapping, List

import json
import boto3

from airbyte_cdk import AirbyteLogger
from airbyte_cdk.destinations import Destination
from airbyte_cdk.models import AirbyteConnectionStatus, AirbyteMessage, ConfiguredAirbyteCatalog, Status, Type

MAX_PUT_ENTRIES = 10

class DestinationEventBridge(Destination):
    def _get_events_client(self, session: boto3.Session, config: Mapping[str, Any]) -> boto3.client:
        return session.client("events", endpoint_url=config.get("endpoint_url"))
    
    def _check_config(self, logger: AirbyteLogger, config: Mapping[str, Any]):
        required_keys = {"credentials"}
        defaults = {
            "region": "us-east-2",
            "bus_name": "default",
            "event_source": "scb-airbyte",
            "event_data_type": "Call via Scanbuy Airbyte"
        }

        for key in required_keys | defaults.keys():
            value = config.get(key)
            if not value:
                if key in required_keys:
                    if logger:
                        logger.debug(f"Amazon Event Bridge Destination Config Check - {key} is missing")   
                    raise Exception(f"'{key}' not found")
                else:
                    config[key] = defaults[key]
                    if logger:
                        logger.debug(f"Amazon Event Bridge Destination Config Check - {key} setting default to {defaults[key]}")

    def _create_session(self, config: Mapping[str, Any]) -> boto3.Session:
        return boto3.Session(
            aws_access_key_id=config.get("credentials")["aws_access_key_id"],
            aws_secret_access_key=config.get("credentials")["aws_secret_access_key"],
            region_name=config.get("region"))
    
    def _put_events(self, client: boto3.client, detail_list: List[dict], config: Mapping[str, Any]) -> dict:
        entries = []
        for detail in detail_list[:MAX_PUT_ENTRIES]:
            entries.append({
                "Source": config.get("event_source"),
                "DetailType": config.get("event_data_type"),
                "Detail": json.dumps(detail),
                "EventBusName": config.get("bus_name")
            })
        return client.put_events(Entries=entries)

    def write(
        self, config: Mapping[str, Any], configured_catalog: ConfiguredAirbyteCatalog, input_messages: Iterable[AirbyteMessage]
    ) -> Iterable[AirbyteMessage]:

        """
        Reads the input stream of messages, config, and catalog to write data to the destination.

        This method returns an iterable (typically a generator of AirbyteMessages via yield) containing state messages received
        in the input message stream. Outputting a state message means that every AirbyteRecordMessage which came before it has been
        successfully persisted to the destination. This is used to ensure fault tolerance in the case that a sync fails before fully completing,
        then the source is given the last state message output from this method as the starting point of the next sync.

        :param config: dict of JSON configuration matching the configuration declared in spec.json
        :param configured_catalog: The Configured Catalog describing the schema of the data being received and how it should be persisted in the
                                    destination
        :param input_messages: The stream of input messages received from the source
        :return: Iterable of AirbyteStateMessages wrapped in AirbyteMessage structs
        """
        streams = {s.stream.name for s in configured_catalog.streams}
        self._check_config(None, config)
        session = self._create_session(config)
        client = self._get_events_client(session, config)
        try:
            batch = []
            for message in input_messages:
                if message.type == Type.STATE:
                    # Emitting a state message indicates that all records which came
                    # before it have been written to the destination. We don't need to
                    # do anything specific to save the data so we just re-emit these
                    yield message
                elif message.type == Type.RECORD:
                    if message.record.stream not in streams:
                        # Message contains record from a stream that is not in the catalog.
                        # Skip
                        continue
                    if message.record.data:
                        batch.append(message.record.data)
                        if len(batch) == MAX_PUT_ENTRIES:
                            response = self._put_events(client, batch, config)
                            batch = []
                else:
                    # Let's ignore other message types for now
                    continue
            if batch:
                response = self._put_events(client, batch, config)
        finally:
            client.close()

    def check(self, logger: AirbyteLogger, config: Mapping[str, Any]) -> AirbyteConnectionStatus:
        """
        Tests if the input configuration can be used to successfully connect to the destination with the needed permissions
            e.g: if a provided API token or password can be used to connect and write to the destination.

        :param logger: Logging object to display debug/info/error to the logs
            (logs will not be accessible via airbyte UI if they are not passed to this logger)
        :param config: Json object containing the configuration of this destination, content of this json is as specified in
        the properties of the spec.json file

        :return: AirbyteConnectionStatus indicating a Success or Failure
        """
        try:
            logger.debug("Amazon Event Bridge Destination Check - Starting connection test ---")
            self._check_config(logger, config)
            session = self._create_session(config)
            client = self._get_events_client(session, config)
            if client:
                # List event buses
                response = client.list_event_buses()
                event_buses = [bus["Arn"] for bus in response["EventBuses"]]
                logger.debug(event_buses)

                # Put event                
                response = self._put_events(client, [{
                    "action": "check"
                }], config)                
                logger.debug(response)

                if len(response.get("Entries", [])) != 1 or response["Entries"][0].get("ErrorCode"):
                    raise Exception(f"Could not perform 'PutEvent'")

                logger.debug("Amazon Event Bridge Destination Check - Connection test successful ---")
                client.close()
                return AirbyteConnectionStatus(status=Status.SUCCEEDED)
            else:
                return AirbyteConnectionStatus(
                    status=Status.FAILED, message="Amazon Event Bridge Destination Check - Could not create client"
                )
        except Exception as e:
            return AirbyteConnectionStatus(status=Status.FAILED, message=f"An exception occurred: {repr(e)}")
