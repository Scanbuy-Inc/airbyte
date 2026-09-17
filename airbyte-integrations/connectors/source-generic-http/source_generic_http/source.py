#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

import logging
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import requests
import json

from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream, HttpSubStream
from .client import APIClient
from .util import Utils
from abc import ABC

logger = logging.getLogger("airbyte")

class GenericStream(HttpStream, ABC):

    def __init__(self, config: Mapping[str, Any], stream_request: Mapping[str, Any], **kwargs: Any):
        self.config = config
        self.stream_request = stream_request

        self._key = self.stream_request.get('name')
        self._use_cache = self.stream_request.get('use_cache')
        self.auth = APIClient(self.config.get('url_base')).build_request(self.config.get('auth_request')).send()
        self.auth.raise_for_status()
        result_key = self.config.get('auth_request').get('result_key')
        if result_key and result_key in self.auth.json():
            self.auth = Utils.get_from_object(self.auth.json(), result_key)
        else:
            token_type = self.auth.json().get('token_type') or 'Bearer'
            token = self.auth.json().get('access_token')
            self.auth = f"{token_type} {token}"

        super().__init__(**kwargs)

    @property
    def name(self) -> str:
        """
        :return: Stream name. By default this is the implementing class name, but it can be overridden as needed.
        """
        return self._key

    @property
    def key(self) -> str:
        return self._key

    @property
    def use_cache(self) -> bool:
        return self._use_cache

    @property
    def http_method(self) -> str:
        return Utils.get_from_object(self.stream_request, 'api.verb', 'GET')

    @property
    def primary_key(self) -> str:
        return Utils.get_from_object(self.stream_request, 'emit_key', 'id')

    def get_json_schema(self):
        if Utils.get_from_object(self.stream_request, 'api.response_schema'):
            return json.loads(Utils.get_from_object(self.stream_request, 'api.response_schema'))
        schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {}
        }
        schema["properties"].update({self.primary_key: {"type": "string"}})
        return schema

    @property
    def url_base(self) -> str:
        """
        :return: URL base for the  API endpoint
        """
        return self.config.get('url_base')

    def path(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        path = Utils.get_from_object(self.stream_request, 'api.url', '/')
        if 'stream_slice' in path:
            path = path.format(**{"stream_slice": stream_slice})
        return path

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        """
        Usually contains common params e.g. pagination size etc.
        """
        params = Utils.build_dict(Utils.get_from_object(self.stream_request, 'api.query_params', {}))
        if next_page_token:
            params.update(next_page_token)
        return params

    def request_body_json(
        self,
        stream_state: Optional[Mapping[str, Any]],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Optional[Mapping[str, Any]]:
        """
        Override when creating POST/PUT/PATCH requests to populate the body of the request with a JSON payload.

        At the same time only one of the 'request_body_data' and 'request_body_json' functions can be overridden.
        """
        if Utils.get_from_object(self.stream_request, 'api.body'):
            return json.loads(Utils.get_from_object(self.stream_request, 'api.body'))
        return None

    def request_headers(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> Mapping[str, Any]:
        """
        Override to return any headers.
        """
        return {
            'Content-Type': 'application/json',
            'Authorization': self.auth
        }

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        """
        :param response: the most recent response from the API
        :return If there is another page in the result, a mapping (e.g: dict) containing information needed to query the next page in the response.
                If there are no more pages in the result, return None.
        """
        type = Utils.get_from_object(self.stream_request, 'pagination.type')
        if not type or type == "None":
            return None

        params = {}
        for key, value in Utils.build_dict(Utils.get_from_object(self.stream_request, 'pagination.query_params')).items():
            next_token = Utils.get_from_object(response.json(), value)
            if next_token is not None:
                params[key] = next_token
        return params

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        :return an iterable containing each record in the response
        """
        try:
            response.raise_for_status()
            res = Utils.get_from_object(response.json(), Utils.get_from_object(self.stream_request, 'api.result_key'), response.json())

            if isinstance(res, dict):
                yield res
            elif isinstance(res, list):
                for item in res:
                    yield {self.primary_key: item}
            else:
                yield {self.primary_key: res}
        except Exception as e:
            logger.error("Failed to parse_response")
            logger.error(e)


class GenericStreamImplementation(GenericStream):
    def __init__(self, config: Mapping[str, Any], stream_request: Mapping[str, Any], **kwargs: Any):
        super().__init__(config, stream_request, **kwargs)


class GenericChildStreamImplementation(HttpSubStream, GenericStream):
    def __init__(self, parent: GenericStreamImplementation, config: Mapping[str, Any], **kwargs: Any):
        super().__init__(parent=parent, config=config, **kwargs)

    def stream_slices(
            self, sync_mode: SyncMode, cursor_field: Optional[List[str]] = None, stream_state: Optional[Mapping[str, Any]] = None
    ) -> Iterable[Optional[Mapping[str, Any]]]:
        parent_stream_slices = self.parent.stream_slices(
            sync_mode=SyncMode.full_refresh, cursor_field=cursor_field, stream_state=stream_state
        )

        # iterate over all parent stream_slices
        for stream_slice in parent_stream_slices:
            parent_records = self.parent.read_records(
                sync_mode=SyncMode.full_refresh, cursor_field=cursor_field, stream_slice=stream_slice, stream_state=stream_state
            )

            # iterate over all parent records with current stream_slice
            for record in parent_records:
                yield {self.parent.name: record}


class SourceGenericHttp(AbstractSource):

    def check_connection(self, logger, config) -> Tuple[bool, any]:
        """
        :param config:  the user-input config object conforming to the connector's spec.yaml
        :param logger:  logger object
        :return Tuple[bool, any]: (True, None) if the input config can be used to connect to the API successfully, (False, error) otherwise.
        """
        try:
            Utils.raise_if_invalid(config)
            auth = APIClient(config.get('url_base')).build_request(config.get('auth_request')).send()
            auth.raise_for_status()
            if config.get('auth_request').get('result_key') is not None:
                keys = [config.get('auth_request').get('result_key')]
            else:
                keys = ['access_token', 'token_type']
            Utils.check_response_has_valid_keys(auth.json(), keys)
            logger.info(f"{config.get('url_base')} - Authenticated")

            # TODO: Add check for each stream

            return True, None
        except Exception as error:
            return False, error

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        :param config: A Mapping of the user input configuration as defined in the connector spec.
        """
        streams = []
        for stream_request in config.get('stream_list'):
            if 'stream_slice' in Utils.get_from_object(stream_request, 'api.url'):
                parent_stream_name, key = Utils.find_parent_stream_name_and_key(Utils.get_from_object(stream_request, 'api.url'))
                for stream in config.get('stream_list'):
                    parent_stream_found = stream.get('name') == parent_stream_name
                    if parent_stream_found:
                        parent = GenericStreamImplementation(config=config, stream_request=stream)
                        if key != parent.primary_key:
                            raise Exception(f"Parent key mismatch: '{key}' != '{parent.primary_key}'")
                        streams.append(GenericChildStreamImplementation(parent=parent, config=config, stream_request=stream_request))
                        break
                if not parent_stream_found:
                    raise Exception(f"Parent stream not found: '{parent_stream_name}'")
            else:
                streams.append(GenericStreamImplementation(config=config, stream_request=stream_request))
        return streams
