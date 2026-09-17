import pydash
import re

class Utils:
    def __init__(self):
        pass

    @staticmethod
    def raise_if_invalid(config):
        required_fields = [
            "url_base",
            "auth_request",
            "stream_list"
        ]
        missing_fields = [field for field in required_fields if field not in config]
        if missing_fields:
            raise Exception(f"Required fields missing: [{', '.join(missing_fields)}]")

    @staticmethod
    def find_parent_stream_name_and_key(url):
        pattern = r"\[(\w+)\]"
        matches = re.findall(pattern, url)
        return matches[0], matches[1]

    @staticmethod
    def check_response_has_valid_keys(response, keys):
        if not all(key in response for key in keys):
            raise Exception(f"Not all keys '{keys}' are present in the Response object '{response.keys()}'")

    @staticmethod
    def build_dict(key_value_list):
        return None if key_value_list is None else {key_val['key']: key_val['value'] for key_val in key_value_list}

    @staticmethod
    def get_from_object(obj, path, default=None):
        return pydash.get(obj, path, default)
