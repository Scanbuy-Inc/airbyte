import requests
import json

from .util import Utils

class APIClient:
    def __init__(self, url_base):
        self.url_base = url_base

    def build_request(self, req):
        if 'url' not in req or 'verb' not in req:
            raise Exception("['url', 'verb'] one of the key is missing")
        endpoint = req.get('url')
        self.verb = req.get('verb')
        self.headers = Utils.build_dict(req.get('headers'))
        self.query_params = Utils.build_dict(req.get('query_params'))
        self.body = json.loads(req.get('body')) if req.get('body') else None

        self.url = endpoint if (endpoint.startswith('http') or endpoint.startswith('https')) else self.url_base + endpoint
        return self

    def send(self):
        if self.verb == 'GET':
            return requests.get(self.url, headers=self.headers, params=self.query_params)
        elif self.verb == 'POST':
            return requests.post(self.url, headers=self.headers, params=self.query_params, json=self.body)
        elif self.verb == 'PUT':
            return requests.put(self.url, headers=self.headers, params=self.query_params, json=self.body)
        elif self.verb == 'PATCH':
            return requests.put(self.url, headers=self.headers, params=self.query_params, json=self.body)
        elif self.verb == 'DELETE':
            return requests.delete(self.url, headers=self.headers, params=self.query_params)
        elif self.verb == 'HEAD':
            return requests.put(self.url, headers=self.headers, params=self.query_params, json=self.body)
        elif self.verb == 'OPTION':
            return requests.put(self.url, headers=self.headers, params=self.query_params, json=self.body)
        else:
            raise Exception('Unsupported HTTP verb')
