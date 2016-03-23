import requests
import logging
import json

logger = logging.getLogger(__name__)


class SGWSManagement:
    def __init__(self, username, password, endpoint):
        self.grid_info = dict(
            username=username,
            password=password,
            endpoint=endpoint
        )
        self.api_version = '/api/v1'
        self.verify = False
        self.bearer_token = self.get_bearer_token()

        self.bearer_token_header = {'Authorization': 'Bearer' + ' ' + self.bearer_token}

    def get_bearer_token(self):
        url = self.grid_info['endpoint'] + self.api_version + '/authorize'
        r = requests.post(url, data=json.dumps({'username': self.grid_info['username'],
                                                'password': self.grid_info['password'],
                                                'cookie': 'true'}), verify=self.verify)
        logger.debug('Response: %s', r.text)
        return r.json()['data']

    def list_tenant_accounts(self):
        url = self.grid_info['endpoint'] + self.api_version + '/grid/accounts'
        r = requests.get(url, headers=self.bearer_token_header, verify=self.verify)
        logger.debug('Response: %s', r.text)
        return r.json()

    def create_tenant_account(self, **options):
        url = self.grid_info['endpoint'] + self.api_version + '/grid/accounts'
        body = {
            'id': '',
            'name': options['name'],
            'capabilities': options['capabilities']
        }
        r = requests.post(url, headers=self.bearer_token_header, data=json.dumps(body), verify=self.verify)
        logger.debug('Response: %s', r.text)
        return r.json()

    def delete_tenant_account(self, account_id):
        url = self.grid_info['endpoint'] + self.api_version + '/grid/accounts/' + account_id
        r = requests.delete(url, headers=self.bearer_token_header, verify=self.verify)
        logger.debug('Response: %s', r.status_code)
        return r.status_code

    def get_tenant_account_usage(self, account_id):
        url = self.grid_info['endpoint'] + self.api_version + '/grid/accounts/' + account_id + '/usage'
        r = requests.get(url, headers=self.bearer_token_header, verify=self.verify)
        logger.debug('Response: %s', r.text)
        return r.json()

    def get_s3_root_tenant_access_keys(self, account_id):
        url = self.grid_info['endpoint'] + self.api_version + '/grid/accounts/' + account_id + '/s3-access-keys'
        r = requests.get(url, headers=self.bearer_token_header, verify=self.verify)
        logger.debug('Response: %s', r.text)
        return r.json()

    def delete_tenant_account_s3key(self, account_id, access_key):
        url = self.grid_info['endpoint'] + self.api_version + '/grid/accounts/' + account_id + '/s3-access-keys/' + \
              access_key
        r = requests.delete(url, headers=self.bearer_token_header, verify=self.verify)
        logger.debug('Response: %s', r.status_code)
        return r.status_code

    def create_tenant_account_s3key(self, account_id):
        url = self.grid_info['endpoint'] + self.api_version + '/grid/accounts/' + account_id + '/s3-access-keys'
        body = {
            "expires": "2020-09-04T00:00:00.000Z"
        }
        r = requests.post(url, headers=self.bearer_token_header, verify=self.verify, data=json.dumps(body))
        logger.debug('Response: %s', r.text)
        return r.json()
