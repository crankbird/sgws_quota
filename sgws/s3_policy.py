import boto
import boto.s3.connection
import logging
import json

logger = logging.getLogger(__name__)


class S3BucketPolicy:
    def __init__(self, credential, account_id, bucket):
        self.conn = boto.connect_s3(
            aws_access_key_id=credential['access_id'],
            aws_secret_access_key=credential['access_secret'],
            host=credential['host'],
            calling_format=boto.s3.connection.OrdinaryCallingFormat(),
            port=int(credential['port']),
            is_secure=credential['is_secure'],
        )
        self.account_id = account_id
        self.bucket = bucket
        self.policy = self._make_read_only_policy(self.account_id, self.bucket)

    def get_bucket_policy(self, bucket):
        try:
            bucket_handle = self.conn.get_bucket(bucket)
        except boto.exception.S3ResponseError, exception:
            logger.error('%s', exception)
        else:
            policy = bucket_handle.get_policy()
            logger.debug('bucket_handle_policy %s', policy)
            return policy

    def delete_bucket_policy(self, bucket):
        try:
            bucket_handle = self.conn.get_bucket(bucket)
        except boto.exception.S3ResponseError, exception:
            logger.error('%s', exception)
        else:
            response = bucket_handle.delete_policy()
            return response

    def set_bucket_policy(self, policy, bucket):
        try:
            bucket_handle = self.conn.get_bucket(bucket)
        except boto.exception.S3ResponseError, exception:
            logger.error('%s', exception)
        else:
            response = bucket_handle.set_policy(policy)
            return response

    @staticmethod
    def _make_read_only_policy(account_id, bucket):
        policy = {
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {
                        "SGWS": ""
                    },
                    "Action": [
                        "s3:GetObject",
                        "s3:ListBucket"
                    ],
                    "Resource": [
                        "urn:sgws:s3:::violator_bucket",
                        "urn:sgws:s3:::violator_bucket/*"
                    ]
                },
                {
                    "Effect": "Deny",
                    "Principal": {
                        "SGWS": "10901795976034390574"
                    },
                    "Action": [
                        "s3:PutObject"
                    ],
                    "Resource": [
                        "urn:sgws:s3:::violator_bucket",
                        "urn:sgws:s3:::violator_bucket/*"
                    ]
                }
            ]
        }

        # Set the account IDs.
        policy['Statement'][0]['Principal']['SGWS'] = account_id
        policy['Statement'][1]['Principal']['SGWS'] = account_id

        bucket_resource_string = "urn:sgws:s3:::" + bucket
        bucket_resource_string_contents = bucket_resource_string + '/*'

        policy['Statement'][0]['Resource'][0] = bucket_resource_string
        policy['Statement'][0]['Resource'][1] = bucket_resource_string_contents
        policy['Statement'][1]['Resource'][0] = bucket_resource_string
        policy['Statement'][1]['Resource'][1] = bucket_resource_string_contents

        return json.dumps(policy)

    def make_bucket_read_only(self):
        return self.set_bucket_policy(self.policy, self.bucket)
