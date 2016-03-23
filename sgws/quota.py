import management
import s3_policy
import logging
import json

logger = logging.getLogger(__name__)


def quota_limit_action(quota_message, bucket, account_id, admin_info, s3_info):
    logger.info('***Account: %s Bucket: %s %s***' % (account_id, bucket, quota_message))
    logger.info('***Setting bucket %s to read-only.***', bucket)

    # Create temporary S3 credentials.
    logger.debug('Creating temporary S3 account.')
    grid = management.SGWSManagement(**admin_info)
    credentials = grid.create_tenant_account_s3key(account_id)
    logger.debug('Temporary S3 account created: %s', json.dumps(credentials))

    # Perform a translation of s3_info so that it's usable in the set_bucket_read_only method.
    port = int(s3_info['port'])
    s3_info.update({'access_secret': credentials['data']['secretAccessKey']})
    s3_info.update({'access_id': credentials['data']['accessKey']})
    s3_info['port'] = port
    access_key = credentials['data']['id']

    # Set bucket to read only.
    logger.info('Setting bucket read only.')
    set_bucket_read_only(s3_info, account_id, bucket)

    # Delete temporary S3 credentials.
    logger.debug('Deleting temporary S3 account.')
    grid.delete_tenant_account_s3key(account_id, access_key)


def enforce_quota(admin_info, quota_info, s3_info):

    if quota_info != {}:
        grid = management.SGWSManagement(**admin_info)

        for account_id, quota_buckets in quota_info.iteritems():
            # Query the grid for account usage based on account ID in the quota file.
            account_usage = grid.get_tenant_account_usage(account_id)
            # Get the 'buckets' section of the account usage output.
            usage_buckets = account_usage['data']['buckets']
            # Iterate through each bucket.  Cross check the bucket name with the quota info to get the quota.
            for bucket in usage_buckets:
                try:
                    # Parse quota information for the bucket.
                    quota_byte_limit = int(quota_info[account_id][bucket['name']]['quota_byte_limit'])
                    object_count_limit = int(quota_info[account_id][bucket['name']]['object_count_limit'])
                except KeyError:
                    logger.info('Bucket %s does not have a quota applied to it.', bucket['name'])
                else:
                    logger.info('Bucket %s has a quota_byte_limit of %s and an object_count_limit of %s' %
                                (bucket['name'], quota_byte_limit, object_count_limit))

                    usage_byte_count = bucket['dataBytes']
                    usage_objects_count = bucket['objectCount']

                    logger.info('Bucket %s is using %s bytes and storing %s objects' %
                                (bucket['name'], usage_byte_count, usage_objects_count))

                    if usage_byte_count >= quota_byte_limit or usage_objects_count >= object_count_limit:
                        if usage_byte_count >= quota_byte_limit and usage_objects_count >= object_count_limit:
                            quota_limit_action('Byte and objects limits exceeded!',
                                               bucket['name'], account_id, admin_info, s3_info)
                        elif usage_objects_count >= object_count_limit:
                            quota_limit_action('Object limit exceeded!',
                                               bucket['name'], account_id, admin_info, s3_info)
                        elif usage_byte_count >= quota_byte_limit:
                            quota_limit_action('Byte limit exceeded!',
                                               bucket['name'], account_id, admin_info, s3_info)

    else:
        logger.info('Quota file is empty.')


def set_bucket_read_only(grid_s3_credentials, account_id, bucket):
    bucket_policy_handler = s3_policy.S3BucketPolicy(grid_s3_credentials, account_id, bucket)
    response = bucket_policy_handler.make_bucket_read_only()
    logger.debug('Making bucket read-only: %s', response)
    return response


def restore_bucket_policy(grid_s3_credentials, account_id, bucket):
    pass

