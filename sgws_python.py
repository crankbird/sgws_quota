import logging
from sgws import parseargs, quota

logger = logging.getLogger(__name__)


def main():
    config = parseargs.parse_arguments()
    logger.info('Starting quota enforcement.')
    quota.enforce_quota(config['admin_node'],
                        config['quotas'],
                        config['s3_endpoint'])
    logger.info('Quota enforcement complete.')


if __name__ == '__main__':
    main()