import argparse
import logging
from configobj import ConfigObj

logger = logging.getLogger(__name__)


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('--DEBUG', '-d', action='store_true', default=False, dest='level',
                        help='Set logging level to DEBUG.')
    parser.add_argument('-c', action='store', type=str, dest='config_file', default='../config/sgws_management.conf',
                        help='Specify the config file.  The default is sgws_management.conf')

    results = parser.parse_args()
    config = ConfigObj(results.config_file)

    set_debug = results.level
    log_file = results.config_file

    if config == {}:
        logger.info('Config file is empty.  Exiting.')
        exit()
    else:
        log_file = config['general']['log']
        set_debug = config['general']['debug']

    log_format = '%(asctime)s %(levelname)s %(message)s'
    console = logging.StreamHandler()

    if set_debug:
        console.setLevel(logging.DEBUG)
        logging.basicConfig(level=logging.DEBUG,
                            filename=log_file,
                            format=log_format,
                            )
    else:
        console.setLevel(logging.INFO)
        logging.basicConfig(level=logging.INFO,
                            filename=log_file,
                            format=log_format
                            )

    formatter = logging.Formatter(log_format)
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)

    return config.dict()
