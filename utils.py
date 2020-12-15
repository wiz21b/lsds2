import logging
import json
import requests

RETRY_TIME = 10  # seconds

BASE_URL = "http://127.0.0.1"

logger = logging.getLogger('peercall')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(ch)

class NetworkException(Exception):
    pass


def call_peer(peer_url, method, **kwargs):

    xlated = dict()
    for k, v in kwargs.items():
        if type(v) == dict:
            xlated[k] = json.dumps(v)
        else:
            xlated[k] = str(v)

    url = peer_url + "/" + method
    logger.debug(f"RPC to {url} : {xlated}")

    r = None
    try:
        r = requests.put(url, data=xlated)
    except Exception as ex:
        logger.error(ex)
        raise NetworkException(ex)

    if r.status_code == 200:  # HTTP status code success
        return json.loads(r.text)
    else:
        logger.error(f"unexpected HTTP status {r.status_code}")
        raise NetworkException(f"Request failure HTTP:{r.status_code}")
