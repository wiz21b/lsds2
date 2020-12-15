import threading
import logging
import json
import requests

RETRY_TIME = 10  # seconds

BASE_URL = "http://127.0.0.1"

log_level = logging.DEBUG
log_level = logging.ERROR

logger = logging.getLogger('peercall')
logger.setLevel(log_level)
ch = logging.StreamHandler()
ch.setLevel(log_level)
ch.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(ch)



class ThreadWithReturnValue(threading.Thread):
    # https://stackoverflow.com/questions/6893968/how-to-get-the-return-value-from-a-thread-in-python

    def __init__(self, group=None, target=None, name=None, args=(), kwargs=None, *, daemon=None):
        threading.Thread.__init__(self, group, target, name, args, kwargs, daemon=daemon)

        self._return = None

    def run(self):
        if self._target is not None:
            self._return = self._target(*self._args, **self._kwargs)

    def join(self):
        threading.Thread.join(self)
        return self._return


class NetworkException(Exception):
    pass

def call_peer_with_dict(peer_url, method, args_dict):
    return call_peer(peer_url, method, **args_dict)

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
        try:
            res = json.loads(r.text)
            logger.debug(f"Return sucess ! : {res}")
            return res
        except json.decoder.JSONDecodeError as ex:
            logger.error(f"Bad JSON : {r.text}")
            raise NetworkException(f"Bad JSON")

    else:
        logger.error(f"unexpected HTTP status {r.status_code}")
        raise NetworkException(f"Request failure HTTP:{r.status_code}")
