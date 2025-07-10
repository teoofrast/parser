from celery import Celery
from redlock import Redlock
import hashlib
import redis

import requests
import random
from custom_exc import *
import logging

logger = logging.getLogger(__name__)


celery_app = Celery(
    'parser',
    broker='redis://parser_redis:6379/0',
    backend='redis://parser_redis:6379/0',
)

redis_client = redis.Redis(host='parser_redis', port=6379, db=0)
dlm = Redlock([redis_client])

SEMAPHORE_SLOTS = 3
LOCK_TTL = 60000
RETRY_DELAY = 15


def get_random_user_agent():
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0",
        "Mozilla/5.0 (Linux; Android 10; SM-G973F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Mobile Safari/537.36",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1"
    ]
    return random.choice(user_agents)



@celery_app.task(
    bind=True,
    name='parse_url',
    autoretry_for=(ManyRespError, ServerError, requests.exceptions.RequestException),
    retry_kwargs={'max_retries': 5},
    retry_backoff=10,
    retry_backoff_max=100,
    retry_jitter=True,
)
def parse_url(self, url: str):

    url_hash = hashlib.md5(url.encode('utf-8')).hexdigest()
    lock_keys = [f"lock:{url_hash}:{i}" for i in range(SEMAPHORE_SLOTS)]

    acquired_lock = None
    for key in lock_keys:
        acquired_lock = dlm.lock(key, LOCK_TTL)
        if acquired_lock:
            logger.info('Task %s acquired lock %s for URL %s', self.request.id, key, url)
            break

    if not acquired_lock:
        logger.warning('Task %s failed to acquire lock for URL %s. All %s slots are busy. Retrying in %ss.', self.request.id, url, SEMAPHORE_SLOTS, RETRY_DELAY)
        raise self.retry(countdown=RETRY_DELAY, max_retries=None) # Retry indefinitely until a lock is free

    try:
        proxies = []
        headers = {
            'User-Agent': get_random_user_agent()
        }

        try:
            response = requests.get(url, headers=headers, proxies=proxies)
            print(response.status_code)

            if response.status_code == 200:
                return response.text

            elif response.status_code == 429:
                raise ManyRespError(f'To many response error to {url}')

            elif response.status_code == 403:
                raise Response403Error(f'Error when get response to {url}')

            elif response.status_code >= 500:
                raise ServerError(f'Server error when get response to {url}')

            else:
                raise Exception(f'Unknown error when get response to {url}')

        except Exception as e:
            raise e

    finally:
        if acquired_lock:
            dlm.unlock(acquired_lock)
            logger.info('Task %s released lock for URL %s', self.request.id, url)
