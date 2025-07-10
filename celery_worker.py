from celery import Celery

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
    autoretry_for=(ManyRespError, ServerError),
    retry_kwargs={'max_retries': 5},
    retry_backoff=True,
    retry_backoff_base=10,
    retry_backoff_max=100,
    retry_jitter=True,
)
def parse_url(self, url: str):
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


    except ManyRespError as e:

        print(f"Caught ManyRespError: {e}. Preparing for retry...")

        self.update_state(

            state='RETRYING',
            meta={
                'current_attempt': self.request.retries + 1,
                'max_attempts': self.max_retries,
                'exc_type': type(e).__name__,
                'exc_message': str(e),
                'reason': 'Too many requests'
            }
        )
        raise self.retry(exc=e)

    except ServerError as e:

        print(f"Caught ServerError: {e}. Preparing for retry...")
        self.update_state(
            state='RETRYING',
            meta={
                'current_attempt': self.request.retries + 1,
                'max_attempts': self.max_retries,
                'exc_type': type(e).__name__,
                'exc_message': str(e),
                'reason': 'Server side issue'
            }
        )
        raise self.retry(exc=e)

    except requests.exceptions.RequestException as e:
        print(f"Caught RequestException (network error): {e}. Retrying if not max retries...")
        self.update_state(
            state='RETRYING' if self.request.retries < self.max_retries else 'FAILURE',
            meta={
                'current_attempt': self.request.retries + 1,
                'max_attempts': self.max_retries,
                'exc_type': type(e).__name__,
                'exc_message': str(e),
                'reason': 'Network error'
            }
        )
        if self.request.retries < self.max_retries:
            raise self.retry(exc=e)
        else:
            raise


    except Exception as e:
        print(f"Caught unhandled Exception: {e}. Marking as FAILURE.")
        self.update_state(state='FAILURE', meta={'exc_type': type(e).__name__, 'exc_message': str(e)})
        raise e
