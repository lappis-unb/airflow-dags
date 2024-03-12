import logging
from functools import wraps


def decople(func):

    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logging.info("Exception caught: %s", e)
            # TODO: Fazer a notificação para avisar que o grafico não foi.
            return None
    return wrapper
