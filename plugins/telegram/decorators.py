import logging
from time import sleep

from telegram.error import RetryAfter, TimedOut
from tenacity import RetryError


class TelegramMaxRetriesError(Exception):
    """Exceção levantada quando o número máximo de tentativas é atingido em chamadas da API do Telegram."""

    pass


def telegram_retry(max_retries=10):
    """Decorador para realizar novas tentativas em chamadas da API do Telegram em caso de exceções.

    Args:
    ----
        max_retries (int): Número máximo de tentativas antes de levantar a exceção TelegramMaxRetriesError.

    Returns:
    -------
        function: Função decorada.

    Raises:
    ------
        TelegramMaxRetriesError: Levantada se o número máximo de tentativas for atingido.

    Exemple:
        @telegram_retry(max_retries=5)
        def minha_funcao_telegram():
            # Sua lógica de chamada da API do Telegram aqui
            pass
    """

    def decorator(func):
        def wrapper(*args, **kwargs):
            made_retries = 0
            while made_retries < max_retries:
                try:
                    result = func(*args, **kwargs)
                    break
                except (RetryError, RetryAfter, TimedOut) as e:
                    logging.info("Exception caught: %s", e)
                    logging.warning(
                        "Message refused by Telegram's flood control. Waiting %s seconds...",
                        30,
                    )
                    sleep(30)
                    made_retries += 1
            if made_retries == max_retries:
                raise TelegramMaxRetriesError
            return result

        return wrapper

    return decorator
