import random
from itertools import combinations
from pathlib import Path
from typing import Any, Dict, List
from unittest import mock

import pytest
from airflow.models.connection import Connection
from telegram.error import RetryAfter, TimedOut
from tenacity import RetryError

from dags.processes_confs.dag_create_processes_config import (
    __file__ as path_to_queries_in_processes_config,
)
from dags.processes_confs.dag_create_processes_config import (
    _create_telegram_topic,
    _get_participatory_space_mapped_to_query_file,
    _search_date_key,
)
from plugins.telegram.decorators import TelegramMaxRetriesError


@pytest.fixture
def time_sleep_mocker(mocker):
    return mocker.patch("plugins.telegram.decorators.sleep", return_value=None)


@pytest.fixture
def create_forum_topic_mocker(mocker) -> mock.MagicMock:
    return mocker.patch("airflow.providers.telegram.hooks.telegram.TelegramHook.get_conn")


@pytest.fixture
def mock_connection(mocker):
    mock_connection = Connection(
        conn_type="http",
        login="some_username",
        password="super_secret_password",
        host="some.host.com",
        port=8080,
    )

    mock_connection_uri = mock_connection.get_uri()
    mocker.patch.dict("os.environ", AIRFLOW_CONN_TELEGRAM_DECIDIM=mock_connection_uri)


@pytest.mark.parametrize(
    "participatory_space,date_key_pattern,expected",
    [
        ({"tes": 23}, r"tes", "tes"),
        ({"tes": 23}, r"tese", None),
        ({"tes_231": 23}, r"^tes", "tes_231"),
        ({"start_date": 23}, r"(start|creation).*Date", "start_date"),
        ({"creation_date": 23}, r"(start|creation).*Date", "creation_date"),
        ({"creationDate": 23}, r"(start|creation).*Date", "creationDate"),
        ({2: 23}, r"(start|creation).*Date", None),
        ({}, r"(start|creation).*Date", None),
        ({"end_date": 23}, r"(end|closing).*Date", "end_date"),
        ({"ENDDATE": 23}, r"(end|closing).*Date", "ENDDATE"),
    ],
)
def test_multi_keys_search(participatory_space: Dict[str, Any], date_key_pattern: str, expected: str):
    assert _search_date_key(participatory_space, date_key_pattern) == expected


def _create_test_cases():
    """
    Cria casos de teste para a função _get_participatory_space_mapped_to_query_file.

    A função gera combinações de participatory_spaces e mapeamentos esperados
    com base em consultas em um diretório especificado. Os casos de teste variam
    em tamanho, cobrindo desde uma única participatory_space até todas as disponíveis.

    Returns
    -------
        list: Uma lista de casos de teste, cada um representado por uma tupla
        contendo uma lista de participatory_spaces e um dicionário com os mapeamentos esperados.
    """
    queries_folder = Path(path_to_queries_in_processes_config).parent.joinpath("./queries")
    test_case = [
        "participatory_processes",
        "initiatives",
        "consultations",
        "conferences",
        "assemblies",
    ]
    sizes_to_test = [*range(1, len(test_case) + 1)]
    tests_cases = []
    for size in sizes_to_test:
        for combination in combinations(test_case, size):
            tests_cases.append(
                (
                    [*combination],
                    {
                        participatory_space: queries_folder.joinpath(
                            f"./components_in_{participatory_space}.gql"
                        )
                        for participatory_space in combination
                    },
                )
            )
    return tests_cases


@pytest.mark.parametrize("participatory_spaces,expected", _create_test_cases())
def test_success_get_participatory_space_mapped(participatory_spaces: List, expected: Dict[str, str]):
    """
    Testa o sucesso da função _obter_espaco_participativo_mapeado_para_arquivo_de_consulta.

    Args:
    ----
        espacos_participativos (list): Lista de espaços participativos a serem testados.
        esperado (dict): Dicionário com os mapeamentos esperados para as consultas.

    Verifica se a função retorna os mapeamentos esperados para as consultas
    associadas aos espaços participativos fornecidos.
    """
    assert (
        _get_participatory_space_mapped_to_query_file(participatory_spaces=participatory_spaces) == expected
    )


class TestingTelegram:  # noqa: D101
    def __init__(self, message_thread_id) -> None:
        self.message_thread_id = message_thread_id


# https://selectfrom.dev/writing-unit-tests-for-airflow-custom-operators-sensors-and-hooks-9771d21fe3b9
@pytest.mark.parametrize(
    "chat_id, name, expected",
    [
        (-15156165165161, "Test Topic 1", 45),
        (-15156165165161, "Test Topic 2", -1516),
    ],
)
def test_success_create_telegram_topic(
    mock_connection,
    create_forum_topic_mocker,
    time_sleep_mocker,
    mocker,
    chat_id,
    name,
    expected,
):
    async def get_expected_value():
        return TestingTelegram(expected)

    create_forum_topic_mocker.return_value.create_forum_topic.return_value = get_expected_value()
    assert _create_telegram_topic(chat_id, name) == expected
    assert create_forum_topic_mocker.call_count == 2


@pytest.mark.parametrize(
    "chat_id, name, expected",
    [
        (None, "Test Topic 1", 45),
        ("-12333", "Test Topic 2", -1516),
        (-12333, None, -1516),
        (None, None, -1516),
    ],
)
def test_fail_create_telegram_topic(
    mock_connection,
    create_forum_topic_mocker,
    time_sleep_mocker,
    chat_id,
    name,
    expected,
):
    with pytest.raises(TypeError):
        assert _create_telegram_topic(chat_id, name) == expected
    assert create_forum_topic_mocker.call_count == 0

    # mock_telegram_hook.return_value.get_conn.return_value.create_forum_topic.return_value = 42


def create_success_retries_sequences():
    possible_raises = [RetryAfter(30), RetryError(False), TimedOut]

    result_test_cases = []
    for i in range(1, 10):
        sequence = random.choices(possible_raises, k=i)
        result_test_cases.append((-123123, "Random Test", sequence, 42))
    return result_test_cases


def name_test_retries(value):
    if isinstance(value, list):
        return f"random-raise-size-{len(value)}"
    return ""


@pytest.mark.parametrize(
    "chat_id, name, sequence, expected",
    create_success_retries_sequences(),
    ids=lambda x: name_test_retries(x),
)
def test_retry_success_create_telegram_topic(
    mock_connection,
    time_sleep_mocker,
    create_forum_topic_mocker,
    chat_id,
    name,
    sequence,
    expected,
):
    async def get_expected_value():
        return TestingTelegram(expected)

    sequence.append(get_expected_value())
    create_forum_topic_mocker.return_value.create_forum_topic.side_effect = sequence

    assert _create_telegram_topic(chat_id, name) == expected
    assert create_forum_topic_mocker.call_count == 2 * len(sequence)
    assert time_sleep_mocker.call_count == len(sequence) - 1

    # mock_telegram_hook.return_value.get_conn.return_value.create_forum_topic.return_value = 42


@pytest.mark.parametrize(
    "chat_id, name, expected",
    [(-15156165165161, "Test Fail Retries", 45)],
)
def test_retry_fail_create_telegram_topic(
    mock_connection,
    create_forum_topic_mocker,
    time_sleep_mocker,
    chat_id,
    name,
    expected,
):
    possible_raises = [RetryAfter(30), RetryError(False), TimedOut]
    sequence = random.choices(possible_raises, k=10)

    create_forum_topic_mocker.return_value.create_forum_topic.side_effect = sequence
    with pytest.raises(TelegramMaxRetriesError):
        _create_telegram_topic(chat_id, name)
    assert create_forum_topic_mocker.call_count == 2 * len(sequence)
    assert time_sleep_mocker.call_count == 10

    # mock_telegram_hook.return_value.get_conn.return_value.create_forum_topic.return_value = 42
