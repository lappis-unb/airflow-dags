from unittest.mock import MagicMock

import pytest

from plugins.telegram.callbacks import send_telegram


@pytest.fixture
def mock_context():
    return {
        "task_instance": MagicMock(dag_id="test_dag_id", task_id="test_task_id"),
        "exception": "Test exception message",
    }


@pytest.fixture
def mock_telegram_hook(mocker):
    return mocker.patch("plugins.telegram.callbacks.TelegramHook")


def test_send_telegram(mock_context, mock_telegram_hook):
    mock_hook_instance = MagicMock()
    mock_telegram_hook.return_value = mock_hook_instance

    send_telegram(mock_context)

    mock_telegram_hook.assert_called_once_with("airflow-telegram-moderation")
    mock_hook_instance.send_message.assert_called_once_with(
        api_params={
            "text": f"""
<b>⚠️ DAG <code>{mock_context['task_instance'].dag_id}</code> has Failed!</b>

    <b>Task: <code>{mock_context['task_instance'].task_id}</code> -FAILED</b>

<b>Exception:</b>
    <pre><code class="language-log">{mock_context['exception']}</code></pre>
""",
            # "message_thread_id": self.telegram_topic_id,
        }
    )


def test_send_telegram_hook_exception(mock_telegram_hook, mock_context):
    mock_telegram_hook.side_effect = Exception("Test hook exception")

    with pytest.raises(Exception, match="Test hook exception"):
        send_telegram(mock_context)

    # Assert that TelegramHook is called
    mock_telegram_hook.assert_called_once_with("airflow-telegram-moderation")
