from typing import TYPE_CHECKING

import pendulum

if TYPE_CHECKING:
    from pendulum.datetime import DateTime


def fix_date(start_date: str, end_date: str):
    start_date: DateTime = pendulum.parse(str(start_date))
    end_date: DateTime = pendulum.parse(str(end_date))

    return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")
