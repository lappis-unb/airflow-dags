class Table:  # noqa: D101
    @classmethod
    def split_tables(cls, table: list, max_elements: int = 20) -> list:
        return [table[i : i + max_elements] for i in range(0, len(table), max_elements)]
