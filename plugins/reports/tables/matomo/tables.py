from io import StringIO

import pandas as pd


class MatotmoTables:
    """Provides methods to generate specific tables for the matomo report."""

    @classmethod
    def generate_table_access_data_overview(
        cls, matomo_visits_summary_csv: str, matomo_visits_frequency_csv: str
    ):
        df_summary = pd.read_csv(StringIO(matomo_visits_summary_csv))
        df_frequency = pd.read_csv(StringIO(matomo_visits_frequency_csv))

        if df_frequency.empty or df_summary.empty:
            return None

        df_acess = pd.concat(
            [
                df_summary[["nb_visits", "bounce_rate"]],
                df_frequency[["nb_visits_new", "nb_visits_returning"]],
            ],
            axis=1,
        )

        df_acess = df_acess.rename(
            columns={
                "nb_visits": "Visitas",
                "bounce_rate": "Taxa de Rejeição",
                "nb_visits_new": "Visitas Novas",
                "nb_visits_returning": "Visitas de Retorno",
            }
        )

        return df_acess.to_dict("records")
