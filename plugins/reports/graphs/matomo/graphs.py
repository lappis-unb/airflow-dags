import json
from io import StringIO
from pathlib import Path

import geopandas as gpd
import pandas as pd
import plotly.express as px

from plugins.reports.decorators import decople
from plugins.reports.graphs.base.graphs import ReportGraphs


class MatomoGraphs(ReportGraphs):
    """Provides methods to generate specific graphs for the Matomo report."""

    def _get_brasil_states_map(self) -> gpd.GeoDataFrame:
        shapefile_path = Path(__file__).parent.joinpath("./geo/shapefile/estados_2010.shp")
        return gpd.read_file(shapefile_path)

    def _get_population_data(self) -> dict:
        population_json_path = Path(__file__).parent.joinpath("geo/population_uf.json")
        with population_json_path.open("r") as f:
            population_data = json.load(f)
        return population_data["population_uf"]

    def generate_device_graph(self, matomo_device_get_type: str):
        df = pd.read_csv(StringIO(matomo_device_get_type))
        if df.empty:
            return None

        matomo_data_sorted = df.sort_values("nb_visits", ascending=False).head(3)
        color_discrete_map = {"Smartphone": "#183EFF", "Desktop": "#FFD000", "Phablet": "#00D000"}

        fig = px.pie(
            matomo_data_sorted,
            names="label",
            values="nb_visits",
            title="Top 3 Dispositivos mais Utilizados",
            hole=0.3,
            labels={"label": "Dispositivos", "nb_visits": "Numero de Visitas"},
            color="label",
            color_discrete_map=color_discrete_map,
        )

        fig.update_layout(
            title=dict(
                text="Top 3 Dispositivos mais Utilizados", x=0.5, y=0.95, xanchor="center", yanchor="top"
            )
        )

        return self.b64_encode_graph(fig)

    def generate_brasil_access_map(
        self,
        matomo_user_get_country_csv: str,
        matomo_user_get_region_csv: str,
    ):
        region_visits = pd.read_csv(StringIO(matomo_user_get_region_csv))
        country_visits = pd.read_csv(StringIO(matomo_user_get_country_csv))

        if region_visits.empty or country_visits.empty:
            return None

        region_visits = region_visits[region_visits["metadata_country"] == "br"].rename(
            columns={"metadata_region": "UF"}
        )
        total_brazil_visits = country_visits.loc[
            country_visits["metadata_code"] == "br", "sum_daily_nb_uniq_visitors"
        ].iloc[0]

        population_data = self._get_population_data()
        region_visits["access_ratio"] = region_visits.apply(
            lambda x: (x["sum_daily_nb_uniq_visitors"] / total_brazil_visits)
            * 100
            / population_data[x["UF"]],
            axis=1,
        )

        brasil_states_map = self._get_brasil_states_map()
        brasil_states_map = brasil_states_map.merge(region_visits, left_on="sigla", right_on="UF", how="left")
        brasil_states_map["access_ratio"].fillna(0, inplace=True)

        scale = [
            (0.0, "cyan"),
            (0.4, "deepskyblue"),
            (0.6, "dodgerblue"),
            (0.8, "blue"),
            (1.0, "midnightblue"),
        ]
        fig = px.choropleth(
            brasil_states_map,
            geojson=brasil_states_map.geometry,
            locations=brasil_states_map.index,
            color="access_ratio",
            color_continuous_scale=scale,
            range_color=(0, 2),
            labels={"access_ratio": "Visitas"},
            title="Taxa de Proporção por Estado no Brasil",
        )

        fig.update_geos(fitbounds="locations", visible=False)
        fig.update_layout(
            title=dict(x=0.5, y=0.95, xanchor="center", yanchor="top"),
            margin=dict(l=0, r=0, b=0, t=0),
            coloraxis_colorbar=dict(
                title="Taxa de Proporção",
                tickvals=[0, 1, 2],
                ticktext=["Baixa Mobilização", "Média Mobilização", "Alta mobilização"],
            ),
        )

        return self.b64_encode_graph(fig)
