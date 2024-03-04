from io import StringIO
from pathlib import Path

import geopandas as gpd
import pandas as pd
import plotly.express as px

from plugins.faker.matomo_faker import MatomoFaker
from plugins.reports.graphs.base.graphs import ReportGraphs


class MatomoGraphs(ReportGraphs):
    """Provides methods to generate specific graphs for the Matomo report."""

    def _get_brasil_states_map(self) -> gpd.GeoDataFrame:
        shapefile_path = Path(__file__).parent.joinpath("./geo/shapefile/estados_2010.shp")
        return gpd.read_file(shapefile_path)

    def generate_device_graph(self, matomo_device_get_type: str):
        df = pd.read_csv(StringIO(matomo_device_get_type))
        matomo_data_sorted = df.sort_values("nb_visits", ascending=False).head(3)

        fig = px.pie(
            matomo_data_sorted,
            names="label",
            values="nb_visits",
            title="Top 3 Categories by Visits",
            hole=0.3,
            labels={"label": "Category", "nb_visits": "Number of Visits"},
            # template="plotly_dark",  # You can change the template as needed
        )
        return self.b64_encode_graph(fig)

    def generate_brasil_access_map(self, matomo_user_country_get_region_csv: str):
        access_data = pd.read_csv(StringIO(matomo_user_country_get_region_csv))
        access_data = access_data[access_data["metadata_country"] == "BR"].rename(
            columns={"metadata_region": "UF"}
        )

        brasil_states_map = self._get_brasil_states_map()
        brasil_states_map = brasil_states_map.merge(access_data, left_on="sigla", right_on="UF", how="left")
        brasil_states_map["nb_visits"].fillna(0, inplace=True)

        fig = px.choropleth(
            brasil_states_map,
            geojson=brasil_states_map.geometry,
            locations=brasil_states_map.index,
            color="nb_visits",
            color_continuous_scale="Blues",
            labels={"nb_visits": "Visitas"},
            title="Visitas por Estado no Brasil",
        )

        # Customize the layout
        fig.update_geos(fitbounds="locations", visible=False)
        fig.update_layout(
            title=dict(x=0.5, y=0.95, xanchor="center", yanchor="top"), margin=dict(l=0, r=0, b=0, t=0)
        )

        with open(Path(__file__).parent.joinpath("./mapa.png"), "wb") as file:
            fig.write_image(file, format="png")
        return self.b64_encode_graph(fig)


def main():
    print(MatomoGraphs().generate_brasil_access_map(MatomoFaker.UserCountry.get_region()))


if __name__ == "__main__":
    main()
