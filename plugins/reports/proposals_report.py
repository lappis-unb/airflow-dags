import pandas as pd
import geopandas as gpd
import json

from io import StringIO
from plugins.reports.base.report import Report
from plugins.reports.tables.bp.tables import BrasilParticipativoTables
from plugins.reports.tables.matomo.tables import MatotmoTables
from pathlib import Path

class ProposalsReport(Report):
    """This class is generating proposals reports."""

    def _generate_general_data(self, bp_df):
        if all(
            col in bp_df.columns
            for col in ["proposal_total_votes", "proposal_total_comments", "proposal_state"]
        ):
            total_votes = bp_df["proposal_total_votes"].fillna(0).astype(int)
            total_comments = bp_df["proposal_total_comments"].fillna(0).astype(int)

            return BrasilParticipativoTables.generate_table_proposals_overview(
                votes_per_proposal=total_votes,
                total_comments_per_proposal=total_comments,
                proposal_states=bp_df["proposal_state"],
            )
        else:
            return None
    def _get_population_data(self) -> dict:
        current_script_path = Path(__file__).parent

        population_json_path = current_script_path / "graphs/matomo/geo/population_uf.json"

        with population_json_path.open("r") as f:
            population_data = json.load(f)

        return population_data["population_uf"]
        
    def _get_state_propotion_data(self, matomo_user_country_csv, matomo_user_region_csv):
        region_visits = pd.read_csv(StringIO(matomo_user_region_csv))
        region_visits = region_visits[region_visits["metadata_country"] == "br"].rename(
            columns={"metadata_region": "UF"}
        )

        country_visits = pd.read_csv(StringIO(matomo_user_country_csv))
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

        max_state = region_visits.loc[region_visits['access_ratio'].idxmax()]['UF']
        min_state = region_visits.loc[region_visits['access_ratio'].idxmin()]['UF']
        one_state = region_visits.iloc[(region_visits['access_ratio']-1).abs().argsort()[:1]]['UF'].values[0]

        return max_state, min_state, one_state
    
    def _render_data(
        self,
        bp_df,
        general_data,
        matomo_visits_summary_csv,
        matomo_visits_frequency_csv,
        matomo_devices_detection_csv,
        matomo_user_country_csv,
        matomo_user_region_csv,
    ):
        document_title = f"Relatório {self.report_name}" if general_data else self.report_name
        introduction_data = None
        daily_graph_data = None
        state_distribution_graph_data = None
        data_access_data = None
        device_graph_data = None
        rank_temas_data = None
        top_proposals_filtered_data = None
        map_graph_data = None

        if general_data:
            introduction_data = {
                "num_proposals": general_data.get("Propostas"),
                "total_votes": general_data.get("Votos"),
                "total_comments": general_data.get("Comentários"),
            }
            daily_graph_data = {
                "file": self.bp_graphs.generate_daily_plot(
                    proposals_publication_date=bp_df["proposal_published_at"],
                    proposals_ids=bp_df["proposal_id"],
                    total_comments_per_proposal=bp_df["proposal_total_comments"],
                    votes_per_proposal=bp_df["proposal_total_votes"],
                ),
            }
            state_distribution_graph_data = {
                "file": self.bp_graphs.generate_state_distribution_donut(bp_df),
            }
            data_access_data = MatotmoTables.generate_table_access_data_overview(
                matomo_visits_summary_csv, matomo_visits_frequency_csv
            )
            device_graph_data = {
                "file": self.matomo_graphs.generate_device_graph(
                    matomo_devices_detection_csv,
                ),
            }
            rank_temas_data = BrasilParticipativoTables.generate_table_theme_ranking(
                proposals_categories=bp_df["proposal_category_title"],
                proposals_ids=bp_df["proposal_id"],
                total_comments_per_proposal=bp_df["proposal_total_comments"].fillna(0).astype(int),
                votes_per_proposal=bp_df["proposal_total_votes"].fillna(0).astype(int),
            )
            top_proposals_filtered_data = BrasilParticipativoTables.generate_top_proposals(
                proposals_ids=bp_df["proposal_id"],
                proposals_titles=bp_df["proposal_title"],
                proposals_category_titles=bp_df["proposal_category_title"],
                votes_per_proposal=bp_df["proposal_total_votes"].fillna(0).astype(int),
                total_comments_per_proposal=bp_df["proposal_total_comments"].fillna(0).astype(int),
            )
            max_state, min_state, one_state = self._get_state_propotion_data(
                matomo_user_country_csv, 
                matomo_user_region_csv
            )
            general_data.update({
                "estado_maior_proporcao": max_state,
                "estado_menor_proporcao": min_state,
                "estado_proporcao_igual_um": one_state
            })
            map_graph_data = {
                "file": self.matomo_graphs.generate_brasil_access_map(
                    matomo_user_country_csv,
                    matomo_user_region_csv,
                ),
            }

        return {
            "document": {
                "component": self.report_name,
                "title": document_title,
                "date": f"{self.start_date} até {self.end_date}",
            },
            "introduction": introduction_data,
            "general_data": 
                general_data,
                "estado_maior_proporcao": max_state,
                "estado_menor_proporcao": min_state,
                "estado_proporcao_igual_um": one_state,
            "daily_graph": daily_graph_data,
            "state_distribution_graph": state_distribution_graph_data,
            "data_access": data_access_data,
            "device_graph": device_graph_data,
            "rank_temas": rank_temas_data,
            "top_proposals_filtered": top_proposals_filtered_data,
            "map_graph": map_graph_data,
        }

    def render_template(
        self,
        bp_df: pd.DataFrame,
        matomo_visits_summary_csv,
        matomo_visits_frequency_csv,
        matomo_devices_detection_csv,
        matomo_user_country_csv,
        matomo_user_region_csv,
        **kwargs,
    ):
        general_data = self._generate_general_data(bp_df)
        data = self._render_data(
            bp_df,
            general_data,
            matomo_visits_summary_csv,
            matomo_visits_frequency_csv,
            matomo_devices_detection_csv,
            matomo_user_country_csv,
            matomo_user_region_csv,
        )
        return self.template.render(data=data)
