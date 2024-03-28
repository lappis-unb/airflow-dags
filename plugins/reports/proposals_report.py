import pandas as pd

from plugins.reports.base.report import Report
from plugins.reports.tables.bp.tables import BrasilParticipativoTables
from plugins.reports.tables.matomo.tables import MatotmoTables


class ProposalsReport(Report):
    """This class is generating proposals reports."""

    def render_template(
        self,
        bp_df: pd.DataFrame,
        matomo_visits_summary_csv: str,
        matomo_visits_frequency_csv: str,
        matomo_user_region_csv: str,
        matomo_user_country_csv: str,
        matomo_devices_detection_csv: str,
    ):
        general_data = BrasilParticipativoTables.generate_table_proposals_overview(
            votes_per_proposal=bp_df["proposal_total_votes"],
            total_comments_per_proposal=bp_df["proposal_total_comments"],
            proposal_states=bp_df["proposal_state"],
        )

        return self.template.render(
            data={
                "document": {
                    "component": self.report_name,
                    "title": f"Relatório {self.report_name}",
                    "date": f"{self.start_date} até {self.end_date}",
                },
                "introduction": {
                    "num_proposals": general_data["Propostas"],
                    "total_votes": general_data["Votos"],
                    "total_comments": general_data["Comentários"],
                },
                "general_data": BrasilParticipativoTables.generate_table_proposals_overview(
                    votes_per_proposal=bp_df["proposal_total_votes"],
                    total_comments_per_proposal=bp_df["proposal_total_comments"],
                    proposal_states=bp_df["proposal_state"],
                ),
                "daily_graph": {
                    "label": "Gráfico Diário",
                    "file": self.bp_graphs.generate_daily_plot(
                        proposals_publication_date=bp_df["proposal_published_at"],
                        proposals_ids=bp_df["proposal_id"],
                        total_comments_per_proposal=bp_df["proposal_total_comments"],
                        votes_per_proposal=bp_df["proposal_total_votes"],
                    ),
                },
                "state_distribution_graph": {
                    "label": "Distribuição de Estados das Propostas",
                    "file": self.bp_graphs.generate_state_distribution_donut(bp_df),
                },
                "data_access": MatotmoTables.generate_table_access_data_overview(
                    matomo_visits_summary_csv, matomo_visits_frequency_csv
                ),
                "device_graph": {
                    "label": "Detecção de Dispositivos",
                    "file": self.matomo_graphs.try_build_graph(
                        self.matomo_graphs.generate_device_graph,
                        matomo_devices_detection_csv,
                    ),
                },
                "rank_temas": BrasilParticipativoTables.generate_table_theme_ranking(
                    proposals_categories=bp_df["proposal_category_title"],
                    proposals_ids=bp_df["proposal_id"],
                    total_comments_per_proposal=bp_df["proposal_total_comments"],
                    votes_per_proposal=bp_df["proposal_total_votes"],
                ),
                "top_proposals_filtered": BrasilParticipativoTables.generate_top_proposals(
                    proposals_ids=bp_df["proposal_id"],
                    proposals_titles=bp_df["proposal_title"],
                    proposals_category_titles=bp_df["proposal_category_title"],
                    votes_per_proposal=bp_df["proposal_total_votes"],
                    total_comments_per_proposal=bp_df["proposal_total_comments"],
                ),
                "map_graph": {
                    "label": "Acesso por Estado",
                    "file": self.matomo_graphs.try_build_graph(
                        self.matomo_graphs.generate_brasil_access_map,
                        matomo_user_country_csv,
                        matomo_user_region_csv,
                    ),
                },
            }
        )
