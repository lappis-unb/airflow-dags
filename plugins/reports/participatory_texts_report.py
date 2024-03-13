from plugins.reports.base.report import Report
from plugins.reports.tables.matomo.tables import MatotmoTables


class ParticipatoryTextsReport(Report):
    """
        This class is generating participatory texts reports.

    Attributes:
    ----------
        Inherits attributes from the base Report class.

    Methods:
    -------
        render_template(report_data):
            Renders the template for the participatory texts report using the provided report data.

    Args:
    ----
        report_data (dict): A dictionary containing data for generating the report.

    Returns:
    -------
        str: The rendered report template.
    """

    def render_template(
        self,
        report_data,
        matomo_visits_summary_csv: str,
        matomo_visits_frequency_csv: str,
        matomo_user_country_csv: str,
        matomo_devices_detection_csv: str,
    ):

        proposals_ids = [proposal["id"] for proposal in report_data["proposals"]]
        proposals_titles = [proposal["title"] for proposal in report_data["proposals"]]
        votes_per_proposal = [proposal["vote_count"] for proposal in report_data["proposals"]]
        total_comments_per_proposal = [proposal["total_comments"] for proposal in report_data["proposals"]]

        top_dispositivos_graph = self.bp_graphs.generate_top_dispositivos(
            titles=proposals_titles, total_comments=total_comments_per_proposal
        )
        participatory_texts_file = self.bp_tables.generate_participatory_texts_proposals(
            proposals_ids,
            proposals_titles,
            votes_per_proposal,
            total_comments_per_proposal,
        )

        participatory_texts_ids = [text["ID"] for text in participatory_texts_file]
        participatory_texts_title = [text["Dispositivo"] for text in participatory_texts_file]
        participatory_texts_comments = [text["Nº de comentários"] for text in participatory_texts_file]
        participatory_texts_votes = [text["Nº de votos"] for text in participatory_texts_file]

        return self.template.render(
            data={
                "document": {
                    "title": f"Relatório {self.report_name}",
                    "date": f"{self.start_date} até {self.end_date}",
                },
                "participation_graph": {
                    "label": "Gráfico De Participação",
                    "file": self.bp_graphs.generate_participation_graph(
                        report_data["total_comments"],
                        report_data["total_unique_participants"],
                    ),
                },
                "participatory_texts": {
                    "ID": participatory_texts_ids,
                    "Dispositivo": participatory_texts_title,
                    "Nº de comentários": participatory_texts_comments,
                    "Nº de votos": participatory_texts_votes,
                },
                "top_dispositivos_graph": {
                    "label": "Dispositivos mais utilizados",
                    "file": top_dispositivos_graph,
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
                "map_graph": {
                    "label": "Acesso por Estado",
                    "file": self.matomo_graphs.try_build_graph(
                        self.matomo_graphs.generate_brasil_access_map,
                        matomo_user_country_csv,
                    ),
                },
            }
        )
