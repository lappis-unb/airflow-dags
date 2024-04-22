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
        matomo_user_region_csv: str,
        matomo_user_country_csv: str,
        matomo_devices_detection_csv: str,
    ):

        if not report_data["proposals"]:
            return self.template.render(
                data={
                    "document": {
                        "title": f"Relatório {self.report_name}",
                        "date": f"{self.start_date} até {self.end_date}",
                    },
                    "introduction": None,
                    "participation_graph": None,
                    "participatory_texts": None,
                    "top_devices_graph": None,
                    "data_access": None,
                    "device_graph": None,
                    "map_graph": None,
                    "state_proportion": None,
                    "comments": {"content": None},
                }
            )
        else:
            proposals_ids = [proposal["id"] for proposal in report_data["proposals"]]
            proposals_titles = [proposal["title"] for proposal in report_data["proposals"]]
            votes_per_proposal = [proposal["vote_count"] for proposal in report_data["proposals"]]
            total_comments_per_proposal = [
                proposal["total_comments"] for proposal in report_data["proposals"]
            ]

            status_per_proposal = [
                [comment["status"] for comment in proposal["comments"] if "status" in comment]
                for proposal in report_data["proposals"]
            ]

            top_devices_graph = self.bp_graphs.generate_top_devices(
                titles=proposals_titles,
                total_comments=total_comments_per_proposal,
                status_list_of_lists=status_per_proposal,
            )

            participatory_texts_file = self.bp_tables.generate_participatory_texts_proposals(
                proposals_ids,
                proposals_titles,
                votes_per_proposal,
                total_comments_per_proposal,
            )

            participatory_texts_title = [text["Parágrafos"] for text in participatory_texts_file]
            participatory_texts_comments = [text["Nº de comentários"] for text in participatory_texts_file]
            participatory_texts_votes = [text["Nº de votos"] for text in participatory_texts_file]

            participatory_texts = report_data["proposals"]

            state_rename = {
                "accepted": "Aceita",
                "withdrawn": "Retirada",
                "rejected": "Rejeitada",
            }

            rename_state = lambda comments: [
                {**comment, "status": state_rename.get(comment["status"], "Avaliando")}
                for comment in comments
            ]

            comments_data = [
                {"title": text["title"], "comments": rename_state(text["comments"])}
                for text in participatory_texts
            ]
            max_state, min_state, one_state = self.get_state_proportion_data(
                matomo_user_country_csv, matomo_user_region_csv
            )

            return self.template.render(
                data={
                    "document": {
                        "component": self.report_name,
                        "title": f"Relatório {self.report_name}",
                        "date": f"{self.start_date} até {self.end_date}",
                    },
                    "introduction": {
                        "total_comments": report_data["total_comments"],
                        "total_unique_participants": report_data["total_unique_participants"],
                    },
                    "participation_graph": {
                        "file": self.bp_graphs.generate_participation_graph(
                            report_data["total_comments"],
                            report_data["total_unique_participants"],
                        ),
                    },
                    "participatory_texts": {
                        "Parágrafos": participatory_texts_title,
                        "Nº de comentários": participatory_texts_comments,
                        "Nº de votos": participatory_texts_votes,
                    },
                    "top_devices_graph": {
                        "file": top_devices_graph,
                    },
                    "data_access": MatotmoTables.generate_table_access_data_overview(
                        matomo_visits_summary_csv, matomo_visits_frequency_csv
                    ),
                    "device_graph": {
                        "file": self.matomo_graphs.try_build_graph(
                            self.matomo_graphs.generate_device_graph,
                            matomo_devices_detection_csv,
                        ),
                    },
                    "map_graph": {
                        "file": self.matomo_graphs.try_build_graph(
                            self.matomo_graphs.generate_brasil_access_map,
                            matomo_user_country_csv,
                            matomo_user_region_csv,
                        ),
                    },
                    "state_proportion": {
                        "estado_maior_proporcao": max_state,
                        "estado_menor_proporcao": min_state,
                        "estado_proporcao_igual_um": one_state,
                    },
                    "comments": {"content": comments_data},
                }
            )
