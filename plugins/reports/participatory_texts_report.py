import pandas as pd

from plugins.reports.base.report import Report
from plugins.reports.tables.base.tables import Table
from plugins.reports.tables.bp.tables import BrasilParticipativoTables

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

    def render_template(self, report_data):

        proposals_ids = [proposal['id'] for proposal in report_data['proposals']]
        proposals_titles = [proposal['title'] for proposal in report_data['proposals']]
        votes_per_proposal = [proposal['vote_count'] for proposal in report_data['proposals']]
        total_comments_per_proposal = [proposal['total_comments'] for proposal in report_data['proposals']]

        print(proposals_ids)


        participatory_texts_file = self.bp_tables.generate_participatory_texts_proposals(
            proposals_ids, proposals_titles, votes_per_proposal, total_comments_per_proposal
        )

        participatory_texts_ids = [text['ID'] for text in participatory_texts_file]
        participatory_texts_title = [text['Dispositivo'] for text in participatory_texts_file]
        participatory_texts_comments = [text['N de comentários'] for text in participatory_texts_file]
        participatory_texts_votes = [text['N de votos'] for text in participatory_texts_file]

        return self.template.render(
            data={
                "document": {
                    "title": f"Relatório {self.report_name}",
                    "date": f"{self.start_date} até {self.end_date}",
                },
                "participation_graph": {
                    "label": "Gráfico De Participação",
                    "file": self.bp_graphs.generate_participation_graph(
                        report_data["total_comments"], report_data["total_unique_participants"]
                    ),
                },
                "participatory_texts": {
                    "ID": participatory_texts_ids,
                    "Dispositivo": participatory_texts_title,
                    "N de comentários": participatory_texts_comments,
                    "N de votos": participatory_texts_votes,
                }
            }
        )
