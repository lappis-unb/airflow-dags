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
                # "participatory_table": Table.split_tables(
                #     BrasilParticipativoTables.generate_participatory_texts_proposals(
                #         proposals_titles=report_data["proposal_title"],
                #         proposals_ids=report_data["proposal_id"],
                #         total_comments_per_proposal=report_data["proposal_total_comments"],
                #         votes_per_proposal=report_data["proposal_total_votes"],
                
                #     ),
                #     max_elements=10,
                # ),
            }
        )
