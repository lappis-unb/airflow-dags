from plugins.reports.base.report import Report


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
                    "date": f"{self.start_date} até {self.start_date}",
                },
                "participation_graph": {
                    "label": "",
                    "file": self.bp_graphs.generate_participation_graph(
                        report_data["total_comments"], report_data["total_unique_participants"]
                    ),
                },
            }
        )
