from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Union

from jinja2 import Environment, FileSystemLoader
from weasyprint import HTML

from plugins.reports.graphs.bp.graphs import BrasilParticipativoGraphs
from plugins.reports.graphs.matomo.graphs import MatomoGraphs


class Report:
    def __init__(
        self, report_name: str, template_path: Union[str, Path], start_date: datetime, end_date: datetime
    ) -> None:
        template_path = Path(template_path)
        self.css_file = template_path.parent.joinpath("./css/styles.css")
        self.template = Environment(loader=FileSystemLoader(template_path.parent)).get_template(
            template_path.name
        )

        self.report_name = report_name
        self.start_date = start_date.strftime("%d/%m/%Y")
        self.end_date = end_date.strftime("%d/%m/%Y")

        self.bp_graphs = BrasilParticipativoGraphs()
        self.matomo_graphs = MatomoGraphs()

    def create_report_pdf(self, **kwargs):
        rendered_html = self.render_template(**kwargs)

        pdf_bytes = BytesIO()
        HTML(string=rendered_html).write_pdf(target=pdf_bytes, stylesheets=[self.css_file.as_posix()])
        pdf_bytes.seek(0)

        return pdf_bytes.getvalue()

    def render_template(self, **kwargs):
        raise NotImplementedError
