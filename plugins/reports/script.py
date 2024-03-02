from datetime import datetime
from io import BytesIO
from pathlib import Path

from jinja2 import Environment, FileSystemLoader
from weasyprint import HTML


def generate_total_contributing_graph(
    total_comments: int,
): ...


def create_report_pdf(report_data):
    template_dir = Path(__file__).parent
    template = Environment(loader=FileSystemLoader(template_dir)).get_template(
        "template_participatory_texts.html"
    )
    css_file = Path(__file__).parent / "styles_participatory_texts.css"

    start_date = datetime.strptime(report_data["start_date"], "%Y-%m-%d").strftime("%d/%m/%Y")
    end_date = datetime.strptime(report_data["end_date"], "%Y-%m-%d").strftime("%d/%m/%Y")

    rendered_html = template.render(
        data={
            "document": {
                "title": f"Relatório {report_data['participatory_space_name']}",
                "date": f"{start_date} até {end_date}",
            },
        }
    )

    pdf_bytes = BytesIO()

    HTML(string=rendered_html).write_pdf(target=pdf_bytes, stylesheets=[css_file.as_posix()])

    pdf_bytes.seek(0)

    return pdf_bytes.getvalue()
