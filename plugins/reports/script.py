from io import BytesIO
from pathlib import Path

from jinja2 import Environment, FileSystemLoader
from weasyprint import HTML


def ensure_iterable(data):
    """Assegura que o dado seja um iterável (lista, neste caso)."""
    if isinstance(data, dict) or not isinstance(data, list):
        return [data]
    return data


def create_report_pdf(filtered_data):

    template_dir = Path(__file__).parent
    env = Environment(loader=FileSystemLoader(template_dir))
    template = env.get_template("template_participatory_texts.html")

    rendered_html = template.render(
        data={
            "document": {"title": "Título do Relatório", "date": "Data do Relatório"},
            "your_custom_dict": filtered_data,
        }
    )

    pdf_bytes = BytesIO()
    css_file = Path(__file__).parent / "styles_participatory_texts.css"

    HTML(string=rendered_html).write_pdf(target=pdf_bytes, stylesheets=[css_file.as_posix()])

    pdf_bytes.seek(0)

    return pdf_bytes.getvalue()
