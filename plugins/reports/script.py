import base64
from contextlib import closing
from datetime import datetime
from io import BytesIO
from pathlib import Path

import plotly.express as px
import plotly.io as pio
from jinja2 import Environment, FileSystemLoader
from weasyprint import HTML


def encode_graph(graph):
    with closing(BytesIO()) as buffer:
        pio.write_image(graph, buffer, format="svg")
        buffer.seek(0)
        graph = base64.b64encode(buffer.getvalue()).decode("utf-8")

    return graph


def generate_participation_graph(
    total_comments: int, total_unique_participants: int, width: int = 704, height: int = 480
):
    data = {
        "Metrica": ["Contribuições", "Participantes"],
        "Valores": [total_comments, total_unique_participants],
    }

    # Create a bar chart using Plotly Express
    fig = px.bar(data, x="Valores", y="Metrica", text="Valores", labels={"Valores": "Total"})

    # Customize the layout if needed
    fig.update_layout(
        yaxis_title="",
        xaxis_title="",
        bargap=0.3,
        width=width,  # Set the width of the plot
        height=height,  # Set the height of the plot
    )
    fig.update_traces(marker_color=["#1f77b4", "#ff7f0e"], insidetextanchor="middle")
    return encode_graph(fig)


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
