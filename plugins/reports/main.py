from io import BytesIO
from pathlib import Path

from jinja2 import Environment, FileSystemLoader
from weasyprint import HTML

from .report import ReportGenerator


def split_tables(table: list, max_elements: int) -> list:
    ret = []
    size = len(table)
    step = int(size / max_elements)
    start = 0
    stop = max_elements

    for i in range(1, step + 1):
        tmp = table[start:stop]
        ret.append(tmp)
        start = stop
        stop = max_elements * (i + 1)

    last_elements = size % max_elements

    if last_elements > 0:
        ret.append(table[-last_elements:])
    return ret


def create_report_pdf(bp_data, visits_summary, visits_frequency, user_country, devices_detection):
    report_generator = ReportGenerator()
    general_data = report_generator.calculate_totals(bp_data)
    daily_graph = report_generator.generate_daily_plot(bp_data)
    data_access = report_generator.generate_acess_data(visits_summary, visits_frequency)
    device_graph = report_generator.generate_device_graph(devices_detection)
    max_elements = 20
    rank_temas = split_tables(report_generator.generate_theme_ranking(bp_data), max_elements)
    top_proposals_filtered = split_tables(
        report_generator.generate_top_proposals(bp_data), int(max_elements / 2)
    )
    shp_path = Path(__file__).parent.joinpath("./shapefile/estados_2010.shp").resolve()
    brasil, dados = report_generator.load_data(shp_path, user_country)
    dados_brasil = report_generator.filter_and_rename(dados, "br", "UF")
    mapa = report_generator.create_map(brasil, dados_brasil, "sigla", "UF")
    plot_map = report_generator.plot_map(mapa, "nb_visits")

    template_dir = Path(__file__).parent
    env = Environment(loader=FileSystemLoader(template_dir))
    template = env.get_template("template.html")

    rendered_html = template.render(
        data={
            "document": {"title": "Título do Relatório", "date": "Data do Relatório"},
            "general_data": general_data,
            "daily_graph": {
                "file": daily_graph,
                "label": "Gráfico Diário",
            },
            "data_access": data_access,
            "device_graph": {
                "file": device_graph,
                "label": "Detecção de Dispositivos",
            },
            "rank_temas": rank_temas,
            "top_proposals_filtered": top_proposals_filtered,
            "map_graph": {
                "file": plot_map,
                "label": "Mapa de Acesso por Estado",
            },
        }
    )
    pdf_bytes = BytesIO()
    css_file = Path(__file__).parent / "styles.css"

    HTML(string=rendered_html).write_pdf(target=pdf_bytes, stylesheets=[css_file.as_posix()])

    pdf_bytes.seek(0)

    return pdf_bytes.getvalue()
