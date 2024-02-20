from pathlib import Path

from jinja2 import Environment, FileSystemLoader
from weasyprint import HTML

from .report import ReportGenerator


def ensure_iterable(data):
    """Assegura que o dado seja um iterável (lista, neste caso)."""
    if isinstance(data, dict) or not isinstance(data, list):
        return [data]
    return data


def create_report_pdf(bp_data, visits_summary, visits_frequency, user_contry, devices_detection, output_path):
    report_generator = ReportGenerator()
    general_data = report_generator.calculate_totals(bp_data)
    daily_graph = report_generator.generate_daily_plot(bp_data)
    data_access = report_generator.generate_acess_data(visits_summary, visits_frequency)
    device_graph = report_generator.generate_device_graph(devices_detection)
    rank_temas = report_generator.generate_theme_ranking(bp_data)
    top_proposals_filtered = report_generator.generate_top_proposals(bp_data)
    shp_path = Path(__file__).parent.joinpath("./shapefile/estados_2010.shp").resolve()
    brasil, dados = report_generator.load_data(shp_path, user_contry)
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
            },  # Adaptar conforme a forma como o SVG é retornado
            "data_access": data_access,
            "device_graph": {
                "file": device_graph,
                "label": "Detecção de Dispositivos",
            },  # Adaptar conforme a forma como o SVG é retornado
            "rank_temas": rank_temas,
            "top_proposals_filtered": top_proposals_filtered,
            "map_graph": {
                "file": plot_map,
                "label": "Mapa de Acesso por Estado",
            },  # Adaptar conforme a forma como o SVG/mapa é retornado
        }
    )
    pdf_out = output_path
    css_file = Path(__file__).parent / "styles.css"
    HTML(string=rendered_html).write_pdf(pdf_out, stylesheets=[css_file.as_posix()])

    return pdf_out
