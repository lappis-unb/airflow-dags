import base64
import re
from io import BytesIO, StringIO

import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd


class ReportGenerator:
    """Classe para poder gerar os relatorios."""

    def __init__(self):
        pass

    def filter_proposals(self, bp_data, states, pattern):
        proposals = []
        for proposal in bp_data:
            proposal_state = proposal.get("proposal_state")
            proposal_title = proposal.get("proposal_title")
            if (
                proposal_state is not None
                and proposal_state not in states
                and bool(re.match(pattern, proposal_title))
            ):
                proposals.append(proposal)
        return proposals

    def create_bp_dataframe(self, bp_data):
        df_bp = pd.DataFrame(bp_data)
        df_bp["publishedAt"] = pd.to_datetime(df_bp["proposal_published_at"])
        df_bp["updatedAt"] = pd.to_datetime(df_bp["proposal_updated_at"])
        df_bp["translation"] = df_bp["proposal_title"]

        return df_bp

    def calculate_totals(self, df_bp):
        num_proposals = len(df_bp)
        num_votes = df_bp["proposal_total_votes"].sum()
        num_comments = df_bp["proposal_total_comments"].sum()
        return num_proposals, num_votes, num_comments

    def generate_daily_plot(self, df_bp):
        df_bp["proposal_published_at"] = pd.to_datetime(df_bp["proposal_published_at"])
        df_bp["Data"] = df_bp["proposal_published_at"].dt.date

        n_proposals = df_bp.groupby("Data")["proposal_id"].count().to_numpy()
        n_comments = df_bp.groupby("Data")["proposal_total_comments"].sum().to_numpy()
        n_votes = df_bp.groupby("Data")["proposal_total_votes"].sum().to_numpy()

        plt.figure(figsize=(12, 6))
        plt.plot(df_bp["Data"].unique(), n_proposals, label="Propostas", color="blue", marker="o")
        plt.plot(df_bp["Data"].unique(), n_comments, label="Comentários", color="green", marker="s")
        plt.plot(df_bp["Data"].unique(), n_votes, label="Votos", color="red", marker="^")

        plt.xlabel("Data")
        plt.ylabel("Quantidade")
        plt.title("Quantidade de Propostas, Comentários e Votos por Dia")
        plt.legend()
        plt.grid(True)
        plt.xticks(rotation=45)
        plt.tight_layout()

        buffer = BytesIO()
        plt.savefig(buffer, format="png")
        with open("/opt/airflow/airflow-tmp/device_graph.png", "wb") as file:
            plt.savefig(file, format="png")
        buffer.seek(0)

        daily_graph = base64.b64encode(buffer.getvalue()).decode("utf-8")

        buffer.close()

        return daily_graph

    def generate_device_graph(self, device_data):
        df = pd.read_csv(StringIO(device_data))
        matomo_data_sorted = df.sort_values("nb_visits", ascending=False).head(3)
        fig, ax = plt.subplots()
        ax.pie(matomo_data_sorted["nb_visits"], labels=matomo_data_sorted["label"], autopct="%1.1f%%")
        ax.axis("equal")

        plt.show()

        buffer = BytesIO()
        plt.savefig(buffer, format="png")
        with open("/opt/airflow/airflow-tmp/device_graph.png", "wb") as file:
            plt.savefig(file, format="png")
        buffer.seek(0)

        device_graph = base64.b64encode(buffer.getvalue()).decode("utf-8")

        buffer.close()

        return device_graph

    def generate_theme_ranking(self, df_bp):
        df_bp["nome_tema"] = df_bp["proposal_category_title"].apply(
            lambda x: (
                x["name"]["translation"]
                if isinstance(x, dict) and "name" in x and "translation" in x["name"]
                else None
            )
        )

        df_filtered = df_bp.dropna(subset=["nome_tema"])

        rank_category = (
            df_filtered.groupby("nome_tema")
            .agg(
                Quantidade_de_Propostas=pd.NamedAgg(column="proposal_id", aggfunc="count"),
                Quantidade_de_Votos=pd.NamedAgg(column="proposal_total_votes", aggfunc="sum"),
                Quantidade_de_Comentários=pd.NamedAgg(column="proposal_total_comments", aggfunc="sum"),
            )
            .reset_index()
        )

        rank_category.columns = [
            "Tema",
            "Quantidade de Propostas",
            "Quantidade de Votos",
            "Quantidade de Comentários",
        ]

        rank_temas = rank_category.sort_values(by="Quantidade de Propostas", ascending=False)

        return rank_temas

    def generate_top_proposals(self, df_bp):
        df_ranking = df_bp.sort_values(by="proposal_total_votes", ascending=False)

        top_proposals = df_ranking.head(20)

        columns = [
            "proposal_id",
            "proposal_title",
            "proposal_category_title",
            "proposal_total_votes",
            "proposal_total_comments",
        ]

        top_proposals_filtered = top_proposals[columns]

        return top_proposals_filtered

    def load_data(self, shp_path, user_contry):
        brasil = gpd.read_file(shp_path)
        dados_visitas = pd.read_csv(StringIO(user_contry))
        return brasil, dados_visitas

    def filter_and_rename(self, dados, pais, coluna):
        dados_filtrados = dados[dados["metadata_country"] == pais]
        dados_filtrados = dados_filtrados.rename(columns={"metadata_region": coluna})
        return dados_filtrados

    def create_map(self, brasil, dados, index_coluna, join_coluna):
        mapa = brasil.set_index(index_coluna).join(dados.set_index(join_coluna))
        return mapa

    def plot_map(self, mapa, coluna):
        fig, ax = plt.subplots(figsize=(12, 8))
        mapa.boundary.plot(ax=ax, linewidth=0.5, color="k")
        mapa.plot(column=coluna, ax=ax, legend=True, cmap="YlOrRd")
        plt.title("Visitas por Estado no Brasil")
        plt.axis("off")
        plt.show()

        buffer = BytesIO()
        plt.savefig(buffer, format="png")
        with open("/opt/airflow/airflow-tmp/map_visitas_por_estado.png", "wb") as file:
            plt.savefig(file, format="png")
        buffer.seek(0)

        map_graph = base64.b64encode(buffer.getvalue()).decode("utf-8")

        buffer.close()

        return map_graph


#     def render_html(template_path, output_path):
#         env = Environment(loader=FileSystemLoader(searchpath="./"))

#         tabela_dados = pd.read_csv("relatorio.csv")

#         data = {
#             "title": "Título Dinâmico",
#             "header": "Visão Geral",
#             "table_data": tabela_dados.to_html(index=False),
#         }
#         content = f"<h1> { header }</h1> <!-- Inclui a tabela dinâmica --> {{ table_data | safe }}"
#         rendered_html = template_path.format(**data)

#         with open(output_path, "w") as output_file:
#             output_file.write(rendered_html)
# # Exemplo de template
# header = "<!DOCTYPE html>
# <html xmlns:th='http://www.thymeleaf.org'>
# <head><style th:replace='inc/bootstrap :: inc'></style><link rel='stylesheet' href='style.css'></head>
# <body><div id='header'>Put your header content here...</div>
# <div id='footer'>Put your footer content here... <br />Page <span class='pagenumber'>
# </span> of <span class='pagecount'></span></div><div id='content'>"

# content = "<h1>{header}</h1> <!-- Inclui a tabela dinâmica --> {table_data}"

# footer_path = "</div></body></html>"
# output_path = "output.html"

# template_path = header + content + footer_path
# render_html(template_path, output_path)
