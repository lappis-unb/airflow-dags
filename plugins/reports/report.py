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
        if not isinstance(df_bp, pd.DataFrame):
            try:
                df_bp = pd.DataFrame(df_bp)
            except ValueError:
                raise ValueError("df_bp deve ser um pandas DataFrame") from None

        num_proposals = len(df_bp)
        num_votes = df_bp["proposal_total_votes"].sum()
        num_comments = df_bp["proposal_total_comments"].sum()

        results_dict = {"Propostas": num_proposals, "Votos": num_votes, "Comentários": num_comments}

        return results_dict

    def generate_acess_data(self, visits_summary, visits_frequency):
        df_frequency = pd.read_csv(StringIO(visits_frequency))
        df_summary = pd.read_csv(StringIO(visits_summary))

        df_acess = pd.concat(
            [
                df_summary[["nb_visits", "bounce_count"]],
                df_frequency[["nb_visits_new", "nb_visits_returning"]],
            ],
            axis=1,
        )

        df_acess = df_acess.rename(
            columns={
                "nb_visits": "Visitas",
                "bounce_count": "Taxa de Rejeição",
                "nb_visits_new": "Visitas Novas",
                "nb_visits_returning": "Visitas de Retorno",
            }
        )

        access_data_list = df_acess.to_dict("records")
        return access_data_list

    def generate_daily_plot(self, df_bp):
        if not isinstance(df_bp, pd.DataFrame):
            try:
                df_bp = pd.DataFrame(df_bp)
            except ValueError:
                raise ValueError("df_bp deve ser um pandas DataFrame") from None

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

        buffer.seek(0)

        device_graph = base64.b64encode(buffer.getvalue()).decode("utf-8")

        buffer.close()

        return device_graph

    def generate_theme_ranking(self, df_bp):
        if not isinstance(df_bp, pd.DataFrame):
            try:
                df_bp = pd.DataFrame(df_bp)
            except ValueError:
                raise ValueError("df_bp deve ser um pandas DataFrame") from None

        df_bp["Tema"] = df_bp["proposal_category_title"].apply(
            lambda x: (
                x["name"]["translation"]
                if isinstance(x, dict) and "name" in x and "translation" in x["name"]
                else x
            )
        )

        grouped = df_bp.groupby("Tema").agg(
            total_proposals=pd.NamedAgg(column="proposal_id", aggfunc="count"),
            total_votes=pd.NamedAgg(column="proposal_total_votes", aggfunc="sum"),
            total_comments=pd.NamedAgg(column="proposal_total_comments", aggfunc="sum"),
        )

        ranked_themes = grouped.sort_values("total_proposals", ascending=False)

        ranked_themes = ranked_themes.rename(
            columns={
                "Tema": "Tema",
                "total_proposals": "Total de Propostas",
                "total_votes": "Total de Votos",
                "total_comments": "Total de Comentários",
            }
        )
        ranking_list = ranked_themes.reset_index().to_dict("records")

        return ranking_list

    def generate_top_proposals(self, df_bp):
        if not isinstance(df_bp, pd.DataFrame):
            try:
                df_bp = pd.DataFrame(df_bp)
            except ValueError:
                raise ValueError("df_bp deve ser um pandas DataFrame") from None

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

        top_proposals_filtered = top_proposals_filtered.rename(
            columns={
                "proposal_id": "ID",
                "proposal_title": "Proposta",
                "proposal_category_title": "Categoria",
                "proposal_total_votes": "Votos",
                "proposal_total_comments": "Comentários",
            }
        )

        top_proposals_list = top_proposals_filtered.to_dict("records")

        return top_proposals_list

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

        buffer.seek(0)

        map_graph = base64.b64encode(buffer.getvalue()).decode("utf-8")

        buffer.close()

        return map_graph
