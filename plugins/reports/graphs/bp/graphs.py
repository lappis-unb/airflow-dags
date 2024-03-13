import pandas as pd
import plotly.express as px

from plugins.reports.graphs.base.graphs import ReportGraphs


class BrasilParticipativoGraphs(ReportGraphs):
    """Provides methods to generate specific graphs for the Brasil Participativo report."""

    def generate_participation_graph(
        self,
        total_comments: int,
        total_unique_participants: int,
        width: int = 704,
        height: int = 480,
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
        return self.b64_encode_graph(fig)

    def generate_daily_plot(
        self,
        proposals_publication_date: list,
        proposals_ids: list,
        total_comments_per_proposal: list[int],
        votes_per_proposal: list[int],
    ):
        assert all(
            len(lst) == len(proposals_ids)
            for lst in [
                proposals_publication_date,
                total_comments_per_proposal,
                votes_per_proposal,
            ]
        )

        df = pd.DataFrame(
            data={
                "proposals_publication_date": proposals_publication_date,
                "proposals_ids": proposals_ids,
                "total_comments_per_proposal": total_comments_per_proposal,
                "votes_per_proposal": votes_per_proposal,
            },
            index=range(len(proposals_ids)),
        )

        df["proposals_publication_date"] = pd.to_datetime(df["proposals_publication_date"])
        df["date"] = df["proposals_publication_date"].dt.date

        daily_data = (
            df.groupby("date")
            .agg(
                proposals=("proposals_ids", "count"),
                total_comments=("total_comments_per_proposal", "sum"),
                total_votes=("votes_per_proposal", "sum"),
            )
            .reset_index()
        )

        fig = px.line()

        fig.add_scatter(
            x=daily_data["date"],
            y=daily_data["proposals"],
            mode="lines+markers",
            name="Propostas",
            marker=dict(size=8),
        )

        fig.add_scatter(
            x=daily_data["date"],
            y=daily_data["total_comments"],
            mode="lines+markers",
            name="Comentários por Propostas",
            marker=dict(size=8),
        )

        fig.add_scatter(
            x=daily_data["date"],
            y=daily_data["total_votes"],
            mode="lines+markers",
            name="Votos por Propostas",
            marker=dict(size=8),
        )

        fig.update_layout(
            legend=dict(
                title="",
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1,
            ),
            xaxis_title="Data",
            yaxis_title="Quantidade",
            xaxis=dict(tickangle=45),
            hovermode="x",
        )

        return self.b64_encode_graph(fig)

    def generate_state_distribution_donut(self, df: pd.DataFrame, width: int = 704, height: int = 480):
        state_counts = df["proposal_state"].value_counts().reset_index()
        state_counts.columns = ["Estado", "Quantidade"]

        state_rename = {
            "accepted": "Aceita",
            "withdrawn": "Retirada",
            "rejected": "Rejeitada",
        }

        state_counts["Estado"] = state_counts["Estado"].map(state_rename)

        fig = px.pie(
            state_counts,
            names="Estado",
            values="Quantidade",
            hole=0.3,
            title="Situação das Propostas",
        )

        return self.b64_encode_graph(fig)

    def generate_top_dispositivos(self, titles: list, total_comments: list):
        # Certifique-se de que titles e total_comments têm o mesmo tamanho
        assert len(titles) == len(total_comments)

        # Criando o DataFrame
        df = pd.DataFrame({"title": titles, "total_comments": total_comments})

        # Ordenando o DataFrame com base no total de comentários
        df_sorted = df.sort_values(by="total_comments", ascending=False).head(10)

        # Criando o gráfico de barras
        fig = px.bar(
            df_sorted,
            y="title",
            x="total_comments",
            orientation="h",
            title="Dispositivos mais comentados",
            text="total_comments",
        )

        # Atualizando o layout do gráfico para combinar com o estilo desejado
        fig.update_layout(
            yaxis={"categoryorder": "total ascending"},
            xaxis_title=None,
            yaxis_title=None,
            showlegend=False,
            title_x=0.5,
            uniformtext_minsize=8,
            uniformtext_mode="hide",
        )

        # Retornando o gráfico codificado em base64
        return self.b64_encode_graph(fig)
