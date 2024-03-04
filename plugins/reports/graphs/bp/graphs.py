from datetime import datetime

import pandas as pd
import plotly.express as px

from plugins.reports.graphs.base.graphs import ReportGraphs


class BrasilParticipativoGraphs(ReportGraphs):
    """Provides methods to generate specific graphs for the Brasil Participativo report."""

    def generate_participation_graph(
        self, total_comments: int, total_unique_participants: int, width: int = 704, height: int = 480
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
        proposals_publication_date: list[datetime],
        proposals_ids: list,
        total_comments_per_proposal: list[int],
        votes_per_proposal: list[int],
    ):
        assert all(
            len(lst) == len(proposals_ids)
            for lst in [proposals_publication_date, total_comments_per_proposal, votes_per_proposal]
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
                {"proposals_ids": "count", "total_comments_per_proposal": "sum", "votes_per_proposal": "sum"}
            )
            .reset_index()
        )

        fig = px.line(
            daily_data,
            x="date",
            y=["proposals_ids", "total_comments_per_proposal", "votes_per_proposal"],
            labels={"value": "Quantidade", "date": "Data"},
            title="Quantidade de Propostas, Comentários e Votos por Dia",
        )

        fig.update_traces(mode="markers+lines", marker=dict(size=8))

        # Customize the legend and axis labels
        fig.update_layout(
            legend=dict(title="", orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            xaxis_title="Data",
            yaxis_title="Quantidade",
            xaxis=dict(tickangle=45),
            legend_traceorder="reversed",
            hovermode="x",
        )

        return self.b64_encode_graph(fig)
