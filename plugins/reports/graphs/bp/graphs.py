import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

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
        state_rename = {
            "accepted": "Aceita",
            "withdrawn": "Retirada",
            "rejected": "Rejeitada",
            None: "Em avaliação",
        }

        df["proposal_state"] = df["proposal_state"].map(state_rename)

        state_counts = df["proposal_state"].value_counts().reset_index()
        state_counts.columns = ["Estado", "Quantidade"]

        color_map = {"Aceita": "green", "Rejeitada": "red", "Retirada": "yellow", "Em avaliação": "blue"}

        fig = px.pie(
            state_counts,
            names="Estado",
            values="Quantidade",
            hole=0.3,
            title="Situação das Propostas",
            width=width,
            height=height,
            color="Estado",
            color_discrete_map=color_map,
        )

        return self.b64_encode_graph(fig)

    def generate_top_devices(self, titles: list, total_comments: list, statuses: list):
        max_title_length = 15

        truncated_titles = []
        for title in titles:
            truncated_title = title[:max_title_length] + "..." if len(title) > max_title_length else title
            truncated_titles.append(truncated_title)

        # Certifique-se de que todas as listas tenham o mesmo comprimento
        min_length = min(len(truncated_titles), len(total_comments), len(statuses))
        truncated_titles = truncated_titles[:min_length]
        total_comments = total_comments[:min_length]
        statuses = statuses[:min_length]

        df = pd.DataFrame({"title": truncated_titles, "total_comments": total_comments, "status": statuses})

        df_sorted = df.sort_values(by=["total_comments", "status"], ascending=[False, True]).head(10)

        traces = []
        colors = ["rgba(246, 78, 139, 0.6)"]  # Adicione outras cores conforme necessário
        for i, col in enumerate(df_sorted.columns[1:]):
            trace = go.Bar(
                y=df_sorted["title"],
                x=df_sorted[col],
                name=col,
                orientation="h",
                marker=dict(color=colors[i], line=dict(color="rgba(0, 0, 0, 1.0)", width=1)),
            )
            traces.append(trace)

        # Criar a figura e adicionar as barras
        fig = go.Figure(data=traces)
        fig.update_layout(
            title="Dispositivos mais comentados",
            barmode="stack",
            yaxis={"categoryorder": "total ascending"},
            xaxis_title="Total de Comentários",
            yaxis_title=None,
            showlegend=True,
            title_x=0.5,
            uniformtext_minsize=8,
            uniformtext_mode="hide",
        )

        return self.b64_encode_graph(fig)

    def generate_state_participatory_text(self, df: pd.DataFrame, width: int = 704, height: int = 480):
        state_rename = {
            "accepted": "Aceita",
            "withdrawn": "Retirada",
            "rejected": "Rejeitada",
            None: "Em avaliação",
        }

        df["proposal_state"] = df["proposal_state"].map(state_rename)

        state_counts = df["proposal_state"].value_counts().reset_index()
        state_counts.columns = ["Estado", "Quantidade"]

        color_map = {"Aceita": "green", "Rejeitada": "red", "Retirada": "yellow", "Em avaliação": "blue"}

        fig = px.pie(
            state_counts,
            names="Estado",
            values="Quantidade",
            hole=0.3,
            title="Situação das Propostas",
            width=width,
            height=height,
            color="Estado",
            color_discrete_map=color_map,
        )

        return self.b64_encode_graph(fig)
