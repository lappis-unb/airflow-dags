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
        """
        Generates a bar graph showing participation metrics and returns it as a base64-encoded image.

        Parameters:
        ----------
        - total_comments (int): The total number of comments.
        - total_unique_participants (int): The number of participants.
        - width (int, optional): The width of the generated graphic in pixels.
        - height (int, optional): The height of the generated chart in pixels.

        Returns:
        -------
        - str: A base64-encoded string of the generated graph.
        """
        data = {
            "Metrica": ["Contribuições", "Participantes"],
            "Valores": [total_comments, total_unique_participants],
        }

        fig = px.bar(data, x="Valores", y="Metrica", text="Valores", labels={"Valores": "Total"})

        fig.update_layout(
            yaxis_title="",
            xaxis_title="",
            bargap=0.3,
            width=width,
            height=height,
        )
        fig.update_traces(marker_color=["#183EFF", "#FFD000"], insidetextanchor="middle")
        return self.b64_encode_graph(fig)

    def generate_daily_plot(
        self,
        proposals_publication_date: list,
        proposals_ids: list,
        total_comments_per_proposal: list[int],
        votes_per_proposal: list[int],
    ):
        """
        Generates a multi-line chart for daily activities and returning the graph as a base64 encoded image.

        Parameters:
        ----------
        - proposals_publication_date (list): List of proposal publication dates.
        - proposals_ids (list): List of proposal ids.
        - total_comments_per_proposal (list[int]): List of total comments per proposals.
        - vote_per_proposal (list[int]): List of total votes per proposal.

        Returns:
        -------
        - str: A base64-encoded string of the generated graph.

        """
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
            marker=dict(size=8, color="#FF0000"),  # vermelho
            line=dict(color="#FF0000"),  # vermelho
        )

        fig.add_scatter(
            x=daily_data["date"],
            y=daily_data["total_comments"],
            mode="lines+markers",
            name="Comentários por Propostas",
            marker=dict(size=8, color="#183EFF"),  # azul
            line=dict(color="#183EFF"),  # azul
        )

        fig.add_scatter(
            x=daily_data["date"],
            y=daily_data["total_votes"],
            mode="lines+markers",
            name="Votos por Propostas",
            marker=dict(size=8, color="#00D000"),  # verde
            line=dict(color="#00D000"),  # verde
        )

        fig.update_layout(
            legend=dict(
                title="Gráfico diário",
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
        """
        Generates a donut chart representing the distribution of proposal states within a dataset.

        Parameters:
        ----------
        - df (pd.DataFrame): A pandas DataFrame containing a column called "proposal_state".
        - width (int, optional): The width of the generated graphic in pixels. The default is 704.
        - height (int, optional): The height of the generated graph in pixels. The default is 480.

        Returns:
        -------
        - str: A base64 encoded string of the generated graph.
        """
        state_rename = {
            "accepted": "Aceita",
            "withdrawn": "Retirada",
            "rejected": "Rejeitada",
            None: "Em avaliação",
        }

        df["proposal_state"] = df["proposal_state"].map(state_rename)

        state_counts = df["proposal_state"].value_counts().reset_index()
        state_counts.columns = ["Estado", "Quantidade"]

        color_map = {
            "Aceita": "#00D000",
            "Rejeitada": "#FF0000",
            "Retirada": "#FFD000",
            "Em avaliação": "#183EFF",
        }

        fig = px.pie(
            state_counts,
            names="Estado",
            values="Quantidade",
            hole=0.3,
            width=width,
            height=height,
            color="Estado",
            color_discrete_map=color_map,
        )
        fig.update_layout(
            title=dict(text="Situação das Propostas", x=0.5, y=0.95, xanchor="center", yanchor="top")
        )

        return self.b64_encode_graph(fig)

    def generate_top_devices(self, titles: list, total_comments: list, status_list_of_lists: list):
        """
        Generates a horizontal stacked bar chart showing the distribution of statuses.

        Parameters:
        ----------
        - titles (list): A list of strings, where each string is a title.
        - total_comments (list): A list of integers, representing the total number of comments.
        - status_list_of_lists (list): A list of lists, where each sublist contains strings.

        Returns:
        -------
        - str: A base64-encoded string of the generated chart.

        """
        assert len(titles) == len(total_comments) == len(status_list_of_lists)

        def limit_title(title, max_length=15):
            if len(title) > max_length:
                return title[:max_length] + "..."
            else:
                return title

        sorted_indices = sorted(range(len(total_comments)), key=lambda k: total_comments[k], reverse=True)
        top_indices = sorted_indices[:5]

        sorted_titles = [limit_title(titles[i]) for i in top_indices]
        sorted_status_list_of_lists = [status_list_of_lists[i] for i in top_indices]

        fig = go.Figure()

        unique_statuses = set(status for i in top_indices for status in status_list_of_lists[i])

        status_name_mapping = {
            "in_discussion": "Em discussão",
            "rejected": "Não incorporado",
            "accepted": "Incorporado",
            None: "Em discussão",
        }

        status_colors = {
            "Em discussão": "#183EFF",
            "Não incorporado": "#FF0000",
            "Incorporado": "#00D000",
        }

        for status in unique_statuses:
            status_counts = [sorted_status_list_of_lists[i].count(status) for i in range(5)]
            fig.add_trace(
                go.Bar(
                    y=sorted_titles,
                    x=status_counts,
                    name=status_name_mapping.get(status, "Em discussão"),
                    orientation="h",
                    marker=dict(color=status_colors[status_name_mapping.get(status, "Em discussão")]),
                )
            )

        fig.update_layout(
            barmode="group",
            yaxis={"categoryorder": "total ascending"},
            bargap=0.3,
            xaxis_title="Número de comentários",
            yaxis_title="Parágrafos",
            title="Top 5 Parágrafos mais Comentados",
            title_x=0.5,
            uniformtext_minsize=8,
            uniformtext_mode="hide",
        )

        return self.b64_encode_graph(fig)

    def generate_state_participatory_text(self, df: pd.DataFrame, width: int = 704, height: int = 480):
        """
        Generates a donut chart visualizing the distribution of proposal states.

        Parameters:
        ----------
        - df (pd.DataFrame): The DataFrame containing a column called 'proposal_state', which indicates the
            status of each proposal.
        - width (int, optional): The width of the generated graphic in pixels.
        - height (int, optional): The height of the generated graph in pixels.

        Returns:
        -------
        - str: A base64-encoded string of the generated chart.

        """
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
