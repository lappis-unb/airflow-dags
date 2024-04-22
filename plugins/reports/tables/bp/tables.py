import logging

import pandas as pd


class BrasilParticipativoTables:
    """Provides methods to generate specific tables for the brasil participativo report."""

    @classmethod
    def generate_table_proposals_overview(
        cls, votes_per_proposal: list[int], total_comments_per_proposal: list[int], proposal_states: list[str]
    ):
        assert len(votes_per_proposal) == len(total_comments_per_proposal) == len(proposal_states)

        filtered_indices = [i for i, state in enumerate(proposal_states) if state != "withdrawn"]

        filtered_votes = [votes_per_proposal[i] for i in filtered_indices]
        filtered_comments = [total_comments_per_proposal[i] for i in filtered_indices]

        num_proposals = len(filtered_indices)

        return {
            "Propostas": num_proposals,
            "Votos": sum(filtered_votes),
            "Comentários": sum(filtered_comments),
        }

    @classmethod
    def generate_table_theme_ranking(
        cls,
        proposals_categories: list[str],
        proposals_ids: list,
        total_comments_per_proposal: list[int],
        votes_per_proposal: list[int],
    ):
        """
        Generates an overview of proposal statistics, excluding withdrawn proposals.

        Parameters:
        ----------
        - votes_per_proposal (list[int]): A list of integers representing the number of votes.
        - total_comments_per_proposal (list[int]): A list of integers representing the total number
        of comments for each proposal.
        - proposal_states (list[str]): A list of strings representing the state of each proposal.

        Returns:
        -------
        - dict: A dictionary containing the total number of considered proposals ('Propostas'),
        the cumulative number
        of votes ('Votes'), and the total number of comments ('Comments') on these proposals.
        """
        assert all(
            len(lst) == len(proposals_categories)
            for lst in [proposals_ids, total_comments_per_proposal, votes_per_proposal]
        )

        df = pd.DataFrame(
            data={
                "proposals_categories": proposals_categories,
                "proposals_ids": proposals_ids,
                "total_comments_per_proposal": total_comments_per_proposal,
                "votes_per_proposal": votes_per_proposal,
            },
            index=range(len(proposals_ids)),
        )

        grouped = df.groupby("proposals_categories").agg(
            total_proposals=pd.NamedAgg(column="proposals_ids", aggfunc="count"),
            total_comments=pd.NamedAgg(column="total_comments_per_proposal", aggfunc="sum"),
            total_votes=pd.NamedAgg(column="votes_per_proposal", aggfunc="sum"),
        )

        grouped = grouped.rename_axis("Tema")

        ranked_themes = grouped.sort_values("total_proposals", ascending=False)

        ranked_themes = ranked_themes.rename(
            columns={
                "total_proposals": "Total de Propostas",
                "total_votes": "Total de Votos",
                "total_comments": "Total de Comentários",
            }
        )

        ranking_list = ranked_themes.reset_index().to_dict("records")
        return ranking_list

    @classmethod
    def generate_top_proposals(
        cls,
        proposals_ids: list,
        proposals_titles: list[str],
        proposals_category_titles: list[str],
        votes_per_proposal: list[int],
        total_comments_per_proposal: list[int],
    ):
        assert all(
            len(lst) == len(proposals_ids)
            for lst in [
                proposals_titles,
                proposals_category_titles,
                votes_per_proposal,
                total_comments_per_proposal,
            ]
        )

        df = pd.DataFrame(
            data={
                "proposals_titles": proposals_titles,
                "proposals_category_titles": proposals_category_titles,
                "votes_per_proposal": votes_per_proposal,
                "total_comments_per_proposal": total_comments_per_proposal,
            },
            index=range(len(proposals_ids)),
        )
        df_ranking = df.sort_values(by="votes_per_proposal", ascending=False).head(20)

        df_ranking = df_ranking.rename(
            columns={
                "proposals_titles": "Proposta",
                "proposals_category_titles": "Categoria",
                "votes_per_proposal": "Votos",
                "total_comments_per_proposal": "Comentários",
            }
        )
        logging.info(len(df_ranking.to_dict("records")))
        return df_ranking.to_dict("records")

    @classmethod
    def generate_participatory_texts_proposals(
        cls,
        proposals_ids: list,
        proposals_titles: list[str],
        votes_per_proposal: list[int],
        total_comments_per_proposal: list[int],
    ):
        """
        Generates a list of the 20 best proposals ranked by the number of votes.

        Parameters:
        ----------
        - proposals_ids (list): A list of proposal ids.
        - proposals_titles (list[str]): A list of titles for the proposals.
        - proposals_category_titles (list[str]): A list of category titles
        for each proposal.
        - vote_per_proposal (list[int]): A list of integers representing the total votes received
        for each proposal.
        - total_comments_per_proposal (list[int]): A list of integers representing the total
        number of comments on each proposal.

        Returns:
        -------
        - list[dict]: A list of dictionaries, where each dictionary contains details of a main proposal.

        """
        assert all(
            len(lst) == len(proposals_ids)
            for lst in [
                proposals_titles,
                votes_per_proposal,
                total_comments_per_proposal,
            ]
        )

        df = pd.DataFrame(
            data={
                "title": proposals_titles,
                "proposal_total_votes": votes_per_proposal,
                "total_comments": total_comments_per_proposal,
            },
            index=range(len(proposals_ids)),
        )
        df_ranking = df.sort_values(by="total_comments", ascending=False).head(5)

        df_ranking = df_ranking.rename(
            columns={
                "title": "Parágrafos",
                "total_comments": "Nº de comentários",
                "proposal_total_votes": "Nº de votos",
            }
        )
        logging.info(len(df_ranking.to_dict("records")))
        return df_ranking.to_dict("records")
