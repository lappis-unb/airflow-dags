import pandas as pd

"""
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

"""


class BrasilParticipativoTables:
    """Provides methods to generate specific tables for the brasil participativo report."""

    @classmethod
    def generate_table_proposals_overview(
        cls, votes_per_proposal: list[int], total_comments_per_proposal: list[int]
    ):
        assert len(votes_per_proposal) == len(total_comments_per_proposal)

        num_proposals = len(votes_per_proposal)

        return {
            "Propostas": num_proposals,
            "Votos": sum(votes_per_proposal),
            "Comentários": sum(total_comments_per_proposal),
        }

    @classmethod
    def generate_table_theme_ranking(
        cls,
        proposals_categories: list[str],
        proposals_ids: list,
        total_comments_per_proposal: list[int],
        votes_per_proposal: list[int],
    ):
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
                "proposals_ids": proposals_ids,
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
                "proposals_ids": "ID",
                "proposals_titles": "Proposta",
                "proposals_category_titles": "Categoria",
                "votes_per_proposal": "Votos",
                "total_comments_per_proposal": "Comentários",
            }
        )
        print(len(df_ranking.to_dict("records")))
        return df_ranking.to_dict("records")
