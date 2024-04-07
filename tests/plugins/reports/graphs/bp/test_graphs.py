import base64

import pandas as pd
import pytest

from plugins.reports.graphs.bp.graphs import BrasilParticipativoGraphs


# Fixture para criar uma instância da classe BrasilParticipativoGraphs
@pytest.fixture
def bp_graphs():
    return BrasilParticipativoGraphs()


def test_generate_participation_graph(bp_graphs):
    total_comments = 100
    total_unique_participants = 50

    result = bp_graphs.generate_participation_graph(total_comments, total_unique_participants)

    figure_data = base64.b64decode(result)
    assert figure_data.startswith(b"<svg")


def test_generate_daily_plot(bp_graphs):
    proposals_publication_date = ["2022-01-01", "2022-01-02"]
    proposals_ids = [1, 2]
    total_comments_per_proposal = [10, 20]
    votes_per_proposal = [5, 15]

    result = bp_graphs.generate_daily_plot(
        proposals_publication_date, proposals_ids, total_comments_per_proposal, votes_per_proposal
    )

    figure_data = base64.b64decode(result)
    assert figure_data.startswith(b"<svg")


def test_generate_state_ditribution_donut(bp_graphs):
    data = {"proposal_state": ["accepted", "withdrawn", "rejected", None, "accepted", "rejected"]}

    df = pd.DataFrame(data)

    result = bp_graphs.generate_state_distribution_donut(df)

    figure_data = base64.b64decode(result)

    assert figure_data.startswith(b"<svg")


def test_generate_top_devices(bp_graphs):
    titles = ["Paragrafo 1", "Paragrafo 2", "Paragrafo 3"]
    total_comments = [10, 20, 30]
    status_list_of_lists = [["in_discussion"], ["rejected"], ["accepted"]]

    result = bp_graphs.generate_top_devices(titles, total_comments, status_list_of_lists)

    figure_data = base64.b64decode(result)

    assert figure_data.startswith(b"<svg")


def test_generate_state_participatory_text(bp_graphs):
    data = {"proposal_state": ["accepted", "withdrawn", "rejected", None, "accepted", "rejected"]}

    df = pd.DataFrame(data)

    result = bp_graphs.generate_state_participatory_text(df)

    figure_data = base64.b64decode(result)

    assert figure_data.startswith(b"<svg")
