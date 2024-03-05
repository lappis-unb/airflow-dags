import logging
from contextlib import closing
from datetime import datetime, timedelta
from itertools import chain
from pathlib import Path
from tempfile import TemporaryDirectory

from airflow.decorators import dag, task
from airflow.providers.smtp.hooks.smtp import SmtpHook

from plugins.components.base_component.component import ComponentBaseHook
from plugins.components.proposals import ProposalsHook
from plugins.reports.participatory_texts_report import ParticipatoryTextsReport

BP_CONN_ID = "bp_conn_prod"
SMPT_CONN_ID = "gmail_smtp"


def _get_components_url(component_id: int):
    component_hook = ComponentBaseHook(BP_CONN_ID, component_id)
    return component_hook.get_component_link()


def _get_participatory_texts_data(component_id: int, start_date: str, end_date: str):
    query = (
        Path(__file__)
        .parent.joinpath("./queries/participatory_texts/get_participatory_texts.gql")
        .open()
        .read()
    )
    proposals_hook = ProposalsHook(BP_CONN_ID, component_id)
    query_result = proposals_hook.graphql.run_graphql_paginated_query(
        query, variables={"id": component_id, "start_date": start_date, "end_date": end_date}
    )

    participatory_space = proposals_hook.get_participatory_space()
    participatory_space_name = participatory_space["title"]["translation"]

    result = {
        "participatory_space_name": participatory_space_name,
        "start_date": start_date,
        "end_date": end_date,
        "total_comments": 0,
        "proposals": [],
    }
    for page in query_result:
        component = page["data"]["component"]
        proposals = component["proposals"]["nodes"]

        for proposal in proposals:
            comments_df = proposals_hook.get_comments_df(
                proposal["comments"],
                proposal["id"],
                start_date_filter=start_date,
                end_date_filter=end_date,
            )
            total_comments_in_proposal = comments_df.shape[0] if not comments_df.empty else 0

            result["total_comments"] += total_comments_in_proposal
            unique_authors = [*comments_df["author_id"].unique()] if not comments_df.empty else []

            result["proposals"].append(
                {
                    "vote_count": proposal["voteCount"],
                    "total_comments": total_comments_in_proposal,
                    "title": proposal["title"]["translation"],
                    "id": proposal["id"],
                    "qt_unique_authors": len(set(unique_authors)),
                    "unique_authors": unique_authors,
                    "comments": (
                        comments_df[["body", "author_id", "author_name", "date_filter"]].to_dict("records")
                        if not comments_df.empty
                        else []
                    ),
                }
            )

    result["total_unique_participants"] = len(
        set(
            chain.from_iterable(
                [current_proposal["unique_authors"] for current_proposal in result["proposals"]]
            )
        )
    )

    logging.info("Total participants: %s", result["total_unique_participants"])

    return result


def _generate_report(filtered_data):
    report_name = filtered_data["participatory_space_name"]
    template_path = Path(__file__).parent.joinpath("./templates/template_participatory_texts.html")
    start_date = datetime.strptime(filtered_data["start_date"], "%Y-%m-%d")
    end_date = datetime.strptime(filtered_data["end_date"], "%Y-%m-%d")

    report_generator = ParticipatoryTextsReport(report_name, template_path, start_date, end_date)

    return {"pdf_bytes": report_generator.create_report_pdf(report_data=filtered_data)}


def send_email_with_pdf(
    email: str,
    pdf_bytes: bytes,
    email_body: str,
    email_subject: str,
    date_start: str,
    date_end: str,
    url: str,
):
    hook = SmtpHook(SMPT_CONN_ID)
    hook = hook.get_conn()
    body = f"""<p>{email_body}</p>
        <br>
        <p>Data de inicio: {date_start}</p>
        <p>Data final: {date_end}</p>
        <br>
        <p>Relatorio gerado apartir da pagina: {url}</p>"""

    with TemporaryDirectory("wb") as tmpdir:
        tmp_file = Path(tmpdir).joinpath(f"./relatorio_{date_start}-{date_end}.pdf")
        with closing(open(tmp_file, "wb")) as file:
            file.write(pdf_bytes)
        hook.send_email_smtp(
            to=email,
            subject=email_subject,
            html_content=body,
            files=[tmp_file],
        )

    print("E-mail enviado com sucesso!")


@dag(
    default_args={
        "owner": "Joyce/Thais",
        "depends_on_past": False,
        "retries": 0,
        "retry_delay": timedelta(minutes=1),
    },
    schedule=None,
    catchup=False,
    start_date=datetime(2023, 11, 10),
    description=__doc__,
    tags=["decidim", "reports", "participatory-texts", "bp"],
)
def generate_report_participatory_texts(email: str, start_date: str, end_date: str, component_id: int):
    """
    Gera um relatorio para o BP.

    1. Pegar todos os componentes do espaço participativo.
        1. Fazer a query para o BP com as datas passadas.
    2. Fazer a requisição para o matomo com as datas passadas.
    3. Gerar o relatorio.
    """

    @task
    def get_components_url(component_id: int):
        return _get_components_url(component_id)

    @task
    def get_component_data(component_id: int, filter_start_date: str, filter_end_date: str):
        return _get_participatory_texts_data(component_id, filter_start_date, filter_end_date)

    @task
    def generate_data(filtered_data):
        return _generate_report(filtered_data)

    @task
    def send_report_email(
        email: str,
        report_data: dict,
        email_body: str,
        email_subject: str,
        date_start: str,
        date_end: str,
        url: str,
    ):
        pdf_bytes = report_data["pdf_bytes"]
        send_email_with_pdf(
            email=email,
            pdf_bytes=pdf_bytes,
            email_body=email_body,
            email_subject=email_subject,
            date_start=date_start,
            date_end=date_end,
            url=url,
        )

    component_data = get_component_data(component_id, filter_start_date=start_date, filter_end_date=end_date)

    get_components_url_task = get_components_url(component_id)

    report_data = generate_data(component_data)
    # print(report_data)
    send_report_email(
        email=email,
        report_data=report_data,
        email_body="Olá, segue em anexo o relatorio gerado.",
        email_subject="Relatorio De Texto Participativo",
        date_start=start_date,
        date_end=end_date,
        url=get_components_url_task,
    )


generate_report_participatory_texts("test@gmail.com", "2023-01-01", "2024-01-01", 77)
