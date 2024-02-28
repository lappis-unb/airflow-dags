import smtplib
from datetime import datetime, timedelta
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

from airflow.decorators import dag, task

from plugins.graphql.hooks.graphql_hook import GraphQLHook
from plugins.reports.participatory_texts import DataFilter
from plugins.reports.script import create_report_pdf

BP_CONN_ID = "bp_conn"


def _get_participatory_texts_data(component_id: int, start_date: str, end_date: str):
    query = (
        Path(__file__)
        .parent.parent.joinpath("./plugins/gql/reports/participatory_texts/get_participatory_texts.gql")
        .open()
        .read()
    )
    query_result = GraphQLHook(BP_CONN_ID).run_graphql_paginated_query(
        query, variables={"id": component_id, "start_date": start_date, "end_date": end_date}
    )

    result_participatory_texts_data = []

    for page in query_result:
        component = page.get("data", {}).get("component", {})
        if not component:
            continue

        page_component_id = component.get("id")
        participatory_space_id = component.get("participatorySpace", {}).get("id")
        participatory_space_type = component.get("participatorySpace", {}).get("type", "").split("::")[-1]
        page_component_name = component.get("name", {}).get("translation", "-")
        page_participatory_texts = component.get("proposals", {}).get("nodes", [])

        for proposal in page_participatory_texts:
            proposal_title = proposal.get("title", {}).get("translation", "-")
            proposal_official = proposal.get("official")
            proposal_total_comments = proposal.get("totalCommentsCount")
            proposal_total_votes = proposal.get("voteCount")
            texts_comments = proposal.get("comments", [])

            comments_with_authors = []
            for comment in texts_comments:
                comment_body = comment.get("body")
                comment_votes_up = comment.get("upVotes")
                comment_votes_down = comment.get("downVotes")
                author_name = comment.get("author", {}).get("name")
                comment_date = comment.get("createdAt")

                comments_with_authors.append(
                    {
                        "body": comment_body,
                        "upVotes": comment_votes_up,
                        "downVotes": comment_votes_down,
                        "author_name": author_name,
                        "createdAt": comment_date,
                    }
                )

            result_participatory_texts_data.append(
                {
                    "page_component_id": page_component_id,
                    "participatory_space_id": participatory_space_id,
                    "participatory_space_type": participatory_space_type,
                    "page_component_name": page_component_name,
                    "proposal_title": proposal_title,
                    "proposal_official": proposal_official,
                    "proposal_total_comments": proposal_total_comments,
                    "proposal_total_votes": proposal_total_votes,
                    "comments": comments_with_authors,
                }
            )

        return result_participatory_texts_data


def apply_filter_data(raw_data):
    data_filter = DataFilter(raw_data)
    return data_filter.filter_data()


def _generate_report(filtered_data):
    pdf_bytes = create_report_pdf(filtered_data)
    return {"pdf_bytes": pdf_bytes}


def send_email_with_pdf(email: str, pdf_bytes: bytes, email_body: str, email_subject: str):
    smtp_server = "smtp.gmail.com"
    smtp_port = 587
    smtp_user = "email"
    smtp_password = "password"

    message = MIMEMultipart()
    message["From"] = smtp_user
    message["To"] = email
    message["Subject"] = email_subject

    message.attach(MIMEText(email_body, "plain"))

    pdf_attachment = MIMEApplication(pdf_bytes, _subtype="pdf")
    pdf_attachment.add_header("Content-Disposition", "attachment", filename="report.pdf")
    message.attach(pdf_attachment)

    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.starttls()
        server.login(smtp_user, smtp_password)
        server.send_message(message)

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
    Generates a report.
    Parameters:
        email(str): email to send a report
        start_date(str): initial date
        end_date(str): final date
        component_id(int): number of component id
    """

    @task
    def get_component_data(component_id: int, filter_start_date: str, filter_end_date: str):
        return _get_participatory_texts_data(component_id, filter_start_date, filter_end_date)

    @task
    def filter_component_data(raw_data):
        return apply_filter_data(raw_data)

    @task
    def generate_data(filtered_data):
        return _generate_report(filtered_data)

    @task
    def send_report_email(
        email: str, report_data: dict, email_body: str, email_subject: str = "Seu Relatório"
    ):
        pdf_bytes = report_data["pdf_bytes"]
        send_email_with_pdf(
            email=email,
            pdf_bytes=pdf_bytes,
            email_body=email_body,
            email_subject=email_subject,
        )

    component_data = get_component_data(component_id, filter_start_date=start_date, filter_end_date=end_date)

    filtered_data = filter_component_data(component_data)

    report_data = generate_data(filtered_data)

    send_report_email(
        email=email,
        report_data=report_data,
        email_body="Here goes your e-mail body",
        email_subject="Relatório de Textos Participativos",
    )


generate_report_participatory_texts("test@gmail.com", "2023-01-01", "2024-01-01", 77)
