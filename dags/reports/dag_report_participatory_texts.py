import logging
from contextlib import closing
from datetime import datetime, timedelta
from itertools import chain
from pathlib import Path
from tempfile import TemporaryDirectory

import pendulum
import requests
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.providers.smtp.hooks.smtp import SmtpHook

from plugins.components.base_component.component import ComponentBaseHook
from plugins.components.proposals import ProposalsHook
from plugins.faker.matomo_faker import MatomoFaker
from plugins.reports.participatory_texts_report import ParticipatoryTextsReport
from plugins.utils.dates import fix_date

BP_CONN_ID = "bp_conn_prod"
SMPT_CONN_ID = "gmail_smtp"


def _get_components_url(component_id: int):
    component_hook = ComponentBaseHook(BP_CONN_ID, component_id)
    return component_hook.get_component_link()


def _get_participatory_texts_data_faker(component_id: int, start_date: str, end_date: str):
    return_file = Path(__file__).parent.joinpath("./mock/participatory_text.txt")
    with open(return_file) as file:
        return eval(file.read())


def _get_matomo_data_faker(url: list, start_date: str, end_date: str, module: str, method: str):
    lookup_table = {
        "VisitsSummary.get": MatomoFaker.VisitsSummary.get,
        "VisitFrequency.get": MatomoFaker.VisitFrequency.get,
        "UserCountry.getRegion": MatomoFaker.UserCountry.get_region,
        "DevicesDetection.getType": MatomoFaker.DeviceDetection.get_type,
    }
    return lookup_table[f"{module}.{method}"]()


def _get_participatory_texts_data(component_id: int, start_date: str, end_date: str):
    start_date, end_date = fix_date(start_date, end_date)
    query = (
        Path(__file__)
        .parent.joinpath("./queries/participatory_texts/get_participatory_texts.gql")
        .open()
        .read()
    )

    proposals_hook = ProposalsHook(BP_CONN_ID, component_id)
    query_result = proposals_hook.graphql.run_graphql_paginated_query(
        query,
        variables={"id": component_id, "start_date": start_date, "end_date": end_date},
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
    start_date_obj = datetime.strptime(start_date, "%Y-%m-%d").date()
    current_date = datetime.now().date()

    if start_date_obj > current_date:
        return result

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
                        comments_df[["body", "author_id", "author_name", "date_filter", "status"]].to_dict(
                            "records"
                        )
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


def _get_matomo_data(url: list, start_date: str, end_date: str, module: str, method: str):
    start_date, end_date = fix_date(start_date, end_date)
    matomo_connection = BaseHook.get_connection("matomo_conn")
    matomo_url = matomo_connection.host
    token_auth = matomo_connection.password
    site_id = matomo_connection.login
    date_filter = f"{start_date},{end_date}"
    params = {
        "module": "API",
        "idSite": site_id,
        "period": "range",
        "date": date_filter,
        "segment": f"pageUrl=^{url}",
        "format": "csv",
        "token_auth": token_auth,
        "method": f"{module}.{method}",
    }
    logging.info("Params para a requisição do matomo \n%s.", params)

    response = requests.get(matomo_url, params=params)
    response.raise_for_status()

    try:
        return response.text
    except requests.exceptions.JSONDecodeError as error:
        logging.exception("Response text: %s", response.text)
        raise error


def _generate_report(
    filtered_data,
    visits_summary,
    visits_frequency,
    user_region,
    user_country,
    devices_detection,
    start_date: str,
    end_date: str,
):
    report_name = filtered_data["participatory_space_name"]

    template_path = Path(__file__).parent.joinpath("./templates/template_participatory_texts.html")
    start_date = pendulum.parse(str(filtered_data["start_date"]))
    end_date = pendulum.parse(str(filtered_data["end_date"]))
    start_date, end_date = fix_date(start_date, end_date)

    report_generator = ParticipatoryTextsReport(report_name, template_path, start_date, end_date)

    return {
        "pdf_bytes": report_generator.create_report_pdf(
            report_data=filtered_data,
            matomo_visits_summary_csv=visits_summary,
            matomo_visits_frequency_csv=visits_frequency,
            matomo_user_region_csv=user_region,
            matomo_user_country_csv=user_country,
            matomo_devices_detection_csv=devices_detection,
        )
    }


def send_email_with_pdf(
    email: str,
    pdf_bytes: bytes,
    email_body: str,
    email_subject: str,
    date_start: str,
    date_end: str,
    url: str,
):
    date_start = datetime.strptime(date_start, "%Y-%m-%d").strftime("%d-%m-%Y")
    date_end = datetime.strptime(date_end, "%Y-%m-%d").strftime("%d-%m-%Y")
    hook = SmtpHook(SMPT_CONN_ID)
    hook = hook.get_conn()
    body = f"""<p>{email_body}</p>
        <br>
        <p>Data de inicio: {date_start}</p>
        <p>Data final: {date_end}</p>
        <br>
        <p>Relatorio gerado apartir da pagina: {url}</p>"""

    with TemporaryDirectory("wb") as tmpdir:
        tmp_file = Path(tmpdir).joinpath(f"./relatorio_texto_participativo_{date_start}-{date_end}.pdf")
        with closing(open(tmp_file, "wb")) as file:
            file.write(pdf_bytes)
        hook.send_email_smtp(
            to=email,
            subject=email_subject,
            html_content=body,
            files=[tmp_file],
        )

    logging.info("E-mail enviado com sucesso!")


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
    ----------
        email(str): email to send a report
        start_date(str): initial date
        end_date(str): final date
        component_id(int): number of component id
    """

    @task
    def get_components_url(component_id: int):
        return _get_components_url(component_id)

    @task
    def get_component_data(component_id: int, filter_start_date: str, filter_end_date: str):
        return _get_participatory_texts_data(component_id, filter_start_date, filter_end_date)

    get_components_url_task = get_components_url(component_id)

    def _get_matomo_extractor(url: str, matomo_module: str, matomo_method: str):
        @task(task_id=f"get_matomo_{matomo_module}_{matomo_method}")
        def matomo_extractor(
            url: str,
            filter_start_date: str,
            filter_end_date: str,
            module: str,
            method: str,
        ):
            try:
                return _get_matomo_data(
                    url=url,
                    start_date=filter_start_date,
                    end_date=filter_end_date,
                    module=module,
                    method=method,
                )
            except Exception:
                # TODO: Adicionar mensagem que não recebeu resposta do matomo.
                return None

        return matomo_extractor(
            url,
            start_date,
            end_date,
            matomo_module,
            matomo_method,
        )

    matomo_visits_summary_task = _get_matomo_extractor(get_components_url_task, "VisitsSummary", "get")
    matomo_visits_frequency_task = _get_matomo_extractor(get_components_url_task, "VisitFrequency", "get")
    matomo_user_region_task = _get_matomo_extractor(get_components_url_task, "UserCountry", "getRegion")
    matomo_user_country_task = _get_matomo_extractor(get_components_url_task, "UserCountry", "getCountry")

    matomo_devices_detection_task = _get_matomo_extractor(
        get_components_url_task, "DevicesDetection", "getType"
    )

    @task(multiple_outputs=True)
    def generate_data(
        filtered_data,
        visits_summary,
        visits_frequency,
        user_region,
        user_country,
        devices_detection,
        filter_start_date: str,
        filter_end_date: str,
    ):
        return _generate_report(
            filtered_data,
            visits_summary,
            visits_frequency,
            user_region,
            user_country,
            devices_detection,
            filter_start_date,
            filter_end_date,
        )

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

    report_data = generate_data(
        component_data,
        visits_summary=matomo_visits_summary_task,
        visits_frequency=matomo_visits_frequency_task,
        user_region=matomo_user_region_task,
        user_country=matomo_user_country_task,
        devices_detection=matomo_devices_detection_task,
        filter_start_date=start_date,
        filter_end_date=end_date,
    )

    send_report_email(
        email=email,
        report_data=report_data,
        email_body="Olá, segue em anexo o relatorio solicitado.",
        email_subject="Relatorio De Texto Participativo",
        date_start=start_date,
        date_end=end_date,
        url=get_components_url_task,
    )


generate_report_participatory_texts("test@gmail.com", "2023-01-01", "2024-01-01", 77)
