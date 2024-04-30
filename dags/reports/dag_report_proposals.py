import logging
from contextlib import closing
from datetime import datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd
import requests
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.providers.smtp.hooks.smtp import SmtpHook

from plugins.components.base_component.component import ComponentBaseHook
from plugins.graphql.hooks.graphql_hook import GraphQLHook
from plugins.reports.proposals_report import ProposalsReport
from plugins.utils.dates import fix_date

BP_CONN_ID = "bp_conn_prod"
SMPT_CONN_ID = "gmail_smtp"


def _get_components_url(component_id: int):
    component_hook = ComponentBaseHook(BP_CONN_ID, component_id)
    return component_hook.get_component_link()


def _get_proposals_data(component_id: int, start_date: str, end_date: str):
    start_date, end_date = fix_date(start_date, end_date)

    query = (
        Path(__file__).parent.joinpath("./queries/proposals/get_proposals_by_component_id.gql").open().read()
    )
    logging.info(query)

    query_result = GraphQLHook(BP_CONN_ID).run_graphql_paginated_query(
        query,
        variables={"id": component_id, "start_date": start_date, "end_date": end_date},
    )

    result_proposals_data = []
    for page in query_result:
        component = page.get("data", {}).get("component", {})
        if not component:
            continue

        page_component_id = component.get("id")
        participatory_space_id = component.get("participatorySpace", {}).get("id")
        participatory_space_type = component.get("participatorySpace", {}).get("type", "").split("::")[-1]
        page_component_name = component.get("name", {}).get("translation", "-")

        result_proposals_data.append(
            {
                "page_component_id": page_component_id,
                "participatory_space_id": participatory_space_id,
                "participatory_space_type": participatory_space_type,
                "page_component_name": page_component_name,
            }
        )
        page_proposals = component.get("proposals", {}).get("nodes", [])

        for proposal in page_proposals:
            proposal_id = proposal.get("id")
            proposal_title = proposal.get("title", {}).get("translation", "-")
            proposal_published_at = proposal.get("publishedAt")
            proposal_updated_at = proposal.get("updatedAt")
            proposal_state = proposal.get("state")
            proposal_total_comments = proposal.get("totalCommentsCount")
            proposal_total_votes = proposal.get("voteCount")
            proposal_category_title = (
                proposal.get("category", {}).get("name", {}).get("translation", "-")
                if proposal.get("category")
                else "-"
            )

            result_proposals_data.append(
                {
                    "proposal_id": proposal_id,
                    "proposal_title": proposal_title,
                    "proposal_published_at": proposal_published_at,
                    "proposal_updated_at": proposal_updated_at,
                    "proposal_state": proposal_state,
                    "proposal_total_comments": proposal_total_comments,
                    "proposal_total_votes": proposal_total_votes,
                    "proposal_category_title": proposal_category_title,
                }
            )

    print(result_proposals_data)
    return result_proposals_data


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
    bp_data,
    visits_summary,
    visits_frequency,
    user_region,
    user_country,
    devices_detection,
    start_date: str,
    end_date: str,
):
    start_date, end_date = fix_date(start_date, end_date)
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")

    report_title = bp_data[0]["page_component_name"]

    template_path = Path(__file__).parent.joinpath("./templates/template_proposals.html")
    report_generator = ProposalsReport(report_title, template_path, start_date, end_date)
    pdf_bytes = report_generator.create_report_pdf(
        bp_df=pd.DataFrame(bp_data),
        matomo_visits_summary_csv=visits_summary,
        matomo_visits_frequency_csv=visits_frequency,
        matomo_user_region_csv=user_region,
        matomo_user_country_csv=user_country,
        matomo_devices_detection_csv=devices_detection,
    )

    return {"pdf_bytes": pdf_bytes}


def send_invalid_email(
    email,
    date_start: str,
    date_end: str,
):
    date_start = pendulum.parse(str(date_start), strict=False).strftime("%d/%m/%Y")
    date_end = pendulum.parse(str(date_end), strict=False).strftime("%d/%m/%Y")

    hook = SmtpHook(SMPT_CONN_ID)
    hook = hook.get_conn()
    body = f"""<p>Periodo selecionado, {date_start} até {date_end}, não possui dados no momento.</p>
               <p>Por favor tente novamente mais tarde.</p>"""

    hook.send_email_smtp(
        to=email,
        subject="Periodo invalido",
        html_content=body,
    )


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
        tmp_file = Path(tmpdir).joinpath(f"./relatorio_propostas_{date_start}-{date_end}.pdf")
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
        "owner": "Joyce/Paulo",
        "depends_on_past": False,
        "retries": 0,
        "retry_delay": timedelta(minutes=1),
    },
    schedule=None,
    catchup=False,
    start_date=datetime(2023, 11, 10),
    description=__doc__,
    tags=["decidim", "reports", "participacao", "bp"],
)
def generate_report_proposals(email: str, start_date: str, end_date: str, component_id: int):
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
        return _get_proposals_data(component_id, filter_start_date, filter_end_date)

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

    @task
    def invalid_email(
        email,
        date_start: str,
        date_end: str,
    ):
        send_invalid_email(email, date_start, date_end)

    @task(multiple_outputs=True)
    def generate_data(
        bp_data,
        visits_summary,
        visits_frequency,
        user_region,
        user_country,
        devices_detection,
        filter_start_date: str,
        filter_end_date: str,
    ):
        return _generate_report(
            bp_data,
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
        pdf_bytes: bytes,
        email_body: str,
        email_subject: str,
        date_start: str,
        date_end: str,
        url: str,
    ):
        send_email_with_pdf(
            email=email,
            pdf_bytes=pdf_bytes,
            email_body=email_body,
            email_subject=email_subject,
            date_start=date_start,
            date_end=date_end,
            url=url,
        )

    get_components_data_task = get_component_data(
        component_id, filter_start_date=start_date, filter_end_date=end_date
    )

    generated_data = generate_data(
        get_components_data_task,
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
        pdf_bytes=generated_data["pdf_bytes"],
        email_body="Olá, segue em anexo o relatorio solicitado.",
        email_subject="Relatorio De Propostas",
        date_start=start_date,
        date_end=end_date,
        url=get_components_url_task,
    )


generate_report_proposals("test@gmail.com", "2023-01-01", "2024-01-01", 10)
