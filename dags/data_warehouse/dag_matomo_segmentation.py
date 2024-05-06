import logging
from datetime import datetime, timedelta
from io import StringIO
from pathlib import Path

import pandas as pd
import requests
from airflow.models import Variable

from airflow.decorators import dag, task, task_group
from airflow.hooks.base_hook import BaseHook
from airflow.operators.empty import EmptyOperator
from plugins.graphql.hooks.graphql_hook import GraphQLHook

default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}
DECIDIM_CONN_ID = "api_decidim"


def _get_query(space):
    """
    Retrieves the query from the specified file and returns it.

    Returns:
    -------
      str: The query string.
    """
    query = Path(__file__).parent.joinpath(f"./queries/{space}_slug_id.gql").open().read()
    return query


ESPACOS = ["processes"]


def get_credentials_matomo(matomo_conn: str = "matomo_conn"):
    """
    Retrieves the credentials required to connect to the Matomo API.

    Args:
    ----
        matomo_conn (str): The name of the Airflow connection for Matomo.

    Returns:
    -------
        tuple: A tuple containing the Matomo URL, token authentication, and site ID.
    """
    matomo_conn = BaseHook.get_connection(matomo_conn)
    matomo_url = matomo_conn.host
    token_auth = matomo_conn.password
    site_id = matomo_conn.login
    return matomo_url, token_auth, site_id


spaces = {"processes": "participatoryProcesses", "assemblies": "assemblies"}


@dag(
    default_args=default_args,
    schedule_interval="0 0 * * *",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["matomo", "segmentation"],
)
def dag_matomo_segmentation():
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    for space in spaces:

        @task_group(group_id=space)
        def group(space):
            start = EmptyOperator(task_id="start")
            end = EmptyOperator(task_id="end")

            @task(provide_context=True)
            def get_url_matomo(space, **context):
                """
                Retrieves the URLs for Matomo segmentation based on the provided space.

                Args:
                ----
                    space (str): The space for which the URLs need to be retrieved.
                    context (dict): The context dictionary containing task information.

                Returns:
                -------
                    list: A list of URLs for Matomo segmentation.

                """
                slug_id = context["ti"].xcom_pull(task_ids=f"{space}.get_slug_id")
                urls = []
                bp = Variable.get("url_bp")
                for item in slug_id:
                    slug = item[0]
                    component_id = item[1]
                    url = f"{bp}/{space}/{slug}/f/{component_id}/"
                    urls.append(url)
                return urls

            @task
            def get_segment_matomo():
                """
                Retrieves segment definitions from Matomo API.

                Returns:
                -------
                    pandas.Series: A series containing the segment definitions.

                Raises:
                ------
                    Exception: If the API request fails.
                """
                matomo_url, token_auth, site_id = get_credentials_matomo()

                params = {
                    "module": "API",
                    "method": "SegmentEditor.getAll",
                    "idSite": site_id,
                    "token_auth": token_auth,
                    "format": "csv",
                }
                response = requests.get(matomo_url, params=params)
                if response.status_code == 200:
                    data = StringIO(response.text)
                    df = pd.read_csv(data)
                    return df["definition"].str.replace("pageUrl=^", "").values
                else:
                    raise Exception("deu ruim", response.status_code)

            @task
            def get_slug_id(space):
                """
                Retrieves the slug and ID for a given space using GraphQL.

                Args:
                ----
                    space (str): The name of the space.

                Returns:
                -------
                    list: A list of tuples containing the slug and ID for each component in the space.
                """
                hook = GraphQLHook(DECIDIM_CONN_ID)
                session = hook.get_session()
                query = _get_query(space)
                response = session.post(
                    hook.api_url,
                    json={
                        "query": query,
                    },
                )
                data = eval(response.text)
                data = data["data"][spaces[space]]
                slug_id = []
                for item in data:
                    slug = item["slug"]
                    components = item["components"]
                    for component in components:
                        _id = component["id"]
                        slug_id.append((slug, _id))
                return slug_id

            @task(provide_context=True)
            def filter_url(space, **context):
                """
                Filter URLs based on segments.

                Args:
                ----
                    space (str): The space identifier.
                    **context: Additional context provided by Airflow.

                Returns:
                -------
                    list: A list of URLs that are not present in the segments.

                """
                urls = context["ti"].xcom_pull(task_ids=f"{space}.get_url_matomo")
                segments = context["ti"].xcom_pull(task_ids=f"{space}.get_segment_matomo")
                new_segments = set(urls).difference(set(segments))
                return list(new_segments)

            @task(provide_context=True)
            def add_segmentation(space, **context):
                """
                Add segmentation to Matomo.

                Args:
                ----
                    space (str): The space name.
                    **context: Additional context provided by Airflow.

                Returns:
                -------
                    str: The response text if the request is successful.

                Raises:
                ------
                    Exception: If the request fails with a non-200 status code.
                """
                segmentations = context["ti"].xcom_pull(task_ids=f"{space}.filter_url")
                matomo_url, token_auth, site_id = get_credentials_matomo()
                segmentations = segmentations[:3]
                for segmentation in segmentations:
                    splited_segmentation = segmentation.split("/")
                    name = (
                        f"component_{splited_segmentation[-5]}_"
                        f"{splited_segmentation[-4]}_{splited_segmentation[-2]}"
                    )
                    name = name.replace("-", "_")
                    #hardcode triste.. mas necessario:(
                    if not segmentation.startswith("https://brasilparticipativo.presidencia.gov.br/"):
                        logging.warning("Segmentation not accepted: %s \t - %s", segmentation, name)
                        continue
                    params = {
                        "module": "API",
                        "method": "SegmentEditor.add",
                        "idSite": site_id,
                        "token_auth": token_auth,
                        "autoArchive": 1,
                        "format": "csv",
                        "name": name,
                        "definition": f"pageUrl=^{segmentation}",
                    }
                    response = requests.post(matomo_url, params=params)
                    if response.status_code == 200:
                        logging.info("Segmentation added: %s", segmentation)
                    else:
                        raise Exception("deu ruim", response.status_code)

            _get_slug_id = get_slug_id(space)
            _get_url_matomo = get_url_matomo(space)
            _get_segment_matomo = get_segment_matomo()
            _filter_url = filter_url(space)
            (start >> [_get_url_matomo, _get_segment_matomo] >> _filter_url >> add_segmentation(space) >> end)
            start >> _get_slug_id >> _get_url_matomo

        start >> group(space) >> end


dag = dag_matomo_segmentation()
