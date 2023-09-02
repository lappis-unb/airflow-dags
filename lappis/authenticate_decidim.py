"""Class to authenticate at decidim with airflow connection host, login
and password.
"""

# pylint: disable=invalid-name

import logging
from urllib.parse import urljoin
import requests
from contextlib import closing
from airflow.hooks.base import BaseHook

class AuthenticateDecidim:
    def __init__(self, conn_id: str):
        conn_values = BaseHook.get_connection(conn_id)
        self.api_url = conn_values.host
        self.auth_url = urljoin(self.api_url, "api/sign_in")
        self.payload = {
            "user[email]": conn_values.login,
            "user[password]": conn_values.password,
        }

    def __run_graphql_post_query__(self, graphql_query):
        response = self.get_session().post(self.api_url, json={"query": graphql_query})
        status_code = response.status_code
        assert status_code == 200, logging.ERROR(f"""Query:
                                                        {graphql_query}
                                                     has returned status code: {status_code}
                                                    """)

        return response


    def __get_proposals_query(self, update_date_filter=None, **kawrgs):
        assert update_date_filter, logging.ERROR("Porposals need the update_date_filter to run.")

        query = f"""
            proposals(filter: {{publishedSince: {update_date_filter}}}, order: {{publishedAt: "desc"}}) {{
                nodes {{
                    id
                    title {{
                        translation(locale: "pt-BR")
                    }}
                    publishedAt
                    updatedAt
                    state
                    author {{
                        name
                        organizationName
                    }}
                    category {{
                        name {{
                            translation(locale: "pt-BR")
                        }}
                    }}
                    body {{
                        translation(locale: "pt-BR")
                    }}
                    official
                        }}
                    }}
            """
        return query


    def get_session(self) -> requests.Session:
        """Create a requests session with decidim based on Airflow
        connection host, login and password values.

        Returns:
            requests.Session: session object authenticaded.
        """

        with closing(requests.Session()) as session:
            try:
                r = session.post(self.auth_url, data=self.payload)
                r.raise_for_status()
            except requests.exceptions.HTTPError as e:
                logging.info("An login error occurred: %s", str(e))
            else:
                return session
