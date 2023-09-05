"""Class to authenticate at decidim with airflow connection host, login
and password.
"""

# pylint: disable=invalid-name

import logging
from urllib.parse import urljoin
import requests
from contextlib import closing
from airflow.hooks.base import BaseHook
import pandas as pd
import re
from inflection import underscore
import inflect
import numpy as np
from datetime import datetime
from bs4 import BeautifulSoup


class DecidimHook(BaseHook):
    def __init__(self, conn_id: str):
        conn_values = self.get_connection(conn_id)
        self.api_url = conn_values.host
        self.auth_url = urljoin(self.api_url, "api/sign_in")
        self.payload = {
            "user[email]": conn_values.login,
            "user[password]": conn_values.password,
        }

    def run_graphql_post_query(self, graphql_query):
        response = self.get_session().post(self.api_url, json={"query": graphql_query})
        status_code = response.status_code
        assert status_code == 200, logging.ERROR(
            f"""Query:
                                                        {graphql_query}
                                                     has returned status code: {status_code}
                                                    """
        )

        return response.json()

    def __get_proposals_query(self, update_date_filter: datetime = None, **kawrgs):
        assert update_date_filter is not None, logging.ERROR(
            "Porposals need the update_date_filter to run."
        )

        query = f"""
            ... on Proposals{{
                id
                name {{
                    translation(locale: "pt-BR")
                }}
                proposals(filter: {{publishedSince: "{update_date_filter.strftime("%Y-%m-%d")}"}}, order: {{publishedAt: "desc"}}) {{
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
                }}
            """
        return query

    def get_component_type(self, component_id: str) -> str:
        logging.info(f"Component id: {component_id}")
        graphql_query = f"""
                    {{
                        component(id: {component_id}) {{
                            __typename
                        }}
                    }}
                    """
        response = self.run_graphql_post_query(graphql_query=graphql_query)

        assert response["data"]["component"] is not None
        return response["data"]["component"]["__typename"]

    def __get_component_query(self, component_id: str, **kawrgs):
        component_type = self.get_component_type(component_id)

        if component_type == "Proposals":
            return self.__get_proposals_query(
                update_date_filter=kawrgs.get("update_date_filter", None)
            )

    def get_participatory_space_from_component_id(
        self, component_id: int
    ) -> dict[str, str]:
        graphql_query = f"""{{
                component(id: {component_id}) {{
                    participatorySpace {{
                        id
                        type
                }}
            }}
        }}
        """

        response = self.run_graphql_post_query(graphql_query)
        participatory_space = response["data"]["component"]["participatorySpace"]

        lower_first_letter = lambda s: s[:1].lower() + s[1:] if s else ""

        participatory_space["type"] = lower_first_letter(
            participatory_space["type"].split("::")[-1]
        )
        graphql_query = f"""
            {{
            {participatory_space["type"]}(id: {participatory_space["id"]}){{
                id
                type
                slug
                title {{
                    translation(locale: "pt-BR")
                }}
            }}
        }}
        """

        response = self.run_graphql_post_query(graphql_query)
        participatory_space = response["data"][participatory_space["type"]]
        participatory_space["type_for_links"] = underscore(
            participatory_space["type"]
        ).split("_")[-1]

        return participatory_space

    def get_component(self, component_id: int, **kawrgs) -> dict[str, str]:
        graphql_query = f"""
                        {{
                            component(id: {component_id}) {{
                                id
                                name {{
                                translation(locale: "pt-BR")
                                }}

                                {
                                    self.__get_component_query(component_id, **kawrgs)
                                }
                            }}
                        }}
        """
        return self.run_graphql_post_query(graphql_query)["data"]["component"]
