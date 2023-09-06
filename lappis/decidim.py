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
    
    def get_component_link_component_by_id(self, component_id: int):

        component_type = self.get_component_type(component_id)
        participatory_space = self.get_participatory_space_from_component_id(
            component_id
        )

        inflect_engine = inflect.engine()
        link_base = urljoin(
            self.api_url,
            f"{inflect_engine.plural(participatory_space['type_for_links'])}/{participatory_space['slug']}/f/{component_id}/{component_type.lower()}",
        )

        del inflect_engine

        return link_base

    def _get_proposals_subquery(self, update_date_filter: datetime = None, **kawrgs):
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

    def _get_component_query(self, component_id: str, **kawrgs):
        component_type = self.get_component_type(component_id)

        if component_type == "Proposals":
            update_date_filter = kawrgs.get("update_date_filter", None)

            return self._get_proposals_subquery(
                update_date_filter=update_date_filter
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

    def get_component_by_component_id(self, component_id: int, **kawrgs) -> dict[str, str]:
        graphql_query = f"""
                        {{
                            component(id: {component_id}) {{
                                id
                                name {{
                                translation(locale: "pt-BR")
                                }}

                                {
                                    self._get_component_query(component_id, **kawrgs)
                                }
                            }}
                        }}
        """
        return self.run_graphql_post_query(graphql_query)["data"]["component"]

    def json_component_to_data_frame(
        self, component_id, json_component, **kawrgs
    ) -> pd.DataFrame:
        """Parse decidim API json return to a pandas DataFrame.

        Args:
            proposals (dict): API json return

        Returns:
            pd.DataFrame: json parsed into pandas DataFrame
        """

        # Decidim::ParticipatoryProcess -> decidim::_participatory_process -> process
        
        component_type = self.get_component_type(component_id)
        link_base = self.get_component_link_component_by_id(component_id)

        json_component = json_component[component_type.lower()]["nodes"]
        print(json_component)
        # TODO: Verificar que são essas as keys que estão chegando para todos os componentes.
        normalized_component_json = pd.json_normalize(json_component)
        df = pd.DataFrame(normalized_component_json)

        if df.empty:
            return df

        df.rename(columns={"category.name.translation": "category"}, inplace=True)

        df["publishedAt"] = pd.to_datetime(df["publishedAt"])
        df["updatedAt"] = pd.to_datetime(df["updatedAt"])

        df["body.translation"] = df["body.translation"].apply(
            lambda x: BeautifulSoup(x, "html.parser").get_text()
        )

        ids = np.char.array(df["id"].values, unicode=True)
        df = df.assign(link=(link_base + "/" + ids).astype(str))

        state_map = {
            "accepted": {"label": "aceita ", "emoji": "✅ ✅ ✅"},
            "evaluating": {"label": "em avaliação ", "emoji": "📥 📥 📥"},
            "withdrawn": {"label": "retirada ", "emoji": "🚫 🚫 🚫"},
            "rejected": {"label": "rejeitada ", "emoji": "⛔ ⛔ ⛔"},
            "others": {"label": "atualizada ", "emoji": "🔄 🔄 🔄"},
            "new": {"label": "", "emoji": "📣 📣 📣 <b>[NOVA]</b>"},
        }

        df_mask = df["updatedAt"] > df["publishedAt"]
        df.loc[df_mask, "state"] = df[df_mask]["state"].apply(
            state_map.get, args=(state_map.get("others"),)
        )
        df.loc[df_mask, "date"] = df[df_mask]["updatedAt"]

        df.loc[(~df_mask), "state"] = df[(~df_mask)]["state"].apply(
            state_map.get, args=(state_map.get("new"),)
        )
        df.loc[(~df_mask), "date"] = df[(~df_mask)]["publishedAt"]

        df.fillna("-", inplace=True)
        df.replace({None: "-", "": "-"}, inplace=True)
        return df

    def get_session(self) -> requests.Session:
        """Create a requests session with decidim based on Airflow
        connection host, login and password values.

        Returns:
            requests.Session: session object authenticaded.
        """
        session = requests.Session()

        try:
            r = session.post(self.auth_url, data=self.payload)
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            logging.info("An login error occurred: %s", str(e))
        else:
            return session
