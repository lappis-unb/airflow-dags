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
from collections import defaultdict

from json import loads


class DecidimHook(BaseHook):
    def __init__(self, conn_id: str):
        conn_values = self.get_connection(conn_id)
        self.api_url = conn_values.host
        self.auth_url = urljoin(self.api_url, "api/sign_in")
        self.payload = {
            "user[email]": conn_values.login,
            "user[password]": conn_values.password,
        }

    def run_graphql_post_query(self, graphql_query) -> dict[str, str]:
        """
            Execute a GraphQL POST query and retrieve the JSON response.

            Args:
                graphql_query (str): The GraphQL query to execute.

            Returns:
                dict[str, str]: A dictionary containing the JSON response.

            Raises:
                AssertionError: If the HTTP status code is not 200 (OK), an error is logged.

            Example:
                To execute a GraphQL query and retrieve the response:
                >>> response = self.run_graphql_post_query(query)
                >>> process_response_data(response)

            Note:
                This function sends a POST request to the specified API URL with the provided GraphQL query.
                It checks if the HTTP status code is 200 (OK) and raises an AssertionError if not.
        """
        response = self.get_session().post(self.api_url, json={"query": graphql_query})
        status_code = response.status_code
        assert status_code == 200, logging.ERROR(
            f"""Query:
                                                        {graphql_query}
                                                     has returned status code: {status_code}
                                                    """
        )

        return response.json()

    def run_graphql_post_pagineted_query(
        self, pagineted_graphql_query: str, endcursor: str = "null"
    ):
        """
        Execute a paginated GraphQL POST query and retrieve results page by page.

        Args:
            paginated_graphql_query (str): The GraphQL query to execute for pagination.
            endcursor (str, optional): The cursor to start pagination. Defaults to "null".

        Yields:
            dict: A dictionary containing the JSON response for each page.

        Note:
            This function is designed to handle paginated GraphQL queries. It starts with the provided endcursor
            and retrieves pages of results until there are no more pages. The results are yielded page by page.

        Example:
            To fetch paginated results:
            >>> for page in self.run_graphql_post_paginated_query(query, endcursor="some_cursor"):
            ...     process_page_data(page)

        """
        variables = {"after": endcursor}
        response = self.get_session().post(
            self.api_url,
            json={"query": pagineted_graphql_query, "variables": variables},
        )
        response_json = response.json()

        # TODO: Trocar para uma forma mais geral e não apenas proposals.
        new_endcursor = response_json["data"]["component"]["proposals"]["pageInfo"][
            "endCursor"
        ]
        hasnextpage = response_json["data"]["component"]["proposals"]["pageInfo"][
            "hasNextPage"
        ]

        if hasnextpage:
            yield from self.run_graphql_post_pagineted_query(
                pagineted_graphql_query, endcursor=new_endcursor
            )

        yield response_json

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
                proposals(filter: {{publishedSince: "{update_date_filter.strftime("%Y-%m-%d")}"}}, order: {{publishedAt: "asc"}}, after: $after) {{
                    pageInfo {{
                        hasNextPage
                        startCursor
                        endCursor
                    }}
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
                        comments{{
                                id
                                body
                                createdAt
                                author {{
                                    id
                                    name
                                }}
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
        graphql_query = f"""
                    {{
                        component(id: {component_id}) {{
                            __typename
                        }}
                    }}
                    """
        response = self.run_graphql_post_query(graphql_query=graphql_query)

        assert response["data"]["component"] is not None, response
        return response["data"]["component"]["__typename"]

    def _get_component_query(self, component_id: str, **kawrgs):
        component_type = self.get_component_type(component_id)

        if component_type == "Proposals":
            update_date_filter = kawrgs.get("update_date_filter", None)

            return self._get_proposals_subquery(update_date_filter=update_date_filter)

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
        type_of_space = participatory_space["type"] = lower_first_letter(
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
        participatory_space["type_for_links"] = underscore(type_of_space).split("_")[-1]

        return participatory_space

    def _format_comment(
        self, comment: dict, proposal_id, parent_id: int = None
    ) -> dict:
        return {
            "proposal_id": proposal_id,  # TODO achar um nome melhora para component pq não é um componente mas sim uma proposta ou uma reunião etc
            "parent_id": comment["id"] if parent_id is None else parent_id,
            "body": comment["body"],
            "author_id": comment["author"]["id"],
            "author_name": comment["author"]["name"],
            "comment_id": comment["id"],
            "creation_date": comment["createdAt"],
        }

    def _build_comment_thread(
        self, parent_comment: dict[str], proposal_id: int, thread_level: int = 1
    ):
        graphql_query = f"""{{
                            commentable(
                                id: "{parent_comment['id']}"
                                type: "Decidim::Comments::Comment"
                                locale: "pt-BR"
                                toggleTranslations: true
                            ) {{
                            id
                            comments {{
                                id
                                body
                                createdAt
                                author {{
                                    id
                                    name
                                }}
                            }}
                        }}
                    }}
                """
        result = self.run_graphql_post_query(graphql_query)
        commentable = result["data"]["commentable"]

        if thread_level == 1:  # Root level
            yield self._format_comment(parent_comment, proposal_id=proposal_id)

        for comment in commentable["comments"]:
            yield self._format_comment(
                comment, proposal_id=proposal_id, parent_id=parent_comment["id"]
            )
            yield from self._build_comment_thread(
                comment, thread_level=thread_level + 1, proposal_id=proposal_id
            )

    def get_comments_from_component_id(
        self, component_id: int, update_date_filter, **kwargs
    ):
        component = self.get_component_by_component_id(
            component_id=component_id, update_date_filter=update_date_filter, **kwargs
        )
        component_type = self.get_component_type(component_id)

        proposals = component[component_type.lower()]["nodes"]

        comments = []

        for proposal in proposals:
            for comment in proposal["comments"]:
                comments.extend(
                    self._build_comment_thread(comment, proposal_id=proposal["id"])
                )

        df = pd.DataFrame(comments)

        if df.empty:
            return loads(df.to_json(orient="records"))

        df["mask_date"] = pd.to_datetime(
            df["creation_date"], utc=True, format="ISO8601"
        )
        df_mask = df["mask_date"] > update_date_filter

        link_base = self.get_component_link_component_by_id(component_id)
        ids = np.char.array(df["proposal_id"].values, unicode=True)
        df = df.assign(link=(link_base + "/" + ids).astype(str))

        return loads(df.loc[df_mask, ::].to_json(orient="records"))

    def get_component_by_component_id(
        self, component_id: int, **kawrgs
    ) -> dict[str, str]:
        graphql_query = f"""
                        query($after: String) {{
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
        component_type = self.get_component_type(component_id).lower()

        result = None
        for page in self.run_graphql_post_pagineted_query(graphql_query):
            if result is None:
                result = page
            else:
                result["data"]["component"][component_type]["nodes"].extend(
                    page["data"]["component"][component_type]["nodes"]
                )

        logging.info(
            f"Total quantity of {component_type.capitalize()}: {len(result['data']['component'][component_type]['nodes'])}"
        )
        return result["data"]["component"]

    def component_json_to_dataframe(
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

        normalized_component_json = pd.json_normalize(json_component)
        df = pd.DataFrame(normalized_component_json)
        if df.empty:
            return df

        df["categoria"] = ""
        for column in set(["category.name.translation", "category"]).intersection(
            set(df.columns)
        ):
            df["categoria"] += df[column]

        df.drop(
            columns=["category.name.translation", "category"],
            inplace=True,
            errors="ignore",
        )
        df.rename(columns={"categoria": "category"}, inplace=True)

        df["publishedAt"] = pd.to_datetime(df["publishedAt"])
        df["updatedAt"] = pd.to_datetime(df["updatedAt"])

        df.fillna("", inplace=True)

        df["body.translation"] = df["body.translation"].apply(
            lambda x: BeautifulSoup(x, "html.parser").get_text()
        )

        # Removes hastag from body.
        df["body.translation"] = df["body.translation"].apply(
            lambda x: re.sub(r"gid:\/\/decide\/Decidim::Hashtag\/\d\/\w*|\n$", "", x)
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
        get_state = lambda state, default: state_map.get(state, state_map.get(default))

        df.loc[df_mask, "state"] = df[df_mask]["state"].apply(
            get_state, args=("others",)
        )
        df.loc[(~df_mask), "state"] = df[(~df_mask)]["state"].apply(
            get_state, args=("new",)
        )

        df.loc[df_mask, "date"] = df[df_mask]["updatedAt"]
        df.loc[(~df_mask), "date"] = df[(~df_mask)]["publishedAt"]

        df.replace({None: "-", "": "-"}, inplace=True)

        if "author.organizationName" in df:
            df["author.organizationName"].replace(
                to_replace=["Brasil Participativo"], value="", inplace=True
            )
            df["author.organizationName"].replace({"-": ""}, inplace=True)

        df.sort_values(by=["updatedAt"], inplace=True)

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
