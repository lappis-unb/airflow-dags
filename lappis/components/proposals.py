
"""
Class for authenticating with Decidim using Airflow connection details,
retrieving information about proposals, and extracting related comments.

Note:
    This class extends the ComponentBaseHook class.
"""

# pylint: disable=invalid-name


import logging
import re
from json import loads
from pathlib import Path

import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
from lappis.components.base_component.component import ComponentBaseHook

class ProposalsHook(ComponentBaseHook):

    def get_component(self, **kwargs) -> dict[str, str]:
        """
        Retrieves information about the Decidim component, proposals.

        Args:
            **kwargs: Additional keyword arguments.

        Returns:
            dict[str, str]: Information about the Decidim component.
        """
        update_date_filter: datetime = kwargs.get("update_date_filter", None)

        graphql_query = self.get_graphql_query_from_file(
            Path(__file__).parent.joinpath(
                "../gql/propostas/get_proposals_by_component_id.gql"
            )
        )

        variables = {
            "id": self.component_id,
            "filter_date": update_date_filter.strftime("%Y-%m-%d"),
        }

        result = None
        for page in self.graphql.run_graphql_paginated_query(
            graphql_query, variables=variables, component_type=self.component_type
        ):
            if result is None:
                result = page
            else:
                result["data"]["component"][self.component_type.lower()][
                    "nodes"
                ].extend(
                    page["data"]["component"][self.component_type.lower()]["nodes"]
                )

        logging.info(
            f"Total quantity of {self.component_type.capitalize()}: {len(result['data']['component'][self.component_type.lower()]['nodes'])}"
        )
        return result["data"]["component"]

    def get_comments(self, **kwargs):
        """
        Retrieves comments associated with the Decidim component's proposals.

        Args:
            **kwargs: Additional keyword arguments.

        Returns:
            dict: Parsed comments in JSON format.
        """
        update_date_filter = kwargs.get("update_date_filter")
        if update_date_filter is None:
            logging.error("update_date_filter-(datetime) is needed.")
            raise AttributeError
        component = self.get_component(**kwargs)

        proposals = component[self.component_type.lower()]["nodes"]

        comments = []
        for proposal in proposals:
            comments.extend(
                self.get_comments_threads(
                    proposal["comments"], root_component_id=proposal["id"]
                )
            )

        df = pd.DataFrame(comments)

        if df.empty:
            return None

        df["mask_date"] = pd.to_datetime(
            df["creation_date"], utc=True, format="ISO8601"
        )
        df_mask = df["mask_date"] > update_date_filter

        link_base = self.get_component_link()
        logging.info(f"Dataframe columns: {df.columns}")
        ids = np.char.array(df["root_component_id"].values, unicode=True)
        df = df.assign(link=(link_base + "/" + ids).astype(str))
        
        comments_as_json = loads(df.loc[df_mask, ::].to_json(orient="records"))
        return comments_as_json

    def component_json_to_dataframe(self, json_component) -> pd.DataFrame:
        """Parse decidim API json return to a pandas DataFrame.

        Args:
            proposals (dict): API json return

        Returns:
            pd.DataFrame: json parsed into pandas DataFrame
        """

        link_base = self.get_component_link()

        json_component = json_component[self.component_type.lower()]["nodes"]

        normalized_component_json = pd.json_normalize(json_component)
        df = pd.DataFrame(normalized_component_json)
        if df.empty:
            return df

        df["_category"] = ""
        category_columns_in_df = set(["category.name.translation", "category"]).intersection(
            set(df.columns)
        )
        for column in category_columns_in_df:
            df["_category"] += df[column]

        df.drop(
            columns=["category.name.translation", "category"],
            inplace=True,
            errors="ignore",
        )
        df.rename(columns={"_category": "category"}, inplace=True)

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
