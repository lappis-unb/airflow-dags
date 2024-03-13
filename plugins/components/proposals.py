# pylint: disable=invalid-name


import logging
import re
from datetime import datetime
from json import loads
from pathlib import Path
from typing import Union

import numpy as np
import pandas as pd
from bs4 import BeautifulSoup

from plugins.components.base_component.component import ComponentBaseHook


class ProposalsHook(ComponentBaseHook):
    """Class for retrieving information about proposals, and extracting related comments.

    Note:
    ----
        This class extends the `ComponentBaseHook` class.

    """

    def get_component(self, **kwargs) -> "dict[str, str]":
        """
        Retrieves information about the Decidim component, proposals.

        Args:
        ----
            **kwargs: Additional keyword arguments.

        Returns:
        -------
            dict[str, str]: Information about the Decidim component.
        """
        update_date_filter: datetime = kwargs.get("update_date_filter", None)
        graphql_query = self.graphql.get_graphql_query_from_file(
            Path(__file__).parent.joinpath(
                "../gql/proposals/get_proposals_by_component_id_without_filter_date.gql"
            )
        )
        variables = {"id": self.component_id}

        if update_date_filter is not None:
            graphql_query = self.graphql.get_graphql_query_from_file(
                Path(__file__).parent.joinpath("../gql/proposals/get_proposals_by_component_id.gql")
            )
            variables["filter_date"] = (
                update_date_filter.strftime("%Y-%m-%d")
                if isinstance(update_date_filter, datetime)
                else update_date_filter
            )

        result = None
        for page in self.graphql.run_graphql_paginated_query(
            graphql_query,
            variables=variables,
        ):
            if result is None:
                result = page
            else:
                result["data"]["component"][self.component_type.lower()]["nodes"].extend(
                    page["data"]["component"][self.component_type.lower()]["nodes"]
                )

        logging.info(
            "Total quantity of %s: %s",
            self.component_type.capitalize(),
            len(result["data"]["component"][self.component_type.lower()]["nodes"]),
        )
        return result["data"]["component"]

    def get_all_comments_from_all_proposals(self, **kwargs):
        """
        Retrieves comments associated with the Decidim component's proposals.

        Args:
        ----
            **kwargs: Additional keyword arguments.

        Returns:
        -------
            dict: Parsed comments in JSON format.
        """
        start_date_filter = kwargs.get("start_date")
        end_date_filter = kwargs.get("end_date")
        if start_date_filter is None or end_date_filter is None:
            logging.error("start_date_filter end end_date_filter is needed.")
            raise AttributeError
        component = self.get_component()  # Get all the proposals.
        proposals = component[self.component_type.lower()]["nodes"]

        comments_df: Union[pd.DataFrame, None] = None
        for proposal in proposals:
            if isinstance(comments_df, pd.DataFrame):
                comments_df = pd.concat(
                    [
                        comments_df,
                        self.get_comments_df(
                            proposal["comments"],
                            root_component_id=proposal["id"],
                            start_date_filter=start_date_filter,
                            end_date_filter=end_date_filter,
                        ),
                    ]
                ).reset_index(drop=True)
            else:
                comments_df = self.get_comments_df(
                    proposal["comments"],
                    root_component_id=proposal["id"],
                    start_date_filter=start_date_filter,
                    end_date_filter=end_date_filter,
                )

        comments_as_json = loads(comments_df.to_json(orient="records"))
        return comments_as_json

    def component_json_to_dataframe(self, json_component) -> pd.DataFrame:
        """Parse decidim API json return to a pandas DataFrame.

        Args:
        ----
            proposals (dict): API json return

        Returns:
        -------
            pd.DataFrame: json parsed into pandas DataFrame
        """
        link_base = self.get_component_link()

        json_component = json_component[self.component_type.lower()]["nodes"]

        normalized_component_json = pd.json_normalize(json_component)
        df = pd.DataFrame(normalized_component_json)
        if df.empty:
            return df

        df["_category"] = ""
        category_columns_in_df = set(["category.name.translation", "category"]).intersection(set(df.columns))
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

        df.loc[df_mask, "state"] = df[df_mask]["state"].apply(get_state, args=("others",))
        df.loc[(~df_mask), "state"] = df[(~df_mask)]["state"].apply(get_state, args=("new",))

        df.loc[df_mask, "date"] = df[df_mask]["updatedAt"]
        df.loc[(~df_mask), "date"] = df[(~df_mask)]["publishedAt"]

        df.replace({None: "-", "": "-"}, inplace=True)

        if "author.organizationName" in df:
            df["author.organizationName"].replace(to_replace=["Brasil Participativo"], value="", inplace=True)
            df["author.organizationName"].replace({"-": ""}, inplace=True)

        df.sort_values(by=["updatedAt"], inplace=True)

        return df

    def get_comments(self, **kwargs):
        start_date = kwargs.get("start_date")
        query = (
            Path(__file__)
            .parent.joinpath("./queries/proposals/get_comments_from_proposals.gql")
            .open()
            .read()
        )
        comments_df = None
        for page in self.graphql.run_graphql_paginated_query(query, variables={"id": self.component_id}):
            proposals = page["data"]["component"]["proposals"]["nodes"]
            for proposal in proposals:
                if comments_df is not None:
                    comments_df = pd.concat(
                        [
                            comments_df,
                            self.get_comments_df(
                                proposal["comments"], proposal["id"], start_date_filter=start_date
                            ),
                        ]
                    )
                else:
                    comments_df = self.get_comments_df(
                        proposal["comments"], proposal["id"], start_date_filter=start_date
                    )
        if isinstance(comments_df, pd.DataFrame):
            return comments_df.to_dict("records")
        else:
            return None
