"""
Classe base com funções fundamentais para componentes Decidim.

Métodos:
    __init__(self, conn_id: str, component_id: int): Inicializa uma nova instância de ComponentBaseHook.
    get_component_link(self) -> str: Recupera o link para o componente.
    get_component_type(self) -> str: Recupera o tipo do componente Decidim via consulta GraphQL.
    get_participatory_id_and_type(self): Recupera ID e tipo do espaço participativo associado.
    get_participatory_space(self) -> dict[str, str]: Recupera info sobre o espaço participativo associado.
    _parse_comment(self, comment: dict, root_component_id, parent_comment_id: int = None) -> dict: Formata um comentário.
    _build_comment_thread(self, parent_comment: dict[str], root_component_id: int, thread_level: int = 1): Constrói thread de comentários recursivamente.
    get_comments_threads(self, root_comments: list[dict[str]], root_component_id): Recupera threads de comentários para lista de comentários raiz.
    get_component(self, **kwargs) -> dict[str, str]: Recupera informações sobre o componente. (Método abstrato)
    get_comments(self, **kwargs): Recupera comentários. (Método abstrato)
    component_json_to_dataframe(self, json_component, **kwargs): Transforma dados do componente em DataFrame do pandas. (Método abstrato)
"""  # noqa: E501

import logging
from datetime import datetime
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin

import inflect
import numpy as np
import pandas as pd
import pendulum
from inflection import underscore

from plugins.graphql.hooks.graphql_hook import GraphQLHook


class ComponentBaseHook:
    """Classe base com funções fundamentais para componentes Decidim."""

    def __init__(self, conn_id: str, component_id: int):
        """
        Inicializa uma nova instância de ComponentBaseHook.

        Args:
        ----
            conn_id (str): O ID de conexão para o Airflow.
            component_id (int): O ID do componente para o qual o hook está sendo criado.
        """
        self.graphql = GraphQLHook(conn_id)
        self.component_id = component_id
        self.component_type: str = self.get_component_type()

    def _lower_first_letter(self, string: str):
        return string[:1].lower() + string[1:] if string else ""

    def get_component_link(self) -> str:
        """
        Recupera o link para o componente.

        Returns
        -------
            str: O link para o componente.
        """
        participatory_space = self.get_participatory_space()
        inflect_engine = inflect.engine()
        link_base = urljoin(
            self.graphql.api_url,
            f"{inflect_engine.plural(participatory_space['type_for_links'])}/{participatory_space['slug']}/f/{self.component_id}/{self.component_type.lower()}",
        )
        del inflect_engine
        return link_base

    def get_component_type(self) -> str:
        """
        Recupera o tipo do componente.

        Returns
        -------
            str: O tipo do componente.
        """
        graphql_query = f"""
                    {{
                        component(id: {self.component_id}) {{
                            __typename
                        }}
                    }}
                    """
        logging.info("Getting component type for component id %s.", self.component_id)
        response = self.graphql.run_graphql_query(graphql_query=graphql_query)
        assert response["data"]["component"] is not None, response
        return response["data"]["component"]["__typename"]

    def get_participatory_escope(self):
        """
        Retrieves the ID and type of the participatory space with scope.

        Returns
        -------
            dict[str, str]: The participatory space ID and type.
        """
        participatory_space = self.get_participatory_id_and_type()

        graphql_query = f"""
            {{
            {participatory_space["type"]}(id: {participatory_space["id"]}){{
                id
                scope {{
                    id
                    name {{
                        translation(locale:"pt-BR")
                    }}
                }}
            }}
        }}
        """
        response = self.graphql.run_graphql_query(graphql_query)
        try:
            participatory_space = response["data"][participatory_space["type"]]
            participatory_space["type_for_links"] = underscore(
                self._lower_first_letter(participatory_space["type"])
            ).split("_")[-1]
        except KeyError as error:
            logging.error(response)
            raise error
        return participatory_space

    def get_participatory_id_and_type(self):
        """
        Recupera o ID e tipo do espaço participativo associado ao componente.

        Returns
        -------
            dict[str, str]: O ID e tipo do espaço participativo.
        """
        graphql_query = f"""{{
                component(id: {self.component_id}) {{
                    participatorySpace {{
                        id
                        type
                }}
            }}
        }}
        """
        response = self.graphql.run_graphql_query(graphql_query)
        participatory_space: dict[str, str] = response["data"]["component"]["participatorySpace"]

        participatory_space["type"] = self._lower_first_letter(participatory_space["type"].split("::")[-1])

        return participatory_space

    def get_participatory_space(self) -> dict[str, str]:
        """
        Recupera informações sobre o espaço participativo associado ao componente.

        Returns
        -------
            dict[str, str]: Informações sobre o espaço participativo.
        """
        participatory_space = self.get_participatory_id_and_type()

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
        response = self.graphql.run_graphql_query(graphql_query)
        try:
            participatory_space = response["data"][participatory_space["type"]]
            participatory_space["type"] = self._lower_first_letter(
                participatory_space["type"].split("::")[-1]
            )
            participatory_space["type_for_links"] = underscore(participatory_space["type"]).split("_")[-1]
        except KeyError as error:
            logging.error(response)
            raise error
        return participatory_space

    def _format_comment(
        self, comment: dict, root_component_id, parent_comment_id: Optional[int] = None
    ) -> dict:
        """
        Formata um comentário em um dicionário.

        Args:
        ----
            comment (dict): O comentário a ser formatado.
            root_component_id: O ID do componente raiz.
            parent_comment_id (int): O ID do comentário pai.

        Returns:
        -------
            dict: O comentário formatado.
        """
        return {
            "root_component_id": root_component_id,
            "parent_comment_id": (parent_comment_id if parent_comment_id else comment["id"]),
            "body": comment["body"],
            "author_id": comment["author"]["id"],
            "author_name": comment["author"]["name"],
            "comment_id": comment["id"],
            "creation_date": comment["createdAt"],
            "update_date": comment["updatedAt"],
            "status": comment["status"],
        }

    def _build_comment_thread(self, parent_comment: dict[str], root_component_id: int, thread_level: int = 1):
        """
        Constrói uma thread de comentários recursivamente.

        Args:
        ----
            parent_comment (dict[str]): O comentário pai.
            root_component_id (int): O ID do componente raiz.
            thread_level (int): O nível da thread de comentários.
        """
        graphql_query = self.graphql.get_graphql_query_from_file(
            Path(__file__).parent.joinpath("../../gql/commentable/get_comments_by_commentable_id.gql")
        )
        query_params = {"id": str(parent_comment["id"])}
        result = self.graphql.run_graphql_query(graphql_query, variables=query_params)
        commentable = result["data"]["commentable"]
        if thread_level == 1:  # Nível raiz
            yield self._format_comment(parent_comment, root_component_id=root_component_id)
        for comment in commentable["comments"]:
            yield self._format_comment(
                comment,
                root_component_id=root_component_id,
                parent_comment_id=parent_comment["id"],
            )
            yield from self._build_comment_thread(
                comment,
                thread_level=thread_level + 1,
                root_component_id=root_component_id,
            )

    def get_comments_threads(self, root_comments: "list[dict[str]]", root_component_id):
        """
        Recupera threads de comentários para uma lista de comentários raiz.

        Args:
        ----
            root_comments (list[dict[str]]): Lista de comentários raiz.
            root_component_id (int): O ID do componente raiz.

        Yields:
        ------
            dict: Um comentário na thread.
        """
        for comment in root_comments:
            yield from self._build_comment_thread(comment, root_component_id=root_component_id)

    def get_comments_df(
        self,
        root_comments: "list[dict[str]]",
        root_component_id,
        start_date_filter: Optional[datetime] = None,
        end_date_filter: Optional[datetime] = None,
    ):

        comments = self.get_comments_threads(root_comments=root_comments, root_component_id=root_component_id)
        df = pd.DataFrame(comments)
        logging.info("Dataframe shape of comments: %s", df.shape)
        if df.empty:
            logging.warning(
                "Dataframe empty. %s",
                [root_comments, root_component_id, start_date_filter, end_date_filter],
            )
            return df

        parse_date = lambda date: pendulum.parse(str(date), strict=False)

        df["creation_date"] = df["creation_date"].apply(parse_date)
        df["update_date"] = df["update_date"].apply(parse_date)

        df["date_filter"] = df[["creation_date", "update_date"]].max(axis=1)
        df["date_filter"] = df["date_filter"].apply(parse_date)

        if start_date_filter:
            df = df[df["date_filter"] > parse_date(start_date_filter)]
        if end_date_filter:
            df = df[df["date_filter"] < parse_date(end_date_filter)]

        logging.info(df["date_filter"].max())
        link_base = self.get_component_link().rstrip("/")

        ids = np.char.array(df["root_component_id"].values, unicode=True)
        df = df.assign(link=(link_base + "/" + ids).astype(str))
        logging.info("New Dataframe shape of comments: %s", df.shape)

        return df

    def get_component(self, **kwargs) -> "dict[str, str]":
        """
        Recupera informações sobre o componente.

        Returns
        -------
            dict[str, str]: Informações sobre o componente.
        """
        raise NotImplementedError

    def get_comments(self, **kwargs):
        """
        Recupera comentários.

        Raises
        ------
            NotImplementedError: Este método deve ser implementado em classes derivadas.
        """
        raise NotImplementedError

        return self.get_comments_df()

    def component_json_to_dataframe(self, json_component, **kwargs):
        """
        Transforma dados do componente em formato JSON em um DataFrame do pandas.

        Raises
        ------
            NotImplementedError: Este método deve ser implementado em classes derivadas.
        """
        raise NotImplementedError
