"""
Classe base fornecendo funções fundamentais para trabalhar com componentes Decidim.

Métodos:
    __init__(self, conn_id: str, component_id: int): Inicializa uma nova instância de ComponentBaseHook.
    get_component_link(self) -> str: Recupera o link para o componente.
    get_component_type(self) -> str: Recupera o tipo do componente Decidim por meio de uma consulta GraphQL.
    get_participatory_id_and_type(self): Recupera o ID e tipo do espaço participativo associado ao componente Decidim por meio de uma consulta GraphQL.
    get_participatory_space(self) -> dict[str, str]: Recupera informações sobre o espaço participativo associado ao componente Decidim.
    _parse_comment(self, comment: dict, root_component_id, parent_comment_id: int = None) -> dict: Formata um comentário em um dicionário.
    _build_comment_thread(self, parent_comment: dict[str], root_component_id: int, thread_level: int = 1): Constrói uma thread de comentários recursivamente.
    get_comments_threads(self, root_comments: list[dict[str]], root_component_id): Recupera threads de comentários para uma lista de comentários raiz.
    get_component(self, **kwargs) -> dict[str, str]: Recupera informações sobre o componente. (Método abstrato)
    get_comments(self, **kwargs): Recupera comentários. (Método abstrato)
    component_json_to_dataframe(self, json_component, **kwargs): Transforma dados do componente em formato JSON em um DataFrame do pandas. (Método abstrato)
"""

from pathlib import Path
from urllib.parse import urljoin

import inflect
from inflection import underscore

from plugins.graphql.hooks.graphql import GraphQLHook

class ComponentBaseHook():

    def __init__(self, conn_id: str, component_id: int):
        """
        Inicializa uma nova instância de ComponentBaseHook.

        Args:
            conn_id (str): O ID de conexão para o Airflow.
            component_id (int): O ID do componente para o qual o hook está sendo criado.
        """
        self.graphql = GraphQLHook(conn_id)
        self.component_id = component_id
        self.component_type: str = self.get_component_type()

    def get_component_link(self) -> str:
        """
        Recupera o link para o componente.

        Returns:
            str: O link para o componente.
        """
        participatory_space = self.get_participatory_space()
        inflect_engine = inflect.engine()
        link_base = urljoin(
            self.api_url,
            f"{inflect_engine.plural(participatory_space['type_for_links'])}/{participatory_space['slug']}/f/{self.component_id}/{self.component_type.lower()}",
        )
        del inflect_engine
        return link_base

    def get_component_type(self) -> str:
        """
        Recupera o tipo do componente.

        Returns:
            str: O tipo do componente.
        """
        graphql_query = f"""
                    {{
                        component(id: {self.component_id}) {{
                            __typename
                        }}
                    }}
                    """
        response = self.graphql.run_graphql_query(graphql_query=graphql_query)
        assert response["data"]["component"] is not None, response
        return response["data"]["component"]["__typename"]

    def get_participatory_escope(self):
        """
        Retrieves the ID and type of the participatory space with scope.

        Returns:
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
        response = self.run_graphql_query(graphql_query)
        try:
            participatory_space = response["data"][participatory_space["type"]]
            participatory_space["type_for_links"] = underscore(type_of_space).split("_")[-1]
        except KeyError as error:
            logging.error(response)
            raise error
        return participatory_space

    def get_participatory_id_and_type(self):
        """
        Recupera o ID e tipo do espaço participativo associado ao componente.

        Returns:
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
        participatory_space: dict[str, str] = response["data"]["component"][
            "participatorySpace"
        ]

        lower_first_letter = lambda s: s[:1].lower() + s[1:] if s else ""
        type_of_space = participatory_space["type"] = lower_first_letter(
            participatory_space["type"].split("::")[-1]
        )

        return participatory_space

    def get_participatory_space(self) -> dict[str, str]:
        """
        Recupera informações sobre o espaço participativo associado ao componente.

        Returns:
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
        response = self.run_graphql_query(graphql_query)
        try:
            participatory_space = response["data"][participatory_space["type"]]
            lower_first_letter = lambda s: s[:1].lower() + s[1:] if s else ""
            type_of_space = participatory_space["type"] = lower_first_letter(
                participatory_space["type"].split("::")[-1]
            )
            participatory_space["type_for_links"] = underscore(type_of_space).split("_")[-1]
        except KeyError as error:
            print(response)
            raise error
        return participatory_space
                
    def _format_comment(
        self, comment: dict, root_component_id, parent_comment_id: int = None
    ) -> dict:
        """
        Formata um comentário em um dicionário.

        Args:
            comment (dict): O comentário a ser formatado.
            root_component_id: O ID do componente raiz.
            parent_comment_id (int): O ID do comentário pai.

        Returns:
            dict: O comentário formatado.
        """
        return {
            "root_component_id": root_component_id,
            "parent_comment_id": parent_comment_id
            if parent_comment_id
            else comment["id"],
            "body": comment["body"],
            "author_id": comment["author"]["id"],
            "author_name": comment["author"]["name"],
            "comment_id": comment["id"],
            "creation_date": comment["createdAt"],
        }

    def _build_comment_thread(
        self, parent_comment: dict[str], root_component_id: int, thread_level: int = 1
    ):
        """
        Constrói uma thread de comentários recursivamente.

        Args:
            parent_comment (dict[str]): O comentário pai.
            root_component_id (int): O ID do componente raiz.
            thread_level (int): O nível da thread de comentários.
        """
        graphql_query = self.get_graphql_query_from_file(
            Path(__file__).parent.joinpath(
                "../../gql/commentable/get_comments_by_commentable_id.gql"
            )
        )
        query_params = {"id": str(parent_comment["id"])}
        result = self.graphql.run_graphql_query(graphql_query, variables=query_params)
        commentable = result["data"]["commentable"]
        if thread_level == 1:  # Nível raiz
            yield self._parse_comment(
                parent_comment, root_component_id=root_component_id
            )
        for comment in commentable["comments"]:
            yield self._parse_comment(
                comment,
                root_component_id=root_component_id,
                parent_comment_id=parent_comment["id"],
            )
            yield from self._build_comment_thread(
                comment,
                thread_level=thread_level + 1,
                root_component_id=root_component_id,
            )

    def get_comments_threads(self, root_comments: list[dict[str]], root_component_id):
        """
        Recupera threads de comentários para uma lista de comentários raiz.

        Args:
            root_comments (list[dict[str]]): Lista de comentários raiz.
            root_component_id (int): O ID do componente raiz.

        Yields:
            dict: Um comentário na thread.
        """
        for comment in root_comments:
            yield from self._build_comment_thread(
                comment, root_component_id=root_component_id
            )

    def get_component(self, **kwargs) -> dict[str, str]:
        """
        Recupera informações sobre o componente.

        Returns:
            dict[str, str]: Informações sobre o componente.
        """
        raise NotImplementedError

    def get_comments(self, **kwargs):
        """
        Recupera comentários.

        Raises:
            NotImplementedError: Este método deve ser implementado em classes derivadas.
        """
        raise NotImplementedError

    def component_json_to_dataframe(
        self, json_component, **kwargs
    ):
        """
        Transforma dados do componente em formato JSON em um DataFrame do pandas.

        Raises:
            NotImplementedError: Este método deve ser implementado em classes derivadas.
        """
        raise NotImplementedError
