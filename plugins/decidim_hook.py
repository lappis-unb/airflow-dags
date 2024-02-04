from plugins.components.base_component.component import ComponentBaseHook
from plugins.components.proposals import ProposalsHook


class ComponentNotSupportedError(Exception):
    """Exceção levantada quando um componente não é suportado pela aplicação.

    Atributos:
        mensagem (str): Descrição opcional da exceção.
    """

    pass


class DecidimHook:
    """Classe responsável por criar instâncias dinâmicas de hooks com base no tipo de componente do Decidim.

    Args:
    ----
        conn_id (str): ID de conexão a ser utilizado pelos ganchos criados.
        component_id (int): ID do componente para o qual o gancho será criado.

    Returns:
    -------
        Um gancho (hook) específico para o tipo de componente, como ProposalsHook.

    Raises:
    ------
        ComponentNotSupportedError: Se o tipo de componente não for suportado pelo sistema.

    Usage:
        Utilize esta classe para criar instâncias de ganchos (hooks) para diferentes tipos
        de componentes no sistema Decidim. Por exemplo:

        >> hook_instance = DecidimHook(conn_id='sua_conexao', component_id=123)
    """

    def __new__(cls, conn_id: str, component_id: int):
        component_type = ComponentBaseHook(conn_id, component_id).get_component_type()

        if component_type == "Proposals":
            return ProposalsHook(conn_id, component_id)
        else:
            raise ComponentNotSupportedError(f"Component type {component_type} is not suported.")
