from lappis.components.base_component.component import ComponentBaseHook
from lappis.components.proposals import ProposalsHook


class ComponentNotSupported(Exception):
    ...


class DecidimHook(object):
    def __new__(cls, conn_id: str, component_id: int):
        component_type = ComponentBaseHook(conn_id, component_id).get_component_type()

        if component_type == "Proposals":
            return ProposalsHook(conn_id, component_id)
        else:
            raise ComponentNotSupported(
                f"Component type {component_type} is not suported."
            )
