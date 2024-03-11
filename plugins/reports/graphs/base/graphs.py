import base64
from contextlib import closing
from io import BytesIO

import plotly.io as pio


class ReportGraphs:
    """
    Provides utility methods for handling and encoding Plotly graphs.

    Methods:
    -------
        b64_encode_graph(graph, format='svg'):
            Encodes a Plotly graph in base64 format.

    Args:
    ----
        graph: The Plotly graph object to be encoded.
        format (str): The format in which the graph should be encoded (default is 'svg').

    Returns:
    -------
        str: The base64-encoded representation of the graph.
    """

    def b64_encode_graph(self, graph, format: str = "svg"):
        """
        Encodes a Plotly graph in base64 format.

        Args:
        ----
            graph: The Plotly graph object to be encoded.
            format (str): The format in which the graph should be encoded (default is 'svg').

        Returns:
        -------
            str: The base64-encoded representation of the graph.
        """
        with closing(BytesIO()) as buffer:
            pio.write_image(graph, buffer, format=format)
            buffer.seek(0)
            graph = base64.b64encode(buffer.getvalue()).decode("utf-8")
        return graph

    @staticmethod
    def try_build_graph(graph_func, *args, **kwargs):
        try:
            return graph_func(*args, **kwargs)
        except KeyError:
            return None
