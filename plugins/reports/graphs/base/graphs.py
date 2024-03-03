import base64
from contextlib import closing
from io import BytesIO

import plotly.io as pio


class ReportGraphs:
    def b64_encode_graph(self, graph, format: str = "svg"):
        with closing(BytesIO()) as buffer:
            pio.write_image(graph, buffer, format=format)
            buffer.seek(0)
            graph = base64.b64encode(buffer.getvalue()).decode("utf-8")
        return graph
