import os
import pandas as pd

import plotly.graph_objects as go
import plotly.express as px

from dir_renderer import DirRenderer


class SuccessPlotRenderer(DirRenderer):

    def _render(self, entry: os.DirEntry[str]) -> go.Figure:
        data = pd.read_csv(entry.path)

        file_name = entry.name.split(".")[0]
        _, criteria = file_name.split("-")

        fig = px.line(
            data,
            x=criteria,
            y="streams",
            title=f"{self._FIELDS_PRETTY_NAMES[criteria]} success",
            labels={
                criteria: self._FIELDS_PRETTY_NAMES[criteria],
                "streams": self._FIELDS_PRETTY_NAMES["streams"],
            },
        )

        return fig
