import os
import pandas as pd

import plotly.graph_objects as go
import plotly.express as px

from dir_renderer import DirRenderer


class PlotRenderer(DirRenderer):

    def _render(self, entry: os.DirEntry[str]) -> go.Figure:
        data = pd.read_csv(entry.path)

        file_name = entry.name.split(".")[0]
        _, criteria = file_name.split("-")

        data["ci_lower"] = (data["mean"] - data["ci_lower"]).abs()
        data["ci_upper"] = (data["mean"] - data["ci_upper"]).abs()

        fig = px.bar(
            data,
            x="year",
            y="mean",
            error_y_minus="ci_lower",
            error_y="ci_upper",
            title=f"{self._FIELDS_PRETTY_NAMES[criteria]}",
        )
        fig.update_yaxes(range=self._FIELDS_VALUE_RANGES[criteria])

        return fig
