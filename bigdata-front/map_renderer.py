import os
import logging
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px

from dir_renderer import DirRenderer


class MapRenderer(DirRenderer):

    def _render(self, entry: os.DirEntry[str]) -> go.Figure:
        data = pd.read_csv(entry.path)
        df = pd.DataFrame(
            {
                "country": data["country"].values,
                "code": data["country"].values,
                "metric": data["metric"].values,
            }
        )

        file_name = entry.name.split(".")[0]
        _, field, year = file_name.split("-")

        fig = px.choropleth(
            df,
            locations="code",
            locationmode="ISO-3",
            color="metric",
            hover_name="country",
            color_continuous_scale="Blues",
            title=f"{self._FIELDS_PRETTY_NAMES[field]} ({year})",
            labels={"metric": self._FIELDS_PRETTY_NAMES[field]},
        )
        fig.update_layout(
            geo=dict(showframe=False, showcoastlines=True),
        )

        return fig
