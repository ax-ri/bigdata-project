import os
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px

from dir_renderer import DirRenderer


class AfMapRenderer(DirRenderer):

    def _render(self, entry: os.DirEntry[str]) -> go.Figure:
        data = pd.read_csv(entry.path)
        df = pd.DataFrame(
            {
                "country": data["country"].values,
                "code": data["country"].values,
                "criteria": data["criteria"].values,
            }
        )

        file_name = entry.name.split(".")[0]
        _, criteria, year = file_name.split("-")

        fig = px.choropleth(
            df,
            locations="code",
            locationmode="ISO-3",
            color="criteria",
            hover_name="country",
            title=f"{self._FIELDS_PRETTY_NAMES[criteria]} ({year})",
            labels={"criteria": self._FIELDS_PRETTY_NAMES[criteria]},
        )
        fig.update_layout(
            geo=dict(showframe=False, showcoastlines=True),
        )

        return fig
