import os
import numpy as np
import pandas as pd

import plotly.graph_objects as go
import plotly.express as px

from dir_renderer import DirRenderer


class ClusterMapRenderer(DirRenderer):

    def _render(self, entry: os.DirEntry[str]) -> go.Figure:
        np.random.seed(0)

        data = pd.read_csv(entry.path)
        df = pd.DataFrame(
            {
                "country": data["country"].values,
                "code": data["country"].values,
                "prediction": data["prediction"].astype(str).values,
            }
        )

        # generate random colors for each value
        color_map = {}
        for v in set(df["prediction"]):
            color_map[v] = (
                f"rgb({','.join(map(str, np.random.choice(range(256), size=3)))})"
            )

        file_name = entry.name.split(".")[0]
        _, year = file_name.split("-")

        fig = px.choropleth(
            df,
            locations="code",
            locationmode="ISO-3",
            color="prediction",
            hover_name="country",
            title=f"Country clusters ({year})",
            color_discrete_map=color_map,
        )
        fig.update_layout(
            geo=dict(showframe=False, showcoastlines=True), showlegend=False
        )

        return fig
