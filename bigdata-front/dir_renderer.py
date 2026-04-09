import os
import logging

from abc import ABC, abstractmethod

import pandas as pd
import plotly.graph_objects as go


class DirRenderer(ABC):

    _FIELDS_PRETTY_NAMES = {
        "id": "Identifier",
        "title": "Title",
        "rank": "Rank",
        "date": "Date",
        "artist": "Artist",
        "url": "URL",
        "region": "Region",
        "chart": "Category",
        "trend": "Trend",
        "streams": "Listening count",
        "track_id": "Track identifier",
        "album": "Album",
        "popularity": "Popularity",
        "duration_ms": "Duration",
        "explicit": "Explicit content",
        "release_date": "Release date",
        "available_markets": "Available markets",
        "af_danceability": "Dancability",
        "af_energy": "Energy",
        "af_key": "Key",
        "af_loudness": "Loudness",
        "af_mode": "Mode (major / minor)",
        "af_speechiness": "Speechiness",
        "af_acousticness": "Acousticness",
        "af_instrumentalness": "Instrumentalness",
        "af_liveness": "Live audience",
        "af_valence": "Valence",
        "af_tempo": "Tempo",
        "af_time_signature": "Time signature",
    }

    _FIELDS_VALUE_RANGES = {
        "af_danceability": [0, 1],
        "af_energy": [0, 1],
        "af_key": [0, 11],
        "af_loudness": [-60, 3],
        "af_mode": [0, 1],
        "af_speechiness": [0, 1],
        "af_acousticness": [0, 1],
        "af_instrumentalness": [0, 1],
        "af_liveness": [0, 1],
        "af_valence": [0, 1],
        "af_tempo": [0, 240],
        "af_time_signature": [0, 5],
    }

    def __init__(self, output_path: str):
        self._output_path = output_path

    def render_dir(self, input_path: str) -> None:
        logging.info(f"rendering {input_path}")
        for entry in filter(lambda d: d.is_file(), os.scandir(input_path)):
            logging.info(
                "----------------------------------------------------------------"
            )
            logging.info("Processing %s", entry.path)
            fig = self._render(entry)
            fig.update_layout(
                margin=dict(l=0, r=0, t=50, b=0),
            )
            output_filepath = os.path.join(
                self._output_path, os.path.basename(entry.path).split(".")[0] + ".png"
            )
            fig.write_image(output_filepath, scale=3)
            logging.info("Wrote %s", output_filepath)

    @abstractmethod
    def _render(self, entry: os.DirEntry[str]) -> go.Figure:
        raise NotImplementedError
