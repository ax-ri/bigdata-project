import os, sys, shutil
import logging
import pandas as pd
import plotly.express as px

fields_pretty_names = {
    "id": "Identifiant",
    "title": "Titre",
    "rank": "Rang",
    "date": "Date",
    "artist": "Artiste",
    "url": "URL",
    "region": "Pays",
    "chart": "Catégorie",
    "trend": "Tendance",
    "streams": "Nombre d'écoutes",
    "track_id": "Identifiant (piste)",
    "album": "Album",
    "popularity": "Popularité",
    "duration_ms": "Durée",
    "explicit": "Contenu explicite",
    "release_date": "Date de sortie",
    "available_markets": "Marchés disponibles",
    "af_danceability": "Dançabilité",
    "af_energy": "Énergie",
    "af_key": "Tonalité",
    "af_loudness": "Volume",
    "af_mode": "Mode (majeur / mineur)",
    "af_speechiness": "Parlé",
    "af_acousticness": "Acousticité",
    "af_instrumentalness": "Instrumental",
    "af_liveness": "Présence de public",
    "af_valence": "Positivité",
    "af_tempo": "Tempo",
    "af_time_signature": "Mesure",
}


def render_dir(input_path: str, output_path: str) -> None:
    logging.info(f"rendering {input_path}")
    for f in filter(lambda d: d.is_file(), os.scandir(input_path)):
        logging.info("----------------------------------------------------------------")
        logging.info("Processing %s", f.path)
        data = pd.read_csv(f.path)
        df = pd.DataFrame(
            {
                "country": data["country"].values,
                "code": data["country"].values,
                "metric": data["metric"].values,
            }
        )

        file_name = f.name.split(".")[0]
        field, year = file_name.split("-")
        fig = px.choropleth(
            df,
            locations="code",
            locationmode="ISO-3",
            color="metric",
            hover_name="country",
            color_continuous_scale="Blues",
            title=f"{fields_pretty_names[field]} ({year})",
            labels={"metric": fields_pretty_names[field]},
        )
        fig.update_layout(
            geo=dict(showframe=False, showcoastlines=True),
            margin=dict(l=0, r=0, t=50, b=0),
        )
        # fig.show()
        output_file = os.path.join(output_path, file_name + ".svg")
        fig.write_image(output_file)
        logging.info("Wrote %s", output_file)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("usage: map-renderer.py <input path> <output path> [--clean]")
        exit(1)
    input_path, output_path = sys.argv[1], sys.argv[2]
    if len(sys.argv) >= 4:
        try:
            shutil.rmtree(output_path)
            os.mkdir(output_path)
        except FileNotFoundError:
            logging.warning("Cannot delete output directory")
            pass
        except Exception as e:
            logging.error("Error with output directory: %s", e)
    logging.basicConfig(level=logging.INFO)
    render_dir(input_path, output_path)
