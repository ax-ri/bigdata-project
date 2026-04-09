import os, sys, shutil
import logging

from map_renderer import MapRenderer
from plot_renderer import PlotRenderer

logging.basicConfig(level=logging.INFO)
argc = len(sys.argv)

if argc < 2:
    logging.error("usage: main.py <input path> <output path> [--clean]")
    exit(1)
do_clean = argc >= 4 and sys.argv[3] == "--clean"

input_path, output_path = sys.argv[1], sys.argv[2]
map_input_path, map_output_path = os.path.join(input_path, "map"), os.path.join(
    output_path, "map"
)
plot_input_path, plot_output_path = os.path.join(input_path, "plot"), os.path.join(
    output_path, "plot"
)

if do_clean:
    try:
        logging.info("Cleaning old output dir")
        shutil.rmtree(output_path)
        os.mkdir(output_path)
        os.mkdir(map_output_path)
        os.mkdir(plot_output_path)
        logging.info("Output dir cleaned")
    except FileNotFoundError:
        logging.warning("Cannot delete output directory")
        pass
    except Exception as e:
        logging.error("Error with output directory: %s", e)

MapRenderer(map_output_path).render_dir(map_input_path)
PlotRenderer(plot_output_path).render_dir(plot_input_path)
