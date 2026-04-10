import os, sys, shutil
import logging

from af_map_renderer import AfMapRenderer
from af_plot_renderer import AfPlotRenderer
from cluster_map_renderer import ClusterMapRenderer
from success_plot_renderer import SuccessPlotRenderer

logging.basicConfig(level=logging.INFO)
argc = len(sys.argv)

if argc < 2:
    logging.error("usage: main.py <input path> <output path> [--clean]")
    exit(1)
do_clean = argc >= 4 and sys.argv[3] == "--clean"

input_path, output_path = sys.argv[1], sys.argv[2]
paths = {}
for dir_name in "af_map", "af_plot", "cluster_map", "success_plot":
    paths[dir_name] = (
        os.path.join(input_path, dir_name),
        os.path.join(output_path, dir_name),
    )

if do_clean:
    try:
        logging.info("Cleaning old output dir")
        shutil.rmtree(output_path)
        os.mkdir(output_path)
        for _, op in paths.values():
            os.mkdir(op)
        logging.info("Output dir cleaned")
    except FileNotFoundError:
        logging.warning("Cannot delete output directory")
        pass
    except Exception as e:
        logging.error("Error with output directory: %s", e)

AfMapRenderer(paths["af_map"][1]).render_dir(paths["af_map"][0])
AfPlotRenderer(paths["af_plot"][1]).render_dir(paths["af_plot"][0])
ClusterMapRenderer(paths["cluster_map"][1]).render_dir(paths["cluster_map"][0])
SuccessPlotRenderer(paths["success_plot"][1]).render_dir(paths["success_plot"][0])
