PREFIX="sample"
CURRENT_DIR=$(pwd)
SCRIPT_DIR="$(dirname "$0")"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

minX=${minX:-"-88.707599"}
maxX=${maxX:-"-87.524535"}
minY=${minY:-"41.201577"}
maxY=${maxY:-"42.495775"}
CITYNAME="chicago"

source ${SCRIPT_DIR}/filter_run_pipeline.sh

SHAPEFILEADDRESS1="${ROOT_DIR}/data/chicago/chicago_landuse_wgs84.shp"
attributes="Reduced"
source ${SCRIPT_DIR}/point_in_polygon1.sh

#Other parts of the scripts require a mongodb instance and will be added later
