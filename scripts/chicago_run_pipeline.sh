PREFIX="sample"
CURRENT_DIR=$(pwd)
ROOT_DIR="$(dirname "$CURRENT_DIR")"

#Copy data to HDFS
hadoop fs -rmr "${PREFIX}_tweets"
hadoop fs -put "${ROOT_DIR}/data/tweets" "${PREFIX}_tweets"

minX=-88.707599
maxX=-87.524535
minY=41.201577
maxY=42.495775
CITYNAME="chicago"

source filter_run_pipeline.sh

SHAPEFILEADDRESS1="$ROOT_DIR/data/chicago/chicago_landuse_wgs84.shp"
attributes="Reduced"
source point_in_polygon1.sh

#Other parts of the scripts require a mongodb instance and will be added later