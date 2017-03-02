PREFIX="sample"
CURRENT_DIR=$(pwd)
SCRIPT_DIR="$(dirname "$0")"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

#Copy data to HDFS
hadoop fs -rmr "${PREFIX}_tweets"
hadoop fs -put "${ROOT_DIR}/data/tweets" "/user/${USER}/${PREFIX}_tweets"
