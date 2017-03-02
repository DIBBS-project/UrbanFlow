#Filter original tweets
IN="${PREFIX}_tweets"
OUT="${PREFIX}_${CITYNAME}_tweets"

hadoop dfs -rmr "/user/${USER}/${OUT}"
hadoop jar $ROOT_DIR/jars/filter.jar "/user/${USER}/${IN}" "/user/${USER}/${OUT}" ${minX} ${maxX} ${minY} ${maxY}
echo "Done Filtering Tweets!"
