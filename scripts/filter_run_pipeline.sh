#Filter original tweets
IN="${PREFIX}_tweets"
OUT="${PREFIX}_${CITYNAME}_tweets"

hadoop dfs -rmr $OUT
hadoop jar $ROOT_DIR/jars/filter.jar $IN $OUT $minX $maxX $minY $maxY
echo "Done Filtering Tweets!"


