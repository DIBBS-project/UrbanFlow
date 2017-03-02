NUMPARTITIONS=16
NUMREDUCERS=16
XTOLERANCE=0.001
YTOLERANCE=0.001
partitionAddress="partitions-${CITYNAME}"

#Split shapefile
rm -r $partitionAddress
mkdir $partitionAddress
$JAVA_HOME/bin/java -jar "$ROOT_DIR/jars/split.jar" $minY $maxY $minX $maxX $NUMPARTITIONS $NUMREDUCERS $SHAPEFILEADDRESS1 $XTOLERANCE $YTOLERANCE $partitionAddress
hadoop dfs -rmr "${PREFIX}_${partitionAddress}"
hadoop dfs -copyFromLocal $partitionAddress "${PREFIX}_${partitionAddress}"
echo "point in polygon1 finished"


# Tag tweets with landuse information
IN="${PREFIX}_${CITYNAME}_tweets"
OUT="${PREFIX}_${CITYNAME}_tweets_2"
hadoop dfs -rmr $OUT
hadoop jar $ROOT_DIR/jars/aggregate.jar $IN $OUT $NUMREDUCERS $YTOLERANCE $XTOLERANCE $attributes "${PREFIX}_${partitionAddress}"
echo "point in polygon finished"
