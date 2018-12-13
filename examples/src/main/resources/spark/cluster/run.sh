#!/usr/bin/env bash

docker exec -it spark-master rm -f /spark/trembita-spark.jar
docker exec -it spark-worker-1 rm -f /spark/trembita-spark.jar
docker cp ${SPARK_APPLICATION_JAR_LOCATION} spark-master:/spark/
docker cp ${SPARK_APPLICATION_JAR_LOCATION} spark-worker-1:/spark/
docker exec -it spark-master /spark/bin/spark-submit \
    --class ${SPARK_APPLICATION_MAIN_CLASS} \
    --master spark://spark-master:7077 \
    /spark/trembita-spark.jar

