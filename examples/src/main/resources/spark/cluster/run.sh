#!/usr/bin/env bash

docker exec -it spark-master rm -f /spark/trembita-spark.jar
docker exec -it spark-worker-1 rm -f /spark/trembita-spark.jar
docker cp examples/target/scala-2.12/trembita-spark.jar spark-master:/spark/
docker cp examples/target/scala-2.12/trembita-spark.jar spark-worker-1:/spark/
docker exec -it spark-master /spark/bin/spark-submit \
    --class com.examples.spark.Main \
    --master spark://spark-master:7077 \
    /spark/trembita-spark.jar

