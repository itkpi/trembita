#!/usr/bin/env bash

docker exec -it spark-master rm -f /spark/trembita-spark.jar
docker exec -it cluster_spark-worker_1 rm -f /spark/trembita-spark.jar
docker exec -it cluster_spark-worker_2 rm -f /spark/trembita-spark.jar
docker exec -it cluster_spark-worker_3 rm -f /spark/trembita-spark.jar
docker cp examples/target/scala-2.12/trembita-spark.jar spark-master:/spark/
docker cp examples/target/scala-2.12/trembita-spark.jar cluster_spark-worker_1:/spark/
docker cp examples/target/scala-2.12/trembita-spark.jar cluster_spark-worker_2:/spark/
docker cp examples/target/scala-2.12/trembita-spark.jar cluster_spark-worker_3:/spark/
docker exec -it spark-master /spark/bin/spark-submit \
    --class com.examples.spark.FSMSample \
    --master spark://spark-master:7077 \
    /spark/trembita-spark.jar

