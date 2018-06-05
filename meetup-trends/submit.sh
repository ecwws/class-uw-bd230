spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0\
  --driver-java-options="-XX:+UseG1GC -XX:MaxGCPauseMillis=20 \
  -XX:InitiatingHeapOccupancyPercent=35 -XX:+ExplicitGCInvokesConcurrent" \
  --driver-memory=6g --executor-memory=16g \
  --conf spark.executor.heartbeatInterval=1200s \
  --conf spark.network.timeout=1500s \
  --num-executors 1 \
  --executor-cores 4 \
  target/scala-2.11/meetup-trends_2.11-1.0.jar
