/usr/bin/spark-submit \
--master yarn \
--deploy-mode client \
--driver-cores 2 \
--driver-memory 2G \
--num-executors 300 \
--executor-cores 2 \
--executor-memory 4G \
--queue root.fdp_batch \
--name secrank \
--conf "spark.dynamicAllocation.enabled=false" \
--class com.secrank.examples.TopDomainDailyRelease \
toplist.jar
