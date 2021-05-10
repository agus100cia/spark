export PYTHONIOENCODING=UTF-8
export LC_ALL="en_US.UTF-8"

spark-submit \
--master yarn \
--num-executors 20 \
--executor-memory 50G \
--executor-cores 20 \
--driver-memory 50G \
--conf spark.driver.memoryOverhead=512 \
--conf spark.executor.memoryOverhead=512 \
etl_py_prl_otc_t_salients_competencia_td.py --ngrams 20200101



pyspark --master yarn \
--deploy-mode cluster \
--num-executors 20 \
--executor-memory 1G \
--executor-cores 2 \
--driver-memory 1G \
--conf spark.driver.memoryOverhead=512 \
--conf spark.executor.memoryOverhead=512



pyspark --master yarn \
--num-executors 20 \
--executor-memory 1G \
--executor-cores 2 \
--driver-memory 1G \
--conf spark.driver.memoryOverhead=512 \
--conf spark.executor.memoryOverhead=512




spark-submit \
--master yarn \
--num-executors 8 \
--executor-memory 16G \
--executor-cores 4 \
--driver-memory 16G \
--conf spark.driver.memoryOverhead=512 \
--conf spark.executor.memoryOverhead=512 \
cabecera.py --ngrams 20200102


spark-submit \
--master yarn \
cabecera.py --ngrams 20200102
