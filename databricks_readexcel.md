## Read excel con Databricks

```py
# Databricks notebook source
vContainerName="sgicoolstgspichdlg201"
vStorageAccountName="sgicoolstgspich01"
vKey=dbutils.secrets.get(scope="sgiscopecoolstoragekey1",key="sgicoolstoragekey1")
vUrl="wasbs://" + vContainerName + "@" + vStorageAccountName + ".blob.core.windows.net/"
vConfig="fs.azure.account.key." + vStorageAccountName + ".blob.core.windows.net"

dbutils.fs.mount(
    vUrl, "/mnt/myblob2",
  extra_configs = {
    vConfig: vKey
  }
)

# COMMAND ----------

vPath="/mnt/myblob2/raw/externo/excel/reaseguros/borderaux_de_primas/Borderaux_de_primas_contabilizado_202212.xlsx"
df = spark.read.format("com.crealytics.spark.excel")\
                .option("dataAddress", "'DATA'!")\
                .option("header", "true")\
                .option("treatEmptyValuesAsNulls", "true")\
                .option("inferSchema", "true")\
                .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")\
                .option("treatEmptyValueAsNulls","true")\
                .option("maxRowsInMemory",20)\
                .load(vPath)

# COMMAND ----------

df.write.mode("overwrite").format("parquet").save("/mnt/myblob2/stage/borderaux_de_primas_contabilizado_202212")

# COMMAND ----------

dbutils.fs.unmount("/mnt/myblob2")

``` 
