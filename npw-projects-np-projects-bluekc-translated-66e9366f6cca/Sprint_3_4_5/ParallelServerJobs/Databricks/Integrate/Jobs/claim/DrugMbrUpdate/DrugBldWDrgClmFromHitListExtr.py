# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Developer                         Date                 Project/Altiris #      		Change Description                                                                 Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------                  --------------------     	   ------------------------      		-----------------------------------------------------------------------                         --------------------------------       -------------------------------   ----------------------------       
# MAGIC Karthik Chintalaphani     2011-12-21             TTR-1164                                          Originally Programmed				IntegrateCurDevl             Bhoomi Dasari           12/29/2013

# MAGIC Find Claims on HitList File that meet criteria and load key fields to the W_DRUG_CLM load file.
# MAGIC This file is an append to the file created in DrugBldWDrgClmExtr. This file will be loaded into W_DRUG_CLM for further processing.
# MAGIC DrugBldWDrgClmFromHitListExtr
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
Source = get_widget_value('Source','ESI')

schema_DrugClmHitList = StructType([
    StructField("CLM_ID", StringType(), True),
    StructField("TRGT_CD", StringType(), True)
])

df_DrugClmHitList = (
    spark.read
    .option("header", False)
    .option("inferSchema", False)
    .option("sep", ",")
    .schema(schema_DrugClmHitList)
    .csv(f"{adls_path}/update/DrugClmMbrUpdtHitList.dat")
)

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

execute_dml("DROP TABLE IF EXISTS STAGING.DrugBldWDrgClmFromHitListExtr_IDSClaim_temp", jdbc_url_ids, jdbc_props_ids)

df_DrugClmHitList.write \
    .mode("append") \
    .jdbc(jdbc_url_ids, "STAGING.DrugBldWDrgClmFromHitListExtr_IDSClaim_temp", properties=jdbc_props_ids)

extract_query = f"""
SELECT
  CLM.CLM_ID,
  CD_MPPNG.TRGT_CD,
  DRUG.DRUG_CLM_SK,
  CLM.SRC_SYS_CD_SK,
  CLM.CLM_SK,
  DRUG.RX_NO,
  DRUG.FILL_DT_SK,
  0 AS PROV_SK,
  'NA' AS VNDR_CLM_NO
FROM {IDSOwner}.CLM CLM
JOIN {IDSOwner}.DRUG_CLM DRUG
  ON CLM.CLM_ID=DRUG.CLM_ID
  AND CLM.SRC_SYS_CD_SK=DRUG.SRC_SYS_CD_SK
JOIN {IDSOwner}.CD_MPPNG CD_MPPNG
  ON CLM.SRC_SYS_CD_SK=CD_MPPNG.CD_MPPNG_SK
JOIN STAGING.DrugBldWDrgClmFromHitListExtr_IDSClaim_temp T
  ON T.CLM_ID=CLM.CLM_ID
  AND T.TRGT_CD=CD_MPPNG.TRGT_CD
"""

df_IDSClaim = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query)
    .load()
)

df_enriched = (
    df_DrugClmHitList.alias("Hitlist")
    .join(
        df_IDSClaim.alias("Extract"),
        [
            trim(F.col("Hitlist.CLM_ID")) == F.col("Extract.CLM_ID"),
            trim(F.col("Hitlist.TRGT_CD")) == F.col("Extract.TRGT_CD")
        ],
        "left"
    )
)

df_enriched = df_enriched.filter(F.col("Extract.CLM_ID").isNotNull())

df_enriched = df_enriched.select(
    F.col("Extract.DRUG_CLM_SK").alias("DRUG_CLM_SK"),
    F.col("Extract.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("Extract.CLM_SK").alias("CLM_SK"),
    F.col("Extract.TRGT_CD").alias("SRC_SYS_CD"),
    F.col("Extract.RX_NO").alias("RX_NO"),
    F.col("Extract.FILL_DT_SK").alias("FILL_DT_SK"),
    F.col("Extract.PROV_SK").alias("PROV_SK"),
    F.col("Hitlist.CLM_ID").alias("CLM_ID"),
    F.col("Extract.VNDR_CLM_NO").alias("VNDR_CLM_NO")
)

df_enriched = df_enriched.withColumn("FILL_DT_SK", F.rpad("FILL_DT_SK", 10, " "))
df_enriched = df_enriched.withColumn("CLM_ID", F.rpad("CLM_ID", <...>, " "))
df_enriched = df_enriched.withColumn("SRC_SYS_CD", F.rpad("SRC_SYS_CD", <...>, " "))

write_files(
    df_enriched,
    f"{adls_path}/load/W_DRUG_CLM.dat",
    ",",
    "append",
    False,
    False,
    "\"",
    None
)