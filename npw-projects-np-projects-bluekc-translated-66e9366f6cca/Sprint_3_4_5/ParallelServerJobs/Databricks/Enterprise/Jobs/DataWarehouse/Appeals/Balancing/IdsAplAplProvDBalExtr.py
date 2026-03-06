# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 11/28/07 12:47:07 Batch  14577_46088 PROMOTE bckcetl edw10 dsadm bls for hs
# MAGIC ^1_1 11/28/07 11:24:20 Batch  14577_41063 INIT bckcett testEDW10 dsadm bls for hs
# MAGIC ^1_1 11/27/07 15:06:26 Batch  14576_54393 PROMOTE bckcett testEDW10 u03651 steph for Hugh
# MAGIC ^1_1 11/27/07 14:52:16 Batch  14576_53540 INIT bckcett devlEDW10 u03651 steffy
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:    IdsEdwMedMgtBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This pulls data from the Concatenated Source Sequential file coming from IDS and loads into the B table in EDW
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                  Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------            ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada              07/12/2007         3264                              Originally Programmed                            devlEDW10          
# MAGIC 
# MAGIC Parikshith Chada              08/29/2007         3264                            Changed the Snapshot file Extract to       devlEDW10                     Steph Goddard            10/18/2007
# MAGIC                                                                                                          Snapshot table extract


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

schema_IdsAplProv = StructType([
    StructField("SRC_SYS_CD_SK", IntegerType(), nullable=False),
    StructField("APL_ID", StringType(), nullable=False),
    StructField("SEQ_NO", IntegerType(), nullable=False),
    StructField("PROV_SK", IntegerType(), nullable=False)
])

df_IdsAplProv = (
    spark.read.format("csv")
    .schema(schema_IdsAplProv)
    .option("header", "false")
    .option("quote", "\"")
    .option("delimiter", ",")
    .load(f"{adls_path}/balancing/snapshot/IDS_APL_PROV.uniq")
)

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query_ProvLkup = f"SELECT PROV.PROV_SK as PROV_SK, PROV.PROV_ID as PROV_ID FROM {IDSOwner}.PROV PROV"
df_ProvLkup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_ProvLkup)
    .load()
)

df_hf_aplbal_provid_lkup = dedup_sort(
    df_ProvLkup,
    ["PROV_SK"],
    []
)

df_hf_cdma_codes = spark.read.parquet(
    f"{adls_path}/hf_cdma_codes.parquet"
)

df_enriched = (
    df_IdsAplProv.alias("Snapshot")
    .join(
        df_hf_cdma_codes.alias("SrcSysCdLkup"),
        (F.col("Snapshot.SRC_SYS_CD_SK") == F.col("SrcSysCdLkup.CD_MPPNG_SK")),
        "left"
    )
    .join(
        df_hf_aplbal_provid_lkup.alias("ProvIdlkup"),
        (F.col("Snapshot.PROV_SK") == F.col("ProvIdlkup.PROV_SK")),
        "left"
    )
)

df_enriched = df_enriched.withColumn(
    "SRC_SYS_CD",
    F.when(
        F.isnull(F.col("SrcSysCdLkup.TRGT_CD")) | (F.length(trim(F.col("SrcSysCdLkup.TRGT_CD"))) == 0),
        F.lit("NA")
    ).otherwise(F.col("SrcSysCdLkup.TRGT_CD"))
).withColumn(
    "APL_ID",
    F.col("Snapshot.APL_ID")
).withColumn(
    "APL_PROV_SEQ_NO",
    F.col("Snapshot.SEQ_NO")
).withColumn(
    "PROV_ID",
    F.when(
        F.isnull(F.col("ProvIdlkup.PROV_ID")) | (F.length(trim(F.col("ProvIdlkup.PROV_ID"))) == 0),
        F.lit("NA")
    ).otherwise(F.col("ProvIdlkup.PROV_ID"))
)

df_enriched = df_enriched.select(
    "SRC_SYS_CD",
    "APL_ID",
    "APL_PROV_SEQ_NO",
    "PROV_ID"
)

df_enriched = df_enriched.withColumn(
    "SRC_SYS_CD",
    F.rpad(F.col("SRC_SYS_CD"), <...>, " ")
).withColumn(
    "APL_ID",
    F.rpad(F.col("APL_ID"), <...>, " ")
).withColumn(
    "PROV_ID",
    F.rpad(F.col("PROV_ID"), <...>, " ")
)

df_enriched = df_enriched.select(
    "SRC_SYS_CD",
    "APL_ID",
    "APL_PROV_SEQ_NO",
    "PROV_ID"
)

write_files(
    df_enriched,
    f"{adls_path}/load/B_APL_PROV_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)