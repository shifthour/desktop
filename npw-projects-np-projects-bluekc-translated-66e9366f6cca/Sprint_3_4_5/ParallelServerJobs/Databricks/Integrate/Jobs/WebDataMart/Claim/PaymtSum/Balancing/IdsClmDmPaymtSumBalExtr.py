# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 01/29/08 14:29:13 Batch  14639_52158 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 01/09/08 13:32:13 Batch  14619_48738 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/24/07 09:35:50 Batch  14573_34558 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_2 11/01/07 15:16:03 Batch  14550_54982 PROMOTE bckcetl ids20 dsadm rc for brent
# MAGIC ^1_2 11/01/07 15:05:36 Batch  14550_54348 INIT bckcett testIDS30 dsadm rc for brent 
# MAGIC ^1_2 10/31/07 10:30:33 Batch  14549_37838 PROMOTE bckcett testIDS30 u08717 Brent
# MAGIC ^1_2 10/31/07 10:24:16 Batch  14549_37464 INIT bckcett devlIDS30 u08717 Brent
# MAGIC ^1_1 10/24/07 15:30:15 Batch  14542_55818 INIT bckcett devlIDS30 u10157 sa - DRG project - moving to ids_current devlopment for coding changes
# MAGIC ^1_1 10/10/07 07:31:23 Batch  14528_27086 INIT bckcett devlIDS30 u08717 Brent
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:    IdsWebdmClmBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This pulls data from the Concatenated Source Sequential file coming from IDS and loads into the B table in Web DataMart
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                   Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------               ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Bhoomi Dasari                 08/31/2007         3264                              Originally Programmed                                devlIDS30


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
ClmMartOwner = get_widget_value('ClmMartOwner','')
clmmart_secret_name = get_widget_value('clmmart_secret_name','')

schema_Ids_Paymt_Sum = StructType([
    StructField("SRC_SYS_CD_SK", IntegerType(), False),
    StructField("PAYMT_REF_ID", StringType(), False),
    StructField("PAYMT_SUM_LOB_CD_SK", IntegerType(), False),
    StructField("NET_AMT", DecimalType(38,10), False)
])

df_hf_b_paymt_sum_lkup = spark.read.parquet(f"{adls_path}/hf_etrnl_cd_mppng.parquet")

df_Ids_Paymt_Sum = (
    spark.read
    .option("header", False)
    .option("quote", '"')
    .option("delimiter", ",")
    .schema(schema_Ids_Paymt_Sum)
    .csv(f"{adls_path}/balancing/snapshot/IDS_PAYMT_SUM.uniq")
)

df_transform = (
    df_Ids_Paymt_Sum.alias("Snapshot")
    .join(
        df_hf_b_paymt_sum_lkup.alias("SrcSysCdLkup"),
        F.col("Snapshot.SRC_SYS_CD_SK") == F.col("SrcSysCdLkup.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_hf_b_paymt_sum_lkup.alias("PaymtsumLobcdlLkup"),
        F.col("Snapshot.PAYMT_SUM_LOB_CD_SK") == F.col("PaymtsumLobcdlLkup.CD_MPPNG_SK"),
        "left"
    )
)

df_transform_final = df_transform.select(
    F.when(
        (F.col("SrcSysCdLkup.TRGT_CD").isNull()) |
        (F.length(trim(F.col("SrcSysCdLkup.TRGT_CD"))) == 0),
        F.lit("NA")
    ).otherwise(F.col("SrcSysCdLkup.TRGT_CD")).alias("SRC_SYS_CD"),
    F.col("Snapshot.PAYMT_REF_ID").alias("PAYMT_REF_ID"),
    F.when(
        (F.col("PaymtsumLobcdlLkup.TRGT_CD").isNull()) |
        (F.length(trim(F.col("PaymtsumLobcdlLkup.TRGT_CD"))) == 0),
        F.lit("NA")
    ).otherwise(F.col("PaymtsumLobcdlLkup.TRGT_CD")).alias("PAYMT_SUM_LOB_CD"),
    F.col("Snapshot.NET_AMT").alias("PAYMT_SUM_NET_AMT")
)

df_transform_final_rpad = (
    df_transform_final
    .withColumn("SRC_SYS_CD", F.rpad(F.col("SRC_SYS_CD"), <...>, " "))
    .withColumn("PAYMT_REF_ID", F.rpad(F.col("PAYMT_REF_ID"), <...>, " "))
    .withColumn("PAYMT_SUM_LOB_CD", F.rpad(F.col("PAYMT_SUM_LOB_CD"), <...>, " "))
)

df_enriched = df_transform_final_rpad.select(
    "SRC_SYS_CD",
    "PAYMT_REF_ID",
    "PAYMT_SUM_LOB_CD",
    "PAYMT_SUM_NET_AMT"
)

jdbc_url, jdbc_props = get_db_config(clmmart_secret_name)
execute_dml("DROP TABLE IF EXISTS STAGING.IdsClmDmPaymtSumBalExtr_BClmDmPaymtSum_temp", jdbc_url, jdbc_props)

(
    df_enriched.write
    .format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", "STAGING.IdsClmDmPaymtSumBalExtr_BClmDmPaymtSum_temp")
    .mode("overwrite")
    .save()
)

merge_sql = f"""
MERGE #$ClmMartOwner#.B_CLM_DM_PAYMT_SUM AS T
USING STAGING.IdsClmDmPaymtSumBalExtr_BClmDmPaymtSum_temp AS S
ON T.SRC_SYS_CD = S.SRC_SYS_CD
AND T.PAYMT_REF_ID = S.PAYMT_REF_ID
AND T.PAYMT_SUM_LOB_CD = S.PAYMT_SUM_LOB_CD
WHEN MATCHED THEN
 UPDATE SET
   T.PAYMT_SUM_NET_AMT = S.PAYMT_SUM_NET_AMT
WHEN NOT MATCHED THEN
 INSERT (SRC_SYS_CD, PAYMT_REF_ID, PAYMT_SUM_LOB_CD, PAYMT_SUM_NET_AMT)
 VALUES (S.SRC_SYS_CD, S.PAYMT_REF_ID, S.PAYMT_SUM_LOB_CD, S.PAYMT_SUM_NET_AMT);
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)