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
# MAGIC Bhoomi Dasari                 08/31/2007         3264                              Originally Programmed                                devlIDS30                     Steph Goddard             09/28/2007


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
ClmMartOwner = get_widget_value('ClmMartOwner','')
clmmart_secret_name = get_widget_value('clmmart_secret_name','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
jdbc_url_clmmart, jdbc_props_clmmart = get_db_config(clmmart_secret_name)

schema_Ids_Clm_Ln = StructType([
    StructField("SRC_SYS_CD_SK", IntegerType(), False),
    StructField("CLM_ID", StringType(), False),
    StructField("CLM_LN_SEQ_NO", IntegerType(), False),
    StructField("PROC_CD_SK", IntegerType(), False),
    StructField("CLM_LN_RVNU_CD_SK", IntegerType(), False),
    StructField("ALW_AMT", DoubleType(), False),
    StructField("CHRG_AMT", DoubleType(), False),
    StructField("PAYBL_AMT", DoubleType(), False)
])

df_Ids_Clm_Ln = (
    spark.read.format("csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .option("delimiter", ",")
    .option("quote", '"')
    .schema(schema_Ids_Clm_Ln)
    .load(f"{adls_path}/balancing/snapshot/IDS_CLM_LN.dat")
)

df_CodesExtr_ProcCd = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", f"SELECT PROC_CD.PROC_CD_SK as PROC_CD_SK, PROC_CD.PROC_CD as PROC_CD FROM {IDSOwner}.PROC_CD PROC_CD")
    .load()
)

df_CodesExtr_RvnuCd = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", f"SELECT RVNU_CD.RVNU_CD_SK as RVNU_CD_SK, RVNU_CD.RVNU_CD as RVNU_CD FROM {IDSOwner}.RVNU_CD RVNU_CD")
    .load()
)

df_ProcCdDedup = dedup_sort(
    df_CodesExtr_ProcCd,
    ["PROC_CD_SK"],
    []
)

df_RvnuCdDedup = dedup_sort(
    df_CodesExtr_RvnuCd,
    ["RVNU_CD_SK"],
    []
)

df_hf_etrnl_cd_mppng = spark.read.parquet(f"{adls_path}/hf_etrnl_cd_mppng.parquet")

df_Transform = (
    df_Ids_Clm_Ln.alias("Snapshot")
    .join(df_hf_etrnl_cd_mppng.alias("SrcSysCdLkup"), F.col("Snapshot.SRC_SYS_CD_SK") == F.col("SrcSysCdLkup.CD_MPPNG_SK"), "left")
    .join(df_ProcCdDedup.alias("ProcCdLkup"), F.col("Snapshot.PROC_CD_SK") == F.col("ProcCdLkup.PROC_CD_SK"), "left")
    .join(df_RvnuCdDedup.alias("RvnuCdLkup"), F.col("Snapshot.CLM_LN_RVNU_CD_SK") == F.col("RvnuCdLkup.RVNU_CD_SK"), "left")
    .select(
        F.when(
            F.isnull(F.col("SrcSysCdLkup.TRGT_CD"))
            | (F.length(trim(F.col("SrcSysCdLkup.TRGT_CD"))) == 0),
            F.lit("NA")
        ).otherwise(F.col("SrcSysCdLkup.TRGT_CD")).alias("SRC_SYS_CD"),
        F.col("Snapshot.CLM_ID").alias("CLM_ID"),
        F.col("Snapshot.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        F.when(
            F.isnull(F.col("RvnuCdLkup.RVNU_CD"))
            | (F.length(trim(F.col("RvnuCdLkup.RVNU_CD"))) == 0),
            F.lit("NA")
        ).otherwise(F.col("RvnuCdLkup.RVNU_CD")).alias("CLM_LN_RVNU_CD"),
        F.when(
            F.isnull(F.col("ProcCdLkup.PROC_CD"))
            | (F.length(trim(F.col("ProcCdLkup.PROC_CD"))) == 0),
            F.lit("NA")
        ).otherwise(F.col("ProcCdLkup.PROC_CD")).alias("CLM_LN_PROC_CD"),
        F.col("Snapshot.CHRG_AMT").alias("CLM_LN_CHRG_AMT"),
        F.col("Snapshot.ALW_AMT").alias("CLM_LN_ALW_AMT"),
        F.col("Snapshot.PAYBL_AMT").alias("CLM_LN_PAYBL_AMT")
    )
)

df_enriched = (
    df_Transform
    .withColumn("SRC_SYS_CD", F.rpad(F.col("SRC_SYS_CD"), <...>, " "))
    .withColumn("CLM_ID", F.rpad(F.col("CLM_ID"), <...>, " "))
    .withColumn("CLM_LN_RVNU_CD", F.rpad(F.col("CLM_LN_RVNU_CD"), <...>, " "))
    .withColumn("CLM_LN_PROC_CD", F.rpad(F.col("CLM_LN_PROC_CD"), 5, " "))
)

df_final = df_enriched.select(
    "SRC_SYS_CD",
    "CLM_ID",
    "CLM_LN_SEQ_NO",
    "CLM_LN_RVNU_CD",
    "CLM_LN_PROC_CD",
    "CLM_LN_CHRG_AMT",
    "CLM_LN_ALW_AMT",
    "CLM_LN_PAYBL_AMT"
)

execute_dml(
    f"DROP TABLE IF EXISTS STAGING.IdsClmDmClmLnBalExtr_BClmDmClmLn_temp",
    jdbc_url_clmmart,
    jdbc_props_clmmart
)

df_final.write.jdbc(
    url=jdbc_url_clmmart,
    table="STAGING.IdsClmDmClmLnBalExtr_BClmDmClmLn_temp",
    mode="overwrite",
    properties=jdbc_props_clmmart
)

merge_sql = f"""
MERGE INTO {ClmMartOwner}.B_CLM_DM_CLM_LN AS T
USING STAGING.IdsClmDmClmLnBalExtr_BClmDmClmLn_temp AS S
ON T.SRC_SYS_CD = S.SRC_SYS_CD
   AND T.CLM_ID = S.CLM_ID
   AND T.CLM_LN_SEQ_NO = S.CLM_LN_SEQ_NO
WHEN MATCHED THEN
  UPDATE SET
    T.CLM_LN_RVNU_CD = S.CLM_LN_RVNU_CD,
    T.CLM_LN_PROC_CD = S.CLM_LN_PROC_CD,
    T.CLM_LN_CHRG_AMT = S.CLM_LN_CHRG_AMT,
    T.CLM_LN_ALW_AMT = S.CLM_LN_ALW_AMT,
    T.CLM_LN_PAYBL_AMT = S.CLM_LN_PAYBL_AMT
WHEN NOT MATCHED THEN
  INSERT (
    SRC_SYS_CD,
    CLM_ID,
    CLM_LN_SEQ_NO,
    CLM_LN_RVNU_CD,
    CLM_LN_PROC_CD,
    CLM_LN_CHRG_AMT,
    CLM_LN_ALW_AMT,
    CLM_LN_PAYBL_AMT
  )
  VALUES (
    S.SRC_SYS_CD,
    S.CLM_ID,
    S.CLM_LN_SEQ_NO,
    S.CLM_LN_RVNU_CD,
    S.CLM_LN_PROC_CD,
    S.CLM_LN_CHRG_AMT,
    S.CLM_LN_ALW_AMT,
    S.CLM_LN_PAYBL_AMT
  )
WHEN NOT MATCHED BY SOURCE THEN
  DELETE
;
"""

execute_dml(merge_sql, jdbc_url_clmmart, jdbc_props_clmmart)