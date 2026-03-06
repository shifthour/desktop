# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2009 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #               Change Description                             Development Project      Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------     ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Raja Gummadi                  03/04/2015      5460                                Originally Programmed                            IntegrateNewDevl        Kalyan Neelam             2015-03-06
# MAGIC Raja Gummadi                  03/24/2015      5460                                Balancing snapshot append instead      IntegrateNewDevl        Kalyan Neelam             2015-03-25
# MAGIC                                                                                                            of overwirte

# MAGIC JobName: CblTalnMaraClnclClsXfrm
# MAGIC 
# MAGIC Strip field function and Tranforamtion rules are applied on the data Extracted from inboundfile
# MAGIC Strip Carriage Return, Line Feed, and Tab.
# MAGIC Transformed Data will land into a Dataset for Primary Keying job.
# MAGIC 
# MAGIC Data is partitioned on Natural Key Columns.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when, length, concat, rpad
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


# Parameters
IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")
SrcSysCd = get_widget_value("SrcSysCd","FACETS")
SrcSysCdSk = get_widget_value("SrcSysCdSk","1581")
RunID = get_widget_value("RunID","")
RunIDTimeStamp = get_widget_value("RunIDTimeStamp","")
FileName = get_widget_value("FileName","")
CurrDate = get_widget_value("CurrDate","")

# Read from DB2ConnectorPX (db2_K_MARA_CLNCL_CLS_In)
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
df_db2_K_MARA_CLNCL_CLS_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"SELECT SRC_SYS_CD, CLNCL_CLS_ID, EFF_DT_SK, CRT_RUN_CYC_EXCTN_SK, MARA_CLNCL_CLS_SK FROM {IDSOwner}.K_MARA_CLNCL_CLS")
    .load()
)

# Read from PxDataSet (ds_MARA_CLNCL_CLS_Extr) -> parquet
df_ds_MARA_CLNCL_CLS_Extr = spark.read.parquet(
    f"{adls_path}/ds/MARA_CLNCL_CLS.{SrcSysCd}.extr.{RunID}.parquet"
)
df_ds_MARA_CLNCL_CLS_Extr = df_ds_MARA_CLNCL_CLS_Extr.select(
    "COND",
    "MARA_SUM_GRP_CD",
    "MARA_SUM_GRP_DESC",
    "PRPSD_BODY_SYS_CD",
    "PRPSD_BODY_SYS",
    "SEC_BODY_SYS",
    "CLNCL_LABEL",
    "COND_CD",
    "AHRQ_CHRNC",
    "AHRQ_MOST_CMN_CHRNC"
)

# CTransformerStage (StripField)
df_StripField = (
    df_ds_MARA_CLNCL_CLS_Extr
    .withColumn("COND", trim(col("COND")))
    .withColumn("MARA_SUM_GRP_CD", trim(col("MARA_SUM_GRP_CD")))
    .withColumn("MARA_SUM_GRP_DESC", trim(col("MARA_SUM_GRP_DESC")))
    .withColumn("PRPSD_BODY_SYS_CD", trim(col("PRPSD_BODY_SYS_CD")))
    .withColumn("PRPSD_BODY_SYS", trim(col("PRPSD_BODY_SYS")))
    .withColumn("SEC_BODY_SYS", trim(col("SEC_BODY_SYS")))
    .withColumn("CLNCL_LABEL", trim(col("CLNCL_LABEL")))
    .withColumn("COND_CD", trim(col("COND_CD")))
    .withColumn("AHRQ_CHRNC", trim(col("AHRQ_CHRNC")))
    .withColumn("AHRQ_MOST_CMN_CHRNC", trim(col("AHRQ_MOST_CMN_CHRNC")))
)

# CTransformerStage (Xfrm_BusinessRules)
df_effdt = (
    df_StripField
    .withColumn("PRI_NAT_KEY_STRING", concat(lit(SrcSysCd), lit(";"), col("COND")))
    .withColumn("FIRST_RECYC_TS", lit(RunIDTimeStamp))
    .withColumn("MARA_CLNCL_CLS_SK", lit("0"))
    .withColumn("CLNCL_CLS_ID", col("COND"))
    .withColumn("SRC_SYS_CD", lit(SrcSysCd))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", lit("0"))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", lit("0"))
    .withColumn("SRC_SYS_CD_SK", lit(SrcSysCdSk))
    .withColumn("MARA_SUM_GRP_CD", col("MARA_SUM_GRP_CD"))
    .withColumn("MARA_SUM_GRP_DESC", col("MARA_SUM_GRP_DESC"))
    .withColumn("PRPSD_BODY_SYS_CD", col("PRPSD_BODY_SYS_CD"))
    .withColumn("PRPSD_BODY_SYS", col("PRPSD_BODY_SYS"))
    .withColumn("SEC_BODY_SYS", when(col("SEC_BODY_SYS").isNull(), "NA").otherwise(col("SEC_BODY_SYS")))
    .withColumn("CLNCL_LABEL", col("CLNCL_LABEL"))
    .withColumn("COND_CD", col("COND_CD"))
    .withColumn("AHRQ_CHRNC", col("AHRQ_CHRNC"))
    .withColumn("AHRQ_MOST_CMN_CHRNC", col("AHRQ_MOST_CMN_CHRNC"))
    .withColumn("EFF_DT_SK", lit(CurrDate))
    .withColumn("TERM_DT_SK", lit("2199-12-31"))
)

# PxLookup (Lookup_49)
df_lookup_49 = (
    df_effdt.alias("effdt")
    .join(
        df_db2_K_MARA_CLNCL_CLS_In.alias("Extr"),
        [
            col("effdt.SRC_SYS_CD") == col("Extr.SRC_SYS_CD"),
            col("effdt.CLNCL_CLS_ID") == col("Extr.CLNCL_CLS_ID"),
        ],
        "left"
    )
    .select(
        col("effdt.PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
        col("effdt.FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
        col("effdt.MARA_CLNCL_CLS_SK").alias("MARA_CLNCL_CLS_SK"),
        col("effdt.CLNCL_CLS_ID").alias("CLNCL_CLS_ID"),
        col("Extr.EFF_DT_SK").alias("EFF_DT_SK"),
        col("effdt.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("effdt.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("effdt.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("effdt.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        col("effdt.MARA_SUM_GRP_CD").alias("MARA_SUM_GRP_CD"),
        col("effdt.MARA_SUM_GRP_DESC").alias("MARA_SUM_GRP_DESC"),
        col("effdt.PRPSD_BODY_SYS_CD").alias("PRPSD_BODY_SYS_CD"),
        col("effdt.PRPSD_BODY_SYS").alias("PRPSD_BODY_SYS"),
        col("effdt.SEC_BODY_SYS").alias("SEC_BODY_SYS"),
        col("effdt.CLNCL_LABEL").alias("CLNCL_LABEL"),
        col("effdt.COND_CD").alias("COND_CD"),
        col("effdt.AHRQ_CHRNC").alias("AHRQ_CHRNC"),
        col("effdt.AHRQ_MOST_CMN_CHRNC").alias("AHRQ_MOST_CMN_CHRNC"),
        col("effdt.TERM_DT_SK").alias("TERM_DT_SK")
    )
)

# CTransformerStage (xfrmdt) - Output Link: PkeyOut
df_xfrmdtPkeyOut = df_lookup_49.withColumn(
    "EFF_DT_SK",
    when(length(trim(col("EFF_DT_SK"))) == 0, lit(CurrDate)).otherwise(col("EFF_DT_SK"))
)
df_xfrmdtPkeyOut = df_xfrmdtPkeyOut.select(
    "PRI_NAT_KEY_STRING",
    "FIRST_RECYC_TS",
    "MARA_CLNCL_CLS_SK",
    "CLNCL_CLS_ID",
    "EFF_DT_SK",
    "SRC_SYS_CD",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "SRC_SYS_CD_SK",
    "MARA_SUM_GRP_CD",
    "MARA_SUM_GRP_DESC",
    "PRPSD_BODY_SYS_CD",
    "PRPSD_BODY_SYS",
    "SEC_BODY_SYS",
    "CLNCL_LABEL",
    "COND_CD",
    "AHRQ_CHRNC",
    "AHRQ_MOST_CMN_CHRNC",
    "TERM_DT_SK"
)

# CTransformerStage (xfrmdt) - Output Link: snapshot
df_xfrmdtSnapshot = df_lookup_49.withColumn(
    "EFF_DT_SK",
    when(length(trim(col("EFF_DT_SK"))) == 0, lit(CurrDate)).otherwise(col("EFF_DT_SK"))
).select(
    col("CLNCL_CLS_ID").alias("CLNCL_CLS_ID"),
    col("EFF_DT_SK").alias("EFF_DT_SK"),
    col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK")
)

# PxDataSet (ds_MARA_CLNCL_CLS_Xfrm) -> write to parquet
df_xfrmdtPkeyOut_final = (
    df_xfrmdtPkeyOut
    .withColumn("CLNCL_CLS_ID", rpad(col("CLNCL_CLS_ID"), 7, " "))
    .withColumn("EFF_DT_SK", rpad(col("EFF_DT_SK"), 10, " "))
    .withColumn("MARA_SUM_GRP_CD", rpad(col("MARA_SUM_GRP_CD"), 6, " "))
    .withColumn("PRPSD_BODY_SYS_CD", rpad(col("PRPSD_BODY_SYS_CD"), 6, " "))
    .withColumn("COND_CD", rpad(col("COND_CD"), 6, " "))
    .withColumn("TERM_DT_SK", rpad(col("TERM_DT_SK"), 10, " "))
    .select(
        "PRI_NAT_KEY_STRING",
        "FIRST_RECYC_TS",
        "MARA_CLNCL_CLS_SK",
        "CLNCL_CLS_ID",
        "EFF_DT_SK",
        "SRC_SYS_CD",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "SRC_SYS_CD_SK",
        "MARA_SUM_GRP_CD",
        "MARA_SUM_GRP_DESC",
        "PRPSD_BODY_SYS_CD",
        "PRPSD_BODY_SYS",
        "SEC_BODY_SYS",
        "CLNCL_LABEL",
        "COND_CD",
        "AHRQ_CHRNC",
        "AHRQ_MOST_CMN_CHRNC",
        "TERM_DT_SK"
    )
)
write_files(
    df_xfrmdtPkeyOut_final,
    f"{adls_path}/ds/MARA_CLNCL_CLS.{SrcSysCd}.xfrm.{RunID}.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

# PxSequentialFile (B_MARA_CLNCL_CLS) -> write to .dat
df_xfrmdtSnapshot_final = (
    df_xfrmdtSnapshot
    .withColumn("CLNCL_CLS_ID", rpad(col("CLNCL_CLS_ID"), 7, " "))
    .withColumn("EFF_DT_SK", rpad(col("EFF_DT_SK"), 10, " "))
    .select(
        "CLNCL_CLS_ID",
        "EFF_DT_SK",
        "SRC_SYS_CD_SK"
    )
)
write_files(
    df_xfrmdtSnapshot_final,
    f"{adls_path}/load/B_MARA_CLNCL_CLS.{SrcSysCd}.dat",
    ",",
    "append",
    False,
    False,
    "^",
    None
)