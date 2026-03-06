# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 11/04/09 08:20:06 Batch  15284_30013 PROMOTE bckcetl:31540 updt dsadm dsadm
# MAGIC ^1_1 10/29/09 10:49:49 Batch  15278_39003 PROMOTE bckcetl:31540 ids20 dsadm bls for sg
# MAGIC ^1_1 10/29/09 10:46:58 Batch  15278_38823 INIT bckcett:31540 testIDSnew dsadm bls for sg
# MAGIC ^1_1 10/14/09 13:57:39 Batch  15263_50281 PROMOTE bckcett:31540 testIDSnew u150906 3500-DNOA_Bhoomi_testIDSnew                Maddy
# MAGIC ^1_1 10/14/09 13:41:03 Batch  15263_49741 INIT bckcett:31540 devlIDSnew u150906 3500-DNOA_Bhoomi_devlIDSnew                Maddy
# MAGIC 
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2009  BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC CALLED BY:  UwsBnfVndrRemitExtrSeq
# MAGIC 
# MAGIC 
# MAGIC PROCESSING:
# MAGIC   Pulls data from UWS table (BNF_VNDR_REMIT_RATE) and assigns the primary key to it.
# MAGIC   
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                            Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                       ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Bhoomi Dasari                 10/01/2009         3500                            Origianlly Programmed                                             devl IDSnew                Steph Goddard            10/12/2009

# MAGIC UWS extract to IDS - BNF VNDR REMIT RATE
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


CurrRunCycle = get_widget_value('CurrRunCycle','100')
CurrDate = get_widget_value('CurrDate','2008-07-21')
SrcSysCdSk = get_widget_value('SrcSysCdSk','105838')
RunID = get_widget_value('RunID','9999')
UWSOwner = get_widget_value('UWSOwner','')
uws_secret_name = get_widget_value('uws_secret_name','')

jdbc_url, jdbc_props = get_db_config(uws_secret_name)

df_hf_bnf_vndr_remit_rate = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        "SELECT SRC_SYS_CD, BNF_VNDR_ID, BNF_SUM_DTL_TYP_CD, EFF_DT_SK, CRT_RUN_CYC_EXCTN_SK, BNF_VNDR_REMIT_RATE_SK "
        f"FROM {UWSOwner}.dummy_hf_bnf_vndr_remit_rate"
    )
    .load()
)

df_Uws_BnfVndrRemitRate = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        "SELECT BNF_VNDR_ID, BNF_SUM_DTL_TYP_CD, EFF_DT_SK, TERM_DT_SK, REMIT_RATE, USER_ID, LAST_UPDT_DT_SK "
        f"FROM {UWSOwner}.BNF_VNDR_REMIT_RATE"
    )
    .load()
)

df_Snapshot = (
    df_Uws_BnfVndrRemitRate
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", F.lit(0))
    .withColumn("INSRT_UPDT_CD", F.lit("I"))
    .withColumn("DISCARD_IN", F.lit("N"))
    .withColumn("PASS_THRU_IN", F.lit("Y"))
    .withColumn("FIRST_RECYC_DT", F.lit(CurrDate))
    .withColumn("ERR_CT", F.lit(0))
    .withColumn("RECYCLE_CT", F.lit(0))
    .withColumn("SRC_SYS_CD", F.lit("FACETS"))
    .withColumn("PRI_KEY_STRING", F.concat_ws(";", F.lit("FACETS"), F.col("BNF_VNDR_ID"), F.col("BNF_SUM_DTL_TYP_CD"), F.col("EFF_DT_SK")))
    .withColumn("BNF_VNDR_REMIT_RATE_SK", F.lit(0))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("BNF_SUM_DTL_TYP_CD_SK", F.lit(0))
    .withColumnRenamed("REMIT_RATE", "BNF_VNDR_REMIT_RATE")
    .select(
        "JOB_EXCTN_RCRD_ERR_SK",
        "INSRT_UPDT_CD",
        "DISCARD_IN",
        "PASS_THRU_IN",
        "FIRST_RECYC_DT",
        "ERR_CT",
        "RECYCLE_CT",
        "SRC_SYS_CD",
        "PRI_KEY_STRING",
        "BNF_VNDR_REMIT_RATE_SK",
        "BNF_VNDR_ID",
        "BNF_SUM_DTL_TYP_CD",
        "EFF_DT_SK",
        "TERM_DT_SK",
        "BNF_VNDR_REMIT_RATE",
        "USER_ID",
        "LAST_UPDT_DT_SK",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "BNF_SUM_DTL_TYP_CD_SK"
    )
)

df_Primary_Keying_Pre = (
    df_Snapshot.alias("Transform")
    .join(
        df_hf_bnf_vndr_remit_rate.alias("lkup"),
        (
            (F.col("Transform.SRC_SYS_CD") == F.col("lkup.SRC_SYS_CD"))
            & (F.col("Transform.BNF_VNDR_ID") == F.col("lkup.BNF_VNDR_ID"))
            & (F.col("Transform.BNF_SUM_DTL_TYP_CD") == F.col("lkup.BNF_SUM_DTL_TYP_CD"))
            & (F.col("Transform.EFF_DT_SK") == F.col("lkup.EFF_DT_SK"))
        ),
        "left"
    )
)

df_Primary_Keying_Pre = df_Primary_Keying_Pre.withColumn(
    "NewCrtRunCycExtcnSk",
    F.when(F.col("lkup.BNF_VNDR_REMIT_RATE_SK").isNull(), F.lit(CurrRunCycle)).otherwise(F.col("lkup.CRT_RUN_CYC_EXCTN_SK"))
)

df_enriched = df_Primary_Keying_Pre.withColumn(
    "BNF_VNDR_REMIT_RATE_SK",
    F.when(F.col("lkup.BNF_VNDR_REMIT_RATE_SK").isNull(), F.lit(None)).otherwise(F.col("lkup.BNF_VNDR_REMIT_RATE_SK"))
)

df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"BNF_VNDR_REMIT_RATE_SK",<schema>,<secret_name>)

df_out_Key = (
    df_enriched
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.col("NewCrtRunCycExtcnSk"))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(CurrRunCycle))
    .withColumn("BNF_SUM_DTL_TYP_CD_SK", F.lit(0))
    .select(
        "JOB_EXCTN_RCRD_ERR_SK",
        "INSRT_UPDT_CD",
        "DISCARD_IN",
        "PASS_THRU_IN",
        "FIRST_RECYC_DT",
        "ERR_CT",
        "RECYCLE_CT",
        "PRI_KEY_STRING",
        "BNF_VNDR_REMIT_RATE_SK",
        "SRC_SYS_CD",
        "BNF_VNDR_ID",
        "BNF_SUM_DTL_TYP_CD",
        "EFF_DT_SK",
        "TERM_DT_SK",
        "BNF_VNDR_REMIT_RATE",
        "USER_ID",
        "LAST_UPDT_DT_SK",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "BNF_SUM_DTL_TYP_CD_SK"
    )
)

df_out_update = df_enriched.filter(F.col("lkup.BNF_VNDR_REMIT_RATE_SK").isNull()).select(
    F.col("SRC_SYS_CD"),
    F.col("BNF_VNDR_ID"),
    F.col("BNF_SUM_DTL_TYP_CD"),
    F.col("EFF_DT_SK"),
    F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("BNF_VNDR_REMIT_RATE_SK")
)

execute_dml(
    "DROP TABLE IF EXISTS STAGING.UwsBnfVndrRemitRateExtr_hf_bnf_vndr_remit_rate_updt_temp",
    jdbc_url,
    jdbc_props
)

(
    df_out_update
    .write
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", "STAGING.UwsBnfVndrRemitRateExtr_hf_bnf_vndr_remit_rate_updt_temp")
    .mode("append")
    .options(**jdbc_props)
    .save()
)

merge_sql = f"""
MERGE INTO {UWSOwner}.dummy_hf_bnf_vndr_remit_rate AS T
USING STAGING.UwsBnfVndrRemitRateExtr_hf_bnf_vndr_remit_rate_updt_temp AS S
ON
    T.SRC_SYS_CD = S.SRC_SYS_CD
    AND T.BNF_VNDR_ID = S.BNF_VNDR_ID
    AND T.BNF_SUM_DTL_TYP_CD = S.BNF_SUM_DTL_TYP_CD
    AND T.EFF_DT_SK = S.EFF_DT_SK
WHEN MATCHED THEN
    UPDATE SET
        T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK,
        T.BNF_VNDR_REMIT_RATE_SK = S.BNF_VNDR_REMIT_RATE_SK
WHEN NOT MATCHED THEN
    INSERT (SRC_SYS_CD, BNF_VNDR_ID, BNF_SUM_DTL_TYP_CD, EFF_DT_SK, CRT_RUN_CYC_EXCTN_SK, BNF_VNDR_REMIT_RATE_SK)
    VALUES (S.SRC_SYS_CD, S.BNF_VNDR_ID, S.BNF_SUM_DTL_TYP_CD, S.EFF_DT_SK, S.CRT_RUN_CYC_EXCTN_SK, S.BNF_VNDR_REMIT_RATE_SK);
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)

df_final = (
    df_out_Key
    .withColumn("INSRT_UPDT_CD", F.rpad(F.col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", F.rpad(F.col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", F.rpad(F.col("PASS_THRU_IN"), 1, " "))
    .withColumn("EFF_DT_SK", F.rpad(F.col("EFF_DT_SK"), 10, " "))
    .withColumn("TERM_DT_SK", F.rpad(F.col("TERM_DT_SK"), 10, " "))
    .withColumn("LAST_UPDT_DT_SK", F.rpad(F.col("LAST_UPDT_DT_SK"), 10, " "))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.rpad(F.col("CRT_RUN_CYC_EXCTN_SK"), 10, " "))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"), 10, " "))
)

write_files(
    df_final,
    f"{adls_path}/key/UwsBnfVndrRemitRateExtr.BnfVndrRemitRate.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)