# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2009 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     FctsUMActvtyExtr
# MAGIC 
# MAGIC DESCRIPTION:    Pulls data from CMC_UMAC_ACTIVITY to a landing file for the IDS
# MAGIC       
# MAGIC 
# MAGIC Developer                          Date                    Project/Altiris #                     Change Description                                            Development Project               Code Reviewer                      Date Reviewed
# MAGIC ----------------------------------      -------------------      -----------------------------------           ---------------------------------------------------------                     ----------------------------------              ---------------------------------               -------------------------
# MAGIC Bhoomi Dasari                 03/03/2009              3808                                  Originally Programmed                                                      devlIDS                       Steph Goddard                     03/30/2009
# MAGIC Prabhu ES                       2022-03-07              S2S Remediation               MSSQL ODBC conn params added                        IntegrateDev5            Manasa Andru                       2022-06-14

# MAGIC Strip Carriage Return, Line Feed, and  Tab
# MAGIC 
# MAGIC Use STRIP.FIELD function on each character column coming in
# MAGIC Assign primary surrogate key
# MAGIC Extract Facets Data
# MAGIC Snapshot used for Balancing
# MAGIC Apply business logic
# MAGIC Writing Sequential File to ../key
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame, functions as F, types as T
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
DriverTable = get_widget_value('DriverTable','')
RunID = get_widget_value('RunID','')
CurrDate = get_widget_value('CurrDate','')
SrcSysCd = get_widget_value('SrcSysCd','')
TmpOutFile = get_widget_value('TmpOutFile','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')

jdbc_url, jdbc_props = get_db_config(facets_secret_name)

extract_query_1 = f"""SELECT 
UMAC.UMUM_REF_ID,
UMAC.UMAC_SEQ_NO,
UMAC.USUS_ID,
UMAC.UMAC_ACTIV_DT,
UMAC.UMAC_MCTR_REAS,
UMAC.UMAC_UMSV_ADDED,
UMAC.UMAC_UMIN_ADDED,
UMAC.UMAC_UMIR_ADDED,
UMAC.UMAC_NTNB_ADDED,
UMAC.UMAC_MCTR_CPLX,
UMAC.UMAC_USID_ROUTE 
FROM {FacetsOwner}.CMC_UMAC_ACTIVITY UMAC,
     tempdb..{DriverTable} as T
WHERE  UMAC.UMUM_REF_ID = T.UM_REF_ID
"""
df_CMC_UMAC_ACTIVITY = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_1)
    .load()
)

df_StripField = (
    df_CMC_UMAC_ACTIVITY
    .withColumn("UMUM_REF_ID", trim(strip_field(F.col("UMUM_REF_ID"))))
    .withColumn("UMAC_SEQ_NO", F.col("UMAC_SEQ_NO"))
    .withColumn("USUS_ID", trim(strip_field(F.col("USUS_ID"))))
    .withColumn("UMAC_USID_ROUTE", trim(strip_field(F.col("UMAC_USID_ROUTE"))))
    .withColumn("UMAC_ACTIV_DT", F.date_format(F.col("UMAC_ACTIV_DT"), "yyyy-MM-dd"))
    .withColumn("UMAC_MCTR_REAS", trim(strip_field(F.col("UMAC_MCTR_REAS"))))
    .withColumn("UMAC_UMSV_ADDED", F.col("UMAC_UMSV_ADDED"))
    .withColumn("UMAC_UMIN_ADDED", F.col("UMAC_UMIN_ADDED"))
    .withColumn("UMAC_UMIR_ADDED", F.col("UMAC_UMIR_ADDED"))
    .withColumn("UMAC_NTNB_ADDED", F.col("UMAC_NTNB_ADDED"))
    .withColumn("UMAC_MCTR_CPLX", trim(strip_field(F.col("UMAC_MCTR_CPLX"))))
)

df_BusinessRules = (
    df_StripField
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", F.lit(0))
    .withColumn("INSRT_UPDT_CD", F.lit("I"))
    .withColumn("DISCARD_IN", F.lit("N"))
    .withColumn("PASS_THRU_IN", F.lit("Y"))
    .withColumn("FIRST_RECYC_DT", F.lit(CurrDate))
    .withColumn("ERR_CT", F.lit(0))
    .withColumn("RECYCLE_CT", F.lit(0))
    .withColumn("SRC_SYS_CD", F.lit(SrcSysCd))
    .withColumn("PRI_KEY_STRING", F.concat(F.lit("FACETS;"), F.col("UMUM_REF_ID"), F.lit(";"), F.col("UMAC_SEQ_NO")))
    .withColumn("UM_ACTVTY_SK", F.lit(0))
    .withColumn("UM_REF_ID", F.col("UMUM_REF_ID"))
    .withColumn("UM_ACTVTY_SEQ_NO", F.col("UMAC_SEQ_NO"))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("USUS_ID",
        F.when(
            F.col("USUS_ID").isNull() | (F.length(F.col("USUS_ID")) == 0),
            F.lit("NA")
        ).otherwise(F.col("USUS_ID"))
    )
    .withColumn("UMAC_USID_ROUTE",
        F.when(
            F.col("UMAC_USID_ROUTE").isNull() | (F.length(F.col("UMAC_USID_ROUTE")) == 0),
            F.lit("NA")
        ).otherwise(F.col("UMAC_USID_ROUTE"))
    )
    .withColumn("ACTVTY_DT_SK", F.col("UMAC_ACTIV_DT"))
    .withColumn("UMAC_MCTR_REAS",
        F.when(
            F.col("UMAC_MCTR_REAS").isNull() | (F.length(F.col("UMAC_MCTR_REAS")) == 0),
            F.lit("NA")
        ).otherwise(F.col("UMAC_MCTR_REAS"))
    )
    .withColumn("UMAC_MCTR_CPLX",
        F.when(
            F.col("UMAC_MCTR_CPLX").isNull() | (F.length(F.col("UMAC_MCTR_CPLX")) == 0),
            F.lit("NA")
        ).otherwise(F.col("UMAC_MCTR_CPLX"))
    )
    .withColumn("SVC_ROWS_ADD_NO",
        F.when(
            F.col("UMAC_UMSV_ADDED").isNull() | (F.length(F.col("UMAC_UMSV_ADDED")) == 0),
            F.lit(0)
        ).otherwise(F.col("UMAC_UMSV_ADDED"))
    )
    .withColumn("IP_ROWS_ADD_NO",
        F.when(
            F.col("UMAC_UMIN_ADDED").isNull() | (F.length(F.col("UMAC_UMIN_ADDED")) == 0),
            F.lit(0)
        ).otherwise(F.col("UMAC_UMIN_ADDED"))
    )
    .withColumn("RVW_ROWS_ADD_NO",
        F.when(
            F.col("UMAC_UMIR_ADDED").isNull() | (F.length(F.col("UMAC_UMIR_ADDED")) == 0),
            F.lit(0)
        ).otherwise(F.col("UMAC_UMIR_ADDED"))
    )
    .withColumn("NOTE_ROWS_ADD_NO",
        F.when(
            F.col("UMAC_NTNB_ADDED").isNull() | (F.length(F.col("UMAC_NTNB_ADDED")) == 0),
            F.lit(0)
        ).otherwise(F.col("UMAC_NTNB_ADDED"))
    )
)

df_hf_lkup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", "SELECT SRC_SYS_CD, UM_REF_ID, UM_ACTVTY_SEQ_NO, CRT_RUN_CYC_EXCTN_SK, UM_ACTVTY_SK FROM IDS.dummy_hf_um_actvty")
    .load()
)

df_PrimaryKeyTmp = (
    df_BusinessRules.alias("T")
    .join(
        df_hf_lkup.alias("L"),
        [
            F.col("T.SRC_SYS_CD") == F.col("L.SRC_SYS_CD"),
            F.col("T.UM_REF_ID") == F.col("L.UM_REF_ID"),
            F.col("T.UM_ACTVTY_SEQ_NO") == F.col("L.UM_ACTVTY_SEQ_NO"),
        ],
        "left"
    )
    .withColumn("temp_UM_ACTVTY_SK",
        F.when(
            F.col("L.UM_ACTVTY_SK").isNull(),
            F.lit(None)
        ).otherwise(F.col("L.UM_ACTVTY_SK"))
    )
    .withColumn("temp_CRT_RUN_CYC_EXCTN_SK",
        F.when(
            F.col("L.UM_ACTVTY_SK").isNull(),
            F.lit(CurrRunCycle)
        ).otherwise(F.col("L.CRT_RUN_CYC_EXCTN_SK"))
    )
)

df_enriched = SurrogateKeyGen(df_PrimaryKeyTmp,<DB sequence name>,"temp_UM_ACTVTY_SK",<schema>,<secret_name>)

df_enriched = (
    df_enriched
    .withColumn("UM_ACTVTY_SK", F.col("temp_UM_ACTVTY_SK"))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.col("temp_CRT_RUN_CYC_EXCTN_SK"))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(CurrRunCycle))
)

df_PrimaryKeyOut = df_enriched.select(
    F.col("T.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("T.INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("T.DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("T.PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("T.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("T.ERR_CT").alias("ERR_CT"),
    F.col("T.RECYCLE_CT").alias("RECYCLE_CT"),
    F.rpad(F.col("T.SRC_SYS_CD"), 999, " ").alias("SRC_SYS_CD"),
    F.col("T.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("UM_ACTVTY_SK").alias("UM_ACTVTY_SK"),
    F.rpad(F.col("T.UM_REF_ID"), 9, " ").alias("UM_REF_ID"),
    F.col("T.UM_ACTVTY_SEQ_NO").alias("UM_ACTVTY_SEQ_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.rpad(F.col("T.USUS_ID"), 10, " ").alias("USUS_ID"),
    F.rpad(F.col("T.UMAC_USID_ROUTE"), 10, " ").alias("UMAC_USID_ROUTE"),
    F.rpad(F.col("T.UMAC_MCTR_REAS"), 4, " ").alias("UMAC_MCTR_REAS"),
    F.rpad(F.col("T.UMAC_MCTR_CPLX"), 4, " ").alias("UMAC_MCTR_CPLX"),
    F.rpad(F.col("T.ACTVTY_DT_SK"), 10, " ").alias("ACTVTY_DT_SK"),
    F.col("T.SVC_ROWS_ADD_NO").alias("SVC_ROWS_ADD_NO"),
    F.col("T.IP_ROWS_ADD_NO").alias("IP_ROWS_ADD_NO"),
    F.col("T.RVW_ROWS_ADD_NO").alias("RVW_ROWS_ADD_NO"),
    F.col("T.NOTE_ROWS_ADD_NO").alias("NOTE_ROWS_ADD_NO")
)

df_updt = (
    df_enriched
    .filter(F.col("L.UM_ACTVTY_SK").isNull())
    .select(
        F.col("T.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("T.UM_REF_ID").alias("UM_REF_ID"),
        F.col("T.UM_ACTVTY_SEQ_NO").alias("UM_ACTVTY_SEQ_NO"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("UM_ACTVTY_SK").alias("UM_ACTVTY_SK")
    )
)

execute_dml("DROP TABLE IF EXISTS STAGING.FctsUMActvtyExtr_hf_write_temp", jdbc_url, jdbc_props)

df_updt.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.FctsUMActvtyExtr_hf_write_temp") \
    .mode("overwrite") \
    .save()

merge_sql = """
MERGE IDS.dummy_hf_um_actvty AS target
USING STAGING.FctsUMActvtyExtr_hf_write_temp AS source
ON target.SRC_SYS_CD=source.SRC_SYS_CD
 AND target.UM_REF_ID=source.UM_REF_ID
 AND target.UM_ACTVTY_SEQ_NO=source.UM_ACTVTY_SEQ_NO
WHEN NOT MATCHED THEN
INSERT (SRC_SYS_CD, UM_REF_ID, UM_ACTVTY_SEQ_NO, CRT_RUN_CYC_EXCTN_SK, UM_ACTVTY_SK)
VALUES (source.SRC_SYS_CD, source.UM_REF_ID, source.UM_ACTVTY_SEQ_NO, source.CRT_RUN_CYC_EXCTN_SK, source.UM_ACTVTY_SK);
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)

write_files(
    df_PrimaryKeyOut,
    f"{adls_path}/key/{TmpOutFile}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

extract_query_2 = f"""SELECT 
UMAC.UMUM_REF_ID,
UMAC.UMAC_SEQ_NO
FROM {FacetsOwner}.CMC_UMAC_ACTIVITY UMAC,
     tempdb..{DriverTable} as T
WHERE  UMAC.UMUM_REF_ID = T.UM_REF_ID
"""
df_Facets_CMC_UMAC_ACTIVITY = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_2)
    .load()
)

df_Strip2 = (
    df_Facets_CMC_UMAC_ACTIVITY
    .withColumn("SRC_SYS_CD_SK", F.lit(SrcSysCdSk))
    .withColumn("UM_REF_ID", trim(strip_field(F.col("UMUM_REF_ID"))))
    .withColumn("UM_ACTVTY_SEQ_NO", F.col("UMAC_SEQ_NO"))
    .select(
        F.col("SRC_SYS_CD_SK"),
        F.col("UM_REF_ID"),
        F.col("UM_ACTVTY_SEQ_NO")
    )
)

write_files(
    df_Strip2,
    f"{adls_path}/load/B_UM_ACTVTY.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)