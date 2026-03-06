# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2010 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY :  IdsEvtExtrSeq
# MAGIC  
# MAGIC PROCESSING:   Processes the EVENT APPT RSN data from WEB User Info and writes out a key file to load into the IDS EVT_APPT_RSN table.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Kalyan Neelam        2011-01-03        4529                      Initial Programming                                                                       IntegrateNewDevl          Steph Goddard          01/10/2011
# MAGIC Kalyan Neelam        2011-01-26        4529                      Added Balancing Snapshot                                                         IntegrateNewDevl          Steph Goddard          02/11/2011

# MAGIC IDS EVENT APPT RSN Extract
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


WebUserInformationOwner = get_widget_value('WebUserInformationOwner','$PROJDEF')
webuserinformation_secret_name = get_widget_value('webuserinformation_secret_name','')
CurrRunCycle = get_widget_value('CurrRunCycle','100')
RunID = get_widget_value('RunID','100')
SrcSysCd = get_widget_value('SrcSysCd','WEBUSERINFO')
CurrDate = get_widget_value('CurrDate','2010-11-04')
LastRunDateTime = get_widget_value('LastRunDateTime','2010-09-01')
CurrDateTimestamp = get_widget_value('CurrDateTimestamp','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')

jdbc_url, jdbc_props = get_db_config(webuserinformation_secret_name)
extract_query = f"SELECT EVT_APPT_RSN.EVT_APPT_RSN_ID, EVT_APPT_RSN.EVT_TYP_CD, EVT_APPT_RSN.EVT_APPT_RSN_NM, EVT_APPT_RSN.EFF_DT, EVT_APPT_RSN.TERM_DT, EVT_APPT_RSN.EVT_APPT_RSN_DESC, EVT_APPT_RSN.LAST_UPDT_USER_ID, EVT_APPT_RSN.LAST_UPDT_DTM FROM {WebUserInformationOwner}.EVT_APPT_RSN EVT_APPT_RSN WHERE EVT_APPT_RSN.LAST_UPDT_DTM > '{LastRunDateTime}'"
df_EVT_APPT_RSN = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
    .select(
        F.col("EVT_APPT_RSN_ID"),
        F.col("EVT_TYP_CD"),
        F.col("EVT_APPT_RSN_NM"),
        F.col("EFF_DT"),
        F.col("TERM_DT"),
        F.col("EVT_APPT_RSN_DESC"),
        F.col("LAST_UPDT_USER_ID"),
        F.col("LAST_UPDT_DTM")
    )
)

df_BusinessRules = (
    df_EVT_APPT_RSN.withColumn(
        "svEffDt",
        F.when(
            F.col("EFF_DT").isNull() | (F.length(F.trim(F.col("EFF_DT"))) == 0),
            F.lit("1753-01-01")
        ).otherwise(F.trim(F.col("EFF_DT")))
    )
)

df_Transform = df_BusinessRules.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    F.lit(CurrDate).alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.concat_ws(
        ";",
        F.upper(F.trim(F.col("EVT_TYP_CD"))),
        F.upper(F.trim(F.col("EVT_APPT_RSN_NM"))),
        F.concat(F.lit(" "), F.col("svEffDt")),
        F.lit(SrcSysCd)
    ).alias("PRI_KEY_STRING"),
    F.lit(0).alias("EVT_APPT_RSN_SK"),
    F.upper(F.trim(F.col("EVT_TYP_CD"))).alias("EVT_TYP_ID"),
    F.upper(F.trim(F.col("EVT_APPT_RSN_NM"))).alias("EVT_APPT_RSN_NM"),
    F.col("svEffDt").alias("EFF_DT_SK"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.when(
        F.col("LAST_UPDT_DTM").isNull() | (F.length(F.trim(F.col("LAST_UPDT_DTM"))) == 0),
        F.lit(CurrDateTimestamp)
    ).otherwise(F.trim(F.col("LAST_UPDT_DTM"))).alias("LAST_UPDT_DTM"),
    F.when(
        F.col("TERM_DT").isNull() | (F.length(F.trim(F.col("TERM_DT"))) == 0),
        F.lit("2199-12-31")
    ).otherwise(F.trim(F.col("TERM_DT"))).alias("TERM_DT_SK"),
    F.upper(F.trim(F.col("EVT_APPT_RSN_DESC"))).alias("EVT_APPT_RSN_DESC"),
    F.col("EVT_APPT_RSN_ID").alias("EVT_APPT_RSN_ID"),
    F.when(
        F.col("LAST_UPDT_USER_ID").isNull() | (F.length(F.trim(F.col("LAST_UPDT_USER_ID"))) == 0),
        F.lit("UNK")
    ).otherwise(F.upper(F.trim(F.col("LAST_UPDT_USER_ID")))).alias("LAST_UPDT_USER_ID")
)

df_Snapshot = df_BusinessRules.select(
    F.upper(F.trim(F.col("EVT_TYP_CD"))).alias("EVT_TYP_ID"),
    F.upper(F.trim(F.col("EVT_APPT_RSN_NM"))).alias("EVT_APPT_RSN_NM"),
    F.col("svEffDt").alias("EFF_DT_SK"),
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK")
)

write_files(
    df_Snapshot.select("EVT_TYP_ID","EVT_APPT_RSN_NM","EFF_DT_SK","SRC_SYS_CD_SK"),
    f"{adls_path}/load/B_EVT_APPT_RSN.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_hf_evt_appt_rsn_lkup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query","SELECT EVT_TYP_ID, EVT_APPT_RSN_NM, EFF_DT_SK, SRC_SYS_CD, CRT_RUN_CYC_EXCTN_SK, EVT_APPT_RSN_SK FROM dummy_hf_evt_appt_rsn")
    .load()
    .select(
        F.col("EVT_TYP_ID"),
        F.col("EVT_APPT_RSN_NM"),
        F.col("EFF_DT_SK"),
        F.col("SRC_SYS_CD"),
        F.col("CRT_RUN_CYC_EXCTN_SK"),
        F.col("EVT_APPT_RSN_SK")
    )
)

df_joined = (
    df_Transform.alias("Transform")
    .join(
        df_hf_evt_appt_rsn_lkup.alias("lkup"),
        on=[
            F.col("Transform.EVT_TYP_ID") == F.col("lkup.EVT_TYP_ID"),
            F.col("Transform.EVT_APPT_RSN_NM") == F.col("lkup.EVT_APPT_RSN_NM"),
            F.col("Transform.EFF_DT_SK") == F.col("lkup.EFF_DT_SK"),
            F.col("Transform.SRC_SYS_CD") == F.col("lkup.SRC_SYS_CD")
        ],
        how="left"
    )
)

df_enriched = df_joined.withColumn(
    "EVT_APPT_RSN_SK",
    F.col("lkup.EVT_APPT_RSN_SK")
)
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"EVT_APPT_RSN_SK",<schema>,<secret_name>)
df_enriched = df_enriched.withColumn(
    "CRT_RUN_CYC_EXCTN_SK",
    F.when(F.col("lkup.EVT_APPT_RSN_SK").isNull(), F.lit(CurrRunCycle)).otherwise(F.col("lkup.CRT_RUN_CYC_EXCTN_SK"))
).withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    F.lit(CurrRunCycle)
)

df_Key = df_enriched.select(
    F.col("Transform.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("Transform.INSRT_UPDT_CD"),10," ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("Transform.DISCARD_IN"),1," ").alias("DISCARD_IN"),
    F.rpad(F.col("Transform.PASS_THRU_IN"),1," ").alias("PASS_THRU_IN"),
    F.col("Transform.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("Transform.ERR_CT").alias("ERR_CT"),
    F.col("Transform.RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Transform.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("EVT_APPT_RSN_SK").alias("EVT_APPT_RSN_SK"),
    F.col("Transform.EVT_TYP_ID").alias("EVT_TYP_ID"),
    F.col("Transform.EVT_APPT_RSN_NM").alias("EVT_APPT_RSN_NM"),
    F.rpad(F.col("Transform.EFF_DT_SK"),10," ").alias("EFF_DT_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Transform.LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
    F.rpad(F.col("Transform.TERM_DT_SK"),10," ").alias("TERM_DT_SK"),
    F.col("Transform.EVT_APPT_RSN_DESC").alias("EVT_APPT_RSN_DESC"),
    F.col("Transform.EVT_APPT_RSN_ID").alias("EVT_APPT_RSN_ID"),
    F.col("Transform.LAST_UPDT_USER_ID").alias("LAST_UPDT_USER_ID")
)

write_files(
    df_Key,
    f"{adls_path}/key/WebUserInfoEvtApptRsnExtr.EvtApptRsn.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_updt = df_enriched.filter(F.col("lkup.EVT_APPT_RSN_SK").isNull()).select(
    F.col("Transform.EVT_TYP_ID").alias("EVT_TYP_ID"),
    F.col("Transform.EVT_APPT_RSN_NM").alias("EVT_APPT_RSN_NM"),
    F.col("Transform.EFF_DT_SK").alias("EFF_DT_SK"),
    F.col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("EVT_APPT_RSN_SK").alias("EVT_APPT_RSN_SK")
)

spark.sql("DROP TABLE IF EXISTS STAGING.WebUserInfoEvtApptRsnExtr_hf_evt_appt_rsn_updt_temp")
(
    df_updt.write.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable","STAGING.WebUserInfoEvtApptRsnExtr_hf_evt_appt_rsn_updt_temp")
    .mode("overwrite")
    .save()
)
merge_sql = """
MERGE dummy_hf_evt_appt_rsn AS T
USING STAGING.WebUserInfoEvtApptRsnExtr_hf_evt_appt_rsn_updt_temp AS S
ON
    T.EVT_TYP_ID = S.EVT_TYP_ID
    AND T.EVT_APPT_RSN_NM = S.EVT_APPT_RSN_NM
    AND T.EFF_DT_SK = S.EFF_DT_SK
    AND T.SRC_SYS_CD = S.SRC_SYS_CD
WHEN MATCHED THEN
    UPDATE SET
        T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK,
        T.EVT_APPT_RSN_SK = S.EVT_APPT_RSN_SK
WHEN NOT MATCHED THEN
    INSERT (EVT_TYP_ID, EVT_APPT_RSN_NM, EFF_DT_SK, SRC_SYS_CD, CRT_RUN_CYC_EXCTN_SK, EVT_APPT_RSN_SK)
    VALUES (S.EVT_TYP_ID, S.EVT_APPT_RSN_NM, S.EFF_DT_SK, S.SRC_SYS_CD, S.CRT_RUN_CYC_EXCTN_SK, S.EVT_APPT_RSN_SK);
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)