# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2010 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY :  IdsEvtExtrSeq
# MAGIC  
# MAGIC PROCESSING:   Processes the EVENT STAFF TYPE data from WEB User Info and writes out a key file to load into the IDS EVT_STAFF_TYP table.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Kalyan Neelam        2010-11-02        4529                      Initial Programming                                                                         IntegrateNewDevl       SAndrew                           12/07/2010
# MAGIC Kalyan Neelam        2011-01-26        4529                      Added Balancing Snapshot                                                           IntegrateNewDevl        Steph Goddard           02/11/2011

# MAGIC IDS EVENT STAFF TYPE Extract
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------

# COMMAND ----------

WebUserInformationOwner = get_widget_value('WebUserInformationOwner','')
webuserinformationowner_secret_name = get_widget_value('webuserinformationowner_secret_name','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
SrcSysCd = get_widget_value('SrcSysCd','')
CurrDate = get_widget_value('CurrDate','')
LastRunDateTime = get_widget_value('LastRunDateTime','')
CurrDateTimestamp = get_widget_value('CurrDateTimestamp','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
ids_secret_name = get_widget_value('ids_secret_name','')
# COMMAND ----------

jdbc_url, jdbc_props = get_db_config(webuserinformationowner_secret_name)
extract_query = f"SELECT STAFF_TYP.STAFF_TYP_CD, STAFF_TYP.STAFF_TYP_NM, STAFF_TYP.LAST_UPDT_USER_ID, STAFF_TYP.LAST_UPDT_DTM FROM {WebUserInformationOwner}.STAFF_TYP STAFF_TYP WHERE STAFF_TYP.LAST_UPDT_DTM > '{LastRunDateTime}'"
df_STAFF_TYP = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)
# COMMAND ----------

df_BusinessRules_Transform = df_STAFF_TYP.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    F.lit(CurrDate).alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    (UpCase(trim(F.col("STAFF_TYP_CD"))) + F.lit(";") + F.lit(SrcSysCd)).alias("PRI_KEY_STRING"),
    F.lit(0).alias("EVT_STAFF_TYP_SK"),
    UpCase(trim(F.col("STAFF_TYP_CD"))).alias("EVT_STAFF_TYP_ID"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.when(
        F.col("STAFF_TYP_NM").isNull() | (F.length(trim(F.col("STAFF_TYP_NM"))) == 0),
        F.lit("UNK")
    ).otherwise(UpCase(trim(F.col("STAFF_TYP_NM")))).alias("EVT_STAFF_TYP_DESC"),
    F.when(
        F.col("LAST_UPDT_DTM").isNull() | (F.length(trim(F.col("LAST_UPDT_DTM"))) == 0),
        F.lit(CurrDateTimestamp)
    ).otherwise(FORMAT.DATE(F.col("LAST_UPDT_DTM"), "SYBASE", "TIMESTAMP", "DB2TIMESTAMP")).alias("LAST_UPDT_DTM"),
    F.when(
        F.col("LAST_UPDT_USER_ID").isNull() | (F.length(trim(F.col("LAST_UPDT_USER_ID"))) == 0),
        F.lit("UNK")
    ).otherwise(UpCase(trim(F.col("LAST_UPDT_USER_ID")))).alias("LAST_UPDT_USER_ID")
)
# COMMAND ----------

df_BusinessRules_Snapshot = df_STAFF_TYP.select(
    UpCase(trim(F.col("STAFF_TYP_CD"))).alias("EVT_STAFF_TYP_ID"),
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK")
)
# COMMAND ----------

write_files(
    df_BusinessRules_Snapshot.select("EVT_STAFF_TYP_ID","SRC_SYS_CD_SK"),
    f"{adls_path}/load/B_EVT_STAFF_TYP.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)
# COMMAND ----------

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
df_hf_evt_staff_typ_lkup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", "SELECT EVT_STAFF_TYP_ID, SRC_SYS_CD, CRT_RUN_CYC_EXCTN_SK, EVT_STAFF_TYP_SK FROM IDS.dummy_hf_evt_staff_typ")
    .load()
)
# COMMAND ----------

df_join = df_BusinessRules_Transform.alias("T").join(
    df_hf_evt_staff_typ_lkup.alias("L"),
    [
        F.col("T.EVT_STAFF_TYP_ID") == F.col("L.EVT_STAFF_TYP_ID"),
        F.col("T.SRC_SYS_CD") == F.col("L.SRC_SYS_CD")
    ],
    "left"
)

df_join2 = df_join.select(
    F.col("T.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("T.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("T.DISCARD_IN").alias("DISCARD_IN"),
    F.col("T.PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("T.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("T.ERR_CT").alias("ERR_CT"),
    F.col("T.RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("T.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("T.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("T.EVT_STAFF_TYP_SK").alias("TRANS_EVT_STAFF_TYP_SK"),
    F.col("T.EVT_STAFF_TYP_ID").alias("EVT_STAFF_TYP_ID"),
    F.col("T.CRT_RUN_CYC_EXCTN_SK").alias("TRANS_CRT_RUN_CYC_EXCTN_SK"),
    F.col("T.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("TRANS_LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("T.EVT_STAFF_TYP_DESC").alias("EVT_STAFF_TYP_DESC"),
    F.col("T.LAST_UPDT_DTM").alias("TRANS_LAST_UPDT_DTM"),
    F.col("T.LAST_UPDT_USER_ID").alias("TRANS_LAST_UPDT_USER_ID"),
    F.col("L.CRT_RUN_CYC_EXCTN_SK").alias("lkup_CRT_RUN_CYC_EXCTN_SK"),
    F.col("L.EVT_STAFF_TYP_SK").alias("lkup_EVT_STAFF_TYP_SK")
)

df_join3 = df_join2.withColumn(
    "is_new",
    F.when(F.col("lkup_EVT_STAFF_TYP_SK").isNull(), F.lit(True)).otherwise(F.lit(False))
).withColumn(
    "EVT_STAFF_TYP_SK",
    F.col("lkup_EVT_STAFF_TYP_SK")
)

df_enriched = SurrogateKeyGen(df_join3,<DB sequence name>,"EVT_STAFF_TYP_SK",<schema>,<secret_name>)

df_Key = df_enriched.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("EVT_STAFF_TYP_SK"),
    F.col("EVT_STAFF_TYP_ID"),
    F.when(F.col("is_new"), F.lit(CurrRunCycle)).otherwise(F.col("lkup_CRT_RUN_CYC_EXCTN_SK")).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("EVT_STAFF_TYP_DESC"),
    F.col("TRANS_LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
    F.col("TRANS_LAST_UPDT_USER_ID").alias("LAST_UPDT_USER_ID")
)
# COMMAND ----------

write_files(
    df_Key.select(
        "JOB_EXCTN_RCRD_ERR_SK",
        "INSRT_UPDT_CD",
        "DISCARD_IN",
        "PASS_THRU_IN",
        "FIRST_RECYC_DT",
        "ERR_CT",
        "RECYCLE_CT",
        "SRC_SYS_CD",
        "PRI_KEY_STRING",
        "EVT_STAFF_TYP_SK",
        "EVT_STAFF_TYP_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "EVT_STAFF_TYP_DESC",
        "LAST_UPDT_DTM",
        "LAST_UPDT_USER_ID"
    ),
    f"{adls_path}/key/WebUserInfoEvtStaffTypExtr.EvtStaffTyp.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)
# COMMAND ----------

df_updt = df_enriched.filter(
    F.col("lkup_EVT_STAFF_TYP_SK").isNull()
).select(
    F.col("EVT_STAFF_TYP_ID"),
    F.col("SRC_SYS_CD"),
    F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("EVT_STAFF_TYP_SK")
)

df_updt.write.format("jdbc") \
  .option("url", jdbc_url_ids) \
  .options(**jdbc_props_ids) \
  .option("dbtable", "IDS.dummy_hf_evt_staff_typ") \
  .mode("append") \
  .save()