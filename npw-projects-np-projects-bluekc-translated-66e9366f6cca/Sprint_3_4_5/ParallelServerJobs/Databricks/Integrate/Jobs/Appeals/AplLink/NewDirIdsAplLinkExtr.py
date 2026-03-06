# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC PROCESSING:    Creates appeals link Evicore data for MTM process
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC 
# MAGIC Ravi Singh               2018-11-27            MTM-5841                      Original Programming                                                                         IntegrateDev2            Kalyan Neelam            2018-12-10

# MAGIC Pass SrcSysCd as NDBH for New Direction process and Apply business logic
# MAGIC Writing Sequential File to /pkey
# MAGIC Read the data from table P_MBR_NEWDIR_APL
# MAGIC This container is used in:
# MAGIC Evicore,NDBH and Telligen Appeals
# MAGIC ......Extr
# MAGIC 
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import lit, when, length, col, concat, rpad
from pyspark.sql.types import *
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# Parameters
CurrRunCycle = get_widget_value("CurrRunCycle","")
RunID = get_widget_value("RunID","")
IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")
RunDate = get_widget_value("RunDate","")
SrcSysCd = get_widget_value("SrcSysCd","")

# Database configuration
jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# Read from DB2Connector Stage: IDS_NDBH
extract_query = f"""
SELECT
NEWDIR_APL.SUB_ID,
NEWDIR_APL.MBR_FULL_NM,
NEWDIR_APL.MBR_BRTH_DT,
NEWDIR_APL.NEWDIR_APL_AUTH_ID,
NEWDIR_APL.NEWDIR_APL_RCVD_DT,
NEWDIR_APL.PRCS_DT,
NEWDIR_APL.MBR_FIRST_NM,
NEWDIR_APL.MBR_MIDINIT,
NEWDIR_APL.MBR_LAST_NM,
NEWDIR_APL.MBR_AGE,
NEWDIR_APL.PROC_AUTH_UNIT_CT,
NEWDIR_APL.NEWDIR_CO_NM,
NEWDIR_APL.APL_DCSN_LTR_SENT_DTM,
NEWDIR_APL.APL_CUR_DCSN_TX,
REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(NEWDIR_APL.APL_CUR_DCSN_TX,'-',''),'&',''),'.',''),' ',''),'''',''),'\"',''),',','') APL_CUR_DCSN_TX_SPACE,
REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(NEWDIR_APL.APL_CAT_TX,'-',''),'&',''),'.',''),' ',''),'''',''),'\"',''),',','') APL_CAT_TX,
NEWDIR_APL.APL_DENIAL_STG_TX,
REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(NEWDIR_APL.APL_DENIAL_STG_TX,'-',''),'&',''),'.',''),' ',''),'''',''),'\"',''),',','') APL_DENIAL_STG_TX_SPACE,
NEWDIR_APL.APL_DENIAL_TYP_TX,
NEWDIR_APL.APL_DENIAL_PHYS_RVWR_ID,
NEWDIR_APL.APL_DENIAL_PHYS_RVWR_FULL_NM,
NEWDIR_APL.APL_STTUS_TX,
NEWDIR_APL.UM_SVC_STRT_DT,
NEWDIR_APL.UM_SVC_END_DT,
NEWDIR_APL.FNCL_LOB_TX,
NEWDIR_APL.FNCL_LOB_DESC,
NEWDIR_APL.APL_NOTE_TX,
NEWDIR_APL.NEWDIR_LAST_UPDT_USER_ID,
NEWDIR_APL.NEWDIR_LAST_UPDT_USER_FULL_NM,
NEWDIR_APL.PROC_RQST_UNIT_CT,
NEWDIR_APL.APL_RQST_DTM,
NEWDIR_APL.PROC_RVSED_UNIT_CT,
NEWDIR_APL.APL_RVW_TYP_TX,
NEWDIR_APL.APL_SVC_DESC,
NEWDIR_APL.APL_SVC_TX,
NEWDIR_APL.APL_DENIAL_SUBTYP_TX,
NEWDIR_APL.APL_SUBMTR_TYP_TX,
NEWDIR_APL.APL_ACKNMT_LTR_SENT_DTM,
APL.CRT_USER_SK,
APL.LAST_UPDT_USER_SK,
APL.LAST_UPDT_DTM
FROM {IDSOwner}.P_MBR_NEWDIR_APL NEWDIR_APL
INNER JOIN {IDSOwner}.APL APL
  ON TRIM(NEWDIR_APL.NEWDIR_APL_AUTH_ID) = TRIM(APL.APL_ID)
"""

df_IDS_NDBH = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# BusinessRules Transformer Stage
df_BusinessRules = df_IDS_NDBH.select(
    lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    lit("I").alias("INSRT_UPDT_CD"),
    lit("N").alias("DISCARD_IN"),
    lit("Y").alias("PASS_THRU_IN"),
    lit(RunDate).alias("FIRST_RECYC_DT"),
    lit(0).alias("ERR_CT"),
    lit(0).alias("RECYCLE_CT"),
    lit(SrcSysCd).alias("SRC_SYS_CD"),
    concat(lit(SrcSysCd), lit(";"), col("NEWDIR_APL_AUTH_ID")).alias("PRI_KEY_STRING"),
    lit(0).alias("APL_LINK_SK"),
    when(col("NEWDIR_APL_AUTH_ID").isNull(), "").otherwise(col("NEWDIR_APL_AUTH_ID")).alias("APL_ID"),
    lit(0).alias("APL_LINK_TYP_CD_SK"),
    lit("UNKNOWN").alias("APL_LINK_ID"),
    lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    when((length(trim(col("NEWDIR_APL_AUTH_ID"))) == 0) | col("NEWDIR_APL_AUTH_ID").isNull(), "NA")
      .otherwise(col("NEWDIR_APL_AUTH_ID")).alias("APL_SK"),
    when((length(trim(col("LAST_UPDT_USER_SK"))) == 0) | col("LAST_UPDT_USER_SK").isNull(), lit(0))
      .otherwise(col("LAST_UPDT_USER_SK")).alias("LAST_UPDT_USER_SK"),
    lit(0).alias("APL_LINK_RSN_CD_SK"),
    col("LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
    lit(None).alias("APL_LINK_DESC"),
    lit(0).alias("CASE_MGT_SK"),
    lit(0).alias("CLM_SK"),
    lit(0).alias("CUST_SVC_SK"),
    lit(1).alias("REL_APL_SK"),
    lit(0).alias("UM_SK")
)

# Shared Container: AplLinkPkey
# MAGIC %run ../../../../shared_containers/PrimaryKey/AplLinkPkey
# COMMAND ----------

params = {
    "CurrRunCycle": CurrRunCycle
}
df_key = AplLinkPkey(df_BusinessRules, params)

# NewDirIdsAplLinkExtr: CSeqFileStage
df_final = (
    df_key
    .withColumn("INSRT_UPDT_CD", rpad(col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", rpad(col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", rpad(col("PASS_THRU_IN"), 1, " "))
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
        "APL_LINK_SK",
        "APL_ID",
        "APL_LINK_TYP_CD_SK",
        "APL_LINK_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "APL_SK",
        "CASE_MGT_SK",
        "CLM_SK",
        "CUST_SVC_SK",
        "LAST_UPDT_USER_SK",
        "REL_APL_SK",
        "UM_SK",
        "APL_LINK_RSN_CD_SK",
        "LAST_UPDT_DTM",
        "APL_LINK_DESC"
    )
)

write_files(
    df_final,
    f"{adls_path}/key/NewDirAplLinkExtr.AplLink.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)