# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC PROCESSING:    Creates appeals activity New Directions (NDBH) data for MTM process
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC 
# MAGIC Ravi Singh          2018-11- 26            MTM-5841                      Original Programming                                                                         IntegrateDev2                Kalyan Neelam             2018-12-10

# MAGIC Pass SrcSysCd as NDBH for 
# MAGIC New Directions (NDBH) process and Apply business logic
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
# MAGIC %run ../../../../shared_containers/PrimaryKey/AplLvlSttusPkey
# COMMAND ----------

from pyspark.sql.functions import col, lit, when, concat, date_format, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
RunDate = get_widget_value('RunDate','')
SrcSysCd = get_widget_value('SrcSysCd','')

# Stage: IDS_NDBH (DB2Connector)
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
query_IDS_NDBH = f"""
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
FROM
{IDSOwner}.P_MBR_NEWDIR_APL NEWDIR_APL
INNER JOIN {IDSOwner}.APL APL ON TRIM(NEWDIR_APL.NEWDIR_APL_AUTH_ID) = TRIM(APL.APL_ID)
"""
df_IDS_NDBH = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_IDS_NDBH)
    .load()
)

# Stage: BusinessRules (CTransformerStage)
df_BusinessRules = df_IDS_NDBH.withColumn(
    "svAplCurDcsnTx",
    when(col("APL_CUR_DCSN_TX").isNull(), lit("")).otherwise(col("APL_CUR_DCSN_TX"))
)

df_BusinessRules = (
    df_BusinessRules
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", lit(0))
    .withColumn("INSRT_UPDT_CD", lit("I"))
    .withColumn("DISCARD_IN", lit("N"))
    .withColumn("PASS_THRU_IN", lit("Y"))
    .withColumn("FIRST_RECYC_DT", lit(RunDate))
    .withColumn("ERR_CT", lit(0))
    .withColumn("RECYCLE_CT", lit(0))
    .withColumn("SRC_SYS_CD", lit(SrcSysCd))
    .withColumn("PRI_KEY_STRING", concat(lit(SrcSysCd), lit(";"), col("NEWDIR_APL_AUTH_ID")))
    .withColumn("APL_LVL_STTUS_SK", lit(0))
    .withColumn("APL_ID", when(col("NEWDIR_APL_AUTH_ID").isNull(), lit("")).otherwise(col("NEWDIR_APL_AUTH_ID")))
    .withColumn("APL_LVL_SEQ_NO", lit(1))
    .withColumn("SEQ_NO", lit(1))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", lit(CurrRunCycle))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", lit(CurrRunCycle))
    .withColumn("APL_LVL_SK", when(col("NEWDIR_APL_AUTH_ID").isNull(), lit("")).otherwise(col("NEWDIR_APL_AUTH_ID")))
    .withColumn("APST_USID", lit(0))
    .withColumn("APST_USID_ROUTE", lit(0))
    .withColumn("APL_LVL_STTUS_CD", lit("CLSD"))
    .withColumn("APL_LVL_STTUS_RSN_CD", lit(1))
    .withColumn(
        "STTUS_DTM",
        when(col("PRCS_DT").isNull(), lit("")).otherwise(concat(date_format(col("PRCS_DT"), "yyyy-MM-dd HH:mm:ss"), lit(".000")))
    )
)

# Stage: AplLvlSttusPkey (CContainerStage)
params_container = {
    "CurrRunCycle": CurrRunCycle
}
df_AplLvlSttusPkey = AplLvlSttusPkey(df_BusinessRules, params_container)

# Stage: NewDirIdsAplLvlSttusExtr (CSeqFileStage)
# Apply rpad for char columns, then select columns in final order before writing
df_final = (
    df_AplLvlSttusPkey
    .withColumn("INSRT_UPDT_CD", rpad(col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", rpad(col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", rpad(col("PASS_THRU_IN"), 1, " "))
    .withColumn("APST_USID", rpad(col("APST_USID"), 10, " "))
    .withColumn("APST_USID_ROUTE", rpad(col("APST_USID_ROUTE"), 10, " "))
    .withColumn("APL_LVL_STTUS_CD", rpad(col("APL_LVL_STTUS_CD"), 2, " "))
    .withColumn("APL_LVL_STTUS_RSN_CD", rpad(col("APL_LVL_STTUS_RSN_CD"), 4, " "))
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
        "APL_LVL_STTUS_SK",
        "APL_ID",
        "APL_LVL_SEQ_NO",
        "SEQ_NO",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "APL_LVL_SK",
        "APST_USID",
        "APST_USID_ROUTE",
        "APL_LVL_STTUS_CD",
        "APL_LVL_STTUS_RSN_CD",
        "STTUS_DTM"
    )
)

file_path_out = f"{adls_path}/key/NewDirAplLvlSttusExtr.AplLvlSttus.dat.{RunID}"
write_files(
    df_final,
    file_path_out,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)