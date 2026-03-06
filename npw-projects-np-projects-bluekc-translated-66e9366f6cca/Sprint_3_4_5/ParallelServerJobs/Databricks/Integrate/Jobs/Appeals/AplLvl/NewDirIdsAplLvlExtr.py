# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC PROCESSING:    Creates appeals activity  New Directions (NDBH)data for MTM process
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC 
# MAGIC Ravi Singh             2018-11- 29            MTM-5841                      Original Programming                                                                              IntegrateDev2         Kalyan Neelam            2018-12-10

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
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../shared_containers/PrimaryKey/AplLvlPkey

CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
RunDate = get_widget_value('RunDate','')
SrcSysCd = get_widget_value('SrcSysCd','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query = """SELECT
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
FROM """ + IDSOwner + """.P_MBR_NEWDIR_APL NEWDIR_APL
INNER JOIN """ + IDSOwner + """.APL APL 
ON TRIM(NEWDIR_APL.NEWDIR_APL_AUTH_ID) = TRIM(APL.APL_ID)
"""

df_IDS_NDBH = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_BusinessRules = (
    df_IDS_NDBH
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", F.lit(0))
    .withColumn("INSRT_UPDT_CD", F.lit("I"))
    .withColumn("DISCARD_IN", F.lit("N"))
    .withColumn("PASS_THRU_IN", F.lit("Y"))
    .withColumn("FIRST_RECYC_DT", F.lit(RunDate))
    .withColumn("ERR_CT", F.lit(0))
    .withColumn("RECYCLE_CT", F.lit(0))
    .withColumn("SRC_SYS_CD", F.lit(SrcSysCd))
    .withColumn("PRI_KEY_STRING", F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("NEWDIR_APL_AUTH_ID")))
    .withColumn("APL_LVL_SK", F.lit(0))
    .withColumn(
        "APL_ID",
        F.when(F.col("NEWDIR_APL_AUTH_ID").isNull(), "").otherwise(F.col("NEWDIR_APL_AUTH_ID"))
    )
    .withColumn("SEQ_NO", F.lit(1))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(CurrRunCycle))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(CurrRunCycle))
    .withColumn(
        "APL_SK",
        F.when(
            F.col("NEWDIR_APL_AUTH_ID").isNull() | (F.length(trim(F.col("NEWDIR_APL_AUTH_ID"))) == 0),
            F.lit("NA")
        ).otherwise(F.col("NEWDIR_APL_AUTH_ID"))
    )
    .withColumn(
        "CRT_USER_SK",
        F.when(
            F.col("CRT_USER_SK").isNull() | (F.length(trim(F.col("CRT_USER_SK"))) == 0),
            F.lit(0)
        ).otherwise(F.col("CRT_USER_SK"))
    )
    .withColumn(
        "LAST_UPDT_USER_SK",
        F.when(
            F.col("LAST_UPDT_USER_SK").isNull() | (F.length(trim(F.col("LAST_UPDT_USER_SK"))) == 0),
            F.lit(0)
        ).otherwise(F.col("LAST_UPDT_USER_SK"))
    )
    .withColumn("PRI_USER_SK", F.lit(0))
    .withColumn("SEC_USER_SK", F.lit(0))
    .withColumn("TRTY_USER_SK", F.lit(0))
    .withColumn("APL_LVL_CD_SK", F.lit("1STAPPEAL"))
    .withColumn("APL_LVL_CUR_STTUS_CD_SK", F.lit("CLSD"))
    .withColumn(
        "APL_LVL_DCSN_CD_SK",
        F.when(
            F.col("APL_CUR_DCSN_TX").isNull() | (F.length(trim(F.col("APL_CUR_DCSN_TX"))) == 0),
            F.lit("UNK")
        ).otherwise(F.col("APL_CUR_DCSN_TX_SPACE"))
    )
    .withColumn("APL_LVL_DCSN_RSN_CD_SK", F.lit(1))
    .withColumn("APL_LVL_DSPT_RSLTN_TYP_CD_SK", F.lit(1))
    .withColumn("APL_LVL_INITN_METH_CD_SK", F.lit(1))
    .withColumn("APL_LVL_LATE_DCSN_RSN_CD_SK", F.lit(1))
    .withColumn("APL_LVL_NTFCTN_CAT_CD_SK", F.lit(1))
    .withColumn("APL_LVL_NTFCTN_METH_CD_SK", F.lit(1))
    .withColumn("EXPDTD_IN", F.lit("N"))
    .withColumn("HRNG_IN", F.lit("N"))
    .withColumn("INITN_DT_SK", FORMAT_DATE(F.col("APL_RQST_DTM"), "SYBASE", "TIMESTAMP", "DATE"))
    .withColumn(
        "CRT_DTM",
        F.concat(
            FORMAT_DATE(F.col("NEWDIR_APL_RCVD_DT"), "DATE", "DATE", "SYBTIMESTAMP"),
            F.lit("000")
        )
    )
    .withColumn("CUR_STTUS_DTM", F.col("PRCS_DT"))
    .withColumn(
        "DCSN_DT_SK",
        FORMAT_DATE(F.col("APL_DCSN_LTR_SENT_DTM"), "SYBASE", "TIMESTAMP", "DATE")
    )
    .withColumn("HRNG_DT_SK", F.lit("1753-01-01"))
    .withColumn(
        "LAST_UPDT_DTM",
        F.concat(
            FORMAT_DATE(F.col("PRCS_DT"), "DATE", "DATE", "SYBTIMESTAMP"),
            F.lit("000")
        )
    )
    .withColumn(
        "NTFCTN_DT_SK",
        FORMAT_DATE(F.col("APL_ACKNMT_LTR_SENT_DTM"), "SYBASE", "TIMESTAMP", "DATE")
    )
    .withColumn("CUR_STTUS_SEQ_NO", F.lit(1))
    .withColumn("LVL_DESC", F.concat(F.col("APL_DENIAL_TYP_TX"), F.lit(","), F.col("APL_STTUS_TX")))
)

params_container = {
    "CurrRunCycle": CurrRunCycle
}

df_AplLvlPkey = AplLvlPkey(df_BusinessRules, params_container)

df_final = df_AplLvlPkey.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "APL_LVL_SK",
    "APL_ID",
    "SEQ_NO",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "APL_SK",
    "CRT_USER_SK",
    "LAST_UPDT_USER_SK",
    "PRI_USER_SK",
    "SEC_USER_SK",
    "TRTY_USER_SK",
    "APL_LVL_CD_SK",
    "APL_LVL_CUR_STTUS_CD_SK",
    "APL_LVL_DCSN_CD_SK",
    "APL_LVL_DCSN_RSN_CD_SK",
    "APL_LVL_DSPT_RSLTN_TYP_CD_SK",
    "APL_LVL_INITN_METH_CD_SK",
    "APL_LVL_LATE_DCSN_RSN_CD_SK",
    "APL_LVL_NTFCTN_CAT_CD_SK",
    "APL_LVL_NTFCTN_METH_CD_SK",
    "EXPDTD_IN",
    "HRNG_IN",
    "INITN_DT_SK",
    "CRT_DTM",
    "CUR_STTUS_DTM",
    "DCSN_DT_SK",
    "HRNG_DT_SK",
    "LAST_UPDT_DTM",
    "NTFCTN_DT_SK",
    "CUR_STTUS_SEQ_NO",
    "LVL_DESC"
).withColumn("INSRT_UPDT_CD", F.rpad(F.col("INSRT_UPDT_CD"), 10, " ")) \
 .withColumn("DISCARD_IN", F.rpad(F.col("DISCARD_IN"), 1, " ")) \
 .withColumn("PASS_THRU_IN", F.rpad(F.col("PASS_THRU_IN"), 1, " ")) \
 .withColumn("EXPDTD_IN", F.rpad(F.col("EXPDTD_IN"), 1, " ")) \
 .withColumn("HRNG_IN", F.rpad(F.col("HRNG_IN"), 1, " ")) \
 .withColumn("INITN_DT_SK", F.rpad(F.col("INITN_DT_SK"), 10, " ")) \
 .withColumn("DCSN_DT_SK", F.rpad(F.col("DCSN_DT_SK"), 10, " ")) \
 .withColumn("HRNG_DT_SK", F.rpad(F.col("HRNG_DT_SK"), 10, " ")) \
 .withColumn("NTFCTN_DT_SK", F.rpad(F.col("NTFCTN_DT_SK"), 10, " "))

final_file_path = f"{adls_path}/key/NewDirAplLvlExtr.AplLvl.dat.{RunID}"

write_files(
    df_final,
    final_file_path,
    delimiter=',',
    mode='overwrite',
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)