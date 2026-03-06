# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC PROCESSING:    Creates appeals NDBH data
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC 
# MAGIC Ravi Singh          2018-08-26              MCAS-5796                             Original Programming                                                                         IntegrateDev1      Kalyan Neelam             2018-09-24

# MAGIC Perform member matching for all 5 pass for New Direction Apl Process
# MAGIC Extract NDBH data
# MAGIC New Direction File
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, TimestampType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


ids_secret_name = get_widget_value('ids_secret_name','')
IDSOwner = get_widget_value('IDSOwner','')
SrcSysCd = get_widget_value('SrcSysCd','')
RunID = get_widget_value('RunID','')
RunDate = get_widget_value('RunDate','')
CurrRunCycle = get_widget_value('CurrRunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

query_db2_Newdir_Extract = f"""
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
A.CRT_USER_SK,
A.PRI_RESP_USER_SK,
A.UM_CLS_PLN_PROD_CAT_CD_SK,
A.FLG as FLG,
A.GRP_SK,
A.MBR_SK,
A.SUBGRP_SK,
A.PROD_SH_NM_SK,
A.SUB_SK,
CASE WHEN NEWDIR_APL.APL_RVW_TYP_TX LIKE '% URGENT%' THEN 'Y' ELSE 'N' END AS CUR_APL_LVL_EXPDTD_IN,
A.GRP_ID,
A.MBR_UNIQ_KEY,
A.SUBGRP_ID,
A.SUB_UNIQ_KEY,
A.USER_ID,
A.PROD_SK,
A.SUB_ADDR_ST_CD_SK,
A.PROD_SH_NM_DLVRY_METH_CD
FROM
{IDSOwner}.P_MBR_NEWDIR_APL NEWDIR_APL
LEFT OUTER JOIN
(
SELECT
DISTINCT
UM.UM_REF_ID,
UM.CRT_USER_SK,
UM.GRP_SK,
UM.PRI_RESP_USER_SK,
UM.MBR_SK,
PROD.PROD_SH_NM_SK,
UM.SUBGRP_SK,
UM.SUB_SK,
UM.UM_CLS_PLN_PROD_CAT_CD_SK,
'1' AS FLG,
GRP.GRP_ID,
MBR.MBR_UNIQ_KEY,
SUBGRP.SUBGRP_ID,
SUB.SUB_UNIQ_KEY,
APP_USER.USER_ID,
PROD.PROD_SK,
SUB_ADDR.SUB_ADDR_ST_CD_SK,
CASE WHEN CD2.TRGT_CD = 'PPO' THEN 'MEMBER' ELSE 'PROVIDER' END PROD_SH_NM_DLVRY_METH_CD
FROM {IDSOwner}.UM UM
INNER JOIN {IDSOwner}.UM_SVC UM_SVC ON UM.UM_SK = UM_SVC.UM_SK
INNER JOIN {IDSOwner}.UM_SVC_STTUS UM_SVC_STTUS ON UM_SVC.UM_SVC_SK = UM_SVC_STTUS.UM_SVC_SK
INNER JOIN {IDSOwner}.PROD PROD ON UM.PROD_SK = PROD.PROD_SK
INNER JOIN {IDSOwner}.GRP GRP ON UM.GRP_SK = GRP.GRP_SK
INNER JOIN {IDSOwner}.MBR MBR ON UM.MBR_SK = MBR.MBR_SK
INNER JOIN {IDSOwner}.SUBGRP SUBGRP ON SUBGRP.SUBGRP_SK=UM.SUBGRP_SK
INNER JOIN {IDSOwner}.SUB SUB ON UM.SUB_SK = SUB.SUB_SK
INNER JOIN {IDSOwner}.CD_MPPNG CD ON CD.CD_MPPNG_SK = UM.UM_CLS_PLN_PROD_CAT_CD_SK
INNER JOIN {IDSOwner}.APP_USER APP_USER ON APP_USER.USER_SK=UM.CRT_USER_SK
INNER JOIN {IDSOwner}.SUB_ADDR SUB_ADDR ON SUB_ADDR.SUB_SK=UM.SUB_SK
INNER JOIN {IDSOwner}.PROD_SH_NM PROD_SH_NM ON PROD.PROD_SH_NM_SK=PROD_SH_NM.PROD_SH_NM_SK
INNER JOIN {IDSOwner}.CD_MPPNG CD2 ON CD2.CD_MPPNG_SK=PROD_SH_NM.PROD_SH_NM_DLVRY_METH_CD_SK
) A
ON NEWDIR_APL.NEWDIR_APL_AUTH_ID = A.UM_REF_ID
"""

df_db2_Newdir_Extract = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_db2_Newdir_Extract)
    .load()
)

query_MEM_DATA_2 = f"""
SELECT
MBR.MBR_UNIQ_KEY,
MBR.FIRST_NM,
MBR.LAST_NM,
MBR.BRTH_DT_SK,
CD.TRGT_CD GNDR_CD,
GRP.GRP_ID,
SUB.SUB_ID,
MBR.MBR_SFX_NO,
ENR.EFF_DT_SK,
ENR.TERM_DT_SK,
ENR.GRP_SK,
MBR.MBR_SK,
ENR.SUBGRP_SK,
ENR.PROD_SK,
PRD.PROD_SH_NM_SK,
MBR.SUB_SK,
SUBGRP.SUBGRP_ID,
SUB.SUB_UNIQ_KEY,
ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK,
SUB_ADDR.SUB_ADDR_ST_CD_SK,
CASE WHEN CD2.TRGT_CD = 'PPO' THEN 'MEMBER' ELSE 'PROVIDER' END PROD_SH_NM_DLVRY_METH_CD
FROM {IDSOwner}.P_MBR_NEWDIR_APL APL
INNER JOIN {IDSOwner}.SUB SUB ON APL.SUB_ID = SUB.SUB_ID
INNER JOIN {IDSOwner}.MBR MBR ON MBR.SUB_SK = SUB.SUB_SK
INNER JOIN {IDSOwner}.MBR_ENR ENR ON MBR.MBR_SK = ENR.MBR_SK
  AND ENR.EFF_DT_SK <= APL.NEWDIR_APL_RCVD_DT
  AND ENR.TERM_DT_SK >= APL.NEWDIR_APL_RCVD_DT
INNER JOIN {IDSOwner}.CD_MPPNG CD ON CD.CD_MPPNG_SK = ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK
INNER JOIN {IDSOwner}.PROD PRD ON ENR.PROD_SK = PRD.PROD_SK
INNER JOIN {IDSOwner}.GRP GRP ON SUB.GRP_SK = GRP.GRP_SK
INNER JOIN {IDSOwner}.SUBGRP SUBGRP ON SUBGRP.SUBGRP_SK=ENR.SUBGRP_SK
INNER JOIN {IDSOwner}.PROD_SH_NM PROD_SH_NM on PRD.PROD_SH_NM_SK=PROD_SH_NM.PROD_SH_NM_SK
INNER JOIN {IDSOwner}.CD_MPPNG CD2 ON CD2.CD_MPPNG_SK=PROD_SH_NM.PROD_SH_NM_DLVRY_METH_CD_SK
INNER JOIN {IDSOwner}.MBR_ADDR_TYP MBR_ADDR_TYP ON  MBR_ADDR_TYP.MBR_SK=MBR.MBR_SK
INNER JOIN {IDSOwner}.CD_MPPNG CD1 ON CD1.CD_MPPNG_SK=MBR_ADDR_TYP.MBR_ADDR_ROLE_TYP_CD_SK AND CD1.TRGT_CD = 'HOME'
INNER JOIN {IDSOwner}.SUB_ADDR SUB_ADDR ON SUB_ADDR.SUB_ADDR_SK=MBR_ADDR_TYP.SUB_ADDR_SK
WHERE
CD.TRGT_CD = 'MED'
AND ENR.ELIG_IN = 'Y'
AND MBR.BRTH_DT_SK = APL.MBR_BRTH_DT
AND MBR.FIRST_NM = APL.MBR_FIRST_NM
AND MBR.LAST_NM = APL.MBR_LAST_NM
"""

df_MEM_DATA_2 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_MEM_DATA_2)
    .load()
)

query_MEM_DATA_3 = f"""
SELECT
MBR.MBR_UNIQ_KEY,
MBR.FIRST_NM,
MBR.LAST_NM,
MBR.BRTH_DT_SK,
CD.TRGT_CD GNDR_CD,
GRP.GRP_ID,
SUB.SUB_ID,
MBR.MBR_SFX_NO,
ENR.EFF_DT_SK,
ENR.TERM_DT_SK,
ENR.GRP_SK,
MBR.MBR_SK,
ENR.SUBGRP_SK,
ENR.PROD_SK,
PRD.PROD_SH_NM_SK,
MBR.SUB_SK,
SUBGRP.SUBGRP_ID,
SUB.SUB_UNIQ_KEY,
ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK,
SUB_ADDR.SUB_ADDR_ST_CD_SK,
CASE WHEN CD2.TRGT_CD = 'PPO' THEN 'MEMBER' ELSE 'PROVIDER' END PROD_SH_NM_DLVRY_METH_CD
FROM {IDSOwner}.P_MBR_NEWDIR_APL APL
INNER JOIN {IDSOwner}.SUB SUB ON APL.SUB_ID = SUB.SUB_ID
INNER JOIN {IDSOwner}.MBR MBR ON MBR.SUB_SK = SUB.SUB_SK
INNER JOIN {IDSOwner}.MBR_ENR ENR ON MBR.MBR_SK = ENR.MBR_SK
  AND ENR.EFF_DT_SK <= APL.NEWDIR_APL_RCVD_DT
  AND ENR.TERM_DT_SK >= APL.NEWDIR_APL_RCVD_DT
INNER JOIN {IDSOwner}.CD_MPPNG CD ON CD.CD_MPPNG_SK = ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK
INNER JOIN {IDSOwner}.PROD PRD ON ENR.PROD_SK = PRD.PROD_SK
INNER JOIN {IDSOwner}.GRP GRP ON SUB.GRP_SK = GRP.GRP_SK
INNER JOIN {IDSOwner}.SUBGRP SUBGRP ON SUBGRP.SUBGRP_SK=ENR.SUBGRP_SK
INNER JOIN {IDSOwner}.PROD_SH_NM PROD_SH_NM on PRD.PROD_SH_NM_SK=PROD_SH_NM.PROD_SH_NM_SK
INNER JOIN {IDSOwner}.CD_MPPNG CD2 ON CD2.CD_MPPNG_SK=PROD_SH_NM.PROD_SH_NM_DLVRY_METH_CD_SK
INNER JOIN {IDSOwner}.MBR_ADDR_TYP MBR_ADDR_TYP ON  MBR_ADDR_TYP.MBR_SK=MBR.MBR_SK
INNER JOIN {IDSOwner}.CD_MPPNG CD1 ON CD1.CD_MPPNG_SK=MBR_ADDR_TYP.MBR_ADDR_ROLE_TYP_CD_SK AND CD1.TRGT_CD = 'HOME'
INNER JOIN {IDSOwner}.SUB_ADDR SUB_ADDR ON SUB_ADDR.SUB_ADDR_SK=MBR_ADDR_TYP.SUB_ADDR_SK
WHERE
CD.TRGT_CD = 'MED'
AND ENR.ELIG_IN = 'Y'
AND MBR.BRTH_DT_SK = APL.MBR_BRTH_DT
AND SUBSTR(MBR.FIRST_NM, 1, 3) = SUBSTR(APL.MBR_FIRST_NM, 1, 3)
AND SUBSTR(MBR.LAST_NM, 1, 3) = SUBSTR(APL.MBR_LAST_NM, 1, 3)
"""

df_MEM_DATA_3 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_MEM_DATA_3)
    .load()
)

query_MEM_DATA_4 = f"""
SELECT
MBR.MBR_UNIQ_KEY,
MBR.FIRST_NM,
MBR.LAST_NM,
MBR.BRTH_DT_SK,
CD.TRGT_CD GNDR_CD,
GRP.GRP_ID,
SUB.SUB_ID,
MBR.MBR_SFX_NO,
ENR.EFF_DT_SK,
ENR.TERM_DT_SK,
ENR.GRP_SK,
MBR.MBR_SK,
ENR.SUBGRP_SK,
ENR.PROD_SK,
PRD.PROD_SH_NM_SK,
MBR.SUB_SK,
SUBGRP.SUBGRP_ID,
SUB.SUB_UNIQ_KEY,
ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK,
SUB_ADDR.SUB_ADDR_ST_CD_SK,
CASE WHEN CD2.TRGT_CD = 'PPO' THEN 'MEMBER' ELSE 'PROVIDER' END PROD_SH_NM_DLVRY_METH_CD
FROM {IDSOwner}.P_MBR_NEWDIR_APL APL
INNER JOIN {IDSOwner}.SUB SUB ON APL.SUB_ID = SUB.SUB_ID
INNER JOIN {IDSOwner}.MBR MBR ON MBR.SUB_SK = SUB.SUB_SK
INNER JOIN {IDSOwner}.MBR_ENR ENR ON MBR.MBR_SK = ENR.MBR_SK
  AND ENR.EFF_DT_SK <= APL.NEWDIR_APL_RCVD_DT
  AND ENR.TERM_DT_SK >= APL.NEWDIR_APL_RCVD_DT
INNER JOIN {IDSOwner}.CD_MPPNG CD ON CD.CD_MPPNG_SK = ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK
INNER JOIN {IDSOwner}.PROD PRD ON ENR.PROD_SK = PRD.PROD_SK
INNER JOIN {IDSOwner}.GRP GRP ON SUB.GRP_SK = GRP.GRP_SK
INNER JOIN {IDSOwner}.SUBGRP SUBGRP ON SUBGRP.SUBGRP_SK=ENR.SUBGRP_SK
INNER JOIN {IDSOwner}.PROD_SH_NM PROD_SH_NM on PRD.PROD_SH_NM_SK=PROD_SH_NM.PROD_SH_NM_SK
INNER JOIN {IDSOwner}.CD_MPPNG CD2 ON CD2.CD_MPPNG_SK=PROD_SH_NM.PROD_SH_NM_DLVRY_METH_CD_SK
INNER JOIN {IDSOwner}.MBR_ADDR_TYP MBR_ADDR_TYP ON  MBR_ADDR_TYP.MBR_SK=MBR.MBR_SK
INNER JOIN {IDSOwner}.CD_MPPNG CD1 ON CD1.CD_MPPNG_SK=MBR_ADDR_TYP.MBR_ADDR_ROLE_TYP_CD_SK AND CD1.TRGT_CD = 'HOME'
INNER JOIN {IDSOwner}.SUB_ADDR SUB_ADDR ON SUB_ADDR.SUB_ADDR_SK=MBR_ADDR_TYP.SUB_ADDR_SK
WHERE
CD.TRGT_CD = 'MED'
AND ENR.ELIG_IN = 'Y'
AND MBR.MBR_SFX_NO = '00'
"""

df_MEM_DATA_4 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_MEM_DATA_4)
    .load()
)

df_xfm_pass1_Mem_Pass_1 = (
    df_db2_Newdir_Extract
    .filter((F.col("FLG").isNotNull()) & (F.length(trim(F.col("FLG"))) != 0))
    .select(
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.lit("I").alias("INSRT_UPDT_CD"),
        F.lit("N").alias("DISCARD_IN"),
        F.lit("Y").alias("PASS_THRU_IN"),
        F.to_timestamp(F.lit(RunDate), "yyyy-MM-dd").alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.lit(SrcSysCd).alias("SRC_SYS_CD"),
        F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("NEWDIR_APL_AUTH_ID")).alias("PRI_KEY_STRING"),
        F.lit(0).alias("APL_SK"),
        F.col("NEWDIR_APL_AUTH_ID").alias("APL_ID"),
        F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.when((F.length(trim(F.col("CRT_USER_SK"))) == 0) | (F.col("CRT_USER_SK").isNull()), "UNK").otherwise(F.col("CRT_USER_SK")).alias("CRT_USER_SK"),
        F.when((F.length(trim(F.col("PRI_RESP_USER_SK"))) == 0) | (F.col("PRI_RESP_USER_SK").isNull()), "UNK").otherwise(F.col("PRI_RESP_USER_SK")).alias("CUR_PRI_USER_SK"),
        F.lit(0).alias("CUR_SEC_USER_SK"),
        F.lit(0).alias("CUR_TRTY_USER_SK"),
        F.when((F.length(trim(F.col("GRP_ID"))) == 0) | (F.col("GRP_ID").isNull()), "UNK").otherwise(F.col("GRP_ID")).alias("GRP_SK"),
        F.when((F.length(trim(F.col("CRT_USER_SK"))) == 0) | (F.col("CRT_USER_SK").isNull()), "UNK").otherwise(F.col("CRT_USER_SK")).alias("LAST_UPDT_USER_SK"),
        F.when((F.length(trim(F.col("MBR_UNIQ_KEY"))) == 0) | (F.col("MBR_UNIQ_KEY").isNull()), "UNK").otherwise(F.col("MBR_UNIQ_KEY")).alias("MBR_SK"),
        F.when((F.length(trim(F.col("SUBGRP_ID"))) == 0) | (F.col("SUBGRP_ID").isNull()), "UNK").otherwise(F.col("SUBGRP_ID")).alias("SUBGRP_SK"),
        F.when((F.length(trim(F.col("CRT_USER_SK"))) == 0) | (F.col("CRT_USER_SK").isNull()), "UNK").otherwise(F.col("CRT_USER_SK")).alias("PROD_SK"),
        F.when((F.length(trim(F.col("PROD_SH_NM_SK"))) == 0) | (F.col("PROD_SH_NM_SK").isNull()), "UNK").otherwise(F.col("PROD_SH_NM_SK")).alias("PROD_SH_NM_SK"),
        F.when((F.length(trim(F.col("SUB_UNIQ_KEY"))) == 0) | (F.col("SUB_UNIQ_KEY").isNull()), "UNK").otherwise(F.col("SUB_UNIQ_KEY")).alias("SUB_SK"),
        F.when((F.length(trim(F.col("APL_CAT_TX"))) == 0) | (F.col("APL_CAT_TX").isNull()), "UNK").otherwise(F.col("APL_CAT_TX")).alias("APL_CAT_CD_SK"),
        F.upper(trim(F.col("APL_CUR_DCSN_TX_SPACE"))).alias("APL_CUR_DCSN_CD_SK"),
        F.when(F.col("APL_DCSN_LTR_SENT_DTM").isNotNull(), "CLSD").otherwise("0").alias("APL_CUR_STTUS_CD_SK"),
        F.when((F.length(trim(F.col("PROD_SH_NM_DLVRY_METH_CD"))) == 0) | (F.col("PROD_SH_NM_DLVRY_METH_CD").isNull()), "UNK").otherwise(F.col("PROD_SH_NM_DLVRY_METH_CD")).alias("APL_INITN_METH_CD_SK"),
        F.when((F.length(trim(F.col("SUB_ADDR_ST_CD_SK"))) == 0) | (F.col("SUB_ADDR_ST_CD_SK").isNull()), F.lit(0)).otherwise(F.col("SUB_ADDR_ST_CD_SK")).alias("APL_MBR_HOME_ADDR_ST_CD_SK"),
        F.lit("0").alias("APL_SUBTYP_CD_SK"),
        F.lit("0").alias("APL_TYP_CD_SK"),
        F.when((F.length(trim(F.col("UM_CLS_PLN_PROD_CAT_CD_SK"))) == 0) | (F.col("UM_CLS_PLN_PROD_CAT_CD_SK").isNull()), "UNK").otherwise(F.col("UM_CLS_PLN_PROD_CAT_CD_SK")).alias("CLS_PLN_PROD_CAT_CD_SK"),
        F.when((F.length(trim(F.col("APL_DENIAL_STG_TX_SPACE"))) == 0) | (F.col("APL_DENIAL_STG_TX_SPACE").isNull()), "UNK").otherwise(F.upper(trim(F.col("APL_DENIAL_STG_TX_SPACE")))).alias("CUR_APL_LVL_CD_SK"),
        F.col("CUR_APL_LVL_EXPDTD_IN").alias("CUR_APL_LVL_EXPDTD_IN"),
        F.to_timestamp(F.date_format(F.col("NEWDIR_APL_RCVD_DT"), "yyyy-MM-dd"), "yyyy-MM-dd").alias("CRT_DTM"),
        F.to_timestamp(F.date_format(F.col("PRCS_DT"), "yyyy-MM-dd"), "yyyy-MM-dd").alias("CUR_STTUS_DTM"),
        F.col("UM_SVC_END_DT").alias("END_DT_SK"),
        F.col("UM_SVC_STRT_DT").alias("INITN_DT_SK"),
        current_timestamp().alias("LAST_UPDT_DTM"),
        F.lit("1753-01-01").alias("NEXT_RVW_DT_SK"),
        F.lit("0").alias("CUR_APL_LVL_SEQ_NO"),
        F.lit("0").alias("CUR_STTUS_SEQ_NO"),
        F.lit("0").alias("NEXT_RVW_INTRVL_NO"),
        F.when((F.length(trim(F.col("APL_CUR_DCSN_TX"))) == 0) | (F.col("APL_CUR_DCSN_TX").isNull()), "").otherwise(F.col("APL_CUR_DCSN_TX")).alias("APL_DESC"),
        F.when((F.length(trim(F.col("APL_DENIAL_SUBTYP_TX"))) == 0) | (F.col("APL_DENIAL_SUBTYP_TX").isNull()), "").otherwise(F.col("APL_DENIAL_SUBTYP_TX")).alias("APL_SUM_DESC"),
    )
)

df_xfm_pass1_Lnk_Src2 = (
    df_db2_Newdir_Extract
    .filter((F.col("FLG").isNull()) | (F.length(trim(F.col("FLG"))) == 0))
    .select(
        F.col("SUB_ID"),
        F.col("MBR_FULL_NM"),
        F.col("MBR_BRTH_DT"),
        F.col("NEWDIR_APL_AUTH_ID"),
        F.col("NEWDIR_APL_RCVD_DT"),
        F.col("PRCS_DT"),
        F.col("MBR_FIRST_NM"),
        F.col("MBR_MIDINIT"),
        F.col("MBR_LAST_NM"),
        F.col("MBR_AGE"),
        F.col("PROC_AUTH_UNIT_CT"),
        F.col("NEWDIR_CO_NM"),
        F.col("APL_DCSN_LTR_SENT_DTM"),
        F.col("APL_CUR_DCSN_TX"),
        F.col("APL_CAT_TX"),
        F.col("APL_DENIAL_STG_TX"),
        F.col("APL_DENIAL_TYP_TX"),
        F.col("APL_DENIAL_PHYS_RVWR_ID"),
        F.col("APL_DENIAL_PHYS_RVWR_FULL_NM"),
        F.col("APL_STTUS_TX"),
        F.col("UM_SVC_STRT_DT"),
        F.col("UM_SVC_END_DT"),
        F.col("FNCL_LOB_TX"),
        F.col("FNCL_LOB_DESC"),
        F.col("APL_NOTE_TX"),
        F.col("NEWDIR_LAST_UPDT_USER_ID"),
        F.col("NEWDIR_LAST_UPDT_USER_FULL_NM"),
        F.col("PROC_RQST_UNIT_CT"),
        F.col("APL_RQST_DTM"),
        F.col("PROC_RVSED_UNIT_CT"),
        F.col("UM_CLS_PLN_PROD_CAT_CD_SK"),
        F.col("APL_RVW_TYP_TX"),
        F.col("APL_SVC_DESC"),
        F.col("APL_SVC_TX"),
        F.col("APL_DENIAL_SUBTYP_TX"),
        F.col("APL_SUBMTR_TYP_TX"),
        F.col("APL_ACKNMT_LTR_SENT_DTM"),
        F.col("CUR_APL_LVL_EXPDTD_IN"),
        F.col("APL_CUR_DCSN_TX_SPACE"),
        F.col("APL_DENIAL_STG_TX_SPACE"),
    )
)

df_lkp_2_lnk_Codes = (
    df_xfm_pass1_Lnk_Src2.alias("Lnk_Src2")
    .join(df_MEM_DATA_2.alias("Lnk_Ref_2"), F.col("Lnk_Src2.SUB_ID") == F.col("Lnk_Ref_2.SUB_ID"), "left")
    .select(
        F.col("Lnk_Src2.SUB_ID").alias("SUB_ID"),
        F.col("Lnk_Src2.MBR_FULL_NM").alias("MBR_FULL_NM"),
        F.col("Lnk_Src2.MBR_BRTH_DT").alias("MBR_BRTH_DT"),
        F.col("Lnk_Src2.NEWDIR_APL_AUTH_ID").alias("NEWDIR_APL_AUTH_ID"),
        F.col("Lnk_Src2.NEWDIR_APL_RCVD_DT").alias("NEWDIR_APL_RCVD_DT"),
        F.col("Lnk_Src2.PRCS_DT").alias("PRCS_DT"),
        F.col("Lnk_Src2.MBR_FIRST_NM").alias("MBR_FIRST_NM"),
        F.col("Lnk_Src2.MBR_MIDINIT").alias("MBR_MIDINIT"),
        F.col("Lnk_Src2.MBR_LAST_NM").alias("MBR_LAST_NM"),
        F.col("Lnk_Src2.MBR_AGE").alias("MBR_AGE"),
        F.col("Lnk_Src2.PROC_AUTH_UNIT_CT").alias("PROC_AUTH_UNIT_CT"),
        F.col("Lnk_Src2.NEWDIR_CO_NM").alias("NEWDIR_CO_NM"),
        F.col("Lnk_Src2.APL_DCSN_LTR_SENT_DTM").alias("APL_DCSN_LTR_SENT_DTM"),
        F.col("Lnk_Src2.APL_CUR_DCSN_TX").alias("APL_CUR_DCSN_TX"),
        F.col("Lnk_Src2.APL_CAT_TX").alias("APL_CAT_TX"),
        F.col("Lnk_Src2.APL_DENIAL_STG_TX").alias("APL_DENIAL_STG_TX"),
        F.col("Lnk_Src2.APL_DENIAL_TYP_TX").alias("APL_DENIAL_TYP_TX"),
        F.col("Lnk_Src2.APL_DENIAL_PHYS_RVWR_ID").alias("APL_DENIAL_PHYS_RVWR_ID"),
        F.col("Lnk_Src2.APL_DENIAL_PHYS_RVWR_FULL_NM").alias("APL_DENIAL_PHYS_RVWR_FULL_NM"),
        F.col("Lnk_Src2.APL_STTUS_TX").alias("APL_STTUS_TX"),
        F.col("Lnk_Src2.UM_SVC_STRT_DT").alias("UM_SVC_STRT_DT"),
        F.col("Lnk_Src2.UM_SVC_END_DT").alias("UM_SVC_END_DT"),
        F.col("Lnk_Src2.FNCL_LOB_TX").alias("FNCL_LOB_TX"),
        F.col("Lnk_Src2.FNCL_LOB_DESC").alias("FNCL_LOB_DESC"),
        F.col("Lnk_Src2.APL_NOTE_TX").alias("APL_NOTE_TX"),
        F.col("Lnk_Src2.NEWDIR_LAST_UPDT_USER_ID").alias("NEWDIR_LAST_UPDT_USER_ID"),
        F.col("Lnk_Src2.NEWDIR_LAST_UPDT_USER_FULL_NM").alias("NEWDIR_LAST_UPDT_USER_FULL_NM"),
        F.col("Lnk_Src2.PROC_RQST_UNIT_CT").alias("PROC_RQST_UNIT_CT"),
        F.col("Lnk_Src2.APL_RQST_DTM").alias("APL_RQST_DTM"),
        F.col("Lnk_Src2.PROC_RVSED_UNIT_CT").alias("PROC_RVSED_UNIT_CT"),
        F.col("Lnk_Src2.UM_CLS_PLN_PROD_CAT_CD_SK").alias("UM_CLS_PLN_PROD_CAT_CD_SK"),
        F.col("Lnk_Src2.APL_RVW_TYP_TX").alias("APL_RVW_TYP_TX"),
        F.col("Lnk_Src2.APL_SVC_DESC").alias("APL_SVC_DESC"),
        F.col("Lnk_Src2.APL_SVC_TX").alias("APL_SVC_TX"),
        F.col("Lnk_Src2.APL_DENIAL_SUBTYP_TX").alias("APL_DENIAL_SUBTYP_TX"),
        F.col("Lnk_Src2.APL_SUBMTR_TYP_TX").alias("APL_SUBMTR_TYP_TX"),
        F.col("Lnk_Src2.APL_ACKNMT_LTR_SENT_DTM").alias("APL_ACKNMT_LTR_SENT_DTM"),
        F.col("Lnk_Ref_2.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        F.col("Lnk_Ref_2.FIRST_NM").alias("FIRST_NM"),
        F.col("Lnk_Ref_2.LAST_NM").alias("LAST_NM"),
        F.col("Lnk_Ref_2.BRTH_DT_SK").alias("MBR_BRTH_DT_SK"),
        F.col("Lnk_Ref_2.GNDR_CD").alias("GNDR_CD"),
        F.col("Lnk_Ref_2.GRP_ID").alias("GRP_ID"),
        F.col("Lnk_Ref_2.MBR_SFX_NO").alias("MBR_SFX_NO"),
        F.col("Lnk_Ref_2.EFF_DT_SK").alias("EFF_DT_SK"),
        F.col("Lnk_Ref_2.TERM_DT_SK").alias("TERM_DT_SK"),
        F.col("Lnk_Ref_2.GRP_SK").alias("GRP_SK"),
        F.col("Lnk_Ref_2.MBR_SK").alias("MBR_SK"),
        F.col("Lnk_Src2.CUR_APL_LVL_EXPDTD_IN").alias("CUR_APL_LVL_EXPDTD_IN"),
        F.col("Lnk_Ref_2.SUBGRP_SK").alias("SUBGRP_SK"),
        F.col("Lnk_Ref_2.PROD_SK").alias("PROD_SK"),
        F.col("Lnk_Ref_2.PROD_SH_NM_SK").alias("PROD_SH_NM_SK"),
        F.col("Lnk_Ref_2.SUB_SK").alias("SUB_SK"),
        F.col("Lnk_Ref_2.SUBGRP_ID").alias("SUBGRP_ID"),
        F.col("Lnk_Ref_2.SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
        F.col("Lnk_Src2.APL_CUR_DCSN_TX_SPACE").alias("APL_CUR_DCSN_TX_SPACE"),
        F.col("Lnk_Src2.APL_DENIAL_STG_TX_SPACE").alias("APL_DENIAL_STG_TX_SPACE"),
        F.col("Lnk_Ref_2.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK").alias("MBR_ENR_CLS_PLN_PROD_CAT_CD_SK"),
        F.col("Lnk_Ref_2.SUB_ADDR_ST_CD_SK").alias("SUB_ADDR_ST_CD_SK"),
        F.col("Lnk_Ref_2.PROD_SH_NM_DLVRY_METH_CD").alias("PROD_SH_NM_DLVRY_METH_CD"),
    )
)

df_lkp_2_Lnk_Src3 = (
    df_xfm_pass1_Lnk_Src2.alias("Lnk_Src2")
    .join(df_MEM_DATA_2.alias("Lnk_Ref_2"), F.col("Lnk_Src2.SUB_ID") == F.col("Lnk_Ref_2.SUB_ID"), "left")
    .select(
        F.col("Lnk_Src2.SUB_ID").alias("SUB_ID"),
        F.col("Lnk_Src2.MBR_FULL_NM").alias("MBR_FULL_NM"),
        F.col("Lnk_Src2.MBR_BRTH_DT").alias("MBR_BRTH_DT"),
        F.col("Lnk_Src2.NEWDIR_APL_AUTH_ID").alias("NEWDIR_APL_AUTH_ID"),
        F.col("Lnk_Src2.NEWDIR_APL_RCVD_DT").alias("NEWDIR_APL_RCVD_DT"),
        F.col("Lnk_Src2.PRCS_DT").alias("PRCS_DT"),
        F.col("Lnk_Src2.MBR_FIRST_NM").alias("MBR_FIRST_NM"),
        F.col("Lnk_Src2.MBR_MIDINIT").alias("MBR_MIDINIT"),
        F.col("Lnk_Src2.MBR_LAST_NM").alias("MBR_LAST_NM"),
        F.col("Lnk_Src2.MBR_AGE").alias("MBR_AGE"),
        F.col("Lnk_Src2.PROC_AUTH_UNIT_CT").alias("PROC_AUTH_UNIT_CT"),
        F.col("Lnk_Src2.NEWDIR_CO_NM").alias("NEWDIR_CO_NM"),
        F.col("Lnk_Src2.APL_DCSN_LTR_SENT_DTM").alias("APL_DCSN_LTR_SENT_DTM"),
        F.col("Lnk_Src2.APL_CUR_DCSN_TX").alias("APL_CUR_DCSN_TX"),
        F.col("Lnk_Src2.APL_CAT_TX").alias("APL_CAT_TX"),
        F.col("Lnk_Src2.APL_DENIAL_STG_TX").alias("APL_DENIAL_STG_TX"),
        F.col("Lnk_Src2.APL_DENIAL_TYP_TX").alias("APL_DENIAL_TYP_TX"),
        F.col("Lnk_Src2.APL_DENIAL_PHYS_RVWR_ID").alias("APL_DENIAL_PHYS_RVWR_ID"),
        F.col("Lnk_Src2.APL_DENIAL_PHYS_RVWR_FULL_NM").alias("APL_DENIAL_PHYS_RVWR_FULL_NM"),
        F.col("Lnk_Src2.APL_STTUS_TX").alias("APL_STTUS_TX"),
        F.col("Lnk_Src2.UM_SVC_STRT_DT").alias("UM_SVC_STRT_DT"),
        F.col("Lnk_Src2.UM_SVC_END_DT").alias("UM_SVC_END_DT"),
        F.col("Lnk_Src2.FNCL_LOB_TX").alias("FNCL_LOB_TX"),
        F.col("Lnk_Src2.FNCL_LOB_DESC").alias("FNCL_LOB_DESC"),
        F.col("Lnk_Src2.APL_NOTE_TX").alias("APL_NOTE_TX"),
        F.col("Lnk_Src2.NEWDIR_LAST_UPDT_USER_ID").alias("NEWDIR_LAST_UPDT_USER_ID"),
        F.col("Lnk_Src2.NEWDIR_LAST_UPDT_USER_FULL_NM").alias("NEWDIR_LAST_UPDT_USER_FULL_NM"),
        F.col("Lnk_Src2.PROC_RQST_UNIT_CT").alias("PROC_RQST_UNIT_CT"),
        F.col("Lnk_Src2.APL_RQST_DTM").alias("APL_RQST_DTM"),
        F.col("Lnk_Src2.PROC_RVSED_UNIT_CT").alias("PROC_RVSED_UNIT_CT"),
        F.col("Lnk_Src2.UM_CLS_PLN_PROD_CAT_CD_SK").alias("UM_CLS_PLN_PROD_CAT_CD_SK"),
        F.col("Lnk_Src2.APL_RVW_TYP_TX").alias("APL_RVW_TYP_TX"),
        F.col("Lnk_Src2.APL_SVC_DESC").alias("APL_SVC_DESC"),
        F.col("Lnk_Src2.APL_SVC_TX").alias("APL_SVC_TX"),
        F.col("Lnk_Src2.APL_DENIAL_SUBTYP_TX").alias("APL_DENIAL_SUBTYP_TX"),
        F.col("Lnk_Src2.APL_SUBMTR_TYP_TX").alias("APL_SUBMTR_TYP_TX"),
        F.col("Lnk_Src2.APL_ACKNMT_LTR_SENT_DTM").alias("APL_ACKNMT_LTR_SENT_DTM"),
        F.col("Lnk_Src2.CUR_APL_LVL_EXPDTD_IN").alias("CUR_APL_LVL_EXPDTD_IN"),
        F.col("Lnk_Src2.APL_CUR_DCSN_TX_SPACE").alias("APL_CUR_DCSN_TX_SPACE"),
        F.col("Lnk_Src2.APL_DENIAL_STG_TX_SPACE").alias("APL_DENIAL_STG_TX_SPACE"),
    )
)

df_xfm_pass2 = (
    df_lkp_2_lnk_Codes.alias("lnk_Codes")
    .select("*")  # we keep them for staging variable usage
    .withColumn("dtchk", F.when(
        (F.to_date(F.col("EFF_DT_SK"), "yyyy-MM-dd") <= F.col("NEWDIR_APL_RCVD_DT")) &
        (F.to_date(F.col("TERM_DT_SK"), "yyyy-MM-dd") >= F.col("NEWDIR_APL_RCVD_DT")),
        "Y").otherwise("N")
    )
    .withColumn("svbirthdtchk", F.when(
        F.to_date(F.col("MBR_BRTH_DT_SK"), "yyyy-MM-dd") == F.col("MBR_BRTH_DT"), "Y").otherwise("N"))
    .withColumn("svfirstnmchk", F.when(
        F.upper(trim(F.col("MBR_FIRST_NM"))) == F.upper(trim(F.col("FIRST_NM"))), "Y").otherwise("N"))
    .withColumn("svlstnmchk", F.when(
        F.upper(trim(F.col("MBR_LAST_NM"))) == F.upper(trim(F.col("LAST_NM"))), "Y").otherwise("N"))
    .withColumn("svsfxchk", F.when(
        trim(F.col("MBR_SFX_NO")) == "00", "Y").otherwise("N"))
    .withColumn("secondpass", F.when(
        (F.col("dtchk") == "Y") &
        (F.col("svfirstnmchk") == "Y") &
        (F.col("svlstnmchk") == "Y") &
        (F.col("svbirthdtchk") == "Y"),
        "PASS").otherwise("REJECT"))
)

df_xfm_pass2_Mem_Pass_2 = (
    df_xfm_pass2
    .select(
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.lit("I").alias("INSRT_UPDT_CD"),
        F.lit("N").alias("DISCARD_IN"),
        F.lit("Y").alias("PASS_THRU_IN"),
        F.to_timestamp(F.lit(RunDate), "yyyy-MM-dd").alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.lit(SrcSysCd).alias("SRC_SYS_CD"),
        F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("NEWDIR_APL_AUTH_ID")).alias("PRI_KEY_STRING"),
        F.lit(0).alias("APL_SK"),
        F.col("NEWDIR_APL_AUTH_ID").alias("APL_ID"),
        F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit("0").alias("CRT_USER_SK"),
        F.lit("0").alias("CUR_PRI_USER_SK"),
        F.lit("0").alias("CUR_SEC_USER_SK"),
        F.lit("0").alias("CUR_TRTY_USER_SK"),
        F.when((F.length(trim(F.col("GRP_ID"))) == 0) | (F.col("GRP_ID").isNull()), "UNK").otherwise(F.col("GRP_ID")).alias("GRP_SK"),
        F.lit("0").alias("LAST_UPDT_USER_SK"),
        F.when((F.length(trim(F.col("MBR_UNIQ_KEY"))) == 0) | (F.col("MBR_UNIQ_KEY").isNull()), "UNK").otherwise(F.col("MBR_UNIQ_KEY")).alias("MBR_SK"),
        F.when((F.length(trim(F.col("SUBGRP_ID"))) == 0) | (F.col("SUBGRP_ID").isNull()), "UNK").otherwise(F.col("SUBGRP_ID")).alias("SUBGRP_SK"),
        F.when((F.length(trim(F.col("PROD_SK"))) == 0) | (F.col("PROD_SK").isNull()), "UNK").otherwise(F.col("PROD_SK")).alias("PROD_SK"),
        F.when((F.length(trim(F.col("PROD_SH_NM_SK"))) == 0) | (F.col("PROD_SH_NM_SK").isNull()), "UNK").otherwise(F.col("PROD_SH_NM_SK")).alias("PROD_SH_NM_SK"),
        F.when((F.length(trim(F.col("SUB_UNIQ_KEY"))) == 0) | (F.col("SUB_UNIQ_KEY").isNull()), "UNK").otherwise(F.col("SUB_UNIQ_KEY")).alias("SUB_SK"),
        F.when((F.length(trim(F.col("APL_CAT_TX"))) == 0) | (F.col("APL_CAT_TX").isNull()), "UNK").otherwise(F.col("APL_CAT_TX")).alias("APL_CAT_CD_SK"),
        F.when((F.length(trim(F.col("APL_CUR_DCSN_TX_SPACE"))) == 0) | (F.col("APL_CUR_DCSN_TX_SPACE").isNull()), "UNK")
         .otherwise(F.upper(trim(F.col("APL_CUR_DCSN_TX_SPACE")))).alias("APL_CUR_DCSN_CD_SK"),
        F.when(F.col("APL_DCSN_LTR_SENT_DTM").isNotNull(), "CLSD").otherwise("0").alias("APL_CUR_STTUS_CD_SK"),
        F.when((F.length(trim(F.col("PROD_SH_NM_DLVRY_METH_CD"))) == 0) | (F.col("PROD_SH_NM_DLVRY_METH_CD").isNull()), "UNK")
         .otherwise(F.col("PROD_SH_NM_DLVRY_METH_CD")).alias("APL_INITN_METH_CD_SK"),
        F.when((F.length(trim(F.col("SUB_ADDR_ST_CD_SK"))) == 0) | (F.col("SUB_ADDR_ST_CD_SK").isNull()), F.lit(0))
         .otherwise(F.col("SUB_ADDR_ST_CD_SK")).alias("APL_MBR_HOME_ADDR_ST_CD_SK"),
        F.lit("0").alias("APL_SUBTYP_CD_SK"),
        F.lit("0").alias("APL_TYP_CD_SK"),
        F.col("MBR_ENR_CLS_PLN_PROD_CAT_CD_SK").alias("CLS_PLN_PROD_CAT_CD_SK"),
        F.when((F.length(trim(F.col("APL_DENIAL_STG_TX_SPACE"))) == 0) | (F.col("APL_DENIAL_STG_TX_SPACE").isNull()), "UNK")
         .otherwise(F.upper(trim(F.col("APL_DENIAL_STG_TX_SPACE")))).alias("CUR_APL_LVL_CD_SK"),
        F.col("CUR_APL_LVL_EXPDTD_IN").alias("CUR_APL_LVL_EXPDTD_IN"),
        F.to_timestamp(F.date_format(F.col("NEWDIR_APL_RCVD_DT"), "yyyy-MM-dd"), "yyyy-MM-dd").alias("CRT_DTM"),
        F.to_timestamp(F.date_format(F.col("PRCS_DT"), "yyyy-MM-dd"), "yyyy-MM-dd").alias("CUR_STTUS_DTM"),
        F.col("UM_SVC_END_DT").alias("END_DT_SK"),
        F.col("UM_SVC_STRT_DT").alias("INITN_DT_SK"),
        current_timestamp().alias("LAST_UPDT_DTM"),
        F.lit("1753-01-01").alias("NEXT_RVW_DT_SK"),
        F.lit("0").alias("CUR_APL_LVL_SEQ_NO"),
        F.lit("0").alias("CUR_STTUS_SEQ_NO"),
        F.lit("0").alias("NEXT_RVW_INTRVL_NO"),
        F.when((F.length(trim(F.col("APL_CUR_DCSN_TX"))) == 0) | (F.col("APL_CUR_DCSN_TX").isNull()), "").otherwise(F.col("APL_CUR_DCSN_TX")).alias("APL_DESC"),
        F.when((F.length(trim(F.col("APL_DENIAL_SUBTYP_TX"))) == 0) | (F.col("APL_DENIAL_SUBTYP_TX").isNull()), "").otherwise(F.col("APL_DENIAL_SUBTYP_TX")).alias("APL_SUM_DESC"),
    )
)

df_lkp_3_lnk_Src3 = (
    df_lkp_2_Lnk_Src3.alias("Lnk_Src3")
    .join(df_MEM_DATA_3.alias("Lnk_Ref_3"), F.col("Lnk_Src3.SUB_ID") == F.col("Lnk_Ref_3.SUB_ID"), "left")
    .select(
        F.col("Lnk_Src3.SUB_ID").alias("SUB_ID"),
        F.col("Lnk_Src3.MBR_FULL_NM").alias("MBR_FULL_NM"),
        F.col("Lnk_Src3.MBR_BRTH_DT").alias("MBR_BRTH_DT"),
        F.col("Lnk_Src3.NEWDIR_APL_AUTH_ID").alias("NEWDIR_APL_AUTH_ID"),
        F.col("Lnk_Src3.NEWDIR_APL_RCVD_DT").alias("NEWDIR_APL_RCVD_DT"),
        F.col("Lnk_Src3.PRCS_DT").alias("PRCS_DT"),
        F.col("Lnk_Src3.MBR_FIRST_NM").alias("MBR_FIRST_NM"),
        F.col("Lnk_Src3.MBR_MIDINIT").alias("MBR_MIDINIT"),
        F.col("Lnk_Src3.MBR_LAST_NM").alias("MBR_LAST_NM"),
        F.col("Lnk_Src3.MBR_AGE").alias("MBR_AGE"),
        F.col("Lnk_Src3.PROC_AUTH_UNIT_CT").alias("PROC_AUTH_UNIT_CT"),
        F.col("Lnk_Src3.NEWDIR_CO_NM").alias("NEWDIR_CO_NM"),
        F.col("Lnk_Src3.APL_DCSN_LTR_SENT_DTM").alias("APL_DCSN_LTR_SENT_DTM"),
        F.col("Lnk_Src3.APL_CUR_DCSN_TX").alias("APL_CUR_DCSN_TX"),
        F.col("Lnk_Src3.APL_CAT_TX").alias("APL_CAT_TX"),
        F.col("Lnk_Src3.APL_DENIAL_STG_TX").alias("APL_DENIAL_STG_TX"),
        F.col("Lnk_Src3.APL_DENIAL_TYP_TX").alias("APL_DENIAL_TYP_TX"),
        F.col("Lnk_Src3.APL_DENIAL_PHYS_RVWR_ID").alias("APL_DENIAL_PHYS_RVWR_ID"),
        F.col("Lnk_Src3.APL_DENIAL_PHYS_RVWR_FULL_NM").alias("APL_DENIAL_PHYS_RVWR_FULL_NM"),
        F.col("Lnk_Src3.APL_STTUS_TX").alias("APL_STTUS_TX"),
        F.col("Lnk_Src3.UM_SVC_STRT_DT").alias("UM_SVC_STRT_DT"),
        F.col("Lnk_Src3.UM_SVC_END_DT").alias("UM_SVC_END_DT"),
        F.col("Lnk_Src3.FNCL_LOB_TX").alias("FNCL_LOB_TX"),
        F.col("Lnk_Src3.FNCL_LOB_DESC").alias("FNCL_LOB_DESC"),
        F.col("Lnk_Src3.APL_NOTE_TX").alias("APL_NOTE_TX"),
        F.col("Lnk_Src3.NEWDIR_LAST_UPDT_USER_ID").alias("NEWDIR_LAST_UPDT_USER_ID"),
        F.col("Lnk_Src3.NEWDIR_LAST_UPDT_USER_FULL_NM").alias("NEWDIR_LAST_UPDT_USER_FULL_NM"),
        F.col("Lnk_Src3.PROC_RQST_UNIT_CT").alias("PROC_RQST_UNIT_CT"),
        F.col("Lnk_Src3.APL_RQST_DTM").alias("APL_RQST_DTM"),
        F.col("Lnk_Src3.PROC_RVSED_UNIT_CT").alias("PROC_RVSED_UNIT_CT"),
        F.col("Lnk_Src3.UM_CLS_PLN_PROD_CAT_CD_SK").alias("UM_CLS_PLN_PROD_CAT_CD_SK"),
        F.col("Lnk_Src3.APL_RVW_TYP_TX").alias("APL_RVW_TYP_TX"),
        F.col("Lnk_Src3.APL_SVC_DESC").alias("APL_SVC_DESC"),
        F.col("Lnk_Src3.APL_SVC_TX").alias("APL_SVC_TX"),
        F.col("Lnk_Src3.APL_DENIAL_SUBTYP_TX").alias("APL_DENIAL_SUBTYP_TX"),
        F.col("Lnk_Src3.APL_SUBMTR_TYP_TX").alias("APL_SUBMTR_TYP_TX"),
        F.col("Lnk_Src3.APL_ACKNMT_LTR_SENT_DTM").alias("APL_ACKNMT_LTR_SENT_DTM"),
        F.col("Lnk_Ref_3.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        F.col("Lnk_Ref_3.FIRST_NM").alias("FIRST_NM"),
        F.col("Lnk_Ref_3.LAST_NM").alias("LAST_NM"),
        F.col("Lnk_Ref_3.BRTH_DT_SK").alias("MBR_BRTH_DT_SK"),
        F.col("Lnk_Ref_3.GNDR_CD").alias("GNDR_CD"),
        F.col("Lnk_Ref_3.GRP_ID").alias("GRP_ID"),
        F.col("Lnk_Ref_3.MBR_SFX_NO").alias("MBR_SFX_NO"),
        F.col("Lnk_Ref_3.EFF_DT_SK").alias("EFF_DT_SK"),
        F.col("Lnk_Ref_3.TERM_DT_SK").alias("TERM_DT_SK"),
        F.col("Lnk_Ref_3.GRP_SK").alias("GRP_SK"),
        F.col("Lnk_Ref_3.MBR_SK").alias("MBR_SK"),
        F.col("Lnk_Src3.CUR_APL_LVL_EXPDTD_IN").alias("CUR_APL_LVL_EXPDTD_IN"),
        F.col("Lnk_Ref_3.SUBGRP_SK").alias("SUBGRP_SK"),
        F.col("Lnk_Ref_3.PROD_SK").alias("PROD_SK"),
        F.col("Lnk_Ref_3.PROD_SH_NM_SK").alias("PROD_SH_NM_SK"),
        F.col("Lnk_Ref_3.SUB_SK").alias("SUB_SK"),
        F.col("Lnk_Ref_3.SUBGRP_ID").alias("SUBGRP_ID"),
        F.col("Lnk_Ref_3.SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
        F.col("Lnk_Src3.APL_CUR_DCSN_TX_SPACE").alias("APL_CUR_DCSN_TX_SPACE"),
        F.col("Lnk_Src3.APL_DENIAL_STG_TX_SPACE").alias("APL_DENIAL_STG_TX_SPACE"),
        F.col("Lnk_Ref_3.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK").alias("MBR_ENR_CLS_PLN_PROD_CAT_CD_SK"),
        F.col("Lnk_Ref_3.SUB_ADDR_ST_CD_SK").alias("SUB_ADDR_ST_CD_SK"),
        F.col("Lnk_Ref_3.PROD_SH_NM_DLVRY_METH_CD").alias("PROD_SH_NM_DLVRY_METH_CD"),
    )
)

df_xfm_pass3 = (
    df_lkp_3_lnk_Src3.alias("lnk_Codes")
    .select("*")
    .withColumn("dtchk", F.when(
        (F.to_date(F.col("EFF_DT_SK"), "yyyy-MM-dd") <= F.col("NEWDIR_APL_RCVD_DT")) &
        (F.to_date(F.col("TERM_DT_SK"), "yyyy-MM-dd") >= F.col("NEWDIR_APL_RCVD_DT")),
        "Y").otherwise("N")
    )
    .withColumn("svbirthdtchk", F.when(
        F.to_date(F.col("MBR_BRTH_DT_SK"), "yyyy-MM-dd") == F.col("MBR_BRTH_DT"), "Y").otherwise("N"))
    .withColumn("svfirstnmchk", F.when(
        F.upper(trim(F.col("MBR_FIRST_NM"))) == F.upper(trim(F.col("FIRST_NM"))), "Y").otherwise("N"))
    .withColumn("svlstnmchk", F.when(
        F.upper(trim(F.col("MBR_LAST_NM"))) == F.upper(trim(F.col("LAST_NM"))), "Y").otherwise("N"))
    .withColumn("svsfxchk", F.when(
        trim(F.col("MBR_SFX_NO")) == "00", "Y").otherwise("N"))
    .withColumn("secondpass", F.when(
        (F.col("dtchk") == "Y") &
        (F.col("svfirstnmchk") == "Y") &
        (F.col("svlstnmchk") == "Y") &
        (F.col("svbirthdtchk") == "Y"),
        "PASS").otherwise("REJECT"))
)

df_xfm_pass3_Mem_Pass_3 = (
    df_xfm_pass3
    .select(
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.lit("I").alias("INSRT_UPDT_CD"),
        F.lit("N").alias("DISCARD_IN"),
        F.lit("Y").alias("PASS_THRU_IN"),
        F.to_timestamp(F.lit(RunDate), "yyyy-MM-dd").alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.lit(SrcSysCd).alias("SRC_SYS_CD"),
        F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("NEWDIR_APL_AUTH_ID")).alias("PRI_KEY_STRING"),
        F.lit(0).alias("APL_SK"),
        F.col("NEWDIR_APL_AUTH_ID").alias("APL_ID"),
        F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit("0").alias("CRT_USER_SK"),
        F.lit("0").alias("CUR_PRI_USER_SK"),
        F.lit("0").alias("CUR_SEC_USER_SK"),
        F.lit("0").alias("CUR_TRTY_USER_SK"),
        F.when((F.length(trim(F.col("GRP_ID"))) == 0) | (F.col("GRP_ID").isNull()), "UNK").otherwise(F.col("GRP_ID")).alias("GRP_SK"),
        F.lit("0").alias("LAST_UPDT_USER_SK"),
        F.when((F.length(trim(F.col("MBR_UNIQ_KEY"))) == 0) | (F.col("MBR_UNIQ_KEY").isNull()), "UNK").otherwise(F.col("MBR_UNIQ_KEY")).alias("MBR_SK"),
        F.when((F.length(trim(F.col("SUBGRP_ID"))) == 0) | (F.col("SUBGRP_ID").isNull()), "UNK").otherwise(F.col("SUBGRP_ID")).alias("SUBGRP_SK"),
        F.when((F.length(trim(F.col("PROD_SK"))) == 0) | (F.col("PROD_SK").isNull()), "UNK").otherwise(F.col("PROD_SK")).alias("PROD_SK"),
        F.when((F.length(trim(F.col("PROD_SH_NM_SK"))) == 0) | (F.col("PROD_SH_NM_SK").isNull()), "UNK").otherwise(F.col("PROD_SH_NM_SK")).alias("PROD_SH_NM_SK"),
        F.when((F.length(trim(F.col("SUB_UNIQ_KEY"))) == 0) | (F.col("SUB_UNIQ_KEY").isNull()), "UNK").otherwise(F.col("SUB_UNIQ_KEY")).alias("SUB_SK"),
        F.when((F.length(trim(F.col("APL_CAT_TX"))) == 0) | (F.col("APL_CAT_TX").isNull()), "UNK").otherwise(F.col("APL_CAT_TX")).alias("APL_CAT_CD_SK"),
        F.when((F.length(trim(F.col("APL_CUR_DCSN_TX_SPACE"))) == 0) | (F.col("APL_CUR_DCSN_TX_SPACE").isNull()), "UNK")
         .otherwise(F.upper(trim(F.col("APL_CUR_DCSN_TX_SPACE")))).alias("APL_CUR_DCSN_CD_SK"),
        F.when(F.col("APL_DCSN_LTR_SENT_DTM").isNotNull(), "CLSD").otherwise("0").alias("APL_CUR_STTUS_CD_SK"),
        F.when((F.length(trim(F.col("PROD_SH_NM_DLVRY_METH_CD"))) == 0) | (F.col("PROD_SH_NM_DLVRY_METH_CD").isNull()), "UNK")
         .otherwise(F.col("PROD_SH_NM_DLVRY_METH_CD")).alias("APL_INITN_METH_CD_SK"),
        F.when((F.length(trim(F.col("SUB_ADDR_ST_CD_SK"))) == 0) | (F.col("SUB_ADDR_ST_CD_SK").isNull()), F.lit(0))
         .otherwise(F.col("SUB_ADDR_ST_CD_SK")).alias("APL_MBR_HOME_ADDR_ST_CD_SK"),
        F.lit("0").alias("APL_SUBTYP_CD_SK"),
        F.lit("0").alias("APL_TYP_CD_SK"),
        F.col("MBR_ENR_CLS_PLN_PROD_CAT_CD_SK").alias("CLS_PLN_PROD_CAT_CD_SK"),
        F.when((F.length(trim(F.col("APL_DENIAL_STG_TX_SPACE"))) == 0) | (F.col("APL_DENIAL_STG_TX_SPACE").isNull()), "UNK")
         .otherwise(F.upper(trim(F.col("APL_DENIAL_STG_TX_SPACE")))).alias("CUR_APL_LVL_CD_SK"),
        F.col("CUR_APL_LVL_EXPDTD_IN").alias("CUR_APL_LVL_EXPDTD_IN"),
        F.to_timestamp(F.date_format(F.col("NEWDIR_APL_RCVD_DT"), "yyyy-MM-dd"), "yyyy-MM-dd").alias("CRT_DTM"),
        F.to_timestamp(F.date_format(F.col("PRCS_DT"), "yyyy-MM-dd"), "yyyy-MM-dd").alias("CUR_STTUS_DTM"),
        F.col("UM_SVC_END_DT").alias("END_DT_SK"),
        F.col("UM_SVC_STRT_DT").alias("INITN_DT_SK"),
        current_timestamp().alias("LAST_UPDT_DTM"),
        F.lit("1753-01-01").alias("NEXT_RVW_DT_SK"),
        F.lit("0").alias("CUR_APL_LVL_SEQ_NO"),
        F.lit("0").alias("CUR_STTUS_SEQ_NO"),
        F.lit("0").alias("NEXT_RVW_INTRVL_NO"),
        F.when((F.length(trim(F.col("APL_CUR_DCSN_TX"))) == 0) | (F.col("APL_CUR_DCSN_TX").isNull()), "").otherwise(F.col("APL_CUR_DCSN_TX")).alias("APL_DESC"),
        F.when((F.length(trim(F.col("APL_DENIAL_SUBTYP_TX"))) == 0) | (F.col("APL_DENIAL_SUBTYP_TX").isNull()), "").otherwise(F.col("APL_DENIAL_SUBTYP_TX")).alias("APL_SUM_DESC"),
    )
)

df_lkp_4_lnk_Src4 = (
    df_lkp_3_lnk_Src3.alias("Lnk_Src4")
    .join(df_MEM_DATA_4.alias("Lnk_Ref4"), F.col("Lnk_Src4.SUB_ID") == F.col("Lnk_Ref4.SUB_ID"), "left")
    .select(
        F.col("Lnk_Src4.SUB_ID").alias("SUB_ID"),
        F.col("Lnk_Src4.MBR_FULL_NM").alias("MBR_FULL_NM"),
        F.col("Lnk_Src4.MBR_BRTH_DT").alias("MBR_BRTH_DT"),
        F.col("Lnk_Src4.NEWDIR_APL_AUTH_ID").alias("NEWDIR_APL_AUTH_ID"),
        F.col("Lnk_Src4.NEWDIR_APL_RCVD_DT").alias("NEWDIR_APL_RCVD_DT"),
        F.col("Lnk_Src4.PRCS_DT").alias("PRCS_DT"),
        F.col("Lnk_Src4.MBR_FIRST_NM").alias("MBR_FIRST_NM"),
        F.col("Lnk_Src4.MBR_MIDINIT").alias("MBR_MIDINIT"),
        F.col("Lnk_Src4.MBR_LAST_NM").alias("MBR_LAST_NM"),
        F.col("Lnk_Src4.MBR_AGE").alias("MBR_AGE"),
        F.col("Lnk_Src4.PROC_AUTH_UNIT_CT").alias("PROC_AUTH_UNIT_CT"),
        F.col("Lnk_Src4.NEWDIR_CO_NM").alias("NEWDIR_CO_NM"),
        F.col("Lnk_Src4.APL_DCSN_LTR_SENT_DTM").alias("APL_DCSN_LTR_SENT_DTM"),
        F.col("Lnk_Src4.APL_CUR_DCSN_TX").alias("APL_CUR_DCSN_TX"),
        F.col("Lnk_Src4.APL_CAT_TX").alias("APL_CAT_TX"),
        F.col("Lnk_Src4.APL_DENIAL_STG_TX").alias("APL_DENIAL_STG_TX"),
        F.col("Lnk_Src4.APL_DENIAL_TYP_TX").alias("APL_DENIAL_TYP_TX"),
        F.col("Lnk_Src4.APL_DENIAL_PHYS_RVWR_ID").alias("APL_DENIAL_PHYS_RVWR_ID"),
        F.col("Lnk_Src4.APL_DENIAL_PHYS_RVWR_FULL_NM").alias("APL_DENIAL_PHYS_RVWR_FULL_NM"),
        F.col("Lnk_Src4.APL_STTUS_TX").alias("APL_STTUS_TX"),
        F.col("Lnk_Src4.UM_SVC_STRT_DT").alias("UM_SVC_STRT_DT"),
        F.col("Lnk_Src4.UM_SVC_END_DT").alias("UM_SVC_END_DT"),
        F.col("Lnk_Src4.FNCL_LOB_TX").alias("FNCL_LOB_TX"),
        F.col("Lnk_Src4.FNCL_LOB_DESC").alias("FNCL_LOB_DESC"),
        F.col("Lnk_Src4.APL_NOTE_TX").alias("APL_NOTE_TX"),
        F.col("Lnk_Src4.NEWDIR_LAST_UPDT_USER_ID").alias("NEWDIR_LAST_UPDT_USER_ID"),
        F.col("Lnk_Src4.NEWDIR_LAST_UPDT_USER_FULL_NM").alias("NEWDIR_LAST_UPDT_USER_FULL_NM"),
        F.col("Lnk_Src4.PROC_RQST_UNIT_CT").alias("PROC_RQST_UNIT_CT"),
        F.col("Lnk_Src4.APL_RQST_DTM").alias("APL_RQST_DTM"),
        F.col("Lnk_Src4.PROC_RVSED_UNIT_CT").alias("PROC_RVSED_UNIT_CT"),
        F.col("Lnk_Src4.UM_CLS_PLN_PROD_CAT_CD_SK").alias("UM_CLS_PLN_PROD_CAT_CD_SK"),
        F.col("Lnk_Src4.APL_RVW_TYP_TX").alias("APL_RVW_TYP_TX"),
        F.col("Lnk_Src4.APL_SVC_DESC").alias("APL_SVC_DESC"),
        F.col("Lnk_Src4.APL_SVC_TX").alias("APL_SVC_TX"),
        F.col("Lnk_Src4.APL_DENIAL_SUBTYP_TX").alias("APL_DENIAL_SUBTYP_TX"),
        F.col("Lnk_Src4.APL_SUBMTR_TYP_TX").alias("APL_SUBMTR_TYP_TX"),
        F.col("Lnk_Src4.APL_ACKNMT_LTR_SENT_DTM").alias("APL_ACKNMT_LTR_SENT_DTM"),
        F.col("Lnk_Ref4.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        F.col("Lnk_Ref4.FIRST_NM").alias("FIRST_NM"),
        F.col("Lnk_Ref4.LAST_NM").alias("LAST_NM"),
        F.col("Lnk_Ref4.BRTH_DT_SK").alias("MBR_BRTH_DT_SK"),
        F.col("Lnk_Ref4.GNDR_CD").alias("GNDR_CD"),
        F.col("Lnk_Ref4.GRP_ID").alias("GRP_ID"),
        F.col("Lnk_Ref4.MBR_SFX_NO").alias("MBR_SFX_NO"),
        F.col("Lnk_Ref4.EFF_DT_SK").alias("EFF_DT_SK"),
        F.col("Lnk_Ref4.TERM_DT_SK").alias("TERM_DT_SK"),
        F.col("Lnk_Ref4.GRP_SK").alias("GRP_SK"),
        F.col("Lnk_Ref4.MBR_SK").alias("MBR_SK"),
        F.col("Lnk_Src4.CUR_APL_LVL_EXPDTD_IN").alias("CUR_APL_LVL_EXPDTD_IN"),
        F.col("Lnk_Ref4.SUBGRP_SK").alias("SUBGRP_SK"),
        F.col("Lnk_Ref4.PROD_SK").alias("PROD_SK"),
        F.col("Lnk_Ref4.PROD_SH_NM_SK").alias("PROD_SH_NM_SK"),
        F.col("Lnk_Ref4.SUB_SK").alias("SUB_SK"),
        F.col("Lnk_Ref4.SUBGRP_ID").alias("SUBGRP_ID"),
        F.col("Lnk_Ref4.SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
        F.col("Lnk_Src4.APL_CUR_DCSN_TX_SPACE").alias("APL_CUR_DCSN_TX_SPACE"),
        F.col("Lnk_Src4.APL_DENIAL_STG_TX_SPACE").alias("APL_DENIAL_STG_TX_SPACE"),
        F.col("Lnk_Ref4.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK").alias("MBR_ENR_CLS_PLN_PROD_CAT_CD_SK"),
        F.col("Lnk_Ref4.SUB_ADDR_ST_CD_SK").alias("SUB_ADDR_ST_CD_SK"),
        F.col("Lnk_Ref4.PROD_SH_NM_DLVRY_METH_CD").alias("PROD_SH_NM_DLVRY_METH_CD"),
    )
)

df_xfm_pass4 = (
    df_lkp_4_lnk_Src4.alias("lnk_Codes_41")
    .select("*")
    .withColumn("dtchk", F.when(
        (F.to_date(F.col("EFF_DT_SK"), "yyyy-MM-dd") <= F.col("NEWDIR_APL_RCVD_DT")) &
        (F.to_date(F.col("TERM_DT_SK"), "yyyy-MM-dd") >= F.col("NEWDIR_APL_RCVD_DT")),
        "Y").otherwise("N")
    )
    .withColumn("svbirthdtchk", F.when(
        F.to_date(F.col("MBR_BRTH_DT_SK"), "yyyy-MM-dd") == F.col("MBR_BRTH_DT"), "Y").otherwise("N"))
    .withColumn("svfirstnmchk", F.when(
        F.upper(trim(F.col("MBR_FIRST_NM"))) == F.upper(trim(F.col("FIRST_NM"))), "Y").otherwise("N"))
    .withColumn("svlstnmchk", F.when(
        F.upper(trim(F.col("MBR_LAST_NM"))) == F.upper(trim(F.col("LAST_NM"))), "Y").otherwise("N"))
    .withColumn("svsfxchk", F.when(
        trim(F.col("MBR_SFX_NO")) == "00", "Y").otherwise("N"))
    .withColumn("secondpass", F.when(
        (F.col("dtchk") == "Y") &
        (F.col("svfirstnmchk") == "Y") &
        (F.col("svlstnmchk") == "Y") &
        (F.col("svbirthdtchk") == "Y"),
        "PASS").otherwise("REJECT"))
)

df_xfm_pass4_Mem_Pass_4 = (
    df_xfm_pass4
    .select(
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.lit("I").alias("INSRT_UPDT_CD"),
        F.lit("N").alias("DISCARD_IN"),
        F.lit("Y").alias("PASS_THRU_IN"),
        F.to_timestamp(F.lit(RunDate), "yyyy-MM-dd").alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.lit(SrcSysCd).alias("SRC_SYS_CD"),
        F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("NEWDIR_APL_AUTH_ID")).alias("PRI_KEY_STRING"),
        F.lit(0).alias("APL_SK"),
        F.col("NEWDIR_APL_AUTH_ID").alias("APL_ID"),
        F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit("0").alias("CRT_USER_SK"),
        F.lit("0").alias("CUR_PRI_USER_SK"),
        F.lit("0").alias("CUR_SEC_USER_SK"),
        F.lit("0").alias("CUR_TRTY_USER_SK"),
        F.when((F.length(trim(F.col("GRP_ID"))) == 0) | (F.col("GRP_ID").isNull()), "UNK").otherwise(F.col("GRP_ID")).alias("GRP_SK"),
        F.lit("0").alias("LAST_UPDT_USER_SK"),
        F.when((F.length(trim(F.col("MBR_UNIQ_KEY"))) == 0) | (F.col("MBR_UNIQ_KEY").isNull()), "UNK").otherwise(F.col("MBR_UNIQ_KEY")).alias("MBR_SK"),
        F.when((F.length(trim(F.col("SUBGRP_ID"))) == 0) | (F.col("SUBGRP_ID").isNull()), "UNK").otherwise(F.col("SUBGRP_ID")).alias("SUBGRP_SK"),
        F.when((F.length(trim(F.col("PROD_SK"))) == 0) | (F.col("PROD_SK").isNull()), "UNK").otherwise(F.col("PROD_SK")).alias("PROD_SK"),
        F.when((F.length(trim(F.col("PROD_SH_NM_SK"))) == 0) | (F.col("PROD_SH_NM_SK").isNull()), "UNK").otherwise(F.col("PROD_SH_NM_SK")).alias("PROD_SH_NM_SK"),
        F.when((F.length(trim(F.col("SUB_UNIQ_KEY"))) == 0) | (F.col("SUB_UNIQ_KEY").isNull()), "UNK").otherwise(F.col("SUB_UNIQ_KEY")).alias("SUB_SK"),
        F.when((F.length(trim(F.col("APL_CAT_TX"))) == 0) | (F.col("APL_CAT_TX").isNull()), "UNK").otherwise(F.col("APL_CAT_TX")).alias("APL_CAT_CD_SK"),
        F.when((F.length(trim(F.col("APL_CUR_DCSN_TX_SPACE"))) == 0) | (F.col("APL_CUR_DCSN_TX_SPACE").isNull()), "UNK")
         .otherwise(F.upper(trim(F.col("APL_CUR_DCSN_TX_SPACE")))).alias("APL_CUR_DCSN_CD_SK"),
        F.when(F.col("APL_DCSN_LTR_SENT_DTM").isNotNull(), "CLSD").otherwise("0").alias("APL_CUR_STTUS_CD_SK"),
        F.when((F.length(trim(F.col("PROD_SH_NM_DLVRY_METH_CD"))) == 0) | (F.col("PROD_SH_NM_DLVRY_METH_CD").isNull()), "UNK")
         .otherwise(F.col("PROD_SH_NM_DLVRY_METH_CD")).alias("APL_INITN_METH_CD_SK"),
        F.when((F.length(trim(F.col("SUB_ADDR_ST_CD_SK"))) == 0) | (F.col("SUB_ADDR_ST_CD_SK").isNull()), F.lit(0))
         .otherwise(F.col("SUB_ADDR_ST_CD_SK")).alias("APL_MBR_HOME_ADDR_ST_CD_SK"),
        F.lit("0").alias("APL_SUBTYP_CD_SK"),
        F.lit("0").alias("APL_TYP_CD_SK"),
        F.col("MBR_ENR_CLS_PLN_PROD_CAT_CD_SK").alias("CLS_PLN_PROD_CAT_CD_SK"),
        F.when((F.length(trim(F.col("APL_DENIAL_STG_TX_SPACE"))) == 0) | (F.col("APL_DENIAL_STG_TX_SPACE").isNull()), "UNK")
         .otherwise(F.upper(trim(F.col("APL_DENIAL_STG_TX_SPACE")))).alias("CUR_APL_LVL_CD_SK"),
        F.col("CUR_APL_LVL_EXPDTD_IN").alias("CUR_APL_LVL_EXPDTD_IN"),
        F.to_timestamp(F.date_format(F.col("NEWDIR_APL_RCVD_DT"), "yyyy-MM-dd"), "yyyy-MM-dd").alias("CRT_DTM"),
        F.to_timestamp(F.date_format(F.col("PRCS_DT"), "yyyy-MM-dd"), "yyyy-MM-dd").alias("CUR_STTUS_DTM"),
        F.col("UM_SVC_END_DT").alias("END_DT_SK"),
        F.col("UM_SVC_STRT_DT").alias("INITN_DT_SK"),
        current_timestamp().alias("LAST_UPDT_DTM"),
        F.lit("1753-01-01").alias("NEXT_RVW_DT_SK"),
        F.lit("0").alias("CUR_APL_LVL_SEQ_NO"),
        F.lit("0").alias("CUR_STTUS_SEQ_NO"),
        F.lit("0").alias("NEXT_RVW_INTRVL_NO"),
        F.when((F.length(trim(F.col("APL_CUR_DCSN_TX"))) == 0) | (F.col("APL_CUR_DCSN_TX").isNull()), "").otherwise(F.col("APL_CUR_DCSN_TX")).alias("APL_DESC"),
        F.when((F.length(trim(F.col("APL_DENIAL_SUBTYP_TX"))) == 0) | (F.col("APL_DENIAL_SUBTYP_TX").isNull()), "").otherwise(F.col("APL_DENIAL_SUBTYP_TX")).alias("APL_SUM_DESC"),
    )
)

df_xfm_pass5 = (
    df_lkp_4_lnk_Src4.alias("lnk_src5")
    .select(
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.lit("I").alias("INSRT_UPDT_CD"),
        F.lit("N").alias("DISCARD_IN"),
        F.lit("Y").alias("PASS_THRU_IN"),
        F.to_timestamp(F.lit(RunDate), "yyyy-MM-dd").alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.lit(SrcSysCd).alias("SRC_SYS_CD"),
        F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("lnk_src5.NEWDIR_APL_AUTH_ID")).alias("PRI_KEY_STRING"),
        F.lit(0).alias("APL_SK"),
        F.col("lnk_src5.NEWDIR_APL_AUTH_ID").alias("APL_ID"),
        F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit("0").alias("CRT_USER_SK"),
        F.lit("0").alias("CUR_PRI_USER_SK"),
        F.lit("0").alias("CUR_SEC_USER_SK"),
        F.lit("0").alias("CUR_TRTY_USER_SK"),
        F.lit("0").alias("GRP_SK"),
        F.lit("0").alias("LAST_UPDT_USER_SK"),
        F.lit("0").alias("MBR_SK"),
        F.lit("0").alias("SUBGRP_SK"),
        F.lit("0").alias("PROD_SK"),
        F.lit("0").alias("PROD_SH_NM_SK"),
        F.lit("0").alias("SUB_SK"),
        F.when((F.length(trim(F.col("lnk_src5.APL_CAT_TX"))) == 0) | (F.col("lnk_src5.APL_CAT_TX").isNull()), "UNK")
         .otherwise(F.col("lnk_src5.APL_CAT_TX")).alias("APL_CAT_CD_SK"),
        F.when((F.length(trim(F.col("lnk_src5.APL_CUR_DCSN_TX_SPACE"))) == 0) | (F.col("lnk_src5.APL_CUR_DCSN_TX_SPACE").isNull()), "UNK")
         .otherwise(F.upper(trim(F.col("lnk_src5.APL_CUR_DCSN_TX_SPACE")))).alias("APL_CUR_DCSN_CD_SK"),
        F.when(F.col("lnk_src5.APL_DCSN_LTR_SENT_DTM").isNotNull(), "CLSD").otherwise("0").alias("APL_CUR_STTUS_CD_SK"),
        F.lit("0").alias("APL_INITN_METH_CD_SK"),
        F.lit("0").alias("APL_MBR_HOME_ADDR_ST_CD_SK"),
        F.lit("0").alias("APL_SUBTYP_CD_SK"),
        F.lit("0").alias("APL_TYP_CD_SK"),
        F.lit("0").alias("CLS_PLN_PROD_CAT_CD_SK"),
        F.when((F.length(trim(F.col("lnk_src5.APL_DENIAL_STG_TX_SPACE"))) == 0) | (F.col("lnk_src5.APL_DENIAL_STG_TX_SPACE").isNull()), "UNK")
         .otherwise(F.upper(trim(F.col("lnk_src5.APL_DENIAL_STG_TX_SPACE")))).alias("CUR_APL_LVL_CD_SK"),
        F.col("lnk_src5.CUR_APL_LVL_EXPDTD_IN").alias("CUR_APL_LVL_EXPDTD_IN"),
        F.to_timestamp(F.date_format(F.col("lnk_src5.NEWDIR_APL_RCVD_DT"), "yyyy-MM-dd"), "yyyy-MM-dd").alias("CRT_DTM"),
        F.to_timestamp(F.date_format(F.col("lnk_src5.PRCS_DT"), "yyyy-MM-dd"), "yyyy-MM-dd").alias("CUR_STTUS_DTM"),
        F.col("lnk_src5.UM_SVC_END_DT").alias("END_DT_SK"),
        F.col("lnk_src5.UM_SVC_STRT_DT").alias("INITN_DT_SK"),
        current_timestamp().alias("LAST_UPDT_DTM"),
        F.lit("1753-01-01").alias("NEXT_RVW_DT_SK"),
        F.lit("0").alias("CUR_APL_LVL_SEQ_NO"),
        F.lit("0").alias("CUR_STTUS_SEQ_NO"),
        F.lit("0").alias("NEXT_RVW_INTRVL_NO"),
        F.when((F.length(trim(F.col("lnk_src5.APL_CUR_DCSN_TX"))) == 0) | (F.col("lnk_src5.APL_CUR_DCSN_TX").isNull()), "").otherwise(F.col("lnk_src5.APL_CUR_DCSN_TX")).alias("APL_DESC"),
        F.when((F.length(trim(F.col("lnk_src5.APL_DENIAL_SUBTYP_TX"))) == 0) | (F.col("lnk_src5.APL_DENIAL_SUBTYP_TX").isNull()), "").otherwise(F.col("lnk_src5.APL_DENIAL_SUBTYP_TX")).alias("APL_SUM_DESC"),
    )
)

df_funnel_all = df_xfm_pass1_Mem_Pass_1.unionByName(df_xfm_pass2_Mem_Pass_2, allowMissingColumns=True) \
    .unionByName(df_xfm_pass3_Mem_Pass_3, allowMissingColumns=True) \
    .unionByName(df_xfm_pass4_Mem_Pass_4, allowMissingColumns=True) \
    .unionByName(df_xfm_pass5, allowMissingColumns=True)

df_final = df_funnel_all.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("APL_SK"),
    F.col("APL_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CRT_USER_SK"),
    F.col("CUR_PRI_USER_SK"),
    F.col("CUR_SEC_USER_SK"),
    F.col("CUR_TRTY_USER_SK"),
    F.col("GRP_SK"),
    F.col("LAST_UPDT_USER_SK"),
    F.col("MBR_SK"),
    F.col("SUBGRP_SK"),
    F.col("PROD_SK"),
    F.col("PROD_SH_NM_SK"),
    F.col("SUB_SK"),
    F.col("APL_CAT_CD_SK"),
    F.col("APL_CUR_DCSN_CD_SK"),
    F.col("APL_CUR_STTUS_CD_SK"),
    F.col("APL_INITN_METH_CD_SK"),
    F.col("APL_MBR_HOME_ADDR_ST_CD_SK"),
    F.col("APL_SUBTYP_CD_SK"),
    F.col("APL_TYP_CD_SK"),
    F.col("CLS_PLN_PROD_CAT_CD_SK"),
    F.col("CUR_APL_LVL_CD_SK"),
    F.rpad(F.col("CUR_APL_LVL_EXPDTD_IN"), 1, " ").alias("CUR_APL_LVL_EXPDTD_IN"),
    F.col("CRT_DTM"),
    F.col("CUR_STTUS_DTM"),
    F.rpad(F.col("END_DT_SK"), 10, " ").alias("END_DT_SK"),
    F.rpad(F.col("INITN_DT_SK"), 10, " ").alias("INITN_DT_SK"),
    F.col("LAST_UPDT_DTM"),
    F.rpad(F.col("NEXT_RVW_DT_SK"), 10, " ").alias("NEXT_RVW_DT_SK"),
    F.col("CUR_APL_LVL_SEQ_NO"),
    F.col("CUR_STTUS_SEQ_NO"),
    F.col("NEXT_RVW_INTRVL_NO"),
    F.col("APL_DESC"),
    F.col("APL_SUM_DESC"),
)

verified_path = f"{adls_path}/verified/{SrcSysCd}_NDBH_Interm.dat.{RunID}"

write_files(
    df_final,
    verified_path,
    delimiter="|",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote=None,
    nullValue=None
)