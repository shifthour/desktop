# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC PROCESSING:  Extracting data from APL_LVL
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date               Project/Altiris #               Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------    ---------------------------------------------------------   ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Bhoomi Dasari                2/08/2007           Appeals/3028              Originally Programmed                        devlEDW10                    Steph Goddard            09/15/2007
# MAGIC 
# MAGIC 
# MAGIC Srikanth Mettpalli            2013-11-01          5114                            Original Programming                         EnterpriseWrhsDevl        Jag Yelavarthi              2014-01-17     
# MAGIC                                                                                              (Server to Parallel Conversion)
# MAGIC 
# MAGIC Ravi Singh                     12/07/2018          MTM- 5841         Added in parameter and soruce query for extract NDBH,                  EnterpriseDev2             Kalyan Neelam 2018-12-10   
# MAGIC                                                                                               Evicore(MEDSLTNS) and Telligen Appeal process data

# MAGIC Code SK lookups for Denormalization
# MAGIC Job Name: IdsEdwAplLvlDExtr
# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Add Defaults and Null Handling.
# MAGIC IDS APL_LVL extract from IDS
# MAGIC Write APL_LVL_D Data into a Sequential file for Load Job IdsEdwAplLvlDLoad.
# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


# Parameters
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
CurrRunCycleDate = get_widget_value('CurrRunCycleDate','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
ExtractRunCycle = get_widget_value('ExtractRunCycle','')
MedSltnsExtractRunCycle = get_widget_value('MedSltnsExtractRunCycle','')
NdbhExtractRunCycle = get_widget_value('NdbhExtractRunCycle','')
TelligenExtractRunCycle = get_widget_value('TelligenExtractRunCycle','')

# Acquire DB config
jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# db2_APL_LVL_RVWR_in
sql_db2_APL_LVL_RVWR_in = f"""
SELECT DISTINCT
APL_LVL_RVWR.APL_ID,
APL_LVL_RVWR.APL_LVL_SEQ_NO,
'A' AS DUMMY
FROM {IDSOwner}.APL_LVL_RVWR APL_LVL_RVWR
"""
df_db2_APL_LVL_RVWR_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", sql_db2_APL_LVL_RVWR_in)
    .load()
)

# db2_APL_LVL_in
sql_db2_APL_LVL_in = f"""
SELECT
APL_LVL.APL_LVL_SK,
COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,
APL_LVL.APL_ID,
APL_LVL.SEQ_NO,
APL_LVL.APL_SK,
APL_LVL.CRT_USER_SK,
APL_LVL.LAST_UPDT_USER_SK,
APL_LVL.PRI_USER_SK,
APL_LVL.SEC_USER_SK,
APL_LVL.TRTY_USER_SK,
APL_LVL.APL_LVL_CD_SK,
APL_LVL.APL_LVL_CUR_STTUS_CD_SK,
APL_LVL.APL_LVL_DCSN_CD_SK,
APL_LVL.APL_LVL_DCSN_RSN_CD_SK,
APL_LVL.APL_LVL_DSPT_RSLTN_TYP_CD_SK,
APL_LVL.APL_LVL_INITN_METH_CD_SK,
APL_LVL.APL_LVL_LATE_DCSN_RSN_CD_SK,
APL_LVL.APL_LVL_NTFCTN_CAT_CD_SK,
APL_LVL.APL_LVL_NTFCTN_METH_CD_SK,
APL_LVL.EXPDTD_IN,
APL_LVL.HRNG_IN,
APL_LVL.INITN_DT_SK,
APL_LVL.CRT_DTM,
APL_LVL.CUR_STTUS_DTM,
APL_LVL.DCSN_DT_SK,
APL_LVL.HRNG_DT_SK,
APL_LVL.LAST_UPDT_DTM,
APL_LVL.NTFCTN_DT_SK,
APL_LVL.CUR_STTUS_SEQ_NO,
APL_LVL.LVL_DESC
FROM {IDSOwner}.APL_LVL APL_LVL
LEFT JOIN {IDSOwner}.CD_MPPNG CD
ON APL_LVL.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
WHERE
( CD.TRGT_CD = 'FACETS' AND APL_LVL.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtractRunCycle} )
OR ( CD.TRGT_CD = 'MEDSLTNS' AND APL_LVL.LAST_UPDT_RUN_CYC_EXCTN_SK >= {MedSltnsExtractRunCycle} )
OR ( CD.TRGT_CD = 'NDBH' AND APL_LVL.LAST_UPDT_RUN_CYC_EXCTN_SK >= {NdbhExtractRunCycle} )
OR ( CD.TRGT_CD = 'TELLIGEN' AND APL_LVL.LAST_UPDT_RUN_CYC_EXCTN_SK >= {TelligenExtractRunCycle} )
"""
df_db2_APL_LVL_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", sql_db2_APL_LVL_in)
    .load()
)

# db2_CD_MPPNG_in
sql_db2_CD_MPPNG_in = f"""
SELECT
CD_MPPNG_SK,
COALESCE(TRGT_CD,'UNK') TRGT_CD,
COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM
FROM {IDSOwner}.CD_MPPNG
"""
df_db2_CD_MPPNG_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", sql_db2_CD_MPPNG_in)
    .load()
)

# cpy_cd_mppng (PxCopy)
df_cpy_cd_mppng = df_db2_CD_MPPNG_in.select(
    F.col("CD_MPPNG_SK"),
    F.col("TRGT_CD"),
    F.col("TRGT_CD_NM")
)

# db2_APP_USER_in
sql_db2_APP_USER_in = f"""
SELECT
APP_USER.USER_SK,
APP_USER.USER_ID
FROM {IDSOwner}.APP_USER APP_USER
"""
df_db2_APP_USER_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", sql_db2_APP_USER_in)
    .load()
)

# Cp_App_User (PxCopy)
df_Cp_App_User = df_db2_APP_USER_in.select(
    F.col("USER_SK"),
    F.col("USER_ID")
)

# lkp_Codes (PxLookup)
lnk_IdsEdwAplLvlDExtr_InABC = df_db2_APL_LVL_in.alias("lnk_IdsEdwAplLvlDExtr_InABC")

Ref_ApllvlCd_Lkup = df_cpy_cd_mppng.alias("Ref_ApllvlCd_Lkup")
Ref_ApllvlCurStusCd_Lkup = df_cpy_cd_mppng.alias("Ref_ApllvlCurStusCd_Lkup")
Ref_AplLvlDcsnSttus_Lkup = df_cpy_cd_mppng.alias("Ref_AplLvlDcsnSttus_Lkup")
Ref_AplLvlDcsnRsnCd_Lkup = df_cpy_cd_mppng.alias("Ref_AplLvlDcsnRsnCd_Lkup")
Ref_AplLvlDsptRsltnCd_Lkup = df_cpy_cd_mppng.alias("Ref_AplLvlDsptRsltnCd_Lkup")
Ref_AplLvlInitnMethCd_Lkup = df_cpy_cd_mppng.alias("Ref_AplLvlInitnMethCd_Lkup")
Ref_AplLvlLateDcsnRsnCd_Lkup = df_cpy_cd_mppng.alias("Ref_AplLvlLateDcsnRsnCd_Lkup")
Ref_AplLvlntfctnCatCd_Lkup = df_cpy_cd_mppng.alias("Ref_AplLvlntfctnCatCd_Lkup")
Ref_AplLvlntfctnMethCd_Lkup_1 = df_cpy_cd_mppng.alias("Ref_AplLvlntfctnMethCd_Lkup")
Ref_AplLvlntfctnMethCd_Lkup_2 = df_cpy_cd_mppng.alias("Ref_AplLvlntfctnMethCd_Lkup_2")

Ref_APL_LVL_RVWR_Lkp = df_db2_APL_LVL_RVWR_in.alias("Ref_APL_LVL_RVWR_Lkp")

CurPri = df_Cp_App_User.alias("CurPri")
CurSec = df_Cp_App_User.alias("CurSec")
crt_usr_sk = df_Cp_App_User.alias("crt_usr_sk")
Lst_updt_sk = df_Cp_App_User.alias("Lst_updt_sk")
CurTrty = df_Cp_App_User.alias("CurTrty")

df_lkp_joined = (
    lnk_IdsEdwAplLvlDExtr_InABC
    .join(Ref_ApllvlCd_Lkup, F.col("lnk_IdsEdwAplLvlDExtr_InABC.APL_LVL_CD_SK") == F.col("Ref_ApllvlCd_Lkup.CD_MPPNG_SK"), "left")
    .join(Ref_ApllvlCurStusCd_Lkup, F.col("lnk_IdsEdwAplLvlDExtr_InABC.APL_LVL_CUR_STTUS_CD_SK") == F.col("Ref_ApllvlCurStusCd_Lkup.CD_MPPNG_SK"), "left")
    .join(Lst_updt_sk, F.col("lnk_IdsEdwAplLvlDExtr_InABC.LAST_UPDT_USER_SK") == F.col("Lst_updt_sk.USER_SK"), "left")
    .join(Ref_AplLvlDcsnSttus_Lkup, F.col("lnk_IdsEdwAplLvlDExtr_InABC.APL_LVL_DCSN_CD_SK") == F.col("Ref_AplLvlDcsnSttus_Lkup.CD_MPPNG_SK"), "left")
    .join(Ref_AplLvlDcsnRsnCd_Lkup, F.col("lnk_IdsEdwAplLvlDExtr_InABC.APL_LVL_DCSN_RSN_CD_SK") == F.col("Ref_AplLvlDcsnRsnCd_Lkup.CD_MPPNG_SK"), "left")
    .join(Ref_AplLvlDsptRsltnCd_Lkup, F.col("lnk_IdsEdwAplLvlDExtr_InABC.APL_LVL_DSPT_RSLTN_TYP_CD_SK") == F.col("Ref_AplLvlDsptRsltnCd_Lkup.CD_MPPNG_SK"), "left")
    .join(Ref_AplLvlInitnMethCd_Lkup, F.col("lnk_IdsEdwAplLvlDExtr_InABC.APL_LVL_INITN_METH_CD_SK") == F.col("Ref_AplLvlInitnMethCd_Lkup.CD_MPPNG_SK"), "left")
    .join(Ref_AplLvlLateDcsnRsnCd_Lkup, F.col("lnk_IdsEdwAplLvlDExtr_InABC.APL_LVL_LATE_DCSN_RSN_CD_SK") == F.col("Ref_AplLvlLateDcsnRsnCd_Lkup.CD_MPPNG_SK"), "left")
    .join(Ref_AplLvlntfctnCatCd_Lkup, F.col("lnk_IdsEdwAplLvlDExtr_InABC.APL_LVL_NTFCTN_CAT_CD_SK") == F.col("Ref_AplLvlntfctnCatCd_Lkup.CD_MPPNG_SK"), "left")
    .join(Ref_APL_LVL_RVWR_Lkp, [
        F.col("lnk_IdsEdwAplLvlDExtr_InABC.APL_ID") == F.col("Ref_APL_LVL_RVWR_Lkp.APL_ID"),
        F.col("lnk_IdsEdwAplLvlDExtr_InABC.SEQ_NO") == F.col("Ref_APL_LVL_RVWR_Lkp.APL_LVL_SEQ_NO")
    ], "left")
    .join(Ref_AplLvlntfctnMethCd_Lkup_1, F.col("lnk_IdsEdwAplLvlDExtr_InABC.APL_LVL_NTFCTN_METH_CD_SK") == F.col("Ref_AplLvlntfctnMethCd_Lkup_1.CD_MPPNG_SK"), "left")
    .join(CurPri, F.col("lnk_IdsEdwAplLvlDExtr_InABC.PRI_USER_SK") == F.col("CurPri.USER_SK"), "left")
    .join(CurSec, F.col("lnk_IdsEdwAplLvlDExtr_InABC.SEC_USER_SK") == F.col("CurSec.USER_SK"), "left")
    .join(CurTrty, F.col("lnk_IdsEdwAplLvlDExtr_InABC.TRTY_USER_SK") == F.col("CurTrty.USER_SK"), "left")
    .join(crt_usr_sk, F.col("lnk_IdsEdwAplLvlDExtr_InABC.CRT_USER_SK") == F.col("crt_usr_sk.USER_SK"), "left")
    # The second reference to notification method (Ref_AplLvlntfctnMethCd_Lkup with "LinkIdentifier": "V53S13P13") is the same lookup, just a different link name. We can ignore duplication since it's the same join condition. 
)

df_lkp_Codes = df_lkp_joined.select(
    F.col("lnk_IdsEdwAplLvlDExtr_InABC.APL_LVL_SK").alias("APL_LVL_SK"),
    F.col("lnk_IdsEdwAplLvlDExtr_InABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnk_IdsEdwAplLvlDExtr_InABC.APL_ID").alias("APL_ID"),
    F.col("lnk_IdsEdwAplLvlDExtr_InABC.SEQ_NO").alias("APL_LVL_SEQ_NO"),
    F.col("lnk_IdsEdwAplLvlDExtr_InABC.APL_SK").alias("APL_SK"),
    F.col("lnk_IdsEdwAplLvlDExtr_InABC.CRT_USER_SK").alias("CRT_USER_SK"),
    F.col("lnk_IdsEdwAplLvlDExtr_InABC.LAST_UPDT_USER_SK").alias("LAST_UPDT_USER_SK"),
    F.col("lnk_IdsEdwAplLvlDExtr_InABC.PRI_USER_SK").alias("PRI_USER_SK"),
    F.col("lnk_IdsEdwAplLvlDExtr_InABC.SEC_USER_SK").alias("SEC_USER_SK"),
    F.col("lnk_IdsEdwAplLvlDExtr_InABC.TRTY_USER_SK").alias("TRTY_USER_SK"),
    F.col("Ref_ApllvlCd_Lkup.TRGT_CD").alias("APL_LVL_CD"),
    F.col("Ref_ApllvlCd_Lkup.TRGT_CD_NM").alias("APL_LVL_NM"),
    F.col("Ref_ApllvlCurStusCd_Lkup.TRGT_CD").alias("APL_LVL_CUR_STTUS_CD"),
    F.col("Ref_ApllvlCurStusCd_Lkup.TRGT_CD_NM").alias("APL_LVL_CUR_STTUS_NM"),
    F.col("Ref_AplLvlDcsnSttus_Lkup.TRGT_CD").alias("APL_LVL_DCSN_CD"),
    F.col("Ref_AplLvlDcsnSttus_Lkup.TRGT_CD_NM").alias("APL_LVL_DCSN_NM"),
    F.col("Ref_AplLvlDcsnRsnCd_Lkup.TRGT_CD").alias("APL_LVL_DCSN_RSN_CD"),
    F.col("Ref_AplLvlDcsnRsnCd_Lkup.TRGT_CD_NM").alias("APL_LVL_DCSN_RSN_NM"),
    F.col("Ref_AplLvlDsptRsltnCd_Lkup.TRGT_CD").alias("APL_LVL_DSPT_RSLTN_TYP_CD"),
    F.col("Ref_AplLvlDsptRsltnCd_Lkup.TRGT_CD_NM").alias("APL_LVL_DSPT_RSLTN_TYP_NM"),
    F.col("Ref_AplLvlInitnMethCd_Lkup.TRGT_CD").alias("APL_LVL_INITN_METH_CD"),
    F.col("Ref_AplLvlInitnMethCd_Lkup.TRGT_CD_NM").alias("APL_LVL_INITN_METH_NM"),
    F.col("Ref_AplLvlLateDcsnRsnCd_Lkup.TRGT_CD").alias("APL_LVL_LATE_DCSN_RSN_CD"),
    F.col("Ref_AplLvlLateDcsnRsnCd_Lkup.TRGT_CD_NM").alias("APL_LVL_LATE_DCSN_RSN_NM"),
    F.col("Ref_AplLvlntfctnCatCd_Lkup.TRGT_CD").alias("APL_LVL_NTFCTN_CAT_CD"),
    F.col("Ref_AplLvlntfctnCatCd_Lkup.TRGT_CD_NM").alias("APL_LVL_NTFCTN_CAT_NM"),
    F.col("Ref_AplLvlntfctnMethCd_Lkup_1.TRGT_CD").alias("APL_LVL_NTFCTN_METH_CD"),
    F.col("Ref_AplLvlntfctnMethCd_Lkup_1.TRGT_CD_NM").alias("APL_LVL_NTFCTN_METH_NM"),
    F.col("lnk_IdsEdwAplLvlDExtr_InABC.EXPDTD_IN").alias("APL_LVL_EXPDTD_IN"),
    F.col("lnk_IdsEdwAplLvlDExtr_InABC.HRNG_IN").alias("APL_LVL_HRNG_IN"),
    F.col("lnk_IdsEdwAplLvlDExtr_InABC.INITN_DT_SK").alias("APL_LVL_INITN_DT_SK"),
    F.col("lnk_IdsEdwAplLvlDExtr_InABC.CRT_DTM").alias("CRT_DTM"),
    F.col("lnk_IdsEdwAplLvlDExtr_InABC.CUR_STTUS_DTM").alias("CUR_STTUS_DTM"),
    F.col("lnk_IdsEdwAplLvlDExtr_InABC.DCSN_DT_SK").alias("APL_LVL_DCSN_DT_SK"),
    F.col("lnk_IdsEdwAplLvlDExtr_InABC.HRNG_DT_SK").alias("APL_LVL_HRNG_DT_SK"),
    F.col("lnk_IdsEdwAplLvlDExtr_InABC.LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
    F.col("lnk_IdsEdwAplLvlDExtr_InABC.NTFCTN_DT_SK").alias("APL_LVL_NTFCTN_DT_SK"),
    F.col("lnk_IdsEdwAplLvlDExtr_InABC.CUR_STTUS_SEQ_NO").alias("APL_LVL_CUR_STTUS_SEQ_NO"),
    F.col("lnk_IdsEdwAplLvlDExtr_InABC.LVL_DESC").alias("APL_LVL_DESC"),
    F.col("crt_usr_sk.USER_ID").alias("CRT_USER_ID"),
    F.col("CurPri.USER_ID").alias("PRI_USER_ID"),
    F.col("CurSec.USER_ID").alias("SEC_USER_ID"),
    F.col("CurTrty.USER_ID").alias("TRTY_USER_ID"),
    F.col("Lst_updt_sk.USER_ID").alias("LAST_UPDT_USER_ID"),
    F.col("lnk_IdsEdwAplLvlDExtr_InABC.APL_LVL_CD_SK").alias("APL_LVL_CD_SK"),
    F.col("lnk_IdsEdwAplLvlDExtr_InABC.APL_LVL_CUR_STTUS_CD_SK").alias("APL_LVL_CUR_STTUS_CD_SK"),
    F.col("lnk_IdsEdwAplLvlDExtr_InABC.APL_LVL_DCSN_CD_SK").alias("APL_LVL_DCSN_CD_SK"),
    F.col("lnk_IdsEdwAplLvlDExtr_InABC.APL_LVL_DCSN_RSN_CD_SK").alias("APL_LVL_DCSN_RSN_CD_SK"),
    F.col("lnk_IdsEdwAplLvlDExtr_InABC.APL_LVL_DSPT_RSLTN_TYP_CD_SK").alias("APL_LVL_DSPT_RSLTN_TYP_CD_SK"),
    F.col("lnk_IdsEdwAplLvlDExtr_InABC.APL_LVL_INITN_METH_CD_SK").alias("APL_LVL_INITN_METH_CD_SK"),
    F.col("lnk_IdsEdwAplLvlDExtr_InABC.APL_LVL_LATE_DCSN_RSN_CD_SK").alias("APL_LVL_LATE_DCSN_RSN_CD_SK"),
    F.col("lnk_IdsEdwAplLvlDExtr_InABC.APL_LVL_NTFCTN_CAT_CD_SK").alias("APL_LVL_NTFCTN_CAT_CD_SK"),
    F.col("lnk_IdsEdwAplLvlDExtr_InABC.APL_LVL_NTFCTN_METH_CD_SK").alias("APL_LVL_NTFCTN_METH_CD_SK"),
    F.col("Ref_APL_LVL_RVWR_Lkp.DUMMY").alias("DUMMY")
)

# xfrm_BusinessLogic (CTransformerStage)

# 1) Main output: lnk_xfm_Data
df_xfm_Data_pre = df_lkp_Codes.filter(
    (F.col("APL_LVL_SK") != 1) & (F.col("APL_LVL_SK") != 0)
)

df_xfm_Data = (
    df_xfm_Data_pre
    .withColumn("SRC_SYS_CD", F.when(F.col("SRC_SYS_CD").isNull() | (trim(F.col("SRC_SYS_CD")) == ""), "NA").otherwise(F.col("SRC_SYS_CD")))
    .withColumn("APL_LVL_CD", F.when(F.col("APL_LVL_CD").isNull() | (trim(F.col("APL_LVL_CD")) == ""), "NA").otherwise(F.col("APL_LVL_CD")))
    .withColumn("APL_LVL_NM", F.when(F.col("APL_LVL_NM").isNull() | (trim(F.col("APL_LVL_NM")) == ""), "NA").otherwise(F.col("APL_LVL_NM")))
    .withColumn("APL_LVL_CUR_STTUS_CD", F.when(F.col("APL_LVL_CUR_STTUS_CD").isNull() | (trim(F.col("APL_LVL_CUR_STTUS_CD")) == ""), "NA").otherwise(F.col("APL_LVL_CUR_STTUS_CD")))
    .withColumn("APL_LVL_CUR_STTUS_NM", F.when(F.col("APL_LVL_CUR_STTUS_NM").isNull() | (trim(F.col("APL_LVL_CUR_STTUS_NM")) == ""), "NA").otherwise(F.col("APL_LVL_CUR_STTUS_NM")))
    .withColumn("APL_LVL_DCSN_CD", F.when(F.col("APL_LVL_DCSN_CD").isNull() | (trim(F.col("APL_LVL_DCSN_CD")) == ""), "NA").otherwise(F.col("APL_LVL_DCSN_CD")))
    .withColumn("APL_LVL_DCSN_NM", F.when(F.col("APL_LVL_DCSN_NM").isNull() | (trim(F.col("APL_LVL_DCSN_NM")) == ""), "NA").otherwise(F.col("APL_LVL_DCSN_NM")))
    .withColumn("APL_LVL_DCSN_RSN_CD", F.when(F.col("APL_LVL_DCSN_RSN_CD").isNull() | (trim(F.col("APL_LVL_DCSN_RSN_CD")) == ""), "NA").otherwise(F.col("APL_LVL_DCSN_RSN_CD")))
    .withColumn("APL_LVL_DCSN_RSN_NM", F.when(F.col("APL_LVL_DCSN_RSN_NM").isNull() | (trim(F.col("APL_LVL_DCSN_RSN_NM")) == ""), "NA").otherwise(F.col("APL_LVL_DCSN_RSN_NM")))
    .withColumn("APL_LVL_DSPT_RSLTN_TYP_CD", F.when(F.col("APL_LVL_DSPT_RSLTN_TYP_CD").isNull() | (trim(F.col("APL_LVL_DSPT_RSLTN_TYP_CD")) == ""), "NA").otherwise(F.col("APL_LVL_DSPT_RSLTN_TYP_CD")))
    .withColumn("APL_LVL_DSPT_RSLTN_TYP_NM", F.when(F.col("APL_LVL_DSPT_RSLTN_TYP_NM").isNull() | (trim(F.col("APL_LVL_DSPT_RSLTN_TYP_NM")) == ""), "NA").otherwise(F.col("APL_LVL_DSPT_RSLTN_TYP_NM")))
    .withColumn("APL_LVL_INITN_METH_CD", F.when(F.col("APL_LVL_INITN_METH_CD").isNull() | (trim(F.col("APL_LVL_INITN_METH_CD")) == ""), "NA").otherwise(F.col("APL_LVL_INITN_METH_CD")))
    .withColumn("APL_LVL_INITN_METH_NM", F.when(F.col("APL_LVL_INITN_METH_NM").isNull() | (trim(F.col("APL_LVL_INITN_METH_NM")) == ""), "NA").otherwise(F.col("APL_LVL_INITN_METH_NM")))
    .withColumn("APL_LVL_LATE_DCSN_RSN_CD", F.when(F.col("APL_LVL_LATE_DCSN_RSN_CD").isNull() | (trim(F.col("APL_LVL_LATE_DCSN_RSN_CD")) == ""), "NA").otherwise(F.col("APL_LVL_LATE_DCSN_RSN_CD")))
    .withColumn("APL_LVL_LATE_DCSN_RSN_NM", F.when(F.col("APL_LVL_LATE_DCSN_RSN_NM").isNull() | (trim(F.col("APL_LVL_LATE_DCSN_RSN_NM")) == ""), "NA").otherwise(F.col("APL_LVL_LATE_DCSN_RSN_NM")))
    .withColumn("APL_LVL_NTFCTN_CAT_CD", F.when(F.col("APL_LVL_NTFCTN_CAT_CD").isNull() | (trim(F.col("APL_LVL_NTFCTN_CAT_CD")) == ""), "NA").otherwise(F.col("APL_LVL_NTFCTN_CAT_CD")))
    .withColumn("APL_LVL_NTFCTN_CAT_NM", F.when(F.col("APL_LVL_NTFCTN_CAT_NM").isNull() | (trim(F.col("APL_LVL_NTFCTN_CAT_NM")) == ""), "NA").otherwise(F.col("APL_LVL_NTFCTN_CAT_NM")))
    .withColumn("APL_LVL_NTFCTN_METH_CD", F.when(F.col("APL_LVL_NTFCTN_METH_CD").isNull() | (trim(F.col("APL_LVL_NTFCTN_METH_CD")) == ""), "NA").otherwise(F.col("APL_LVL_NTFCTN_METH_CD")))
    .withColumn("APL_LVL_NTFCTN_METH_NM", F.when(F.col("APL_LVL_NTFCTN_METH_NM").isNull() | (trim(F.col("APL_LVL_NTFCTN_METH_NM")) == ""), "NA").otherwise(F.col("APL_LVL_NTFCTN_METH_NM")))
    .withColumn("APL_LVL_RVW_IN", F.when(F.col("DUMMY").isNull(), "N").otherwise("Y"))
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.lit(CurrRunCycleDate))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.lit(CurrRunCycleDate))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(CurrRunCycle))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(CurrRunCycle))
    .withColumn("APL_LVL_CRT_DT_SK", F.expr("TimestampToDate(CRT_DTM)"))
    .withColumn("APL_LVL_CUR_STTUS_DT_SK", F.expr("TimestampToDate(CUR_STTUS_DTM)"))
    .withColumn("LAST_UPDT_DT_SK", F.expr("TimestampToDate(LAST_UPDT_DTM)"))
    .withColumn("CRT_USER_ID", F.when((trim(F.col("CRT_USER_ID")) == "") | F.col("CRT_USER_ID").isNull(), "NA").otherwise(F.col("CRT_USER_ID")))
    .withColumn("PRI_USER_ID", F.when((trim(F.col("PRI_USER_ID")) == "") | F.col("PRI_USER_ID").isNull(), "NA").otherwise(F.col("PRI_USER_ID")))
    .withColumn("SEC_USER_ID", F.when((trim(F.col("SEC_USER_ID")) == "") | F.col("SEC_USER_ID").isNull(), "NA").otherwise(F.col("SEC_USER_ID")))
    .withColumn("TRTY_USER_ID", F.when((trim(F.col("TRTY_USER_ID")) == "") | F.col("TRTY_USER_ID").isNull(), "NA").otherwise(F.col("TRTY_USER_ID")))
    .withColumn("LAST_UPDT_USER_ID", F.when((trim(F.col("LAST_UPDT_USER_ID")) == "") | F.col("LAST_UPDT_USER_ID").isNull(), "NA").otherwise(F.col("LAST_UPDT_USER_ID")))
)

# 2) lnk_NA_out => single row with specified values
schema_na = StructType([
    StructField("APL_LVL_SK", IntegerType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("APL_ID", StringType(), True),
    StructField("APL_LVL_SEQ_NO", IntegerType(), True),
    StructField("CRT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("APL_SK", IntegerType(), True),
    StructField("CRT_USER_SK", IntegerType(), True),
    StructField("LAST_UPDT_USER_SK", IntegerType(), True),
    StructField("PRI_USER_SK", IntegerType(), True),
    StructField("SEC_USER_SK", IntegerType(), True),
    StructField("TRTY_USER_SK", IntegerType(), True),
    StructField("APL_LVL_CD", StringType(), True),
    StructField("APL_LVL_NM", StringType(), True),
    StructField("APL_LVL_CUR_STTUS_CD", StringType(), True),
    StructField("APL_LVL_CUR_STTUS_NM", StringType(), True),
    StructField("APL_LVL_DCSN_CD", StringType(), True),
    StructField("APL_LVL_DCSN_NM", StringType(), True),
    StructField("APL_LVL_DCSN_RSN_CD", StringType(), True),
    StructField("APL_LVL_DCSN_RSN_NM", StringType(), True),
    StructField("APL_LVL_DSPT_RSLTN_TYP_CD", StringType(), True),
    StructField("APL_LVL_DSPT_RSLTN_TYP_NM", StringType(), True),
    StructField("APL_LVL_INITN_METH_CD", StringType(), True),
    StructField("APL_LVL_INITN_METH_NM", StringType(), True),
    StructField("APL_LVL_LATE_DCSN_RSN_CD", StringType(), True),
    StructField("APL_LVL_LATE_DCSN_RSN_NM", StringType(), True),
    StructField("APL_LVL_NTFCTN_CAT_CD", StringType(), True),
    StructField("APL_LVL_NTFCTN_CAT_NM", StringType(), True),
    StructField("APL_LVL_NTFCTN_METH_CD", StringType(), True),
    StructField("APL_LVL_NTFCTN_METH_NM", StringType(), True),
    StructField("APL_LVL_EXPDTD_IN", StringType(), True),
    StructField("APL_LVL_HRNG_IN", StringType(), True),
    StructField("APL_LVL_RVW_IN", StringType(), True),
    StructField("APL_LVL_INITN_DT_SK", StringType(), True),
    StructField("APL_LVL_CRT_DT_SK", StringType(), True),
    StructField("APL_LVL_CUR_STTUS_DT_SK", StringType(), True),
    StructField("APL_LVL_DCSN_DT_SK", StringType(), True),
    StructField("APL_LVL_HRNG_DT_SK", StringType(), True),
    StructField("LAST_UPDT_DT_SK", StringType(), True),
    StructField("APL_LVL_NTFCTN_DT_SK", StringType(), True),
    StructField("APL_LVL_CUR_STTUS_SEQ_NO", IntegerType(), True),
    StructField("APL_LVL_DESC", StringType(), True),
    StructField("CRT_USER_ID", StringType(), True),
    StructField("PRI_USER_ID", StringType(), True),
    StructField("SEC_USER_ID", StringType(), True),
    StructField("TRTY_USER_ID", StringType(), True),
    StructField("LAST_UPDT_USER_ID", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("APL_LVL_CD_SK", IntegerType(), True),
    StructField("APL_LVL_CUR_STTUS_CD_SK", IntegerType(), True),
    StructField("APL_LVL_DCSN_CD_SK", IntegerType(), True),
    StructField("APL_LVL_DCSN_RSN_CD_SK", IntegerType(), True),
    StructField("APL_LVL_DSPT_RSLTN_TYP_CD_SK", IntegerType(), True),
    StructField("APL_LVL_INITN_METH_CD_SK", IntegerType(), True),
    StructField("APL_LVL_LATE_DCSN_RSN_CD_SK", IntegerType(), True),
    StructField("APL_LVL_NTFCTN_CAT_CD_SK", IntegerType(), True),
    StructField("APL_LVL_NTFCTN_METH_CD_SK", IntegerType(), True),
])
row_na = [(
    1, "NA", "NA", 0, "1753-01-01", "1753-01-01", 1, 1, 1, 1, 1, 1,
    "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA",
    "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA",