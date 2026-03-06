# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC ******************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:
# MAGIC 
# MAGIC PROCESSING:
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date               Project/Altiris #               Change Description                                      Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------    ---------------------------------------------------------             ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Naren Garapaty                09/02/2007                                              Originally Programmed                                devlEDW10        
# MAGIC 
# MAGIC Srikanth Mettpalli              10/30/2013        5114                           Original Programming                                    EnterpriseWrhsDevl        Jag Yelavarthi             2014-01-17
# MAGIC                                                                                                        (Server to Parallel Conversion)  
# MAGIC 
# MAGIC Jag Yelavarthi                    2014-05-28      TFS#1169                    Added SmallInt Check for CT columns         EnterpriseNewDevl          Kalyan Neelam            2014-07-30
# MAGIC                                                                                                        ACKNMT_LTR_WORK_DAYS_CT
# MAGIC                                                                                                       TWENTY_DAY_LTR_WORK_DAYS_CT
# MAGIC                                                                                                        DEPT_DT_RCVD_WORK_DAYS_CT
# MAGIC                                                                                                       CLSD_WORK_DAYS_CT

# MAGIC Job Name: IdsEdwAplLvlOrigActvtyFExtr
# MAGIC Read all the Data from IDS APL_LVL Table;
# MAGIC Add Defaults and Null Handling.
# MAGIC Write APL_LVL_ORIG_ACTVTY_F Data into a Sequential file for Load Job IdsEdwAplLvlOrigActvtyFLoad.
# MAGIC Logic to Calculate Number of BCBS Working Days Using Range Lookup
# MAGIC Read all the Data from IDS CLNDR_DT Table;
# MAGIC Logic to Aggregate number of working days to use in the next transformer
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


# Parameters
IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")
CurrRunCycleDate = get_widget_value("CurrRunCycleDate","")
ExtractRunCycle = get_widget_value("ExtractRunCycle","")
EDWRunCycle = get_widget_value("EDWRunCycle","")

# DB2ConnectorPX Stages - Read from IDS Database
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

# Stage: db2_CLNDR_DT_in
extract_query_db2_CLNDR_DT_in = f"SELECT CLNDR_DT_SK FROM {IDSOwner}.CLNDR_DT WHERE BCBSKC_BUS_DAY_IN = 'Y'"
df_db2_CLNDR_DT_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_CLNDR_DT_in)
    .load()
)

# Stage: xfrm_BusinessLogic1 has 1 input (df_db2_CLNDR_DT_in) and 4 outputs
df_xfrm_BusinessLogic1_in = df_db2_CLNDR_DT_in.alias("lnk_IdsEdwAplLvlOrigActvtyFExtr_InABC")

df_Ref_TwentyDayLtrWorkDaysCt_Lkp = df_xfrm_BusinessLogic1_in.select(
    F.col("lnk_IdsEdwAplLvlOrigActvtyFExtr_InABC.CLNDR_DT_SK").alias("CLNDR_DT_SK"),
    F.lit(0).alias("ACKNMT_LTR_WORK_DAYS_CT"),
    F.lit(0).alias("CLSD_WORK_DAYS_CT"),
    F.lit(1).alias("TWENTY_DAY_LTR_WORK_DAYS_CT"),
    F.lit(0).alias("DEPT_DT_RCVD_WORK_DAYS_CT"),
    F.lit("A").alias("DUMMY")
)

df_Ref_AcknmtLtrWorkDaysCt_Lkp = df_xfrm_BusinessLogic1_in.select(
    F.col("lnk_IdsEdwAplLvlOrigActvtyFExtr_InABC.CLNDR_DT_SK").alias("CLNDR_DT_SK"),
    F.lit(1).alias("ACKNMT_LTR_WORK_DAYS_CT"),
    F.lit(0).alias("CLSD_WORK_DAYS_CT"),
    F.lit(0).alias("TWENTY_DAY_LTR_WORK_DAYS_CT"),
    F.lit(0).alias("DEPT_DT_RCVD_WORK_DAYS_CT"),
    F.lit("A").alias("DUMMY")
)

df_Ref_ClsdWorkDaysCt_Lkp = df_xfrm_BusinessLogic1_in.select(
    F.col("lnk_IdsEdwAplLvlOrigActvtyFExtr_InABC.CLNDR_DT_SK").alias("CLNDR_DT_SK"),
    F.lit(0).alias("ACKNMT_LTR_WORK_DAYS_CT"),
    F.lit(1).alias("CLSD_WORK_DAYS_CT"),
    F.lit(0).alias("TWENTY_DAY_LTR_WORK_DAYS_CT"),
    F.lit(0).alias("DEPT_DT_RCVD_WORK_DAYS_CT"),
    F.lit("A").alias("DUMMY")
)

df_Ref_DeptDtRcvdWorkDaysCt_Lkp = df_xfrm_BusinessLogic1_in.select(
    F.col("lnk_IdsEdwAplLvlOrigActvtyFExtr_InABC.CLNDR_DT_SK").alias("CLNDR_DT_SK"),
    F.lit(0).alias("ACKNMT_LTR_WORK_DAYS_CT"),
    F.lit(0).alias("CLSD_WORK_DAYS_CT"),
    F.lit(0).alias("TWENTY_DAY_LTR_WORK_DAYS_CT"),
    F.lit(1).alias("DEPT_DT_RCVD_WORK_DAYS_CT"),
    F.lit("A").alias("DUMMY")
)

# Other DB2 Connector Stages
extract_query_db2_DeptDays_in = f"""
SELECT
APL_LVL.APL_LVL_SK,
APL.INITN_DT_SK INITN_DT_SK_DPNDT,
max(APL_ACTVTY.ACTVTY_DT_SK) AS ACTVTY_DT_SK_DPNDT_DAYS
FROM {IDSOwner}.APL_LVL APL_LVL,
     {IDSOwner}.APL APL,
     {IDSOwner}.APL_ACTVTY APL_ACTVTY,
     {IDSOwner}.CD_MPPNG CD_MPPNG
WHERE
  APL_LVL.APL_SK = APL.APL_SK
  AND APL_LVL.APL_ID = APL_ACTVTY.APL_ID
  AND APL_LVL.SEQ_NO = APL_ACTVTY.APL_LVL_SEQ_NO
  AND APL_ACTVTY.APL_ACTVTY_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
  AND CD_MPPNG.TRGT_CD = 'AT23'
GROUP BY
  APL_LVL.APL_LVL_SK,
  APL.INITN_DT_SK
"""
df_db2_DeptDays_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_DeptDays_in)
    .load()
    .alias("lnk_DeptDays_Lkup")
)

extract_query_db2_DeptDt_in = f"""
SELECT
APL_LVL.APL_LVL_SK,
max(APL_ACTVTY.ACTVTY_DT_SK) AS ACTVTY_DT_SK_DPNDT
FROM {IDSOwner}.APL_ACTVTY APL_ACTVTY,
     {IDSOwner}.CD_MPPNG CD_MPPNG,
     {IDSOwner}.APL_LVL APL_LVL
WHERE
  APL_LVL.APL_ID = APL_ACTVTY.APL_ID
  AND APL_LVL.SEQ_NO = APL_ACTVTY.APL_LVL_SEQ_NO
  AND APL_ACTVTY.APL_ACTVTY_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
  AND CD_MPPNG.TRGT_CD = 'AT23'
GROUP BY
  APL_LVL.APL_LVL_SK
"""
df_db2_DeptDt_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_DeptDt_in)
    .load()
    .alias("lnk_DeptDt_Lkup")
)

extract_query_db2_DeptCt_in = f"""
SELECT
APL_LVL.APL_LVL_SK,
COUNT(APL_LVL.APL_LVL_SK) AS DEPT_DT_RCVD_CT
FROM {IDSOwner}.APL_ACTVTY APL_ACTVTY,
     {IDSOwner}.CD_MPPNG CD_MPPNG,
     {IDSOwner}.APL_LVL APL_LVL
WHERE
  APL_LVL.APL_ID = APL_ACTVTY.APL_ID
  AND APL_LVL.SEQ_NO = APL_ACTVTY.APL_LVL_SEQ_NO
  AND APL_ACTVTY.APL_ACTVTY_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
  AND CD_MPPNG.TRGT_CD = 'AT23'
GROUP BY
  APL_LVL.APL_LVL_SK
"""
df_db2_DeptCt_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_DeptCt_in)
    .load()
    .alias("lnk_DeptCt_Lkup")
)

extract_query_db2_TwentyDays_in = f"""
SELECT
APL_LVL.APL_LVL_SK,
APL.INITN_DT_SK AS INITN_DT_SK_TWNTY,
max(APL_ACTVTY.ACTVTY_DT_SK) AS ACTVTY_DT_SK_TWNTY_DAYS
FROM {IDSOwner}.APL_LVL APL_LVL,
     {IDSOwner}.APL APL,
     {IDSOwner}.APL_ACTVTY APL_ACTVTY,
     {IDSOwner}.CD_MPPNG CD_MPPNG
WHERE
  APL_LVL.APL_SK = APL.APL_SK
  AND APL_LVL.APL_ID = APL_ACTVTY.APL_ID
  AND APL_LVL.SEQ_NO = APL_ACTVTY.APL_LVL_SEQ_NO
  AND APL_ACTVTY.APL_ACTVTY_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
  AND CD_MPPNG.TRGT_CD ='AT14'
GROUP BY
  APL_LVL.APL_LVL_SK,
  APL.INITN_DT_SK
"""
df_db2_TwentyDays_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_TwentyDays_in)
    .load()
    .alias("lnk_TwentyDays_Lkup")
)

extract_query_db2_TwentyDt_in = f"""
SELECT
APL_LVL.APL_LVL_SK,
max(APL_ACTVTY.ACTVTY_DT_SK) AS ACTVTY_DT_SK_TWNTY
FROM {IDSOwner}.APL_ACTVTY APL_ACTVTY,
     {IDSOwner}.CD_MPPNG CD_MPPNG,
     {IDSOwner}.APL_LVL APL_LVL
WHERE
  APL_LVL.APL_ID = APL_ACTVTY.APL_ID
  AND APL_LVL.SEQ_NO = APL_ACTVTY.APL_LVL_SEQ_NO
  AND APL_ACTVTY.APL_ACTVTY_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
  AND CD_MPPNG.TRGT_CD = 'AT14'
GROUP BY
  APL_LVL.APL_LVL_SK
"""
df_db2_TwentyDt_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_TwentyDt_in)
    .load()
    .alias("lnk_TwentyDt_Lkup")
)

extract_query_db2_TwentyCt_in = f"""
SELECT
APL_LVL.APL_LVL_SK,
COUNT(APL_LVL.APL_LVL_SK) AS TWENTY_DAY_LTR_CT
FROM {IDSOwner}.APL_ACTVTY APL_ACTVTY,
     {IDSOwner}.CD_MPPNG CD_MPPNG,
     {IDSOwner}.APL_LVL APL_LVL
WHERE
  APL_LVL.APL_ID = APL_ACTVTY.APL_ID
  AND APL_LVL.SEQ_NO = APL_ACTVTY.APL_LVL_SEQ_NO
  AND APL_ACTVTY.APL_ACTVTY_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
  AND CD_MPPNG.TRGT_CD = 'AT14'
GROUP BY
  APL_LVL.APL_LVL_SK
"""
df_db2_TwentyCt_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_TwentyCt_in)
    .load()
    .alias("lnk_TwentyCt_Lkup")
)

extract_query_db2_ClsdDays_in = f"""
SELECT
APL_LVL.APL_LVL_SK,
APL.INITN_DT_SK AS INITN_DT_SK_CLSD,
max(APL_ACTVTY.ACTVTY_DT_SK) AS ACTVTY_DT_SK_CLSD_DAYS
FROM {IDSOwner}.APL_LVL APL_LVL,
     {IDSOwner}.APL APL,
     {IDSOwner}.APL_ACTVTY APL_ACTVTY,
     {IDSOwner}.CD_MPPNG CD_MPPNG
WHERE
  APL_LVL.APL_SK = APL.APL_SK
  AND APL_LVL.APL_ID = APL_ACTVTY.APL_ID
  AND APL_LVL.SEQ_NO = APL_ACTVTY.APL_LVL_SEQ_NO
  AND APL_ACTVTY.APL_ACTVTY_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
  AND CD_MPPNG.TRGT_CD = 'AT13'
GROUP BY
  APL_LVL.APL_LVL_SK,
  APL.INITN_DT_SK
"""
df_db2_ClsdDays_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_ClsdDays_in)
    .load()
    .alias("lnk_ClsdDays_Lkup")
)

extract_query_db2_ClsdDt_in = f"""
SELECT
APL_LVL.APL_LVL_SK,
max(APL_ACTVTY.ACTVTY_DT_SK) AS ACTVTY_DT_SK_CLSD
FROM {IDSOwner}.APL_ACTVTY APL_ACTVTY,
     {IDSOwner}.CD_MPPNG CD_MPPNG,
     {IDSOwner}.APL_LVL APL_LVL
WHERE
  APL_LVL.APL_ID = APL_ACTVTY.APL_ID
  AND APL_LVL.SEQ_NO = APL_ACTVTY.APL_LVL_SEQ_NO
  AND APL_ACTVTY.APL_ACTVTY_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
  AND CD_MPPNG.TRGT_CD = 'AT13'
GROUP BY
  APL_LVL.APL_LVL_SK
"""
df_db2_ClsdDt_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_ClsdDt_in)
    .load()
    .alias("lnk_ClsdDt_Lkup")
)

extract_query_db2_AckDays_in = f"""
SELECT
APL_LVL.APL_LVL_SK,
APL.INITN_DT_SK INITN_DT_SK_ACK,
max(APL_ACTVTY.ACTVTY_DT_SK) AS ACTVTY_DT_SK_ACK_DAYS
FROM {IDSOwner}.APL_LVL APL_LVL,
     {IDSOwner}.APL APL,
     {IDSOwner}.APL_ACTVTY APL_ACTVTY,
     {IDSOwner}.CD_MPPNG CD_MPPNG
WHERE
  APL_LVL.APL_SK = APL.APL_SK
  AND APL_LVL.APL_ID = APL_ACTVTY.APL_ID
  AND APL_LVL.SEQ_NO = APL_ACTVTY.APL_LVL_SEQ_NO
  AND APL_ACTVTY.APL_ACTVTY_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
  AND CD_MPPNG.TRGT_CD = 'AT04'
GROUP BY
  APL_LVL.APL_LVL_SK,
  APL.INITN_DT_SK
"""
df_db2_AckDays_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_AckDays_in)
    .load()
    .alias("lnk_AckDays_Lkup")
)

extract_query_db2_ClsdCt_in = f"""
SELECT
APL_LVL.APL_LVL_SK,
COUNT(APL_LVL.APL_LVL_SK) AS CLSD_CT
FROM {IDSOwner}.APL_ACTVTY APL_ACTVTY,
     {IDSOwner}.CD_MPPNG CD_MPPNG,
     {IDSOwner}.APL_LVL APL_LVL
WHERE
  APL_LVL.APL_ID = APL_ACTVTY.APL_ID
  AND APL_LVL.SEQ_NO = APL_ACTVTY.APL_LVL_SEQ_NO
  AND APL_ACTVTY.APL_ACTVTY_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
  AND CD_MPPNG.TRGT_CD = 'AT13'
GROUP BY
  APL_LVL.APL_LVL_SK
"""
df_db2_ClsdCt_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_ClsdCt_in)
    .load()
    .alias("lnk_ClsdCt_Lkup")
)

extract_query_db2_AckLtrDt_in = f"""
SELECT
APL_LVL.APL_LVL_SK,
max(APL_ACTVTY.ACTVTY_DT_SK) AS ACTVTY_DT_SK_ACK
FROM {IDSOwner}.APL_ACTVTY APL_ACTVTY,
     {IDSOwner}.CD_MPPNG CD_MPPNG,
     {IDSOwner}.APL_LVL APL_LVL
WHERE
  APL_LVL.APL_ID = APL_ACTVTY.APL_ID
  AND APL_LVL.SEQ_NO = APL_ACTVTY.APL_LVL_SEQ_NO
  AND APL_ACTVTY.APL_ACTVTY_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
  AND CD_MPPNG.TRGT_CD = 'AT04'
GROUP BY
  APL_LVL.APL_LVL_SK
"""
df_db2_AckLtrDt_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_AckLtrDt_in)
    .load()
    .alias("lnk_AckLtrDt_Lkup")
)

extract_query_db2_AckLtrCt_in = f"""
SELECT
APL_LVL.APL_LVL_SK,
COUNT(APL_LVL.APL_LVL_SK) AS ACKNMT_LTR_CT
FROM {IDSOwner}.APL_ACTVTY APL_ACTVTY,
     {IDSOwner}.CD_MPPNG CD_MPPNG,
     {IDSOwner}.APL_LVL APL_LVL
WHERE
  APL_LVL.APL_ID = APL_ACTVTY.APL_ID
  AND APL_LVL.SEQ_NO = APL_ACTVTY.APL_LVL_SEQ_NO
  AND APL_ACTVTY.APL_ACTVTY_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
  AND CD_MPPNG.TRGT_CD = 'AT04'
GROUP BY
  APL_LVL.APL_LVL_SK
"""
df_db2_AckLtrCt_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_AckLtrCt_in)
    .load()
    .alias("lnk_AckLtrCt_Lkup")
)

extract_query_db2_APL_LVL_in = f"""
SELECT DISTINCT
 APL_LVL.APL_LVL_SK,
 COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,
 APL_LVL.APL_ID,
 APL_LVL.SEQ_NO,
 'A' AS DUMMY
FROM {IDSOwner}.APL_LVL APL_LVL
INNER JOIN {IDSOwner}.APL APL
   ON APL_LVL.APL_SK = APL.APL_SK
INNER JOIN {IDSOwner}.APL_ACTVTY APL_ACTVTY
   ON APL_LVL.APL_ID = APL_ACTVTY.APL_ID
   AND APL_LVL.SEQ_NO = APL_ACTVTY.APL_LVL_SEQ_NO
INNER JOIN {IDSOwner}.CD_MPPNG CD_MPPNG
   ON APL_ACTVTY.APL_ACTVTY_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
   AND CD_MPPNG.TRGT_CD IN ('AT04','AT13','AT14','AT23')
LEFT JOIN {IDSOwner}.CD_MPPNG CD
   ON APL_LVL.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
WHERE
 APL.INITN_DT_SK > '2004-12-31'
 AND APL_LVL.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtractRunCycle}
"""
df_db2_APL_LVL_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_APL_LVL_in)
    .load()
    .alias("lnk_IdsEdwAplLvlOrigActvtyFExtr_InABC")
)

# Stage: Lkp_Apl_Data (PxLookup) - Big left joins
df_Lkp_Apl_Data_pre = (
    df_db2_APL_LVL_in
    .join(df_db2_AckLtrCt_in, F.col("lnk_IdsEdwAplLvlOrigActvtyFExtr_InABC.APL_LVL_SK") == F.col("lnk_AckLtrCt_Lkup.APL_LVL_SK"), "left")
    .join(df_db2_AckLtrDt_in, F.col("lnk_IdsEdwAplLvlOrigActvtyFExtr_InABC.APL_LVL_SK") == F.col("lnk_AckLtrDt_Lkup.APL_LVL_SK"), "left")
    .join(df_db2_AckDays_in, F.col("lnk_IdsEdwAplLvlOrigActvtyFExtr_InABC.APL_LVL_SK") == F.col("lnk_AckDays_Lkup.APL_LVL_SK"), "left")
    .join(df_db2_ClsdCt_in, F.col("lnk_IdsEdwAplLvlOrigActvtyFExtr_InABC.APL_LVL_SK") == F.col("lnk_ClsdCt_Lkup.APL_LVL_SK"), "left")
    .join(df_db2_ClsdDt_in, F.col("lnk_IdsEdwAplLvlOrigActvtyFExtr_InABC.APL_LVL_SK") == F.col("lnk_ClsdDt_Lkup.APL_LVL_SK"), "left")
    .join(df_db2_ClsdDays_in, F.col("lnk_IdsEdwAplLvlOrigActvtyFExtr_InABC.APL_LVL_SK") == F.col("lnk_ClsdDays_Lkup.APL_LVL_SK"), "left")
    .join(df_db2_TwentyCt_in, F.col("lnk_IdsEdwAplLvlOrigActvtyFExtr_InABC.APL_LVL_SK") == F.col("lnk_TwentyCt_Lkup.APL_LVL_SK"), "left")
    .join(df_db2_TwentyDt_in, F.col("lnk_IdsEdwAplLvlOrigActvtyFExtr_InABC.APL_LVL_SK") == F.col("lnk_TwentyDt_Lkup.APL_LVL_SK"), "left")
    .join(df_db2_TwentyDays_in, F.col("lnk_IdsEdwAplLvlOrigActvtyFExtr_InABC.APL_LVL_SK") == F.col("lnk_TwentyDays_Lkup.APL_LVL_SK"), "left")
    .join(df_db2_DeptCt_in, F.col("lnk_IdsEdwAplLvlOrigActvtyFExtr_InABC.APL_LVL_SK") == F.col("lnk_DeptCt_Lkup.APL_LVL_SK"), "left")
    .join(df_db2_DeptDt_in, F.col("lnk_IdsEdwAplLvlOrigActvtyFExtr_InABC.APL_LVL_SK") == F.col("lnk_DeptDt_Lkup.APL_LVL_SK"), "left")
    .join(df_db2_DeptDays_in, F.col("lnk_IdsEdwAplLvlOrigActvtyFExtr_InABC.APL_LVL_SK") == F.col("lnk_DeptDays_Lkup.APL_LVL_SK"), "left")
)

df_Lkp_Apl_Data = df_Lkp_Apl_Data_pre.select(
    F.col("lnk_IdsEdwAplLvlOrigActvtyFExtr_InABC.APL_LVL_SK").alias("APL_LVL_SK"),
    F.col("lnk_IdsEdwAplLvlOrigActvtyFExtr_InABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnk_IdsEdwAplLvlOrigActvtyFExtr_InABC.APL_ID").alias("APL_ID"),
    F.col("lnk_IdsEdwAplLvlOrigActvtyFExtr_InABC.SEQ_NO").alias("SEQ_NO"),
    F.col("lnk_AckLtrCt_Lkup.ACKNMT_LTR_CT").alias("ACKNMT_LTR_CT"),
    F.col("lnk_AckLtrDt_Lkup.ACTVTY_DT_SK_ACK").cast(StringType()).alias("ACTVTY_DT_SK_ACK"),
    F.col("lnk_AckDays_Lkup.INITN_DT_SK_ACK").cast(StringType()).alias("INITN_DT_SK_ACK"),
    F.col("lnk_AckDays_Lkup.ACTVTY_DT_SK_ACK_DAYS").cast(StringType()).alias("ACTVTY_DT_SK_ACK_DAYS"),
    F.col("lnk_ClsdCt_Lkup.CLSD_CT").alias("CLSD_CT"),
    F.col("lnk_ClsdDt_Lkup.ACTVTY_DT_SK_CLSD").cast(StringType()).alias("ACTVTY_DT_SK_CLSD"),
    F.col("lnk_ClsdDays_Lkup.INITN_DT_SK_CLSD").cast(StringType()).alias("INITN_DT_SK_CLSD"),
    F.col("lnk_ClsdDays_Lkup.ACTVTY_DT_SK_CLSD_DAYS").cast(StringType()).alias("ACTVTY_DT_SK_CLSD_DAYS"),
    F.col("lnk_TwentyCt_Lkup.TWENTY_DAY_LTR_CT").alias("TWENTY_DAY_LTR_CT"),
    F.col("lnk_TwentyDt_Lkup.ACTVTY_DT_SK_TWNTY").cast(StringType()).alias("ACTVTY_DT_SK_TWNTY"),
    F.col("lnk_TwentyDays_Lkup.INITN_DT_SK_TWNTY").cast(StringType()).alias("INITN_DT_SK_TWNTY"),
    F.col("lnk_TwentyDays_Lkup.ACTVTY_DT_SK_TWNTY_DAYS").cast(StringType()).alias("ACTVTY_DT_SK_TWNTY_DAYS"),
    F.col("lnk_DeptCt_Lkup.DEPT_DT_RCVD_CT").alias("DEPT_DT_RCVD_CT"),
    F.col("lnk_DeptDt_Lkup.ACTVTY_DT_SK_DPNDT").cast(StringType()).alias("ACTVTY_DT_SK_DPNDT"),
    F.col("lnk_DeptDays_Lkup.INITN_DT_SK_DPNDT").cast(StringType()).alias("INITN_DT_SK_DPNDT"),
    F.col("lnk_DeptDays_Lkup.ACTVTY_DT_SK_DPNDT_DAYS").cast(StringType()).alias("ACTVTY_DT_SK_DPNDT_DAYS"),
    F.col("lnk_IdsEdwAplLvlOrigActvtyFExtr_InABC.DUMMY").alias("DUMMY")
)

# Stage: Transformer_92 => from df_Lkp_Apl_Data => produce multiple output pins
df_Transformer_92_in = df_Lkp_Apl_Data.alias("Lnk_lkp_Data_Out")

def select_Transformer_92_columns(df_in):
    return df_in.select(
        F.col("APL_LVL_SK"),
        F.col("SRC_SYS_CD"),
        F.col("APL_ID"),
        F.col("SEQ_NO"),
        F.col("ACKNMT_LTR_CT"),
        F.col("ACTVTY_DT_SK_ACK").cast(StringType()).alias("ACTVTY_DT_SK_ACK"),
        F.col("INITN_DT_SK_ACK").cast(StringType()).alias("INITN_DT_SK_ACK"),
        F.col("ACTVTY_DT_SK_ACK_DAYS").cast(StringType()).alias("ACTVTY_DT_SK_ACK_DAYS"),
        F.col("CLSD_CT"),
        F.col("ACTVTY_DT_SK_CLSD").cast(StringType()).alias("ACTVTY_DT_SK_CLSD"),
        F.col("INITN_DT_SK_CLSD").cast(StringType()).alias("INITN_DT_SK_CLSD"),
        F.col("ACTVTY_DT_SK_CLSD_DAYS").cast(StringType()).alias("ACTVTY_DT_SK_CLSD_DAYS"),
        F.col("TWENTY_DAY_LTR_CT"),
        F.col("ACTVTY_DT_SK_TWNTY").cast(StringType()).alias("ACTVTY_DT_SK_TWNTY"),
        F.col("INITN_DT_SK_TWNTY").cast(StringType()).alias("INITN_DT_SK_TWNTY"),
        F.col("ACTVTY_DT_SK_TWNTY_DAYS").cast(StringType()).alias("ACTVTY_DT_SK_TWNTY_DAYS"),
        F.col("DEPT_DT_RCVD_CT"),
        F.col("ACTVTY_DT_SK_DPNDT").cast(StringType()).alias("ACTVTY_DT_SK_DPNDT"),
        F.col("INITN_DT_SK_DPNDT").cast(StringType()).alias("INITN_DT_SK_DPNDT"),
        F.col("ACTVTY_DT_SK_DPNDT_DAYS").cast(StringType()).alias("ACTVTY_DT_SK_DPNDT_DAYS"),
        F.col("DUMMY")
    )

df_Lnk_AcknmtLtrWorkDaysCt_Lkp = select_Transformer_92_columns(df_Transformer_92_in).alias("Lnk_AcknmtLtrWorkDaysCt_Lkp")
df_Lnk_ClsdWorkDaysCt_Lkp = select_Transformer_92_columns(df_Transformer_92_in).alias("Lnk_ClsdWorkDaysCt_Lkp")
df_Lnk_TwentyDayLtrWorkDaysCt_Lkp = select_Transformer_92_columns(df_Transformer_92_in).alias("Lnk_TwentyDayLtrWorkDaysCt_Lkp")
df_Lnk_DeptDtRcvdWorkDaysCt_Lkp = select_Transformer_92_columns(df_Transformer_92_in).alias("Lnk_DeptDtRcvdWorkDaysCt_Lkp")

# Next: Lkup4 => primary link = Lnk_DeptDtRcvdWorkDaysCt_Lkp, lookup link = Ref_DeptDtRcvdWorkDaysCt_Lkp

# We assume a user-defined function Range(...) that returns a boolean. 
# We'll join on that function plus DUMMY match. The DataStage code suggests:
# Range(Lnk_DeptDtRcvdWorkDaysCt_Lkp.ACTVTY_DT_SK_DPNDT_DAYS, "<=", Lnk_DeptDtRcvdWorkDaysCt_Lkp.INITN_DT_SK_DPNDT, ">=", 0)
# We'll treat it as a join condition. Typically, that might produce no actual rows unless that range is true. 
# To preserve the “left” nature, we do a left join on the expression. This is a notional stand-in for the real logic:

df_Lkup4 = (
    df_Lnk_DeptDtRcvdWorkDaysCt_Lkp.join(
        df_Ref_DeptDtRcvdWorkDaysCt_Lkp,
        (F.expr('Range('
                'Lnk_DeptDtRcvdWorkDaysCt_Lkp.ACTVTY_DT_SK_DPNDT_DAYS, '
                '"<=", '
                'Lnk_DeptDtRcvdWorkDaysCt_Lkp.INITN_DT_SK_DPNDT, '
                '">=", 0)') 
         & (F.col("Lnk_DeptDtRcvdWorkDaysCt_Lkp.DUMMY") == F.col("Ref_DeptDtRcvdWorkDaysCt_Lkp.DUMMY"))),
        "left"
    )
)

df_Lkup4_out = df_Lkup4.select(
    F.col("Lnk_DeptDtRcvdWorkDaysCt_Lkp.APL_LVL_SK"),
    F.col("Lnk_DeptDtRcvdWorkDaysCt_Lkp.SRC_SYS_CD"),
    F.col("Lnk_DeptDtRcvdWorkDaysCt_Lkp.APL_ID"),
    F.col("Lnk_DeptDtRcvdWorkDaysCt_Lkp.SEQ_NO"),
    F.col("Lnk_DeptDtRcvdWorkDaysCt_Lkp.ACKNMT_LTR_CT"),
    F.col("Lnk_DeptDtRcvdWorkDaysCt_Lkp.ACTVTY_DT_SK_ACK"),
    F.col("Lnk_DeptDtRcvdWorkDaysCt_Lkp.INITN_DT_SK_ACK"),
    F.col("Lnk_DeptDtRcvdWorkDaysCt_Lkp.ACTVTY_DT_SK_ACK_DAYS"),
    F.col("Lnk_DeptDtRcvdWorkDaysCt_Lkp.CLSD_CT"),
    F.col("Lnk_DeptDtRcvdWorkDaysCt_Lkp.ACTVTY_DT_SK_CLSD"),
    F.col("Lnk_DeptDtRcvdWorkDaysCt_Lkp.INITN_DT_SK_CLSD"),
    F.col("Lnk_DeptDtRcvdWorkDaysCt_Lkp.ACTVTY_DT_SK_CLSD_DAYS"),
    F.col("Lnk_DeptDtRcvdWorkDaysCt_Lkp.TWENTY_DAY_LTR_CT"),
    F.col("Lnk_DeptDtRcvdWorkDaysCt_Lkp.ACTVTY_DT_SK_TWNTY"),
    F.col("Lnk_DeptDtRcvdWorkDaysCt_Lkp.INITN_DT_SK_TWNTY"),
    F.col("Lnk_DeptDtRcvdWorkDaysCt_Lkp.ACTVTY_DT_SK_TWNTY_DAYS"),
    F.col("Lnk_DeptDtRcvdWorkDaysCt_Lkp.DEPT_DT_RCVD_CT"),
    F.col("Lnk_DeptDtRcvdWorkDaysCt_Lkp.ACTVTY_DT_SK_DPNDT"),
    F.col("Lnk_DeptDtRcvdWorkDaysCt_Lkp.INITN_DT_SK_DPNDT"),
    F.col("Lnk_DeptDtRcvdWorkDaysCt_Lkp.ACTVTY_DT_SK_DPNDT_DAYS"),
    F.col("Ref_DeptDtRcvdWorkDaysCt_Lkp.ACKNMT_LTR_WORK_DAYS_CT"),
    F.col("Ref_DeptDtRcvdWorkDaysCt_Lkp.CLSD_WORK_DAYS_CT"),
    F.col("Ref_DeptDtRcvdWorkDaysCt_Lkp.TWENTY_DAY_LTR_WORK_DAYS_CT"),
    F.col("Ref_DeptDtRcvdWorkDaysCt_Lkp.DEPT_DT_RCVD_WORK_DAYS_CT")
).alias("DSLink58")

# Lkup3 => primary link = Lnk_TwentyDayLtrWorkDaysCt_Lkp, lookup link = Ref_TwentyDayLtrWorkDaysCt_Lkp
df_Lkup3 = (
    df_Lnk_TwentyDayLtrWorkDaysCt_Lkp.join(
        df_Ref_TwentyDayLtrWorkDaysCt_Lkp,
        (F.expr('Range('
                'Lnk_TwentyDayLtrWorkDaysCt_Lkp.ACTVTY_DT_SK_TWNTY_DAYS, '
                '"<=", '
                'Lnk_TwentyDayLtrWorkDaysCt_Lkp.INITN_DT_SK_TWNTY, '
                '">=", 0)') 
         & (F.col("Lnk_TwentyDayLtrWorkDaysCt_Lkp.DUMMY") == F.col("Ref_TwentyDayLtrWorkDaysCt_Lkp.DUMMY"))),
        "left"
    )
)

df_Lkup3_out = df_Lkup3.select(
    F.col("Lnk_TwentyDayLtrWorkDaysCt_Lkp.APL_LVL_SK"),
    F.col("Lnk_TwentyDayLtrWorkDaysCt_Lkp.SRC_SYS_CD"),
    F.col("Lnk_TwentyDayLtrWorkDaysCt_Lkp.APL_ID"),
    F.col("Lnk_TwentyDayLtrWorkDaysCt_Lkp.SEQ_NO"),
    F.col("Lnk_TwentyDayLtrWorkDaysCt_Lkp.ACKNMT_LTR_CT"),
    F.col("Lnk_TwentyDayLtrWorkDaysCt_Lkp.ACTVTY_DT_SK_ACK"),
    F.col("Lnk_TwentyDayLtrWorkDaysCt_Lkp.INITN_DT_SK_ACK"),
    F.col("Lnk_TwentyDayLtrWorkDaysCt_Lkp.ACTVTY_DT_SK_ACK_DAYS"),
    F.col("Lnk_TwentyDayLtrWorkDaysCt_Lkp.CLSD_CT"),
    F.col("Lnk_TwentyDayLtrWorkDaysCt_Lkp.ACTVTY_DT_SK_CLSD"),
    F.col("Lnk_TwentyDayLtrWorkDaysCt_Lkp.INITN_DT_SK_CLSD"),
    F.col("Lnk_TwentyDayLtrWorkDaysCt_Lkp.ACTVTY_DT_SK_CLSD_DAYS"),
    F.col("Lnk_TwentyDayLtrWorkDaysCt_Lkp.TWENTY_DAY_LTR_CT"),
    F.col("Lnk_TwentyDayLtrWorkDaysCt_Lkp.ACTVTY_DT_SK_TWNTY"),
    F.col("Lnk_TwentyDayLtrWorkDaysCt_Lkp.INITN_DT_SK_TWNTY"),
    F.col("Lnk_TwentyDayLtrWorkDaysCt_Lkp.ACTVTY_DT_SK_TWNTY_DAYS"),
    F.col("Lnk_TwentyDayLtrWorkDaysCt_Lkp.DEPT_DT_RCVD_CT"),
    F.col("Lnk_TwentyDayLtrWorkDaysCt_Lkp.ACTVTY_DT_SK_DPNDT"),
    F.col("Lnk_TwentyDayLtrWorkDaysCt_Lkp.INITN_DT_SK_DPNDT"),
    F.col("Lnk_TwentyDayLtrWorkDaysCt_Lkp.ACTVTY_DT_SK_DPNDT_DAYS"),
    F.col("Ref_TwentyDayLtrWorkDaysCt_Lkp.ACKNMT_LTR_WORK_DAYS_CT"),
    F.col("Ref_TwentyDayLtrWorkDaysCt_Lkp.CLSD_WORK_DAYS_CT"),
    F.col("Ref_TwentyDayLtrWorkDaysCt_Lkp.TWENTY_DAY_LTR_WORK_DAYS_CT"),
    F.col("Ref_TwentyDayLtrWorkDaysCt_Lkp.DEPT_DT_RCVD_WORK_DAYS_CT")
).alias("DSLink56")

# Lkup2 => primary link = Lnk_ClsdWorkDaysCt_Lkp, lookup link = Ref_ClsdWorkDaysCt_Lkp
df_Lkup2 = (
    df_Lnk_ClsdWorkDaysCt_Lkp.join(
        df_Ref_ClsdWorkDaysCt_Lkp,
        (F.expr('Range('
                'Lnk_ClsdWorkDaysCt_Lkp.ACTVTY_DT_SK_CLSD_DAYS, '
                '"<=", '
                'Lnk_ClsdWorkDaysCt_Lkp.INITN_DT_SK_CLSD, '
                '">=", 0)') 
         & (F.col("Lnk_ClsdWorkDaysCt_Lkp.DUMMY") == F.col("Ref_ClsdWorkDaysCt_Lkp.DUMMY"))),
        "left"
    )
)

df_Lkup2_out = df_Lkup2.select(
    F.col("Lnk_ClsdWorkDaysCt_Lkp.APL_LVL_SK"),
    F.col("Lnk_ClsdWorkDaysCt_Lkp.SRC_SYS_CD"),
    F.col("Lnk_ClsdWorkDaysCt_Lkp.APL_ID"),
    F.col("Lnk_ClsdWorkDaysCt_Lkp.SEQ_NO"),
    F.col("Lnk_ClsdWorkDaysCt_Lkp.ACKNMT_LTR_CT"),
    F.col("Lnk_ClsdWorkDaysCt_Lkp.ACTVTY_DT_SK_ACK"),
    F.col("Lnk_ClsdWorkDaysCt_Lkp.INITN_DT_SK_ACK"),
    F.col("Lnk_ClsdWorkDaysCt_Lkp.ACTVTY_DT_SK_ACK_DAYS"),
    F.col("Lnk_ClsdWorkDaysCt_Lkp.CLSD_CT"),
    F.col("Lnk_ClsdWorkDaysCt_Lkp.ACTVTY_DT_SK_CLSD"),
    F.col("Lnk_ClsdWorkDaysCt_Lkp.INITN_DT_SK_CLSD"),
    F.col("Lnk_ClsdWorkDaysCt_Lkp.ACTVTY_DT_SK_CLSD_DAYS"),
    F.col("Lnk_ClsdWorkDaysCt_Lkp.TWENTY_DAY_LTR_CT"),
    F.col("Lnk_ClsdWorkDaysCt_Lkp.ACTVTY_DT_SK_TWNTY"),
    F.col("Lnk_ClsdWorkDaysCt_Lkp.INITN_DT_SK_TWNTY"),
    F.col("Lnk_ClsdWorkDaysCt_Lkp.ACTVTY_DT_SK_TWNTY_DAYS"),
    F.col("Lnk_ClsdWorkDaysCt_Lkp.DEPT_DT_RCVD_CT"),
    F.col("Lnk_ClsdWorkDaysCt_Lkp.ACTVTY_DT_SK_DPNDT"),
    F.col("Lnk_ClsdWorkDaysCt_Lkp.INITN_DT_SK_DPNDT"),
    F.col("Lnk_ClsdWorkDaysCt_Lkp.ACTVTY_DT_SK_DPNDT_DAYS"),
    F.col("Ref_ClsdWorkDaysCt_Lkp.ACKNMT_LTR_WORK_DAYS_CT"),
    F.col("Ref_ClsdWorkDaysCt_Lkp.CLSD_WORK_DAYS_CT"),
    F.col("Ref_ClsdWorkDaysCt_Lkp.TWENTY_DAY_LTR_WORK_DAYS_CT"),
    F.col("Ref_ClsdWorkDaysCt_Lkp.DEPT_DT_RCVD_WORK_DAYS_CT")
).alias("DSLink55")

# Lkup1 => primary link = Lnk_AcknmtLtrWorkDaysCt_Lkp, lookup link = Ref_AcknmtLtrWorkDaysCt_Lkp
df_Lkup1 = (
    df_Lnk_AcknmtLtrWorkDaysCt_Lkp.join(
        df_Ref_AcknmtLtrWorkDaysCt_Lkp,
        (F.expr('Range('
                'Lnk_AcknmtLtrWorkDaysCt_Lkp.ACTVTY_DT_SK_ACK_DAYS, '
                '"<=", '
                'Lnk_AcknmtLtrWorkDaysCt_Lkp.INITN_DT_SK_ACK, '
                '">=", 0)')
         & (F.col("Lnk_AcknmtLtrWorkDaysCt_Lkp.DUMMY") == F.col("Ref_AcknmtLtrWorkDaysCt_Lkp.DUMMY"))),
        "left"
    )
)

df_Lkup1_out = df_Lkup1.select(
    F.col("Lnk_AcknmtLtrWorkDaysCt_Lkp.APL_LVL_SK"),
    F.col("Lnk_AcknmtLtrWorkDaysCt_Lkp.SRC_SYS_CD"),
    F.col("Lnk_AcknmtLtrWorkDaysCt_Lkp.APL_ID"),
    F.col("Lnk_AcknmtLtrWorkDaysCt_Lkp.SEQ_NO"),
    F.col("Lnk_AcknmtLtrWorkDaysCt_Lkp.ACKNMT_LTR_CT"),
    F.col("Lnk_AcknmtLtrWorkDaysCt_Lkp.ACTVTY_DT_SK_ACK"),
    F.col("Lnk_AcknmtLtrWorkDaysCt_Lkp.INITN_DT_SK_ACK"),
    F.col("Lnk_AcknmtLtrWorkDaysCt_Lkp.ACTVTY_DT_SK_ACK_DAYS"),
    F.col("Lnk_AcknmtLtrWorkDaysCt_Lkp.CLSD_CT"),
    F.col("Lnk_AcknmtLtrWorkDaysCt_Lkp.ACTVTY_DT_SK_CLSD"),
    F.col("Lnk_AcknmtLtrWorkDaysCt_Lkp.INITN_DT_SK_CLSD"),
    F.col("Lnk_AcknmtLtrWorkDaysCt_Lkp.ACTVTY_DT_SK_CLSD_DAYS"),
    F.col("Lnk_AcknmtLtrWorkDaysCt_Lkp.TWENTY_DAY_LTR_CT"),
    F.col("Lnk_AcknmtLtrWorkDaysCt_Lkp.ACTVTY_DT_SK_TWNTY"),
    F.col("Lnk_AcknmtLtrWorkDaysCt_Lkp.INITN_DT_SK_TWNTY"),
    F.col("Lnk_AcknmtLtrWorkDaysCt_Lkp.ACTVTY_DT_SK_TWNTY_DAYS"),
    F.col("Lnk_AcknmtLtrWorkDaysCt_Lkp.DEPT_DT_RCVD_CT"),
    F.col("Lnk_AcknmtLtrWorkDaysCt_Lkp.ACTVTY_DT_SK_DPNDT"),
    F.col("Lnk_AcknmtLtrWorkDaysCt_Lkp.INITN_DT_SK_DPNDT"),
    F.col("Lnk_AcknmtLtrWorkDaysCt_Lkp.ACTVTY_DT_SK_DPNDT_DAYS"),
    F.col("Ref_AcknmtLtrWorkDaysCt_Lkp.ACKNMT_LTR_WORK_DAYS_CT"),
    F.col("Ref_AcknmtLtrWorkDaysCt_Lkp.CLSD_WORK_DAYS_CT"),
    F.col("Ref_AcknmtLtrWorkDaysCt_Lkp.TWENTY_DAY_LTR_WORK_DAYS_CT"),
    F.col("Ref_AcknmtLtrWorkDaysCt_Lkp.DEPT_DT_RCVD_WORK_DAYS_CT")
).alias("DSLink46")

# Funnel Stage: Fnl_Apl_Data => union of DSLink46, DSLink55, DSLink58, DSLink56
df_fnl_apl_data = DSLink46.unionByName(DSLink55).unionByName(DSLink58).unionByName(DSLink56).alias("Lnk_Fnl_Data")

# Stage: CExp_Apl_Data => column export => we flatten columns
df_CExp_Apl_Data = df_fnl_apl_data.select(
    F.concat_ws(",", 
        F.col("APL_LVL_SK"),
        F.col("SRC_SYS_CD"),
        F.col("APL_ID"),
        F.col("SEQ_NO"),
        F.col("ACKNMT_LTR_CT"),
        F.col("ACTVTY_DT_SK_ACK"),
        F.col("INITN_DT_SK_ACK"),
        F.col("ACTVTY_DT_SK_ACK_DAYS"),
        F.col("CLSD_CT"),
        F.col("ACTVTY_DT_SK_CLSD"),
        F.col("INITN_DT_SK_CLSD"),
        F.col("ACTVTY_DT_SK_CLSD_DAYS"),
        F.col("TWENTY_DAY_LTR_CT"),
        F.col("ACTVTY_DT_SK_TWNTY"),
        F.col("INITN_DT_SK_TWNTY"),
        F.col("ACTVTY_DT_SK_TWNTY_DAYS"),
        F.col("DEPT_DT_RCVD_CT"),
        F.col("ACTVTY_DT_SK_DPNDT"),
        F.col("INITN_DT_SK_DPNDT"),
        F.col("ACTVTY_DT_SK_DPNDT_DAYS"),
        F.lit("KEY")  # The stage sets last param as KEY but also references the strings for ColumnExport
    ).alias("KEY"),
    F.col("ACKNMT_LTR_WORK_DAYS_CT"),
    F.col("CLSD_WORK_DAYS_CT"),
    F.col("TWENTY_DAY_LTR_WORK_DAYS_CT"),
    F.col("DEPT_DT_RCVD_WORK_DAYS_CT")
).alias("Lnk_CExp_Data")

# Aggregator: Agg_BCBS_Working_Days => group by KEY => sum of the four columns
df_Agg_BCBS_Working_Days = (
    Lnk_CExp_Data
    .groupBy("KEY")
    .agg(
       F.sum("ACKNMT_LTR_WORK_DAYS_CT").alias("ACKNMT_LTR_WORK_DAYS_CT"),
       F.sum("CLSD_WORK_DAYS_CT").alias("CLSD_WORK_DAYS_CT"),
       F.sum("TWENTY_DAY_LTR_WORK_DAYS_CT").alias("TWENTY_DAY_LTR_WORK_DAYS_CT"),
       F.sum("DEPT_DT_RCVD_WORK_DAYS_CT").alias("DEPT_DT_RCVD_WORK_DAYS_CT")
    )
).alias("Lnk_Agg_Data")

# ColumnImport: CImp_Apl_Data => parse KEY back into fields
split_cols = F.split(F.col("KEY"), ",")
df_CImp_Apl_Data = df_Agg_BCBS_Working_Days.select(
    split_cols.getItem(0).alias("APL_LVL_SK"),
    split_cols.getItem(1).alias("SRC_SYS_CD"),
    split_cols.getItem(2).alias("APL_ID"),
    split_cols.getItem(3).alias("SEQ_NO"),
    split_cols.getItem(4).alias("ACKNMT_LTR_CT"),
    split_cols.getItem(5).cast(StringType()).alias("ACTVTY_DT_SK_ACK"),
    split_cols.getItem(6).cast(StringType()).alias("INITN_DT_SK_ACK"),
    split_cols.getItem(7).cast(StringType()).alias("ACTVTY_DT_SK_ACK_DAYS"),
    split_cols.getItem(8).alias("CLSD_CT"),
    split_cols.getItem(9).cast(StringType()).alias("ACTVTY_DT_SK_CLSD"),
    split_cols.getItem(10).cast(StringType()).alias("INITN_DT_SK_CLSD"),
    split_cols.getItem(11).cast(StringType()).alias("ACTVTY_DT_SK_CLSD_DAYS"),
    split_cols.getItem(12).alias("TWENTY_DAY_LTR_CT"),
    split_cols.getItem(13).cast(StringType()).alias("ACTVTY_DT_SK_TWNTY"),
    split_cols.getItem(14).cast(StringType()).alias("INITN_DT_SK_TWNTY"),
    split_cols.getItem(15).cast(StringType()).alias("ACTVTY_DT_SK_TWNTY_DAYS"),
    split_cols.getItem(16).alias("DEPT_DT_RCVD_CT"),
    split_cols.getItem(17).cast(StringType()).alias("ACTVTY_DT_SK_DPNDT"),
    split_cols.getItem(18).cast(StringType()).alias("INITN_DT_SK_DPNDT"),
    split_cols.getItem(19).cast(StringType()).alias("ACTVTY_DT_SK_DPNDT_DAYS"),
    F.col("ACKNMT_LTR_WORK_DAYS_CT"),
    F.col("CLSD_WORK_DAYS_CT"),
    F.col("TWENTY_DAY_LTR_WORK_DAYS_CT"),
    F.col("DEPT_DT_RCVD_WORK_DAYS_CT")
).alias("Lnk_CImp_Data")

# Stage: xfrm_BusinessLogic => final transform with 3 outputs and 1 main
df_xfrm_BusinessLogic_in = df_CImp_Apl_Data_in = df_CImp_Apl_Data.alias("Lnk_CImp_Data")

# We replicate the constraints:
# "lnk_xfm_Data": (ACKNMT_LTR_CT>0 OR CLSD_CT>0 OR TWENTY_DAY_LTR_CT>0 OR DEPT_DT_RCVD_CT>0)
df_xfrm_main = df_xfrm_BusinessLogic_in.filter(
    "(ACKNMT_LTR_CT > 0) OR (CLSD_CT > 0) OR (TWENTY_DAY_LTR_CT > 0) OR (DEPT_DT_RCVD_CT > 0)"
).select(
    F.col("Lnk_CImp_Data.APL_LVL_SK").alias("APL_LVL_SK"),
    F.col("Lnk_CImp_Data.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Lnk_CImp_Data.APL_ID").alias("APL_ID"),
    F.col("Lnk_CImp_Data.SEQ_NO").alias("APL_LVL_SEQ_NO"),
    F.lit(CurrRunCycleDate).cast(StringType()).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(CurrRunCycleDate).cast(StringType()).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("Lnk_CImp_Data.ACKNMT_LTR_CT").alias("ACKNMT_LTR_CT"),
    F.when(F.col("Lnk_CImp_Data.ACTVTY_DT_SK_ACK").isNull(), '1753-01-01').otherwise(F.col("Lnk_CImp_Data.ACTVTY_DT_SK_ACK")).alias("ACKNMT_LTR_DT_SK"),
    F.expr("if((ACTVTY_DT_SK_ACK_DAYS is null) or (INITN_DT_SK_ACK is null), SetNull(), if(INITN_DT_SK_ACK>ACTVTY_DT_SK_ACK_DAYS, -1, if((ACKNMT_LTR_WORK_DAYS_CT-1)<-31999, -31999, if((ACKNMT_LTR_WORK_DAYS_CT-1)>31999, 31999, ACKNMT_LTR_WORK_DAYS_CT-1))))").alias("ACKNMT_LTR_WORK_DAYS_CT"),
    F.expr("if((ACTVTY_DT_SK_ACK_DAYS is null) or (INITN_DT_SK_ACK is null), SetNull(), if(asInteger(DaysSinceFromDate(ACTVTY_DT_SK_ACK_DAYS, INITN_DT_SK_ACK)) < -31999, -31999, if(asInteger(DaysSinceFromDate(ACTVTY_DT_SK_ACK_DAYS, INITN_DT_SK_ACK))>31999, 31999, DaysSinceFromDate(ACTVTY_DT_SK_ACK_DAYS, INITN_DT_SK_ACK))))").alias("ACKNMT_LTR_CLNDR_DAYS_CT"),
    F.col("Lnk_CImp_Data.CLSD_CT").alias("CLSD_CT"),
    F.when(F.col("Lnk_CImp_Data.ACTVTY_DT_SK_CLSD").isNull(), '1753-01-01').otherwise(F.col("Lnk_CImp_Data.ACTVTY_DT_SK_CLSD")).alias("CLSD_DT_SK"),
    F.expr("if((ACTVTY_DT_SK_CLSD_DAYS is null) or (INITN_DT_SK_CLSD is null), SetNull(), if(INITN_DT_SK_CLSD>ACTVTY_DT_SK_CLSD_DAYS, -1, if((CLSD_WORK_DAYS_CT-1)<-31999, -31999, if((CLSD_WORK_DAYS_CT-1)>31999, 31999, CLSD_WORK_DAYS_CT-1))))").alias("CLSD_WORK_DAYS_CT"),
    F.expr("if((ACTVTY_DT_SK_CLSD_DAYS is null) or (INITN_DT_SK_CLSD is null), SetNull(), if(asInteger(DaysSinceFromDate(ACTVTY_DT_SK_CLSD_DAYS, INITN_DT_SK_CLSD))< -31999, -31999, if(asInteger(DaysSinceFromDate(ACTVTY_DT_SK_CLSD_DAYS, INITN_DT_SK_CLSD))>31999, 31999, DaysSinceFromDate(ACTVTY_DT_SK_CLSD_DAYS, INITN_DT_SK_CLSD))))").alias("CLSD_CLNDR_DAYS_CT"),
    F.col("Lnk_CImp_Data.TWENTY_DAY_LTR_CT").alias("TWENTY_DAY_LTR_CT"),
    F.when(F.col("Lnk_CImp_Data.ACTVTY_DT_SK_TWNTY").isNull(), '1753-01-01').otherwise(F.col("Lnk_CImp_Data.ACTVTY_DT_SK_TWNTY")).alias("TWENTY_DAY_LTR_DT_SK"),
    F.expr("if((ACTVTY_DT_SK_TWNTY_DAYS is null) or (INITN_DT_SK_TWNTY is null), SetNull(), if(INITN_DT_SK_TWNTY>ACTVTY_DT_SK_TWNTY_DAYS, -1, if((TWENTY_DAY_LTR_WORK_DAYS_CT-1)<-31999, -31999, if((TWENTY_DAY_LTR_WORK_DAYS_CT-1)>31999, 31999, TWENTY_DAY_LTR_WORK_DAYS_CT-1))))").alias("TWENTY_DAY_LTR_WORK_DAYS_CT"),
    F.expr("if((ACTVTY_DT_SK_TWNTY_DAYS is null) or (INITN_DT_SK_TWNTY is null), SetNull(), if(asInteger(DaysSinceFromDate(ACTVTY_DT_SK_TWNTY_DAYS, INITN_DT_SK_TWNTY))< -31999, -31999, if(asInteger(DaysSinceFromDate(ACTVTY_DT_SK_TWNTY_DAYS, INITN_DT_SK_TWNTY))>31999, 31999, DaysSinceFromDate(ACTVTY_DT_SK_TWNTY_DAYS, INITN_DT_SK_TWNTY))))").alias("TWENTY_DAY_LTR_CLNDR_DAYS_CT"),
    F.col("Lnk_CImp_Data.DEPT_DT_RCVD_CT").alias("DEPT_DT_RCVD_CT"),
    F.when(F.col("Lnk_CImp_Data.ACTVTY_DT_SK_DPNDT").isNull(), '1753-01-01').otherwise(F.col("Lnk_CImp_Data.ACTVTY_DT_SK_DPNDT")).alias("DEPT_DT_RCVD_DT_SK"),
    F.expr("if((ACTVTY_DT_SK_DPNDT_DAYS is null) or (INITN_DT_SK_DPNDT is null), SetNull(), if(INITN_DT_SK_DPNDT>ACTVTY_DT_SK_DPNDT_DAYS, -1, if((DEPT_DT_RCVD_WORK_DAYS_CT-1)<-31999, -31999, if((DEPT_DT_RCVD_WORK_DAYS_CT-1)>31999, 31999, DEPT_DT_RCVD_WORK_DAYS_CT-1))))").alias("DEPT_DT_RCVD_WORK_DAYS_CT"),
    F.expr("if((ACTVTY_DT_SK_DPNDT_DAYS is null) or (INITN_DT_SK_DPNDT is null), SetNull(), if(asInteger(DaysSinceFromDate(ACTVTY_DT_SK_DPNDT_DAYS, INITN_DT_SK_DPNDT))< -31999, -31999, if(asInteger(DaysSinceFromDate(ACTVTY_DT_SK_DPNDT_DAYS, INITN_DT_SK_DPNDT))>31999, 31999, DaysSinceFromDate(ACTVTY_DT_SK_DPNDT_DAYS, INITN_DT_SK_DPNDT))))").alias("DEPT_DT_RCVD_CLNDR_DAYS_CT"),
    F.lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
).alias("lnk_xfm_Data")

# Next two links "lnk_NA_out" and "lnk_UNK_out" each emit exactly 1 row if the partition condition is matched. We replicate the columns with the given WhereExpression.
df_xfrm_BusinessLogic_out_NA = df_xfrm_BusinessLogic_in.filter(
    "((inrownum()-1)*numpartitions() + partitionnum() +1) = 1"  # notional stand-in for the DS condition
).select(
    F.lit(1).alias("APL_LVL_SK"),
    F.lit("NA").alias("SRC_SYS_CD"),
    F.lit("NA").alias("APL_ID"),
    F.lit(0).alias("APL_LVL_SEQ_NO"),
    F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(0).alias("ACKNMT_LTR_CT"),
    F.lit("1753-01-01").alias("ACKNMT_LTR_DT_SK"),
    F.expr("SetNull()").alias("ACKNMT_LTR_WORK_DAYS_CT"),
    F.expr("SetNull()").alias("ACKNMT_LTR_CLNDR_DAYS_CT"),
    F.lit(0).alias("CLSD_CT"),
    F.lit("1753-01-01").alias("CLSD_DT_SK"),
    F.expr("SetNull()").alias("CLSD_WORK_DAYS_CT"),
    F.expr("SetNull()").alias("CLSD_CLNDR_DAYS_CT"),
    F.lit(0).alias("TWENTY_DAY_LTR_CT"),
    F.lit("1753-01-01").alias("TWENTY_DAY_LTR_DT_SK"),
    F.expr("SetNull()").alias("TWENTY_DAY_LTR_WORK_DAYS_CT"),
    F.expr("SetNull()").alias("TWENTY_DAY_LTR_CLNDR_DAYS_CT"),
    F.lit(0).alias("DEPT_DT_RCVD_CT"),
    F.lit("1753-01-01").alias("DEPT_DT_RCVD_DT_SK"),
    F.expr("SetNull()").alias("DEPT_DT_RCVD_WORK_DAYS_CT"),
    F.expr("SetNull()").alias("DEPT_DT_RCVD_CLNDR_DAYS_CT"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
).alias("lnk_NA_out")

df_xfrm_BusinessLogic_out_UNK = df_xfrm_BusinessLogic_in.filter(
    "((inrownum()-1)*numpartitions() + partitionnum() +1) = 1"
).select(
    F.lit(0).alias("APL_LVL_SK"),
    F.lit("UNK").alias("SRC_SYS_CD"),
    F.lit("UNK").alias("APL_ID"),
    F.lit(0).alias("APL_LVL_SEQ_NO"),
    F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(0).alias("ACKNMT_LTR_CT"),
    F.lit("1753-01-01").alias("ACKNMT_LTR_DT_SK"),
    F.expr("SetNull()").alias("ACKNMT_LTR_WORK_DAYS_CT"),
    F.expr("SetNull()").alias("ACKNMT_LTR_CLNDR_DAYS_CT"),
    F.lit(0).alias("CLSD_CT"),
    F.lit("1753-01-01").alias("CLSD_DT_SK"),
    F.expr("SetNull()").alias("CLSD_WORK_DAYS_CT"),
    F.expr("SetNull()").alias("CLSD_CLNDR_DAYS_CT"),
    F.lit(0).alias("TWENTY_DAY_LTR_CT"),
    F.lit("1753-01-01").alias("TWENTY_DAY_LTR_DT_SK"),
    F.expr("SetNull()").alias("TWENTY_DAY_LTR_WORK_DAYS_CT"),
    F.expr("SetNull()").alias("TWENTY_DAY_LTR_CLNDR_DAYS_CT"),
    F.lit(0).alias("DEPT_DT_RCVD_CT"),
    F.lit("1753-01-01").alias("DEPT_DT_RCVD_DT_SK"),
    F.expr("SetNull()").alias("DEPT_DT_RCVD_WORK_DAYS_CT"),
    F.expr("SetNull()").alias("DEPT_DT_RCVD_CLNDR_DAYS_CT"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK")
).alias("lnk_UNK_out")

# Funnel: fnl_Data => union of lnk_NA_out, lnk_UNK_out, lnk_xfm_Data
df_fnl_Data = lnk_NA_out.unionByName(lnk_UNK_out).unionByName(lnk_xfm_Data).alias("lnk_IdsEdwAplLvlNoteDExtr_OutABC")

# Stage: seq_APL_LVL_ORIG_ACTVTY_F_csv_load => PxSequentialFile => write final
# The file is in "load/APL_LVL_ORIG_ACTVTY_F.dat" => that goes under f"{adls_path}/load/APL_LVL_ORIG_ACTVTY_F.dat"
# Delimiter = "," quote = "^" no header
# We must preserve column order and apply rpad if char:

final_columns = [
 ("APL_LVL_SK", None),
 ("SRC_SYS_CD", None),
 ("APL_ID", None),
 ("APL_LVL_SEQ_NO", None),
 ("CRT_RUN_CYC_EXCTN_DT_SK", "char"),
 ("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", "char"),
 ("ACKNMT_LTR_CT", None),
 ("ACKNMT_LTR_DT_SK", "char"),
 ("ACKNMT_LTR_WORK_DAYS_CT", None),
 ("ACKNMT_LTR_CLNDR_DAYS_CT", None),
 ("CLSD_CT", None),
 ("CLSD_DT_SK", "char"),
 ("CLSD_WORK_DAYS_CT", None),
 ("CLSD_CLNDR_DAYS_CT", None),
 ("TWENTY_DAY_LTR_CT", None),
 ("TWENTY_DAY_LTR_DT_SK", "char"),
 ("TWENTY_DAY_LTR_WORK_DAYS_CT", None),
 ("TWENTY_DAY_LTR_CLNDR_DAYS_CT", None),
 ("DEPT_DT_RCVD_CT", None),
 ("DEPT_DT_RCVD_DT_SK", "char"),
 ("DEPT_DT_RCVD_WORK_DAYS_CT", None),
 ("DEPT_DT_RCVD_CLNDR_DAYS_CT", None),
 ("CRT_RUN_CYC_EXCTN_SK", None),
 ("LAST_UPDT_RUN_CYC_EXCTN_SK", None)
]

df_final = df_fnl_Data
for c, ctype in final_columns:
    if ctype == "char":
        # rpad to length 10 (since the JSON uses length=10 for those char columns)
        df_final = df_final.withColumn(c, F.rpad(F.col(c).cast(StringType()), 10, " "))

final_col_names = [c for c, _ in final_columns]
df_final_sel = df_final.select([F.col(c) for c in final_col_names])

write_files(
    df_final_sel,
    f"{adls_path}/load/APL_LVL_ORIG_ACTVTY_F.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)