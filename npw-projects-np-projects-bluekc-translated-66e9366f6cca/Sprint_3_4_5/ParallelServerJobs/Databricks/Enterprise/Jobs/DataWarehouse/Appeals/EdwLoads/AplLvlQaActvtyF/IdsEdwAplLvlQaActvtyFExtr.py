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
# MAGIC Developer                          Date               Project/Altiris #               Change Description                                                           Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------    ---------------------------------------------------------                                      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Naren Garapaty                09/02/2007                                              Originally Programmed                                                        devlEDW10        
# MAGIC 
# MAGIC Srikanth Mettpalli              10/30/2013        5114                           Original Programming                                                             EnterpriseWrhsDevl        Jag Yelavarthi            2014-01-17
# MAGIC                                                                                                        (Server to Parallel Conversion)  
# MAGIC 
# MAGIC Jag Yelavarthi                   2014-08-05         TFS#1011                    Added logic to check Small Integer                                    EnterpriseNewDevl        Bhoomi Dasari             8/12/2014
# MAGIC                                                                                                          Data range for below columns in 
# MAGIC                                                                                                          xfm_Businesslogic stage
# MAGIC                                                                                                          QA_RVW_WORK_DAYS_CT
# MAGIC                                                                                                          QA_ERR_RTRN_WORK_DAYS_CT
# MAGIC                                                                                                          QA_RVSN_RTRN_WORK_DAYS_CT
# MAGIC                                                                                                          QA_SIGNOFF_WORK_DAYS_CT

# MAGIC Read all the Data from IDS CLNDR_DT Table;
# MAGIC Logic to Calculate Number of BCBS Working Days Using Range Lookup
# MAGIC Write APL_LVL_QA_ACTVTY_F Data into a Sequential file for Load Job IdsEdwAplLvlQaActvtyFLoad.
# MAGIC Add Defaults and Null Handling.
# MAGIC Read all the Data from IDS APL_LVL Table;
# MAGIC Job Name: IdsEdwAplLvlQaActvtyFExtr
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
from pyspark.sql.types import StringType, IntegerType, StructType, StructField
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


# Retrieve parameter values
IDSOwner = get_widget_value('IDSOwner','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
CurrRunCycleDate = get_widget_value('CurrRunCycleDate','')
ExtractRunCycle = get_widget_value('ExtractRunCycle','')
ids_secret_name = get_widget_value('ids_secret_name','')

# Get DB configuration for IDS
jdbc_url_IDS, jdbc_props_IDS = get_db_config(ids_secret_name)

# --------------------------------------------------------------------------------------------------
# Stage: db2_CLNDR_DT_in
# --------------------------------------------------------------------------------------------------
query_db2_CLNDR_DT_in = f"""
SELECT CLNDR_DT_SK
FROM {IDSOwner}.CLNDR_DT
WHERE BCBSKC_BUS_DAY_IN = 'Y'
"""
df_db2_CLNDR_DT_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_IDS)
    .options(**jdbc_props_IDS)
    .option("query", query_db2_CLNDR_DT_in)
    .load()
)

# --------------------------------------------------------------------------------------------------
# Stage: xfrm_BusinessLogic1
# This Transformer has 4 output links, each assigning literal values to columns
# --------------------------------------------------------------------------------------------------
df_Ref_QaRvsnRtrnWorkDaysCt_Lkp = df_db2_CLNDR_DT_in.select(
    F.col("CLNDR_DT_SK").alias("CLNDR_DT_SK"),
    F.lit(0).alias("QA_RVW_WORK_DAYS_CT"),
    F.lit(0).alias("QA_ERR_RTRN_WORK_DAYS_CT"),
    F.lit(1).alias("QA_RVSN_RTRN_WORK_DAYS_CT"),
    F.lit(0).alias("QA_SIGNOFF_WORK_DAYS_CT"),
    F.lit("A").alias("DUMMY")
)

df_Ref_QaRvwWorkDaysCt_Lkp = df_db2_CLNDR_DT_in.select(
    F.col("CLNDR_DT_SK").alias("CLNDR_DT_SK"),
    F.lit(1).alias("QA_RVW_WORK_DAYS_CT"),
    F.lit(0).alias("QA_ERR_RTRN_WORK_DAYS_CT"),
    F.lit(0).alias("QA_RVSN_RTRN_WORK_DAYS_CT"),
    F.lit(0).alias("QA_SIGNOFF_WORK_DAYS_CT"),
    F.lit("A").alias("DUMMY")
)

df_Ref_QaErrRtrnWorkDaysCt_Lkp = df_db2_CLNDR_DT_in.select(
    F.col("CLNDR_DT_SK").alias("CLNDR_DT_SK"),
    F.lit(0).alias("QA_RVW_WORK_DAYS_CT"),
    F.lit(1).alias("QA_ERR_RTRN_WORK_DAYS_CT"),
    F.lit(0).alias("QA_RVSN_RTRN_WORK_DAYS_CT"),
    F.lit(0).alias("QA_SIGNOFF_WORK_DAYS_CT"),
    F.lit("A").alias("DUMMY")
)

df_Ref_QaSignoffWorkDaysCt_Lkp = df_db2_CLNDR_DT_in.select(
    F.col("CLNDR_DT_SK").alias("CLNDR_DT_SK"),
    F.lit(0).alias("QA_RVW_WORK_DAYS_CT"),
    F.lit(0).alias("QA_ERR_RTRN_WORK_DAYS_CT"),
    F.lit(0).alias("QA_RVSN_RTRN_WORK_DAYS_CT"),
    F.lit(1).alias("QA_SIGNOFF_WORK_DAYS_CT"),
    F.lit("A").alias("DUMMY")
)

# --------------------------------------------------------------------------------------------------
# Stage: db2_APL_LVL_in
# --------------------------------------------------------------------------------------------------
query_db2_APL_LVL_in = f"""
SELECT 
APL_LVL.APL_LVL_SK,
COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,
APL_LVL.APL_ID,
APL_LVL.SEQ_NO,
APL_LVL.CRT_RUN_CYC_EXCTN_SK,
'A' AS DUMMY
FROM {IDSOwner}.APL_LVL APL_LVL
INNER JOIN {IDSOwner}.APL APL
ON APL_LVL.APL_SK = APL.APL_SK
LEFT JOIN {IDSOwner}.CD_MPPNG CD
ON APL_LVL.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
WHERE 
APL.INITN_DT_SK > '2004-12-31'
AND APL_LVL.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtractRunCycle}
"""
df_db2_APL_LVL_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_IDS)
    .options(**jdbc_props_IDS)
    .option("query", query_db2_APL_LVL_in)
    .load()
)

# --------------------------------------------------------------------------------------------------
# Stage: db2_QaRvwCt_in
# --------------------------------------------------------------------------------------------------
query_db2_QaRvwCt_in = f"""
SELECT 
APL_LVL.APL_LVL_SK,
COUNT(APL_LVL.APL_LVL_SK) AS QA_RVW_CT
FROM 
{IDSOwner}.APL_ACTVTY APL_ACTVTY,
{IDSOwner}.CD_MPPNG CD_MPPNG,
{IDSOwner}.APL_LVL APL_LVL
WHERE
APL_LVL.APL_ID = APL_ACTVTY.APL_ID
AND APL_LVL.SEQ_NO = APL_ACTVTY.APL_LVL_SEQ_NO
AND APL_ACTVTY.APL_ACTVTY_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
AND CD_MPPNG.TRGT_CD = 'AT24'
GROUP BY
APL_LVL.APL_LVL_SK
"""
df_db2_QaRvwCt_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_IDS)
    .options(**jdbc_props_IDS)
    .option("query", query_db2_QaRvwCt_in)
    .load()
)

# --------------------------------------------------------------------------------------------------
# Stage: db2_QaRvwDt_in
# --------------------------------------------------------------------------------------------------
query_db2_QaRvwDt_in = f"""
SELECT 
APL_LVL.APL_LVL_SK,
max(APL_ACTVTY.ACTVTY_DT_SK) AS ACTVTY_DT_SK_QA_RVW
FROM 
{IDSOwner}.APL_ACTVTY APL_ACTVTY,
{IDSOwner}.CD_MPPNG CD_MPPNG,
{IDSOwner}.APL_LVL APL_LVL
WHERE
APL_LVL.APL_ID = APL_ACTVTY.APL_ID
AND APL_LVL.SEQ_NO = APL_ACTVTY.APL_LVL_SEQ_NO
AND APL_ACTVTY.APL_ACTVTY_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
AND CD_MPPNG.TRGT_CD = 'AT24'
GROUP BY
APL_LVL.APL_LVL_SK
"""
df_db2_QaRvwDt_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_IDS)
    .options(**jdbc_props_IDS)
    .option("query", query_db2_QaRvwDt_in)
    .load()
)

# --------------------------------------------------------------------------------------------------
# Stage: db2_ErrCt_in
# --------------------------------------------------------------------------------------------------
query_db2_ErrCt_in = f"""
SELECT 
APL_LVL.APL_LVL_SK,
COUNT(APL_LVL.APL_LVL_SK) AS QA_ERR_RTRN_CT
 FROM 
{IDSOwner}.APL_ACTVTY APL_ACTVTY,
{IDSOwner}.CD_MPPNG CD_MPPNG,
{IDSOwner}.APL_LVL APL_LVL
WHERE
APL_LVL.APL_ID = APL_ACTVTY.APL_ID
AND APL_LVL.SEQ_NO = APL_ACTVTY.APL_LVL_SEQ_NO
AND APL_ACTVTY.APL_ACTVTY_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
AND CD_MPPNG.TRGT_CD = 'AT25'
GROUP BY
APL_LVL.APL_LVL_SK
"""
df_db2_ErrCt_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_IDS)
    .options(**jdbc_props_IDS)
    .option("query", query_db2_ErrCt_in)
    .load()
)

# --------------------------------------------------------------------------------------------------
# Stage: db2_QaRvwDays_in
# --------------------------------------------------------------------------------------------------
query_db2_QaRvwDays_in = f"""
SELECT
APL_LVL.APL_LVL_SK,
APL.INITN_DT_SK INITN_DT_SK_QA_RVW,
max(APL_ACTVTY.ACTVTY_DT_SK) AS ACTVTY_DT_SK_QA_RVW_DAYS
FROM 
{IDSOwner}.APL_LVL APL_LVL,
{IDSOwner}.APL APL,
{IDSOwner}.APL_ACTVTY APL_ACTVTY,
{IDSOwner}.CD_MPPNG CD_MPPNG
WHERE
APL_LVL.APL_SK = APL.APL_SK
AND APL_LVL.APL_ID = APL_ACTVTY.APL_ID
AND APL_LVL.SEQ_NO = APL_ACTVTY.APL_LVL_SEQ_NO
AND APL_ACTVTY.APL_ACTVTY_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
AND CD_MPPNG.TRGT_CD = 'AT24'
GROUP BY
APL_LVL.APL_LVL_SK,
APL.INITN_DT_SK
"""
df_db2_QaRvwDays_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_IDS)
    .options(**jdbc_props_IDS)
    .option("query", query_db2_QaRvwDays_in)
    .load()
)

# --------------------------------------------------------------------------------------------------
# Stage: db2_ErrDt_in
# --------------------------------------------------------------------------------------------------
query_db2_ErrDt_in = f"""
SELECT 
APL_LVL.APL_LVL_SK,
max(APL_ACTVTY.ACTVTY_DT_SK) AS ACTVTY_DT_SK_ERR
 FROM 
{IDSOwner}.APL_ACTVTY APL_ACTVTY,
{IDSOwner}.CD_MPPNG CD_MPPNG,
{IDSOwner}.APL_LVL APL_LVL
WHERE
APL_LVL.APL_ID = APL_ACTVTY.APL_ID
AND APL_LVL.SEQ_NO = APL_ACTVTY.APL_LVL_SEQ_NO
AND APL_ACTVTY.APL_ACTVTY_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
AND CD_MPPNG.TRGT_CD = 'AT25'
GROUP BY
APL_LVL.APL_LVL_SK
"""
df_db2_ErrDt_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_IDS)
    .options(**jdbc_props_IDS)
    .option("query", query_db2_ErrDt_in)
    .load()
)

# --------------------------------------------------------------------------------------------------
# Stage: db2_ErrDays_in
# --------------------------------------------------------------------------------------------------
query_db2_ErrDays_in = f"""
SELECT
APL_LVL.APL_LVL_SK,
APL.INITN_DT_SK AS INITN_DT_SK_ERR,
max(APL_ACTVTY.ACTVTY_DT_SK) AS ACTVTY_DT_SK_ERR_DAYS
FROM 
{IDSOwner}.APL_LVL APL_LVL,
{IDSOwner}.APL APL,
{IDSOwner}.APL_ACTVTY APL_ACTVTY,
{IDSOwner}.CD_MPPNG CD_MPPNG
WHERE
APL_LVL.APL_SK = APL.APL_SK
AND APL_LVL.APL_ID = APL_ACTVTY.APL_ID
AND APL_LVL.SEQ_NO = APL_ACTVTY.APL_LVL_SEQ_NO
AND APL_ACTVTY.APL_ACTVTY_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
AND CD_MPPNG.TRGT_CD = 'AT25'
GROUP BY
APL_LVL.APL_LVL_SK,
APL.INITN_DT_SK
"""
df_db2_ErrDays_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_IDS)
    .options(**jdbc_props_IDS)
    .option("query", query_db2_ErrDays_in)
    .load()
)

# --------------------------------------------------------------------------------------------------
# Stage: db2_RvsnCt_in
# --------------------------------------------------------------------------------------------------
query_db2_RvsnCt_in = f"""
SELECT 
APL_LVL.APL_LVL_SK ,
COUNT(APL_LVL.APL_LVL_SK) AS QA_RVSN_RTRN_CT
 FROM 
{IDSOwner}.APL_ACTVTY APL_ACTVTY,
{IDSOwner}.CD_MPPNG CD_MPPNG,
{IDSOwner}.APL_LVL APL_LVL
WHERE
APL_LVL.APL_ID = APL_ACTVTY.APL_ID
AND APL_LVL.SEQ_NO = APL_ACTVTY.APL_LVL_SEQ_NO
AND APL_ACTVTY.APL_ACTVTY_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
AND CD_MPPNG.TRGT_CD = 'AT26'
GROUP BY
APL_LVL.APL_LVL_SK
"""
df_db2_RvsnCt_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_IDS)
    .options(**jdbc_props_IDS)
    .option("query", query_db2_RvsnCt_in)
    .load()
)

# --------------------------------------------------------------------------------------------------
# Stage: db2_RvsnDt_in
# --------------------------------------------------------------------------------------------------
query_db2_RvsnDt_in = f"""
SELECT 
APL_LVL.APL_LVL_SK,
max(APL_ACTVTY.ACTVTY_DT_SK) AS ACTVTY_DT_SK_RVSN
 FROM 
{IDSOwner}.APL_ACTVTY APL_ACTVTY,
{IDSOwner}.CD_MPPNG CD_MPPNG,
{IDSOwner}.APL_LVL APL_LVL
WHERE
APL_LVL.APL_ID = APL_ACTVTY.APL_ID
AND APL_LVL.SEQ_NO = APL_ACTVTY.APL_LVL_SEQ_NO
AND APL_ACTVTY.APL_ACTVTY_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
AND CD_MPPNG.TRGT_CD = 'AT26'
group by 
APL_LVL.APL_LVL_SK
"""
df_db2_RvsnDt_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_IDS)
    .options(**jdbc_props_IDS)
    .option("query", query_db2_RvsnDt_in)
    .load()
)

# --------------------------------------------------------------------------------------------------
# Stage: db2_RvsnDays_in
# --------------------------------------------------------------------------------------------------
query_db2_RvsnDays_in = f"""
SELECT
APL_LVL.APL_LVL_SK,
APL.INITN_DT_SK AS INITN_DT_SK_RVSN,
max(APL_ACTVTY.ACTVTY_DT_SK) AS ACTVTY_DT_SK_RVSN_DAYS
FROM 
{IDSOwner}.APL_LVL APL_LVL,
{IDSOwner}.APL APL,
{IDSOwner}.APL_ACTVTY APL_ACTVTY,
{IDSOwner}.CD_MPPNG CD_MPPNG
WHERE
APL_LVL.APL_SK = APL.APL_SK
AND APL_LVL.APL_ID = APL_ACTVTY.APL_ID
AND APL_LVL.SEQ_NO = APL_ACTVTY.APL_LVL_SEQ_NO
AND APL_ACTVTY.APL_ACTVTY_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
AND CD_MPPNG.TRGT_CD = 'AT26'
group by
APL_LVL.APL_LVL_SK,
APL.INITN_DT_SK
"""
df_db2_RvsnDays_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_IDS)
    .options(**jdbc_props_IDS)
    .option("query", query_db2_RvsnDays_in)
    .load()
)

# --------------------------------------------------------------------------------------------------
# Stage: db2_SignOffCt_in
# --------------------------------------------------------------------------------------------------
query_db2_SignOffCt_in = f"""
SELECT 
APL_LVL.APL_LVL_SK ,
COUNT(APL_LVL.APL_LVL_SK) AS QA_SIGNOFF_CT
 FROM 
{IDSOwner}.APL_ACTVTY APL_ACTVTY,
{IDSOwner}.CD_MPPNG CD_MPPNG,
{IDSOwner}.APL_LVL APL_LVL
WHERE
APL_LVL.APL_ID = APL_ACTVTY.APL_ID
AND APL_LVL.SEQ_NO = APL_ACTVTY.APL_LVL_SEQ_NO
AND APL_ACTVTY.APL_ACTVTY_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
AND CD_MPPNG.TRGT_CD = 'AT27'
GROUP BY
APL_LVL.APL_LVL_SK
"""
df_db2_SignOffCt_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_IDS)
    .options(**jdbc_props_IDS)
    .option("query", query_db2_SignOffCt_in)
    .load()
)

# --------------------------------------------------------------------------------------------------
# Stage: db2_SignOffDt_in
# --------------------------------------------------------------------------------------------------
query_db2_SignOffDt_in = f"""
SELECT 
APL_LVL.APL_LVL_SK,
max(APL_ACTVTY.ACTVTY_DT_SK) AS ACTVTY_DT_SK_SIGNOFF
 FROM 
{IDSOwner}.APL_ACTVTY APL_ACTVTY,
{IDSOwner}.CD_MPPNG CD_MPPNG,
{IDSOwner}.APL_LVL APL_LVL
WHERE
APL_LVL.APL_ID = APL_ACTVTY.APL_ID
AND APL_LVL.SEQ_NO = APL_ACTVTY.APL_LVL_SEQ_NO
AND APL_ACTVTY.APL_ACTVTY_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
AND CD_MPPNG.TRGT_CD = 'AT27'
group by 
APL_LVL.APL_LVL_SK
"""
df_db2_SignOffDt_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_IDS)
    .options(**jdbc_props_IDS)
    .option("query", query_db2_SignOffDt_in)
    .load()
)

# --------------------------------------------------------------------------------------------------
# Stage: db2_SignOffDays_in
# --------------------------------------------------------------------------------------------------
query_db2_SignOffDays_in = f"""
SELECT
APL_LVL.APL_LVL_SK,
APL.INITN_DT_SK INITN_DT_SK_SIGNOFF,
max(APL_ACTVTY.ACTVTY_DT_SK) AS ACTVTY_DT_SK_SIGNOFF_DAYS
FROM 
{IDSOwner}.APL_LVL APL_LVL,
{IDSOwner}.APL APL,
{IDSOwner}.APL_ACTVTY APL_ACTVTY,
{IDSOwner}.CD_MPPNG CD_MPPNG
WHERE
APL_LVL.APL_SK = APL.APL_SK
AND APL_LVL.APL_ID = APL_ACTVTY.APL_ID
AND APL_LVL.SEQ_NO = APL_ACTVTY.APL_LVL_SEQ_NO
AND APL_ACTVTY.APL_ACTVTY_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
AND CD_MPPNG.TRGT_CD = 'AT27'
group by
APL_LVL.APL_LVL_SK,
APL.INITN_DT_SK
"""
df_db2_SignOffDays_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_IDS)
    .options(**jdbc_props_IDS)
    .option("query", query_db2_SignOffDays_in)
    .load()
)

# --------------------------------------------------------------------------------------------------
# Stage: Lkp_Apl_Data (multiple left joins)
# --------------------------------------------------------------------------------------------------
df_Lkp_Apl_Data = (
    df_db2_APL_LVL_in.alias("lnk_IdsEdwAplLvlQaActvtyFExtr_InABC")
    .join(
        df_db2_QaRvwCt_in.alias("lnk_QaRvwCt_Lkup"),
        F.col("lnk_IdsEdwAplLvlQaActvtyFExtr_InABC.APL_LVL_SK") == F.col("lnk_QaRvwCt_Lkup.APL_LVL_SK"),
        "left"
    )
    .join(
        df_db2_QaRvwDt_in.alias("lnk_QaRvwDt_Lkup"),
        F.col("lnk_IdsEdwAplLvlQaActvtyFExtr_InABC.APL_LVL_SK") == F.col("lnk_QaRvwDt_Lkup.APL_LVL_SK"),
        "left"
    )
    .join(
        df_db2_QaRvwDays_in.alias("lnk_QaRvwDays_Lkup"),
        F.col("lnk_IdsEdwAplLvlQaActvtyFExtr_InABC.APL_LVL_SK") == F.col("lnk_QaRvwDays_Lkup.APL_LVL_SK"),
        "left"
    )
    .join(
        df_db2_ErrCt_in.alias("lnk_ErrCt_Lkup"),
        F.col("lnk_IdsEdwAplLvlQaActvtyFExtr_InABC.APL_LVL_SK") == F.col("lnk_ErrCt_Lkup.APL_LVL_SK"),
        "left"
    )
    .join(
        df_db2_ErrDt_in.alias("lnk_ErrDt_Lkup"),
        F.col("lnk_IdsEdwAplLvlQaActvtyFExtr_InABC.APL_LVL_SK") == F.col("lnk_ErrDt_Lkup.APL_LVL_SK"),
        "left"
    )
    .join(
        df_db2_ErrDays_in.alias("lnk_ErrDays_Lkup"),
        F.col("lnk_IdsEdwAplLvlQaActvtyFExtr_InABC.APL_LVL_SK") == F.col("lnk_ErrDays_Lkup.APL_LVL_SK"),
        "left"
    )
    .join(
        df_db2_RvsnCt_in.alias("lnk_RvsnCt_Lkup"),
        F.col("lnk_IdsEdwAplLvlQaActvtyFExtr_InABC.APL_LVL_SK") == F.col("lnk_RvsnCt_Lkup.APL_LVL_SK"),
        "left"
    )
    .join(
        df_db2_RvsnDt_in.alias("lnk_RvsnDt_Lkup"),
        F.col("lnk_IdsEdwAplLvlQaActvtyFExtr_InABC.APL_LVL_SK") == F.col("lnk_RvsnDt_Lkup.APL_LVL_SK"),
        "left"
    )
    .join(
        df_db2_RvsnDays_in.alias("lnk_RvsnDays_Lkup"),
        F.col("lnk_IdsEdwAplLvlQaActvtyFExtr_InABC.APL_LVL_SK") == F.col("lnk_RvsnDays_Lkup.APL_LVL_SK"),
        "left"
    )
    .join(
        df_db2_SignOffCt_in.alias("lnk_SignOffCt_Lkup"),
        F.col("lnk_IdsEdwAplLvlQaActvtyFExtr_InABC.APL_LVL_SK") == F.col("lnk_SignOffCt_Lkup.APL_LVL_SK"),
        "left"
    )
    .join(
        df_db2_SignOffDt_in.alias("lnk_SignOffDt_Lkup"),
        F.col("lnk_IdsEdwAplLvlQaActvtyFExtr_InABC.APL_LVL_SK") == F.col("lnk_SignOffDt_Lkup.APL_LVL_SK"),
        "left"
    )
    .join(
        df_db2_SignOffDays_in.alias("lnk_SignOffDays_Lkup"),
        F.col("lnk_IdsEdwAplLvlQaActvtyFExtr_InABC.APL_LVL_SK") == F.col("lnk_SignOffDays_Lkup.APL_LVL_SK"),
        "left"
    )
    .select(
        F.col("lnk_IdsEdwAplLvlQaActvtyFExtr_InABC.APL_LVL_SK").alias("APL_LVL_SK"),
        F.col("lnk_IdsEdwAplLvlQaActvtyFExtr_InABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("lnk_IdsEdwAplLvlQaActvtyFExtr_InABC.APL_ID").alias("APL_ID"),
        F.col("lnk_IdsEdwAplLvlQaActvtyFExtr_InABC.SEQ_NO").alias("SEQ_NO"),
        F.col("lnk_IdsEdwAplLvlQaActvtyFExtr_InABC.DUMMY").alias("DUMMY"),
        F.col("lnk_QaRvwCt_Lkup.QA_RVW_CT").alias("QA_RVW_CT"),
        F.col("lnk_QaRvwDt_Lkup.ACTVTY_DT_SK_QA_RVW").alias("ACTVTY_DT_SK_QA_RVW"),
        F.col("lnk_QaRvwDays_Lkup.INITN_DT_SK_QA_RVW").alias("INITN_DT_SK_QA_RVW"),
        F.col("lnk_QaRvwDays_Lkup.ACTVTY_DT_SK_QA_RVW_DAYS").alias("ACTVTY_DT_SK_QA_RVW_DAYS"),
        F.col("lnk_ErrCt_Lkup.QA_ERR_RTRN_CT").alias("QA_ERR_RTRN_CT"),
        F.col("lnk_ErrDt_Lkup.ACTVTY_DT_SK_ERR").alias("ACTVTY_DT_SK_ERR"),
        F.col("lnk_ErrDays_Lkup.INITN_DT_SK_ERR").alias("INITN_DT_SK_ERR"),
        F.col("lnk_ErrDays_Lkup.ACTVTY_DT_SK_ERR_DAYS").alias("ACTVTY_DT_SK_ERR_DAYS"),
        F.col("lnk_RvsnCt_Lkup.QA_RVSN_RTRN_CT").alias("QA_RVSN_RTRN_CT"),
        F.col("lnk_RvsnDt_Lkup.ACTVTY_DT_SK_RVSN").alias("ACTVTY_DT_SK_RVSN"),
        F.col("lnk_RvsnDays_Lkup.INITN_DT_SK_RVSN").alias("INITN_DT_SK_RVSN"),
        F.col("lnk_RvsnDays_Lkup.ACTVTY_DT_SK_RVSN_DAYS").alias("ACTVTY_DT_SK_RVSN_DAYS"),
        F.col("lnk_SignOffCt_Lkup.QA_SIGNOFF_CT").alias("QA_SIGNOFF_CT"),
        F.col("lnk_SignOffDt_Lkup.ACTVTY_DT_SK_SIGNOFF").alias("ACTVTY_DT_SK_SIGNOFF"),
        F.col("lnk_SignOffDays_Lkup.INITN_DT_SK_SIGNOFF").alias("INITN_DT_SK_SIGNOFF"),
        F.col("lnk_SignOffDays_Lkup.ACTVTY_DT_SK_SIGNOFF_DAYS").alias("ACTVTY_DT_SK_SIGNOFF_DAYS")
    )
)

# --------------------------------------------------------------------------------------------------
# Stage: Copy_37 (just copies the same columns to multiple output links)
# --------------------------------------------------------------------------------------------------
df_Copy_37 = df_Lkp_Apl_Data

df_Lnk_QaRvwWorkDaysCt_Lkp = df_Copy_37.select(
    F.col("APL_LVL_SK"),
    F.col("SRC_SYS_CD"),
    F.col("APL_ID"),
    F.col("SEQ_NO"),
    F.col("DUMMY"),
    F.col("QA_RVW_CT"),
    F.col("ACTVTY_DT_SK_QA_RVW"),
    F.col("INITN_DT_SK_QA_RVW"),
    F.col("ACTVTY_DT_SK_QA_RVW_DAYS"),
    F.col("QA_ERR_RTRN_CT"),
    F.col("ACTVTY_DT_SK_ERR"),
    F.col("INITN_DT_SK_ERR"),
    F.col("ACTVTY_DT_SK_ERR_DAYS"),
    F.col("QA_RVSN_RTRN_CT"),
    F.col("ACTVTY_DT_SK_RVSN"),
    F.col("INITN_DT_SK_RVSN"),
    F.col("ACTVTY_DT_SK_RVSN_DAYS"),
    F.col("QA_SIGNOFF_CT"),
    F.col("ACTVTY_DT_SK_SIGNOFF"),
    F.col("INITN_DT_SK_SIGNOFF"),
    F.col("ACTVTY_DT_SK_SIGNOFF_DAYS")
)

df_Lnk_QaErrRtrnWorkDaysCt_Lkp = df_Copy_37.select(
    F.col("APL_LVL_SK"),
    F.col("SRC_SYS_CD"),
    F.col("APL_ID"),
    F.col("SEQ_NO"),
    F.col("DUMMY"),
    F.col("QA_RVW_CT"),
    F.col("ACTVTY_DT_SK_QA_RVW"),
    F.col("INITN_DT_SK_QA_RVW"),
    F.col("ACTVTY_DT_SK_QA_RVW_DAYS"),
    F.col("QA_ERR_RTRN_CT"),
    F.col("ACTVTY_DT_SK_ERR"),
    F.col("INITN_DT_SK_ERR"),
    F.col("ACTVTY_DT_SK_ERR_DAYS"),
    F.col("QA_RVSN_RTRN_CT"),
    F.col("ACTVTY_DT_SK_RVSN"),
    F.col("INITN_DT_SK_RVSN"),
    F.col("ACTVTY_DT_SK_RVSN_DAYS"),
    F.col("QA_SIGNOFF_CT"),
    F.col("ACTVTY_DT_SK_SIGNOFF"),
    F.col("INITN_DT_SK_SIGNOFF"),
    F.col("ACTVTY_DT_SK_SIGNOFF_DAYS")
)

df_Lnk_QaSignoffWorkDaysCt_Lkp = df_Copy_37.select(
    F.col("APL_LVL_SK"),
    F.col("SRC_SYS_CD"),
    F.col("APL_ID"),
    F.col("SEQ_NO"),
    F.col("DUMMY"),
    F.col("QA_RVW_CT"),
    F.col("ACTVTY_DT_SK_QA_RVW"),
    F.col("INITN_DT_SK_QA_RVW"),
    F.col("ACTVTY_DT_SK_QA_RVW_DAYS"),
    F.col("QA_ERR_RTRN_CT"),
    F.col("ACTVTY_DT_SK_ERR"),
    F.col("INITN_DT_SK_ERR"),
    F.col("ACTVTY_DT_SK_ERR_DAYS"),
    F.col("QA_RVSN_RTRN_CT"),
    F.col("ACTVTY_DT_SK_RVSN"),
    F.col("INITN_DT_SK_RVSN"),
    F.col("ACTVTY_DT_SK_RVSN_DAYS"),
    F.col("QA_SIGNOFF_CT"),
    F.col("ACTVTY_DT_SK_SIGNOFF"),
    F.col("INITN_DT_SK_SIGNOFF"),
    F.col("ACTVTY_DT_SK_SIGNOFF_DAYS")
)

df_Lnk_QaRvsnRtrnWorkDaysCt_Lkp = df_Copy_37.select(
    F.col("APL_LVL_SK"),
    F.col("SRC_SYS_CD"),
    F.col("APL_ID"),
    F.col("SEQ_NO"),
    F.col("DUMMY"),
    F.col("QA_RVW_CT"),
    F.col("ACTVTY_DT_SK_QA_RVW"),
    F.col("INITN_DT_SK_QA_RVW"),
    F.col("ACTVTY_DT_SK_QA_RVW_DAYS"),
    F.col("QA_ERR_RTRN_CT"),
    F.col("ACTVTY_DT_SK_ERR"),
    F.col("INITN_DT_SK_ERR"),
    F.col("ACTVTY_DT_SK_ERR_DAYS"),
    F.col("QA_RVSN_RTRN_CT"),
    F.col("ACTVTY_DT_SK_RVSN"),
    F.col("INITN_DT_SK_RVSN"),
    F.col("ACTVTY_DT_SK_RVSN_DAYS"),
    F.col("QA_SIGNOFF_CT"),
    F.col("ACTVTY_DT_SK_SIGNOFF"),
    F.col("INITN_DT_SK_SIGNOFF"),
    F.col("ACTVTY_DT_SK_SIGNOFF_DAYS")
)

# --------------------------------------------------------------------------------------------------
# Stage: Lkup1 (primary link => Lnk_QaRvwWorkDaysCt_Lkp, lookup link => Ref_QaRvwWorkDaysCt_Lkp)
# Join condition => Range(...) = Ref_QaRvwWorkDaysCt_Lkp.CLNDR_DT_SK and DUMMY match
# --------------------------------------------------------------------------------------------------
df_Lkup1_prep = df_Lnk_QaRvwWorkDaysCt_Lkp.alias("Lnk_QaRvwWorkDaysCt_Lkp").withColumn(
    "range_key",
    Range(
        F.col("Lnk_QaRvwWorkDaysCt_Lkp.ACTVTY_DT_SK_QA_RVW_DAYS"),
        "<=",
        F.col("Lnk_QaRvwWorkDaysCt_Lkp.INITN_DT_SK_QA_RVW"),
        ">=",
        F.lit(0)
    )
)

df_Lkup1 = (
    df_Lkup1_prep.join(
        df_Ref_QaRvwWorkDaysCt_Lkp.alias("Ref_QaRvwWorkDaysCt_Lkp"),
        [
            F.col("range_key") == F.col("Ref_QaRvwWorkDaysCt_Lkp.CLNDR_DT_SK"),
            F.col("Lnk_QaRvwWorkDaysCt_Lkp.DUMMY") == F.col("Ref_QaRvwWorkDaysCt_Lkp.DUMMY")
        ],
        "left"
    )
    .select(
        F.col("Lnk_QaRvwWorkDaysCt_Lkp.APL_LVL_SK").alias("APL_LVL_SK"),
        F.col("Lnk_QaRvwWorkDaysCt_Lkp.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("Lnk_QaRvwWorkDaysCt_Lkp.APL_ID").alias("APL_ID"),
        F.col("Lnk_QaRvwWorkDaysCt_Lkp.SEQ_NO").alias("SEQ_NO"),
        F.col("Lnk_QaRvwWorkDaysCt_Lkp.QA_RVW_CT").alias("QA_RVW_CT"),
        F.col("Lnk_QaRvwWorkDaysCt_Lkp.ACTVTY_DT_SK_QA_RVW").alias("ACTVTY_DT_SK_QA_RVW"),
        F.col("Lnk_QaRvwWorkDaysCt_Lkp.INITN_DT_SK_QA_RVW").alias("INITN_DT_SK_QA_RVW"),
        F.col("Lnk_QaRvwWorkDaysCt_Lkp.ACTVTY_DT_SK_QA_RVW_DAYS").alias("ACTVTY_DT_SK_QA_RVW_DAYS"),
        F.col("Lnk_QaRvwWorkDaysCt_Lkp.QA_ERR_RTRN_CT").alias("QA_ERR_RTRN_CT"),
        F.col("Lnk_QaRvwWorkDaysCt_Lkp.ACTVTY_DT_SK_ERR").alias("ACTVTY_DT_SK_ERR"),
        F.col("Lnk_QaRvwWorkDaysCt_Lkp.INITN_DT_SK_ERR").alias("INITN_DT_SK_ERR"),
        F.col("Lnk_QaRvwWorkDaysCt_Lkp.ACTVTY_DT_SK_ERR_DAYS").alias("ACTVTY_DT_SK_ERR_DAYS"),
        F.col("Lnk_QaRvwWorkDaysCt_Lkp.QA_RVSN_RTRN_CT").alias("QA_RVSN_RTRN_CT"),
        F.col("Lnk_QaRvwWorkDaysCt_Lkp.ACTVTY_DT_SK_RVSN").alias("ACTVTY_DT_SK_RVSN"),
        F.col("Lnk_QaRvwWorkDaysCt_Lkp.INITN_DT_SK_RVSN").alias("INITN_DT_SK_RVSN"),
        F.col("Lnk_QaRvwWorkDaysCt_Lkp.ACTVTY_DT_SK_RVSN_DAYS").alias("ACTVTY_DT_SK_RVSN_DAYS"),
        F.col("Lnk_QaRvwWorkDaysCt_Lkp.QA_SIGNOFF_CT").alias("QA_SIGNOFF_CT"),
        F.col("Lnk_QaRvwWorkDaysCt_Lkp.ACTVTY_DT_SK_SIGNOFF").alias("ACTVTY_DT_SK_SIGNOFF"),
        F.col("Lnk_QaRvwWorkDaysCt_Lkp.INITN_DT_SK_SIGNOFF").alias("INITN_DT_SK_SIGNOFF"),
        F.col("Lnk_QaRvwWorkDaysCt_Lkp.ACTVTY_DT_SK_SIGNOFF_DAYS").alias("ACTVTY_DT_SK_SIGNOFF_DAYS"),
        F.col("Ref_QaRvwWorkDaysCt_Lkp.QA_RVW_WORK_DAYS_CT").alias("QA_RVW_WORK_DAYS_CT"),
        F.col("Ref_QaRvwWorkDaysCt_Lkp.QA_ERR_RTRN_WORK_DAYS_CT").alias("QA_ERR_RTRN_WORK_DAYS_CT"),
        F.col("Ref_QaRvwWorkDaysCt_Lkp.QA_RVSN_RTRN_WORK_DAYS_CT").alias("QA_RVSN_RTRN_WORK_DAYS_CT"),
        F.col("Ref_QaRvwWorkDaysCt_Lkp.QA_SIGNOFF_WORK_DAYS_CT").alias("QA_SIGNOFF_WORK_DAYS_CT"),
    )
)

# --------------------------------------------------------------------------------------------------
# Stage: Lkup2 (lookup link => Ref_QaErrRtrnWorkDaysCt_Lkp, primary link => Lnk_QaErrRtrnWorkDaysCt_Lkp)
# Join condition => Range(...) = Ref_QaErrRtrnWorkDaysCt_Lkp.CLNDR_DT_SK and DUMMY match
# --------------------------------------------------------------------------------------------------
df_Lkup2_prep = df_Lnk_QaErrRtrnWorkDaysCt_Lkp.alias("Lnk_QaErrRtrnWorkDaysCt_Lkp").withColumn(
    "range_key",
    Range(
        F.col("Lnk_QaErrRtrnWorkDaysCt_Lkp.ACTVTY_DT_SK_ERR_DAYS"),
        "<=",
        F.col("Lnk_QaErrRtrnWorkDaysCt_Lkp.INITN_DT_SK_ERR"),
        ">=",
        F.lit(0)
    )
)

df_Lkup2 = (
    df_Lkup2_prep.join(
        df_Ref_QaErrRtrnWorkDaysCt_Lkp.alias("Ref_QaErrRtrnWorkDaysCt_Lkp"),
        [
            F.col("range_key") == F.col("Ref_QaErrRtrnWorkDaysCt_Lkp.CLNDR_DT_SK"),
            F.col("Lnk_QaErrRtrnWorkDaysCt_Lkp.DUMMY") == F.col("Ref_QaErrRtrnWorkDaysCt_Lkp.DUMMY")
        ],
        "left"
    )
    .select(
        F.col("Lnk_QaErrRtrnWorkDaysCt_Lkp.APL_LVL_SK").alias("APL_LVL_SK"),
        F.col("Lnk_QaErrRtrnWorkDaysCt_Lkp.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("Lnk_QaErrRtrnWorkDaysCt_Lkp.APL_ID").alias("APL_ID"),
        F.col("Lnk_QaErrRtrnWorkDaysCt_Lkp.SEQ_NO").alias("SEQ_NO"),
        F.col("Lnk_QaErrRtrnWorkDaysCt_Lkp.QA_RVW_CT").alias("QA_RVW_CT"),
        F.col("Lnk_QaErrRtrnWorkDaysCt_Lkp.ACTVTY_DT_SK_QA_RVW").alias("ACTVTY_DT_SK_QA_RVW"),
        F.col("Lnk_QaErrRtrnWorkDaysCt_Lkp.INITN_DT_SK_QA_RVW").alias("INITN_DT_SK_QA_RVW"),
        F.col("Lnk_QaErrRtrnWorkDaysCt_Lkp.ACTVTY_DT_SK_QA_RVW_DAYS").alias("ACTVTY_DT_SK_QA_RVW_DAYS"),
        F.col("Lnk_QaErrRtrnWorkDaysCt_Lkp.QA_ERR_RTRN_CT").alias("QA_ERR_RTRN_CT"),
        F.col("Lnk_QaErrRtrnWorkDaysCt_Lkp.ACTVTY_DT_SK_ERR").alias("ACTVTY_DT_SK_ERR"),
        F.col("Lnk_QaErrRtrnWorkDaysCt_Lkp.INITN_DT_SK_ERR").alias("INITN_DT_SK_ERR"),
        F.col("Lnk_QaErrRtrnWorkDaysCt_Lkp.ACTVTY_DT_SK_ERR_DAYS").alias("ACTVTY_DT_SK_ERR_DAYS"),
        F.col("Lnk_QaErrRtrnWorkDaysCt_Lkp.QA_RVSN_RTRN_CT").alias("QA_RVSN_RTRN_CT"),
        F.col("Lnk_QaErrRtrnWorkDaysCt_Lkp.ACTVTY_DT_SK_RVSN").alias("ACTVTY_DT_SK_RVSN"),
        F.col("Lnk_QaErrRtrnWorkDaysCt_Lkp.INITN_DT_SK_RVSN").alias("INITN_DT_SK_RVSN"),
        F.col("Lnk_QaErrRtrnWorkDaysCt_Lkp.ACTVTY_DT_SK_RVSN_DAYS").alias("ACTVTY_DT_SK_RVSN_DAYS"),
        F.col("Lnk_QaErrRtrnWorkDaysCt_Lkp.QA_SIGNOFF_CT").alias("QA_SIGNOFF_CT"),
        F.col("Lnk_QaErrRtrnWorkDaysCt_Lkp.ACTVTY_DT_SK_SIGNOFF").alias("ACTVTY_DT_SK_SIGNOFF"),
        F.col("Lnk_QaErrRtrnWorkDaysCt_Lkp.INITN_DT_SK_SIGNOFF").alias("INITN_DT_SK_SIGNOFF"),
        F.col("Lnk_QaErrRtrnWorkDaysCt_Lkp.ACTVTY_DT_SK_SIGNOFF_DAYS").alias("ACTVTY_DT_SK_SIGNOFF_DAYS"),
        F.col("Ref_QaErrRtrnWorkDaysCt_Lkp.QA_RVW_WORK_DAYS_CT").alias("QA_RVW_WORK_DAYS_CT"),
        F.col("Ref_QaErrRtrnWorkDaysCt_Lkp.QA_ERR_RTRN_WORK_DAYS_CT").alias("QA_ERR_RTRN_WORK_DAYS_CT"),
        F.col("Ref_QaErrRtrnWorkDaysCt_Lkp.QA_RVSN_RTRN_WORK_DAYS_CT").alias("QA_RVSN_RTRN_WORK_DAYS_CT"),
        F.col("Ref_QaErrRtrnWorkDaysCt_Lkp.QA_SIGNOFF_WORK_DAYS_CT").alias("QA_SIGNOFF_WORK_DAYS_CT"),
    )
)

# --------------------------------------------------------------------------------------------------
# Stage: Lkup3 (lookup link => Ref_QaRvsnRtrnWorkDaysCt_Lkp, primary link => Lnk_QaRvsnRtrnWorkDaysCt_Lkp)
# Join condition => Range(...) = Ref_QaRvsnRtrnWorkDaysCt_Lkp.CLNDR_DT_SK and DUMMY match
# --------------------------------------------------------------------------------------------------
df_Lkup3_prep = df_Lnk_QaRvsnRtrnWorkDaysCt_Lkp.alias("Lnk_QaRvsnRtrnWorkDaysCt_Lkp").withColumn(
    "range_key",
    Range(
        F.col("Lnk_QaRvsnRtrnWorkDaysCt_Lkp.ACTVTY_DT_SK_RVSN_DAYS"),
        "<=",
        F.col("Lnk_QaRvsnRtrnWorkDaysCt_Lkp.INITN_DT_SK_RVSN"),
        ">=",
        F.lit(0)
    )
)

df_Lkup3 = (
    df_Lkup3_prep.join(
        df_Ref_QaRvsnRtrnWorkDaysCt_Lkp.alias("Ref_QaRvsnRtrnWorkDaysCt_Lkp"),
        [
            F.col("range_key") == F.col("Ref_QaRvsnRtrnWorkDaysCt_Lkp.CLNDR_DT_SK"),
            F.col("Lnk_QaRvsnRtrnWorkDaysCt_Lkp.DUMMY") == F.col("Ref_QaRvsnRtrnWorkDaysCt_Lkp.DUMMY")
        ],
        "left"
    )
    .select(
        F.col("Lnk_QaRvsnRtrnWorkDaysCt_Lkp.APL_LVL_SK").alias("APL_LVL_SK"),
        F.col("Lnk_QaRvsnRtrnWorkDaysCt_Lkp.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("Lnk_QaRvsnRtrnWorkDaysCt_Lkp.APL_ID").alias("APL_ID"),
        F.col("Lnk_QaRvsnRtrnWorkDaysCt_Lkp.SEQ_NO").alias("SEQ_NO"),
        F.col("Lnk_QaRvsnRtrnWorkDaysCt_Lkp.QA_RVW_CT").alias("QA_RVW_CT"),
        F.col("Lnk_QaRvsnRtrnWorkDaysCt_Lkp.ACTVTY_DT_SK_QA_RVW").alias("ACTVTY_DT_SK_QA_RVW"),
        F.col("Lnk_QaRvsnRtrnWorkDaysCt_Lkp.INITN_DT_SK_QA_RVW").alias("INITN_DT_SK_QA_RVW"),
        F.col("Lnk_QaRvsnRtrnWorkDaysCt_Lkp.ACTVTY_DT_SK_QA_RVW_DAYS").alias("ACTVTY_DT_SK_QA_RVW_DAYS"),
        F.col("Lnk_QaRvsnRtrnWorkDaysCt_Lkp.QA_ERR_RTRN_CT").alias("QA_ERR_RTRN_CT"),
        F.col("Lnk_QaRvsnRtrnWorkDaysCt_Lkp.ACTVTY_DT_SK_ERR").alias("ACTVTY_DT_SK_ERR"),
        F.col("Lnk_QaRvsnRtrnWorkDaysCt_Lkp.INITN_DT_SK_ERR").alias("INITN_DT_SK_ERR"),
        F.col("Lnk_QaRvsnRtrnWorkDaysCt_Lkp.ACTVTY_DT_SK_ERR_DAYS").alias("ACTVTY_DT_SK_ERR_DAYS"),
        F.col("Lnk_QaRvsnRtrnWorkDaysCt_Lkp.QA_RVSN_RTRN_CT").alias("QA_RVSN_RTRN_CT"),
        F.col("Lnk_QaRvsnRtrnWorkDaysCt_Lkp.ACTVTY_DT_SK_RVSN").alias("ACTVTY_DT_SK_RVSN"),
        F.col("Lnk_QaRvsnRtrnWorkDaysCt_Lkp.INITN_DT_SK_RVSN").alias("INITN_DT_SK_RVSN"),
        F.col("Lnk_QaRvsnRtrnWorkDaysCt_Lkp.ACTVTY_DT_SK_RVSN_DAYS").alias("ACTVTY_DT_SK_RVSN_DAYS"),
        F.col("Lnk_QaRvsnRtrnWorkDaysCt_Lkp.QA_SIGNOFF_CT").alias("QA_SIGNOFF_CT"),
        F.col("Lnk_QaRvsnRtrnWorkDaysCt_Lkp.ACTVTY_DT_SK_SIGNOFF").alias("ACTVTY_DT_SK_SIGNOFF"),
        F.col("Lnk_QaRvsnRtrnWorkDaysCt_Lkp.INITN_DT_SK_SIGNOFF").alias("INITN_DT_SK_SIGNOFF"),
        F.col("Lnk_QaRvsnRtrnWorkDaysCt_Lkp.ACTVTY_DT_SK_SIGNOFF_DAYS").alias("ACTVTY_DT_SK_SIGNOFF_DAYS"),
        F.col("Ref_QaRvsnRtrnWorkDaysCt_Lkp.QA_RVW_WORK_DAYS_CT").alias("QA_RVW_WORK_DAYS_CT"),
        F.col("Ref_QaRvsnRtrnWorkDaysCt_Lkp.QA_ERR_RTRN_WORK_DAYS_CT").alias("QA_ERR_RTRN_WORK_DAYS_CT"),
        F.col("Ref_QaRvsnRtrnWorkDaysCt_Lkp.QA_RVSN_RTRN_WORK_DAYS_CT").alias("QA_RVSN_RTRN_WORK_DAYS_CT"),
        F.col("Ref_QaRvsnRtrnWorkDaysCt_Lkp.QA_SIGNOFF_WORK_DAYS_CT").alias("QA_SIGNOFF_WORK_DAYS_CT"),
    )
)

# --------------------------------------------------------------------------------------------------
# Stage: Lkup4 (lookup link => Ref_QaSignoffWorkDaysCt_Lkp, primary link => Lnk_QaSignoffWorkDaysCt_Lkp)
# Join condition => Range(...) = Ref_QaSignoffWorkDaysCt_Lkp.CLNDR_DT_SK and DUMMY match
# --------------------------------------------------------------------------------------------------
df_Lkup4_prep = df_Lnk_QaSignoffWorkDaysCt_Lkp.alias("Lnk_QaSignoffWorkDaysCt_Lkp").withColumn(
    "range_key",
    Range(
        F.col("Lnk_QaSignoffWorkDaysCt_Lkp.ACTVTY_DT_SK_SIGNOFF_DAYS"),
        "<=",
        F.col("Lnk_QaSignoffWorkDaysCt_Lkp.INITN_DT_SK_SIGNOFF"),
        ">=",
        F.lit(0)
    )
)

df_Lkup4 = (
    df_Lkup4_prep.join(
        df_Ref_QaSignoffWorkDaysCt_Lkp.alias("Ref_QaSignoffWorkDaysCt_Lkp"),
        [
            F.col("range_key") == F.col("Ref_QaSignoffWorkDaysCt_Lkp.CLNDR_DT_SK"),
            F.col("Lnk_QaSignoffWorkDaysCt_Lkp.DUMMY") == F.col("Ref_QaSignoffWorkDaysCt_Lkp.DUMMY")
        ],
        "left"
    )
    .select(
        F.col("Lnk_QaSignoffWorkDaysCt_Lkp.APL_LVL_SK").alias("APL_LVL_SK"),
        F.col("Lnk_QaSignoffWorkDaysCt_Lkp.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("Lnk_QaSignoffWorkDaysCt_Lkp.APL_ID").alias("APL_ID"),
        F.col("Lnk_QaSignoffWorkDaysCt_Lkp.SEQ_NO").alias("SEQ_NO"),
        F.col("Lnk_QaSignoffWorkDaysCt_Lkp.QA_RVW_CT").alias("QA_RVW_CT"),
        F.col("Lnk_QaSignoffWorkDaysCt_Lkp.ACTVTY_DT_SK_QA_RVW").alias("ACTVTY_DT_SK_QA_RVW"),
        F.col("Lnk_QaSignoffWorkDaysCt_Lkp.INITN_DT_SK_QA_RVW").alias("INITN_DT_SK_QA_RVW"),
        F.col("Lnk_QaSignoffWorkDaysCt_Lkp.ACTVTY_DT_SK_QA_RVW_DAYS").alias("ACTVTY_DT_SK_QA_RVW_DAYS"),
        F.col("Lnk_QaSignoffWorkDaysCt_Lkp.QA_ERR_RTRN_CT").alias("QA_ERR_RTRN_CT"),
        F.col("Lnk_QaSignoffWorkDaysCt_Lkp.ACTVTY_DT_SK_ERR").alias("ACTVTY_DT_SK_ERR"),
        F.col("Lnk_QaSignoffWorkDaysCt_Lkp.INITN_DT_SK_ERR").alias("INITN_DT_SK_ERR"),
        F.col("Lnk_QaSignoffWorkDaysCt_Lkp.ACTVTY_DT_SK_ERR_DAYS").alias("ACTVTY_DT_SK_ERR_DAYS"),
        F.col("Lnk_QaSignoffWorkDaysCt_Lkp.QA_RVSN_RTRN_CT").alias("QA_RVSN_RTRN_CT"),
        F.col("Lnk_QaSignoffWorkDaysCt_Lkp.ACTVTY_DT_SK_RVSN").alias("ACTVTY_DT_SK_RVSN"),
        F.col("Lnk_QaSignoffWorkDaysCt_Lkp.INITN_DT_SK_RVSN").alias("INITN_DT_SK_RVSN"),
        F.col("Lnk_QaSignoffWorkDaysCt_Lkp.ACTVTY_DT_SK_RVSN_DAYS").alias("ACTVTY_DT_SK_RVSN_DAYS"),
        F.col("Lnk_QaSignoffWorkDaysCt_Lkp.QA_SIGNOFF_CT").alias("QA_SIGNOFF_CT"),
        F.col("Lnk_QaSignoffWorkDaysCt_Lkp.ACTVTY_DT_SK_SIGNOFF").alias("ACTVTY_DT_SK_SIGNOFF"),
        F.col("Lnk_QaSignoffWorkDaysCt_Lkp.INITN_DT_SK_SIGNOFF").alias("INITN_DT_SK_SIGNOFF"),
        F.col("Lnk_QaSignoffWorkDaysCt_Lkp.ACTVTY_DT_SK_SIGNOFF_DAYS").alias("ACTVTY_DT_SK_SIGNOFF_DAYS"),
        F.col("Ref_QaSignoffWorkDaysCt_Lkp.QA_RVW_WORK_DAYS_CT").alias("QA_RVW_WORK_DAYS_CT"),
        F.col("Ref_QaSignoffWorkDaysCt_Lkp.QA_ERR_RTRN_WORK_DAYS_CT").alias("QA_ERR_RTRN_WORK_DAYS_CT"),
        F.col("Ref_QaSignoffWorkDaysCt_Lkp.QA_RVSN_RTRN_WORK_DAYS_CT").alias("QA_RVSN_RTRN_WORK_DAYS_CT"),
        F.col("Ref_QaSignoffWorkDaysCt_Lkp.QA_SIGNOFF_WORK_DAYS_CT").alias("QA_SIGNOFF_WORK_DAYS_CT"),
    )
)

# --------------------------------------------------------------------------------------------------
# Stage: Fnl_Apl_Data (PxFunnel with 4 inputs: Lkup1, Lkup2, Lkup4, Lkup3)
# --------------------------------------------------------------------------------------------------
df_Fnl_Apl_Data = df_Lkup1.unionByName(df_Lkup2).unionByName(df_Lkup4).unionByName(df_Lkup3)

# --------------------------------------------------------------------------------------------------
# Stage: CExp_Apl_Data (Column Export) => produces KEY plus the 4 day columns
# --------------------------------------------------------------------------------------------------
# Simulate ColumnExport(...) by concatenating or grouping columns. We keep the aggregator logic.
df_CExp_Apl_Data = df_Fnl_Apl_Data.select(
    F.concat_ws(
        ";",
        F.col("APL_LVL_SK"),
        F.col("SRC_SYS_CD"),
        F.col("APL_ID"),
        F.col("SEQ_NO"),
        F.col("QA_RVW_CT"),
        F.col("ACTVTY_DT_SK_QA_RVW"),
        F.col("INITN_DT_SK_QA_RVW"),
        F.col("ACTVTY_DT_SK_QA_RVW_DAYS"),
        F.col("QA_ERR_RTRN_CT"),
        F.col("ACTVTY_DT_SK_ERR"),
        F.col("INITN_DT_SK_ERR"),
        F.col("ACTVTY_DT_SK_ERR_DAYS"),
        F.col("QA_RVSN_RTRN_CT"),
        F.col("ACTVTY_DT_SK_RVSN"),
        F.col("INITN_DT_SK_RVSN"),
        F.col("ACTVTY_DT_SK_RVSN_DAYS"),
        F.col("QA_SIGNOFF_CT"),
        F.col("ACTVTY_DT_SK_SIGNOFF"),
        F.col("INITN_DT_SK_SIGNOFF"),
        F.col("ACTVTY_DT_SK_SIGNOFF_DAYS"),
        F.lit("KEY")
    ).alias("KEY"),
    F.col("QA_RVW_WORK_DAYS_CT"),
    F.col("QA_ERR_RTRN_WORK_DAYS_CT"),
    F.col("QA_RVSN_RTRN_WORK_DAYS_CT"),
    F.col("QA_SIGNOFF_WORK_DAYS_CT")
)

# --------------------------------------------------------------------------------------------------
# Stage: Agg_BCBS_Working_Days (PxAggregator, group by KEY, sum the four day columns)
# --------------------------------------------------------------------------------------------------
df_Agg_BCBS_Working_Days = (
    df_CExp_Apl_Data.groupBy("KEY")
    .agg(
        F.sum("QA_RVW_WORK_DAYS_CT").alias("QA_RVW_WORK_DAYS_CT"),
        F.sum("QA_ERR_RTRN_WORK_DAYS_CT").alias("QA_ERR_RTRN_WORK_DAYS_CT"),
        F.sum("QA_RVSN_RTRN_WORK_DAYS_CT").alias("QA_RVSN_RTRN_WORK_DAYS_CT"),
        F.sum("QA_SIGNOFF_WORK_DAYS_CT").alias("QA_SIGNOFF_WORK_DAYS_CT"),
    )
)

# --------------------------------------------------------------------------------------------------
# Stage: CImp_Apl_Data (Column Import) => re-split KEY into multiple columns plus the aggregated fields
# --------------------------------------------------------------------------------------------------
# We mimic ColumnImport(...) by splitting on ';'
split_cols = F.split(F.col("KEY"), ";")

df_CImp_Apl_Data = df_Agg_BCBS_Working_Days.select(
    split_cols.getItem(0).alias("APL_LVL_SK"),
    split_cols.getItem(1).alias("SRC_SYS_CD"),
    split_cols.getItem(2).alias("APL_ID"),
    split_cols.getItem(3).alias("SEQ_NO"),
    split_cols.getItem(4).alias("QA_RVW_CT"),
    split_cols.getItem(5).alias("ACTVTY_DT_SK_QA_RVW"),
    split_cols.getItem(6).alias("INITN_DT_SK_QA_RVW"),
    split_cols.getItem(7).alias("ACTVTY_DT_SK_QA_RVW_DAYS"),
    split_cols.getItem(8).alias("QA_ERR_RTRN_CT"),
    split_cols.getItem(9).alias("ACTVTY_DT_SK_ERR"),
    split_cols.getItem(10).alias("INITN_DT_SK_ERR"),
    split_cols.getItem(11).alias("ACTVTY_DT_SK_ERR_DAYS"),
    split_cols.getItem(12).alias("QA_RVSN_RTRN_CT"),
    split_cols.getItem(13).alias("ACTVTY_DT_SK_RVSN"),
    split_cols.getItem(14).alias("INITN_DT_SK_RVSN"),
    split_cols.getItem(15).alias("ACTVTY_DT_SK_RVSN_DAYS"),
    split_cols.getItem(16).alias("QA_SIGNOFF_CT"),
    split_cols.getItem(17).alias("ACTVTY_DT_SK_SIGNOFF"),
    split_cols.getItem(18).alias("INITN_DT_SK_SIGNOFF"),
    split_cols.getItem(19).alias("ACTVTY_DT_SK_SIGNOFF_DAYS"),
    F.col("QA_RVW_WORK_DAYS_CT"),
    F.col("QA_ERR_RTRN_WORK_DAYS_CT"),
    F.col("QA_RVSN_RTRN_WORK_DAYS_CT"),
    F.col("QA_SIGNOFF_WORK_DAYS_CT"),
)

# --------------------------------------------------------------------------------------------------
# Stage: xfrm_BusinessLogic
# Has 3 output links with constraints, then we funnel them.
# Here, we produce the columns with expressions
# --------------------------------------------------------------------------------------------------

# Primary Link => lnk_xfm_Data
df_xfrm_BusinessLogic_primary = df_CImp_Apl_Data.where(
    (F.col("QA_RVW_CT") > 0) | (F.col("QA_ERR_RTRN_CT") > 0) | (F.col("QA_RVSN_RTRN_CT") > 0) | (F.col("QA_SIGNOFF_CT") > 0)
).select(
    F.col("APL_LVL_SK").alias("APL_LVL_SK"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("APL_ID").alias("APL_ID"),
    F.col("SEQ_NO").alias("APL_LVL_SEQ_NO"),
    F.lit(CurrRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(CurrRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("QA_RVW_CT").alias("QA_RVW_CT"),
    F.when(F.col("ACTVTY_DT_SK_QA_RVW").isNull(), F.lit("1753-01-01")).otherwise(F.col("ACTVTY_DT_SK_QA_RVW")).alias("QA_RVW_DT_SK"),
    F.when(
        F.col("ACTVTY_DT_SK_QA_RVW_DAYS").isNull() | F.col("INITN_DT_SK_QA_RVW").isNull(),
        F.lit(None)
    ).otherwise(
        F.when(F.col("INITN_DT_SK_QA_RVW") > F.col("ACTVTY_DT_SK_QA_RVW_DAYS"), F.lit(-1))
        .otherwise(
            F.when((F.col("QA_RVW_WORK_DAYS_CT") - 1) < -31999, F.lit(-31999))
            .otherwise(
                F.when((F.col("QA_RVW_WORK_DAYS_CT") - 1) > 31999, F.lit(31999))
                .otherwise(F.col("QA_RVW_WORK_DAYS_CT") - 1)
            )
        )
    ).alias("QA_RVW_WORK_DAYS_CT"),
    F.when(
        F.col("ACTVTY_DT_SK_QA_RVW_DAYS").isNull() | F.col("INITN_DT_SK_QA_RVW").isNull(),
        F.lit(None)
    ).otherwise(
        F.when(
            F.expr("AsInteger(DaysSinceFromDate(ACTVTY_DT_SK_QA_RVW_DAYS, INITN_DT_SK_QA_RVW))") < -31999,
            F.lit(-31999)
        ).otherwise(
            F.when(
                F.expr("AsInteger(DaysSinceFromDate(ACTVTY_DT_SK_QA_RVW_DAYS, INITN_DT_SK_QA_RVW))") > 31999,
                F.lit(31999)
            ).otherwise(
                F.expr("DaysSinceFromDate(ACTVTY_DT_SK_QA_RVW_DAYS, INITN_DT_SK_QA_RVW)")
            )
        )
    ).alias("QA_RVW_CLNDR_DAYS_CT"),
    F.col("QA_ERR_RTRN_CT").alias("QA_ERR_RTRN_CT"),
    F.when(F.col("ACTVTY_DT_SK_ERR").isNull(), F.lit("1753-01-01")).otherwise(F.col("ACTVTY_DT_SK_ERR")).alias("QA_ERR_RTRN_DT_SK"),
    F.when(
        F.col("ACTVTY_DT_SK_ERR_DAYS").isNull() | F.col("INITN_DT_SK_ERR").isNull(),
        F.lit(None)
    ).otherwise(
        F.when(F.col("INITN_DT_SK_ERR") > F.col("ACTVTY_DT_SK_ERR_DAYS"), F.lit(-1))
        .otherwise(
            F.when((F.col("QA_ERR_RTRN_WORK_DAYS_CT") - 1) < -31999, F.lit(-31999))
            .otherwise(
                F.when((F.col("QA_ERR_RTRN_WORK_DAYS_CT") - 1) > 31999, F.lit(31999))
                .otherwise(F.col("QA_ERR_RTRN_WORK_DAYS_CT") - 1)
            )
        )
    ).alias("QA_ERR_RTRN_WORK_DAYS_CT"),
    F.when(
        F.col("ACTVTY_DT_SK_ERR_DAYS").isNull() | F.col("INITN_DT_SK_ERR").isNull(),
        F.lit(None)
    ).otherwise(
        F.when(
            F.expr("AsInteger(DaysSinceFromDate(ACTVTY_DT_SK_ERR_DAYS, INITN_DT_SK_ERR))") < -31999,
            F.lit(-31999)
        ).otherwise(
            F.when(
                F.expr("AsInteger(DaysSinceFromDate(ACTVTY_DT_SK_ERR_DAYS, INITN_DT_SK_ERR))") > 31999,
                F.lit(31999)
            ).otherwise(
                F.expr("DaysSinceFromDate(ACTVTY_DT_SK_ERR_DAYS, INITN_DT_SK_ERR)")
            )
        )
    ).alias("QA_ERR_RTRN_CLNDR_DAYS_CT"),
    F.col("QA_RVSN_RTRN_CT").alias("QA_RVSN_RTRN_CT"),
    F.when(F.col("ACTVTY_DT_SK_RVSN").isNull(), F.lit("1753-01-01")).otherwise(F.col("ACTVTY_DT_SK_RVSN")).alias("QA_RVSN_RTRN_DT_SK"),
    F.when(
        F.col("ACTVTY_DT_SK_RVSN_DAYS").isNull() | F.col("INITN_DT_SK_RVSN").isNull(),
        F.lit(None)
    ).otherwise(
        F.when(F.col("INITN_DT_SK_RVSN") > F.col("ACTVTY_DT_SK_RVSN_DAYS"), F.lit(-1))
        .otherwise(
            F.when((F.col("QA_RVSN_RTRN_WORK_DAYS_CT") - 1) < -31999, F.lit(-31999))
            .otherwise(
                F.when((F.col("QA_RVSN_RTRN_WORK_DAYS_CT") - 1) > 31999, F.lit(31999))
                .otherwise(F.col("QA_RVSN_RTRN_WORK_DAYS_CT") - 1)
            )
        )
    ).alias("QA_RVSN_RTRN_WORK_DAYS_CT"),
    F.when(
        F.col("ACTVTY_DT_SK_RVSN_DAYS").isNull() | F.col("INITN_DT_SK_RVSN").isNull(),
        F.lit(None)
    ).otherwise(
        F.when(
            F.expr("AsInteger(DaysSinceFromDate(ACTVTY_DT_SK_RVSN_DAYS, INITN_DT_SK_RVSN))") < -31999,
            F.lit(-31999)
        ).otherwise(
            F.when(
                F.expr("AsInteger(DaysSinceFromDate(ACTVTY_DT_SK_RVSN_DAYS, INITN_DT_SK_RVSN))") > 31999,
                F.lit(31999)
            ).otherwise(
                F.expr("DaysSinceFromDate(ACTVTY_DT_SK_RVSN_DAYS, INITN_DT_SK_RVSN)")
            )
        )
    ).alias("QA_RVSN_RTRN_CLNDR_DAYS_CT"),
    F.col("QA_SIGNOFF_CT").alias("QA_SIGNOFF_CT"),
    F.when(F.col("ACTVTY_DT_SK_SIGNOFF").isNull(), F.lit("1753-01-01")).otherwise(F.col("ACTVTY_DT_SK_SIGNOFF")).alias("QA_SIGNOFF_DT_SK"),
    F.when(
        F.col("ACTVTY_DT_SK_SIGNOFF_DAYS").isNull() | F.col("INITN_DT_SK_SIGNOFF").isNull(),
        F.lit(None)
    ).otherwise(
        F.when(F.col("INITN_DT_SK_SIGNOFF") > F.col("ACTVTY_DT_SK_SIGNOFF_DAYS"), F.lit(-1))
        .otherwise(
            F.when((F.col("QA_SIGNOFF_WORK_DAYS_CT") - 1) < -31999, F.lit(-31999))
            .otherwise(
                F.when((F.col("QA_SIGNOFF_WORK_DAYS_CT") - 1) > 31999, F.lit(31999))
                .otherwise(F.col("QA_SIGNOFF_WORK_DAYS_CT") - 1)
            )
        )
    ).alias("QA_SIGNOFF_WORK_DAYS_CT"),
    F.when(
        F.col("ACTVTY_DT_SK_SIGNOFF_DAYS").isNull() | F.col("INITN_DT_SK_SIGNOFF").isNull(),
        F.lit(None)
    ).otherwise(
        F.when(
            F.expr("AsInteger(DaysSinceFromDate(ACTVTY_DT_SK_SIGNOFF_DAYS, INITN_DT_SK_SIGNOFF))") < -31999,
            F.lit(-31999)
        ).otherwise(
            F.when(
                F.expr("AsInteger(DaysSinceFromDate(ACTVTY_DT_SK_SIGNOFF_DAYS, INITN_DT_SK_SIGNOFF))") > 31999,
                F.lit(31999)
            ).otherwise(
                F.expr("DaysSinceFromDate(ACTVTY_DT_SK_SIGNOFF_DAYS, INITN_DT_SK_SIGNOFF)")
            )
        )
    ).alias("QA_SIGNOFF_CLNDR_DAYS_CT"),
    F.lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

# lnk_NA_out
df_xfrm_BusinessLogic_na = df_CImp_Apl_Data.limit(1).select(
    F.lit("1").alias("APL_LVL_SK"),
    F.lit("NA").alias("SRC_SYS_CD"),
    F.lit("NA").alias("APL_ID"),
    F.lit("0").alias("APL_LVL_SEQ_NO"),
    F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.lit("0").alias("QA_RVW_CT"),
    F.lit("1753-01-01").alias("QA_RVW_DT_SK"),
    F.lit(None).alias("QA_RVW_WORK_DAYS_CT"),
    F.lit(None).alias("QA_RVW_CLNDR_DAYS_CT"),
    F.lit("0").alias("QA_ERR_RTRN_CT"),
    F.lit("1753-01-01").alias("QA_ERR_RTRN_DT_SK"),
    F.lit(None).alias("QA_ERR_RTRN_WORK_DAYS_CT"),
    F.lit(None).alias("QA_ERR_RTRN_CLNDR_DAYS_CT"),
    F.lit("0").alias("QA_RVSN_RTRN_CT"),
    F.lit("1753-01-01").alias("QA_RVSN_RTRN_DT_SK"),
    F.lit(None).alias("QA_RVSN_RTRN_WORK_DAYS_CT"),
    F.lit(None).alias("QA_RVSN_RTRN_CLNDR_DAYS_CT"),
    F.lit("0").alias("QA_SIGNOFF_CT"),
    F.lit("1753-01-01").alias("QA_SIGNOFF_DT_SK"),
    F.lit(None).alias("QA_SIGNOFF_WORK_DAYS_CT"),
    F.lit(None).alias("QA_SIGNOFF_CLNDR_DAYS_CT"),
    F.lit("100").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit("100").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

# lnk_UNK_out
df_xfrm_BusinessLogic_unk = df_CImp_Apl_Data.limit(1).select(
    F.lit("0").alias("APL_LVL_SK"),
    F.lit("UNK").alias("SRC_SYS_CD"),
    F.lit("UNK").alias("APL_ID"),
    F.lit("0").alias("APL_LVL_SEQ_NO"),
    F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.lit("0").alias("QA_RVW_CT"),
    F.lit("1753-01-01").alias("QA_RVW_DT_SK"),
    F.lit(None).alias("QA_RVW_WORK_DAYS_CT"),
    F.lit(None).alias("QA_RVW_CLNDR_DAYS_CT"),
    F.lit("0").alias("QA_ERR_RTRN_CT"),
    F.lit("1753-01-01").alias("QA_ERR_RTRN_DT_SK"),
    F.lit(None).alias("QA_ERR_RTRN_WORK_DAYS_CT"),
    F.lit(None).alias("QA_ERR_RTRN_CLNDR_DAYS_CT"),
    F.lit("0").alias("QA_RVSN_RTRN_CT"),
    F.lit("1753-01-01").alias("QA_RVSN_RTRN_DT_SK"),
    F.lit(None).alias("QA_RVSN_RTRN_WORK_DAYS_CT"),
    F.lit(None).alias("QA_RVSN_RTRN_CLNDR_DAYS_CT"),
    F.lit("0").alias("QA_SIGNOFF_CT"),
    F.lit("1753-01-01").alias("QA_SIGNOFF_DT_SK"),
    F.lit(None).alias("QA_SIGNOFF_WORK_DAYS_CT"),
    F.lit(None).alias("QA_SIGNOFF_CLNDR_DAYS_CT"),
    F.lit("100").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit("100").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

# --------------------------------------------------------------------------------------------------
# Stage: fnl_Data (PxFunnel of the three xfrm outputs)
# --------------------------------------------------------------------------------------------------
df_fnl_Data = df_xfrm_BusinessLogic_na.unionByName(df_xfrm_BusinessLogic_unk).unionByName(df_xfrm_BusinessLogic_primary)

# --------------------------------------------------------------------------------------------------
# Stage: seq_APL_LVL_QA_ACTVTY_F_csv_load (PxSequentialFile) writing the final file
# We must preserve column order and apply rpad for char/varchar columns with length=10 from the job
# --------------------------------------------------------------------------------------------------

df_final = df_fnl_Data.select(
    F.col("APL_LVL_SK"),
    F.col("SRC_SYS_CD"),
    F.col("APL_ID"),
    F.col("APL_LVL_SEQ_NO"),
    F.col("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("QA_RVW_CT"),
    F.col("QA_RVW_DT_SK"),
    F.col("QA_RVW_WORK_DAYS_CT"),
    F.col("QA_RVW_CLNDR_DAYS_CT"),
    F.col("QA_ERR_RTRN_CT"),
    F.col("QA_ERR_RTRN_DT_SK"),
    F.col("QA_ERR_RTRN_WORK_DAYS_CT"),
    F.col("QA_ERR_RTRN_CLNDR_DAYS_CT"),
    F.col("QA_RVSN_RTRN_CT"),
    F.col("QA_RVSN_RTRN_DT_SK"),
    F.col("QA_RVSN_RTRN_WORK_DAYS_CT"),
    F.col("QA_RVSN_RTRN_CLNDR_DAYS_CT"),
    F.col("QA_SIGNOFF_CT"),
    F.col("QA_SIGNOFF_DT_SK"),
    F.col("QA_SIGNOFF_WORK_DAYS_CT"),
    F.col("QA_SIGNOFF_CLNDR_DAYS_CT"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

# Apply rpad for those columns in the job that are "char(10)" (or any "SqlType":"char","Length":"10")
# The JSON shows these columns: (just as they appear in the pipeline)
df_final_wrapped = (
    df_final
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("QA_RVW_DT_SK", F.rpad(F.col("QA_RVW_DT_SK"), 10, " "))
    .withColumn("QA_ERR_RTRN_DT_SK", F.rpad(F.col("QA_ERR_RTRN_DT_SK"), 10, " "))
    .withColumn("QA_RVSN_RTRN_DT_SK", F.rpad(F.col("QA_RVSN_RTRN_DT_SK"), 10, " "))
    .withColumn("QA_SIGNOFF_DT_SK", F.rpad(F.col("QA_SIGNOFF_DT_SK"), 10, " "))
)

# Write to the .dat file in the "load" folder => f"{adls_path}/load/APL_LVL_QA_ACTVTY_F.dat"
write_files(
    df_final_wrapped,
    f"{adls_path}/load/APL_LVL_QA_ACTVTY_F.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)