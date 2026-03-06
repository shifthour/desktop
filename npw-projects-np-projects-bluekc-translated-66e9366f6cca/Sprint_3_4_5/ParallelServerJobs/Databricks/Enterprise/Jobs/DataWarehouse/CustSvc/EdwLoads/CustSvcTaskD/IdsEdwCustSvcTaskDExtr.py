# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date               Project/Altiris #               Change Description                                                 Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------    ---------------------------------------------------------                          ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Bhoomi Dasari                2/12/2007           Cust Svc/3028              Originally Programmed                                                 devlEDW10                Steph Goddard            02/21/2007
# MAGIC 
# MAGIC Rama Kamjula               12/13/2013           5114                           Rewritten from Server to Parallel version                  EnterpriseWrhsDevl         Jag Yelavarthi             2014-01-29


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.types import *
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------

# MAGIC %run ../../../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

# 1) Retrieve job parameters
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')

# 2) Define DB connection properties for IDS
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

# 3) Read from DB2_Connector_531
sql_str_DB2_Connector_531 = f"""
SELECT 
TASK.CUST_SVC_TASK_SK,
COALESCE(CD.TRGT_CD, 'NA')  SRC_SYS_CD,
TASK.CUST_SVC_ID,
TASK.TASK_SEQ_NO,
TASK.CRT_RUN_CYC_EXCTN_SK,
TASK.LAST_UPDT_RUN_CYC_EXCTN_SK,
TASK.CUST_SVC_SK,
TASK.GRP_SK,
TASK.INPT_USER_SK,
TASK.LAST_UPDT_USER_SK,
TASK.MBR_SK,
TASK.PROD_SK,
TASK.PROV_SK,
TASK.SUBGRP_SK,
TASK.SUB_SK,
TASK.CUST_SVC_TASK_CAT_CD_SK,
TASK.CS_TASK_CLS_PLN_PROD_CAT_CD_SK,
TASK.CUST_SVC_TASK_CLSR_PRF_CD_SK,
TASK.CUST_SVC_TASK_CUST_TYP_CD_SK,
TASK.CUST_SVC_TASK_FINL_ACTN_CD_SK,
TASK.CUST_SVC_TASK_ITS_TYP_CD_SK,
TASK.CS_TASK_LTR_RECPNT_TYP_CD_SK,
TASK.CUST_SVC_TASK_PG_TYP_CD_SK,
TASK.CUST_SVC_TASK_PRTY_CD_SK,
TASK.CUST_SVC_TASK_STTUS_CD_SK,
TASK.CUST_SVC_TASK_STTUS_RSN_CD_SK,
TASK.CUST_SVC_TASK_SUBJ_CD_SK,
TASK.CMPLNT_IN,
TASK.CLSD_DT_SK,
TASK.INPT_DTM,
TASK.LAST_UPDT_DTM,
TASK.NEXT_RVW_DT_SK,
TASK.RCVD_DT_SK,
TASK.NEXT_RVW_INTRVL_DAYS,
TASK.CUST_ID,
TASK.CUST_SVC_TASK_USER_SITE_ID,
TASK.INPT_USER_CC_ID,
TASK.SUM_DESC
FROM {IDSOwner}.CUST_SVC_TASK TASK
INNER JOIN {IDSOwner}.W_CUST_SVC_DRVR DRVR
  ON TASK.SRC_SYS_CD_SK = DRVR.SRC_SYS_CD_SK
     AND TASK.CUST_SVC_ID = DRVR.CUST_SVC_ID
LEFT JOIN {IDSOwner}.CD_MPPNG CD
  ON CD.CD_MPPNG_SK = TASK.SRC_SYS_CD_SK
"""
df_DB2_Connector_531 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", sql_str_DB2_Connector_531)
    .load()
)

# 4) Read from db2_GrpLkp
sql_str_db2_GrpLkp = f"""
SELECT
GRP.GRP_SK,
GRP.GRP_ID
FROM {IDSOwner}.GRP GRP
"""
df_db2_GrpLkp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", sql_str_db2_GrpLkp)
    .load()
)

# 5) Read from db2_ProdLkp
sql_str_db2_ProdLkp = f"""
SELECT DISTINCT
PROD.PROD_SK,
PROD.PROD_ID
FROM
{IDSOwner}.CUST_SVC_TASK TASK,
{IDSOwner}.PROD PROD,
{IDSOwner}.W_CUST_SVC_DRVR DRVR
WHERE
TASK.SRC_SYS_CD_SK = DRVR.SRC_SYS_CD_SK
AND TASK.CUST_SVC_ID = DRVR.CUST_SVC_ID
"""
df_db2_ProdLkp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", sql_str_db2_ProdLkp)
    .load()
)

# 6) Read from db2_ProvLkp
sql_str_db2_ProvLkp = f"""
SELECT
PROV.PROV_SK,
PROV.PROV_ID
FROM
{IDSOwner}.PROV PROV
"""
df_db2_ProvLkp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", sql_str_db2_ProvLkp)
    .load()
)

# 7) Read from db2_CustSvcTskLtr
sql_str_db2_CustSvcTskLtr = f"""
SELECT DISTINCT
LTR.CUST_SVC_TASK_SK,
LTR.CUST_SVC_TASK_SK AS CUST_SVC_TASK_SK_1
FROM
{IDSOwner}.CUST_SVC_TASK_LTR LTR,
{IDSOwner}.W_CUST_SVC_DRVR DRVR
WHERE
LTR.SRC_SYS_CD_SK = DRVR.SRC_SYS_CD_SK
AND LTR.CUST_SVC_ID = DRVR.CUST_SVC_ID
"""
df_db2_CustSvcTskLtr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", sql_str_db2_CustSvcTskLtr)
    .load()
)

# 8) Read from db2_CustSvcTskCstmDtl
sql_str_db2_CustSvcTskCstmDtl = f"""
SELECT DISTINCT
CSTM_DTL.CUST_SVC_TASK_SK,
CD_MPPNG.TRGT_CD
FROM
{IDSOwner}.CUST_SVC_TASK_CSTM_DTL CSTM_DTL,
{IDSOwner}.CD_MPPNG CD_MPPNG,
{IDSOwner}.CUST_SVC_TASK TASK,
{IDSOwner}.W_CUST_SVC_DRVR DRVR
WHERE
TASK.CUST_SVC_TASK_SK = CSTM_DTL.CUST_SVC_TASK_SK
AND CD_MPPNG.CD_MPPNG_SK = CSTM_DTL.CUST_SVC_TASK_CSTM_DTL_CD_SK
AND CD_MPPNG.TRGT_CD = 'BCKDT'
AND TASK.SRC_SYS_CD_SK = DRVR.SRC_SYS_CD_SK
AND TASK.CUST_SVC_ID = DRVR.CUST_SVC_ID
"""
df_db2_CustSvcTskCstmDtl = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", sql_str_db2_CustSvcTskCstmDtl)
    .load()
)

# 9) Read from db2_CustSvcLkp
sql_str_db2_CustSvcLkp = f"""
SELECT DISTINCT
SVC.CUST_SVC_SK,
CD_MPPNG.SRC_CD AS CNTCT_SRC_CD,
CD_MPPNG2.SRC_CD AS METH_SRC_CD
FROM
{IDSOwner}.CUST_SVC SVC,
{IDSOwner}.CD_MPPNG CD_MPPNG,
{IDSOwner}.CD_MPPNG CD_MPPNG2,
{IDSOwner}.W_CUST_SVC_DRVR DRVR
WHERE
SVC.CUST_SVC_CNTCT_RELSHP_CD_SK = CD_MPPNG.CD_MPPNG_SK
AND SVC.CUST_SVC_METH_CD_SK = CD_MPPNG2.CD_MPPNG_SK
AND SVC.SRC_SYS_CD_SK = DRVR.SRC_SYS_CD_SK
AND SVC.CUST_SVC_ID = DRVR.CUST_SVC_ID
"""
df_db2_CustSvcLkp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", sql_str_db2_CustSvcLkp)
    .load()
)

# 10) Read from db2_CustSvcTskSttusLkp_Max
sql_str_db2_CustSvcTskSttusLkp_Max = f"""
SELECT
STTUS.CUST_SVC_TASK_SK,
max(STTUS.STTUS_DTM) AS STTUS_DTM
FROM
{IDSOwner}.CUST_SVC_TASK TASK,
{IDSOwner}.CUST_SVC_TASK_STTUS STTUS,
{IDSOwner}.CD_MPPNG CD_MPPNG,
{IDSOwner}.W_CUST_SVC_DRVR DRVR
WHERE
TASK.CUST_SVC_TASK_SK = STTUS.CUST_SVC_TASK_SK
AND CD_MPPNG.CD_MPPNG_SK = STTUS.CUST_SVC_TASK_STTUS_CD_SK
AND CD_MPPNG.TRGT_CD = 'CLSD'
AND STTUS.SRC_SYS_CD_SK = DRVR.SRC_SYS_CD_SK
AND STTUS.CUST_SVC_ID = DRVR.CUST_SVC_ID
GROUP BY
STTUS.CUST_SVC_TASK_SK
"""
df_db2_CustSvcTskSttusLkp_Max = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", sql_str_db2_CustSvcTskSttusLkp_Max)
    .load()
)

# 11) Read from db2_CustSvcTskSttusLkp2_Min
sql_str_db2_CustSvcTskSttusLkp2_Min = f"""
SELECT
STTUS.CUST_SVC_TASK_SK,
min(STTUS.STTUS_DTM) AS STTUS_DTM
FROM
{IDSOwner}.CUST_SVC_TASK TASK,
{IDSOwner}.CUST_SVC_TASK_STTUS STTUS,
{IDSOwner}.CD_MPPNG CD_MPPNG,
{IDSOwner}.W_CUST_SVC_DRVR DRVR
WHERE
TASK.CUST_SVC_TASK_SK = STTUS.CUST_SVC_TASK_SK
AND STTUS.CUST_SVC_TASK_STTUS_CD_SK = CD_MPPNG.CD_MPPNG_SK
AND CD_MPPNG.TRGT_CD = 'CLSD'
AND TASK.SRC_SYS_CD_SK = DRVR.SRC_SYS_CD_SK
AND TASK.CUST_SVC_ID = DRVR.CUST_SVC_ID
GROUP BY
STTUS.CUST_SVC_TASK_SK
"""
df_db2_CustSvcTskSttusLkp2_Min = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", sql_str_db2_CustSvcTskSttusLkp2_Min)
    .load()
)

# 12) Read from db2_CustSvcTskNoteLkp1
sql_str_db2_CustSvcTskNoteLkp1 = f"""
SELECT DISTINCT
NOTE.CUST_SVC_TASK_SK,
NOTE.CUST_SVC_TASK_SK AS CUST_SVC_TASK_SK1
FROM
{IDSOwner}.CUST_SVC_TASK_NOTE NOTE,
{IDSOwner}.W_CUST_SVC_DRVR DRVR
WHERE
NOTE.SRC_SYS_CD_SK = DRVR.SRC_SYS_CD_SK
AND NOTE.CUST_SVC_ID = DRVR.CUST_SVC_ID
"""
df_db2_CustSvcTskNoteLkp1 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", sql_str_db2_CustSvcTskNoteLkp1)
    .load()
)

# 13) Read from db2_ProdShNmLkp
sql_str_db2_ProdShNmLkp = f"""
SELECT DISTINCT
PROD.PROD_SK,
PROD.PROD_ID,
PROD_SH_NM.PROD_SH_NM
FROM
{IDSOwner}.CUST_SVC_TASK TASK,
{IDSOwner}.PROD PROD,
{IDSOwner}.PROD_SH_NM PROD_SH_NM,
{IDSOwner}.W_CUST_SVC_DRVR DRVR
WHERE
TASK.PROD_SK = PROD.PROD_SK
AND PROD.PROD_SH_NM_SK = PROD_SH_NM.PROD_SH_NM_SK
AND TASK.SRC_SYS_CD_SK = DRVR.SRC_SYS_CD_SK
AND TASK.CUST_SVC_ID = DRVR.CUST_SVC_ID
"""
df_db2_ProdShNmLkp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", sql_str_db2_ProdShNmLkp)
    .load()
)

# 14) Read from db2_CustSvcTskCstmDtlLkp2 (DOCID)
sql_str_db2_CustSvcTskCstmDtlLkp2 = f"""
SELECT DISTINCT
CSTM_DTL.CUST_SVC_TASK_SK,
CD_MPPNG.TRGT_CD
FROM
{IDSOwner}.CUST_SVC_TASK_CSTM_DTL CSTM_DTL,
{IDSOwner}.CD_MPPNG CD_MPPNG,
{IDSOwner}.CUST_SVC_TASK TASK,
{IDSOwner}.W_CUST_SVC_DRVR DRVR
WHERE
TASK.CUST_SVC_TASK_SK = CSTM_DTL.CUST_SVC_TASK_SK
AND CD_MPPNG.CD_MPPNG_SK = CSTM_DTL.CUST_SVC_TASK_CSTM_DTL_CD_SK
AND CD_MPPNG.TRGT_CD = 'DOCID'
AND TASK.SRC_SYS_CD_SK = DRVR.SRC_SYS_CD_SK
AND TASK.CUST_SVC_ID = DRVR.CUST_SVC_ID
"""
df_db2_CustSvcTskCstmDtlLkp2 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", sql_str_db2_CustSvcTskCstmDtlLkp2)
    .load()
)

# 15) Read from db2_CustSvcTskCstmDtlLkp4 (FUND)
sql_str_db2_CustSvcTskCstmDtlLkp4 = f"""
SELECT DISTINCT
CSTM_DTL.CUST_SVC_TASK_SK,
CD_MPPNG.TRGT_CD
FROM
{IDSOwner}.CUST_SVC_TASK_CSTM_DTL CSTM_DTL,
{IDSOwner}.CD_MPPNG CD_MPPNG,
{IDSOwner}.CUST_SVC_TASK TASK,
{IDSOwner}.W_CUST_SVC_DRVR DRVR
WHERE
TASK.CUST_SVC_TASK_SK = CSTM_DTL.CUST_SVC_TASK_SK
AND CD_MPPNG.CD_MPPNG_SK = CSTM_DTL.CUST_SVC_TASK_CSTM_DTL_CD_SK
AND CD_MPPNG.TRGT_CD = 'FUND'
AND TASK.SRC_SYS_CD_SK = DRVR.SRC_SYS_CD_SK
AND TASK.CUST_SVC_ID = DRVR.CUST_SVC_ID
"""
df_db2_CustSvcTskCstmDtlLkp4 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", sql_str_db2_CustSvcTskCstmDtlLkp4)
    .load()
)

# 16) Read from db2_CustSvcTskSrccdlkup
sql_str_db2_CustSvcTskSrccdlkup = f"""
SELECT
CD_MPPNG.CD_MPPNG_SK,
CD_MPPNG.SRC_CD
FROM
{IDSOwner}.CD_MPPNG CD_MPPNG
"""
df_db2_CustSvcTskSrccdlkup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", sql_str_db2_CustSvcTskSrccdlkup)
    .load()
)

# 17) Read from db2_AppUserLkp
sql_str_db2_AppUserLkp = f"""
SELECT
USER.USER_SK,
USER.USER_ID
FROM
{IDSOwner}.APP_USER USER
"""
df_db2_AppUserLkp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", sql_str_db2_AppUserLkp)
    .load()
)

# 18) PxCopy: Cpy_cust_srvc_app_user
#    Two outputs: lnk_LastUpdtUserLookup and lnk_CrtByUserLookup
df_lnk_LastUpdtUserLookup = df_db2_AppUserLkp.select(
    F.col("USER_SK").alias("USER_SK"),
    F.rpad(F.col("USER_ID"),10," ").alias("USER_ID")  # char(10)
)
df_lnk_CrtByUserLookup = df_db2_AppUserLkp.select(
    F.col("USER_SK").alias("USER_SK"),
    F.rpad(F.col("USER_ID"),10," ").alias("USER_ID")  # char(10)
)

# 19) Read from db2_Cd_Mppng
sql_str_db2_Cd_Mppng = f"""
SELECT
CD_MPPNG_SK,
TRGT_CD,
TRGT_CD_NM,
SRC_CD
FROM {IDSOwner}.CD_MPPNG
"""
df_db2_Cd_Mppng = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", sql_str_db2_Cd_Mppng)
    .load()
)

# 20) PxCopy: cpy_Cd_Mppng
#    Multiple outputs from same input
df_lnk_CustSvcTskPrtyCdLkup = df_db2_Cd_Mppng.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_CustSvcTskCatCdLkup = df_db2_Cd_Mppng.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_lnk_CustSvcTskCustTypCdLkup = df_db2_Cd_Mppng.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_lnk_CsTskClsPlnProdCatCdLkup = df_db2_Cd_Mppng.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_lnk_CustSvcTskSubjCdLkup = df_db2_Cd_Mppng.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_lnk_CustSvcTskItsTypCdLkup = df_db2_Cd_Mppng.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_lnk_CustSvcTskFinlActnCdLkup = df_db2_Cd_Mppng.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_lnk_CustSvcTskPgCdLkup = df_db2_Cd_Mppng.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_lnk_CsTskLtrRecpntCdLkup = df_db2_Cd_Mppng.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_lnk_CustSvcTskClsrPrfCdLkup = df_db2_Cd_Mppng.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_lnk_CustSvctskSttusRsnCdLkup = df_db2_Cd_Mppng.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_CdmppngLkp = df_db2_Cd_Mppng.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("SRC_CD").alias("SRC_CD")
)
df_lnk_CustSvcStttusCdLkp = df_db2_Cd_Mppng.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

# 21) Read from db2_CustSvcTaskSttus
sql_str_db2_CustSvcTaskSttus = f"""
SELECT 
CUST_SVC_STTUS.CUST_SVC_TASK_SK,
CUST_SVC_STTUS.STTUS_SEQ_NO,
CD_MPPNG.TRGT_CD,
CUST_SVC_STTUS.STTUS_DTM
FROM {IDSOwner}.CUST_SVC_TASK_STTUS CUST_SVC_STTUS,
     {IDSOwner}.CUST_SVC_TASK CUST_SVC_TASK,
     {IDSOwner}.CD_MPPNG CD_MPPNG,
     {IDSOwner}.W_CUST_SVC_DRVR DRVR
WHERE CUST_SVC_STTUS.CUST_SVC_TASK_SK=CUST_SVC_TASK.CUST_SVC_TASK_SK
  AND CUST_SVC_STTUS.CUST_SVC_TASK_STTUS_CD_SK=CD_MPPNG.CD_MPPNG_SK
  AND CD_MPPNG.TRGT_CD<>'CLSD'
  AND CUST_SVC_TASK.SRC_SYS_CD_SK = DRVR.SRC_SYS_CD_SK
  AND CUST_SVC_TASK.CUST_SVC_ID = DRVR.CUST_SVC_ID
ORDER BY CUST_SVC_STTUS.CUST_SVC_TASK_SK asc,
         CUST_SVC_STTUS.STTUS_SEQ_NO asc
"""
df_db2_CustSvcTaskSttus = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", sql_str_db2_CustSvcTaskSttus)
    .load()
)

# 22) Read from db2_MinStusDtm
sql_str_db2_MinStusDtm = f"""
SELECT 
CUST_SVC_STTUS.CUST_SVC_TASK_SK,
MIN(CUST_SVC_STTUS.STTUS_DTM) AS STTUS_DTM,
CD_MPPNG.TRGT_CD AS TRGT_CD
FROM {IDSOwner}.CUST_SVC_TASK_STTUS CUST_SVC_STTUS,
     {IDSOwner}.CUST_SVC_TASK CUST_SVC_TASK,
     {IDSOwner}.CD_MPPNG CD_MPPNG,
     {IDSOwner}.W_CUST_SVC_DRVR DRVR
WHERE CUST_SVC_STTUS.CUST_SVC_TASK_SK = CUST_SVC_TASK.CUST_SVC_TASK_SK
  AND CUST_SVC_STTUS.CUST_SVC_TASK_STTUS_CD_SK = CD_MPPNG.CD_MPPNG_SK
  AND CD_MPPNG.TRGT_CD = 'CLSD'
  AND CUST_SVC_TASK.SRC_SYS_CD_SK = DRVR.SRC_SYS_CD_SK
  AND CUST_SVC_TASK.CUST_SVC_ID = DRVR.CUST_SVC_ID
GROUP BY CUST_SVC_STTUS.CUST_SVC_TASK_SK, CD_MPPNG.TRGT_CD
"""
df_db2_MinStusDtm = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", sql_str_db2_MinStusDtm)
    .load()
)

# 23) Read from db2_SttusSeqNo
sql_str_db2_SttusSeqNo = f"""
SELECT
CUST_SVC_STTUS.CUST_SVC_TASK_SK,
CUST_SVC_STTUS.STTUS_DTM,
CD_MPPNG.TRGT_CD,
CUST_SVC_STTUS.STTUS_SEQ_NO
FROM {IDSOwner}.CUST_SVC_TASK_STTUS CUST_SVC_STTUS,
     {IDSOwner}.CUST_SVC_TASK CUST_SVC_TASK,
     {IDSOwner}.CD_MPPNG CD_MPPNG,
     {IDSOwner}.W_CUST_SVC_DRVR DRVR
WHERE CUST_SVC_STTUS.CUST_SVC_TASK_SK = CUST_SVC_TASK.CUST_SVC_TASK_SK
  AND CUST_SVC_STTUS.CUST_SVC_TASK_STTUS_CD_SK = CD_MPPNG.CD_MPPNG_SK
  AND CD_MPPNG.TRGT_CD='CLSD'
  AND CUST_SVC_TASK.SRC_SYS_CD_SK = DRVR.SRC_SYS_CD_SK
  AND CUST_SVC_TASK.CUST_SVC_ID = DRVR.CUST_SVC_ID
"""
df_db2_SttusSeqNo = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", sql_str_db2_SttusSeqNo)
    .load()
)

# 24) lkp_Codes
# Primary link: db2_MinStusDtm => "lnk_MinStusDtm"
# Lookup link: db2_SttusSeqNo => "lnk_SttusSeqNo" (left join on CUST_SVC_TASK_SK, STTUS_DTM, TRGT_CD)

df_lkp_Codes_temp = df_db2_MinStusDtm.alias("lnk_MinStusDtm").join(
    df_db2_SttusSeqNo.alias("lnk_SttusSeqNo"),
    on=[
        F.col("lnk_MinStusDtm.CUST_SVC_TASK_SK") == F.col("lnk_SttusSeqNo.CUST_SVC_TASK_SK"),
        F.col("lnk_MinStusDtm.STTUS_DTM") == F.col("lnk_SttusSeqNo.STTUS_DTM"),
        F.col("lnk_MinStusDtm.TRGT_CD") == F.col("lnk_SttusSeqNo.TRGT_CD")
    ],
    how="left"
)
df_lkp_Codes = df_lkp_Codes_temp.select(
    F.col("lnk_MinStusDtm.CUST_SVC_TASK_SK").alias("CUST_SVC_TASK_SK"),
    F.col("lnk_SttusSeqNo.STTUS_SEQ_NO").alias("STTUS_SEQ_NO")
)

# 25) lkp_Codes1
# Primary link: db2_CustSvcTaskSttus => "lnk_SttusNo"
# Lookup link: df_lkp_Codes => "DSLink486" (left join on CUST_SVC_TASK_SK)

df_lkp_Codes1_temp = df_db2_CustSvcTaskSttus.alias("lnk_SttusNo").join(
    df_lkp_Codes.alias("DSLink486"),
    on=[F.col("lnk_SttusNo.CUST_SVC_TASK_SK") == F.col("DSLink486.CUST_SVC_TASK_SK")],
    how="left"
)
df_lkp_Codes1 = df_lkp_Codes1_temp.select(
    F.col("DSLink486.CUST_SVC_TASK_SK").alias("CUST_SVC_TASK_SK"),
    F.col("DSLink486.STTUS_SEQ_NO").alias("STTUS_SEQ_NO_Ref"),
    F.col("lnk_SttusNo.STTUS_SEQ_NO").alias("STTUS_SEQ_NO_SttusNo"),
    F.col("lnk_SttusNo.STTUS_DTM").alias("STTUS_DTM")
)

# 26) xfm_ReOpenRules
#    stageVars:
#      svReOpenIn = if STTUS_SEQ_NO_SttusNo > STTUS_SEQ_NO_Ref then 'Y' else 'N'
#      svSttusDtm = if STTUS_SEQ_NO_SttusNo > STTUS_SEQ_NO_Ref then DateToString(..."CCYY-MM-DD") else 'NA'
#    Output link constraint = svReOpenIn = 'Y'

df_xfm_ReOpenRules_stagevars = df_lkp_Codes1.select(
    F.col("CUST_SVC_TASK_SK"),
    (F.when(F.col("STTUS_SEQ_NO_SttusNo") > F.col("STTUS_SEQ_NO_Ref"), "Y").otherwise("N")).alias("svReOpenIn"),
    (F.when(F.col("STTUS_SEQ_NO_SttusNo") > F.col("STTUS_SEQ_NO_Ref"),
            F.date_format(F.col("STTUS_DTM"), "yyyy-MM-dd")
           ).otherwise("NA")).alias("svSttusDtm")
)

df_xfm_ReOpenRules_output = df_xfm_ReOpenRules_stagevars.filter(F.col("svReOpenIn") == "Y").select(
    F.col("CUST_SVC_TASK_SK").alias("CUST_SVC_TASK_SK"),
    F.rpad(F.col("svReOpenIn"),1," ").alias("REOPEN_IN"),
    F.rpad(F.col("svSttusDtm"),10," ").alias("STTUS_DTM"),
    F.col("CUST_SVC_TASK_SK").alias("CUST_SVC_TASK_SK_1")
)

# 27) Aggregator_491
# group by (CUST_SVC_TASK_SK, REOPEN_IN, CUST_SVC_TASK_SK_1), reduce -> min(STTUS_DTM)
df_Aggregator_491 = (
    df_xfm_ReOpenRules_output
    .groupBy("CUST_SVC_TASK_SK","REOPEN_IN","CUST_SVC_TASK_SK_1")
    .agg(F.min("STTUS_DTM").alias("STTUS_DTM"))
    .select(
        F.col("CUST_SVC_TASK_SK"),
        F.col("REOPEN_IN"),
        F.rpad(F.col("STTUS_DTM"),10," ").alias("STTUS_DTM"),
        F.col("CUST_SVC_TASK_SK_1")
    )
)

# 28) Lookup_533
# Primary link: df_DB2_Connector_531 => "lnk_Extract"
# Then many left joins with provided keys

df_Lookup_533_0 = df_DB2_Connector_531.alias("lnk_Extract")

# left join lnk_CustSvcStttusCdLkp on (CUST_SVC_TASK_STTUS_CD_SK = CD_MPPNG_SK)
df_Lookup_533_1 = df_Lookup_533_0.join(
    df_lnk_CustSvcStttusCdLkp.alias("lnk_CustSvcStttusCdLkp"),
    on=[F.col("lnk_Extract.CUST_SVC_TASK_STTUS_CD_SK") == F.col("lnk_CustSvcStttusCdLkp.CD_MPPNG_SK")],
    how="left"
)

# left join CdmppngLkp on (CS_TASK_CLS_PLN_PROD_CAT_CD_SK = CD_MPPNG_SK)
df_Lookup_533_2 = df_Lookup_533_1.join(
    df_CdmppngLkp.alias("CdmppngLkp"),
    on=[F.col("lnk_Extract.CS_TASK_CLS_PLN_PROD_CAT_CD_SK") == F.col("CdmppngLkp.CD_MPPNG_SK")],
    how="left"
)

# left join db2_CustSvcTskSrccdlkup => "lnk_CustSvcTskSrccdlkup1" on (CUST_SVC_TASK_CAT_CD_SK = CD_MPPNG_SK)
df_Lookup_533_3 = df_Lookup_533_2.join(
    df_db2_CustSvcTskSrccdlkup.alias("lnk_CustSvcTskSrccdlkup1"),
    on=[F.col("lnk_Extract.CUST_SVC_TASK_CAT_CD_SK") == F.col("lnk_CustSvcTskSrccdlkup1.CD_MPPNG_SK")],
    how="left"
)

# left join df_Aggregator_491 => "lnk_CustTaskReOpenIN" on (CUST_SVC_TASK_SK)
df_Lookup_533_4 = df_Lookup_533_3.join(
    df_Aggregator_491.alias("lnk_CustTaskReOpenIN"),
    on=[F.col("lnk_Extract.CUST_SVC_TASK_SK") == F.col("lnk_CustTaskReOpenIN.CUST_SVC_TASK_SK")],
    how="left"
)

# left join db2_ProdShNmLkp => "lnk_ProdShNm" on (PROD_SK)
df_Lookup_533_5 = df_Lookup_533_4.join(
    df_db2_ProdShNmLkp.alias("lnk_ProdShNm"),
    on=[F.col("lnk_Extract.PROD_SK") == F.col("lnk_ProdShNm.PROD_SK")],
    how="left"
)

# left join db2_CustSvcTskNoteLkp1 => "lnk_CustSvcTskNoteLkp1" on (CUST_SVC_TASK_SK)
df_Lookup_533_6 = df_Lookup_533_5.join(
    df_db2_CustSvcTskNoteLkp1.alias("lnk_CustSvcTskNoteLkp1"),
    on=[F.col("lnk_Extract.CUST_SVC_TASK_SK") == F.col("lnk_CustSvcTskNoteLkp1.CUST_SVC_TASK_SK")],
    how="left"
)

# left join db2_CustSvcTskCstmDtl => "lnk_CustSvcTskCstmDtl" on (CUST_SVC_TASK_SK)
df_Lookup_533_7 = df_Lookup_533_6.join(
    df_db2_CustSvcTskCstmDtl.alias("lnk_CustSvcTskCstmDtl"),
    on=[F.col("lnk_Extract.CUST_SVC_TASK_SK") == F.col("lnk_CustSvcTskCstmDtl.CUST_SVC_TASK_SK")],
    how="left"
)

# left join db2_CustSvcTskCstmDtlLkp2 => "lnk_CustSvcTskCstmDtlLkp3" on (CUST_SVC_TASK_SK)
df_Lookup_533_8 = df_Lookup_533_7.join(
    df_db2_CustSvcTskCstmDtlLkp2.alias("lnk_CustSvcTskCstmDtlLkp3"),
    on=[F.col("lnk_Extract.CUST_SVC_TASK_SK") == F.col("lnk_CustSvcTskCstmDtlLkp3.CUST_SVC_TASK_SK")],
    how="left"
)

# left join db2_CustSvcTskCstmDtlLkp4 => "lnk_CustSvcTskCstmDtlLkp5" on (CUST_SVC_TASK_SK)
df_Lookup_533_9 = df_Lookup_533_8.join(
    df_db2_CustSvcTskCstmDtlLkp4.alias("lnk_CustSvcTskCstmDtlLkp5"),
    on=[F.col("lnk_Extract.CUST_SVC_TASK_SK") == F.col("lnk_CustSvcTskCstmDtlLkp5.CUST_SVC_TASK_SK")],
    how="left"
)

# left join lnk_CsTskClsPlnProdCatCdLkup => on (CS_TASK_CLS_PLN_PROD_CAT_CD_SK)
df_Lookup_533_10 = df_Lookup_533_9.join(
    df_lnk_CsTskClsPlnProdCatCdLkup.alias("lnk_CsTskClsPlnProdCatCdLkup"),
    on=[F.col("lnk_Extract.CS_TASK_CLS_PLN_PROD_CAT_CD_SK") == F.col("lnk_CsTskClsPlnProdCatCdLkup.CD_MPPNG_SK")],
    how="left"
)

# left join db2_CustSvcTskSttusLkp2_Min => "lnk_CustSvcTskSttusLkp3" on (CUST_SVC_TASK_SK)
df_Lookup_533_11 = df_Lookup_533_10.join(
    df_db2_CustSvcTskSttusLkp2_Min.alias("lnk_CustSvcTskSttusLkp3"),
    on=[F.col("lnk_Extract.CUST_SVC_TASK_SK") == F.col("lnk_CustSvcTskSttusLkp3.CUST_SVC_TASK_SK")],
    how="left"
)

# left join db2_CustSvcTskSttusLkp_Max => "lnk_CustSvcTskSttusLkp1" on (CUST_SVC_TASK_SK)
df_Lookup_533_12 = df_Lookup_533_11.join(
    df_db2_CustSvcTskSttusLkp_Max.alias("lnk_CustSvcTskSttusLkp1"),
    on=[F.col("lnk_Extract.CUST_SVC_TASK_SK") == F.col("lnk_CustSvcTskSttusLkp1.CUST_SVC_TASK_SK")],
    how="left"
)

# left join db2_CustSvcLkp => "lnk_CustSvc" on (CUST_SVC_SK)
df_Lookup_533_13 = df_Lookup_533_12.join(
    df_db2_CustSvcLkp.alias("lnk_CustSvc"),
    on=[F.col("lnk_Extract.CUST_SVC_SK") == F.col("lnk_CustSvc.CUST_SVC_SK")],
    how="left"
)

# left join db2_CustSvcTskLtr => "lnk_CustSvcTskLtr" on (CUST_SVC_TASK_SK)
df_Lookup_533_14 = df_Lookup_533_13.join(
    df_db2_CustSvcTskLtr.alias("lnk_CustSvcTskLtr"),
    on=[F.col("lnk_Extract.CUST_SVC_TASK_SK") == F.col("lnk_CustSvcTskLtr.CUST_SVC_TASK_SK")],
    how="left"
)

# left join lnk_CustSvcTskSubjCdLkup => on (CUST_SVC_TASK_SUBJ_CD_SK)
df_Lookup_533_15 = df_Lookup_533_14.join(
    df_lnk_CustSvcTskSubjCdLkup.alias("lnk_CustSvcTskSubjCdLkup"),
    on=[F.col("lnk_Extract.CUST_SVC_TASK_SUBJ_CD_SK") == F.col("lnk_CustSvcTskSubjCdLkup.CD_MPPNG_SK")],
    how="left"
)

# left join lnk_CustSvctskSttusRsnCdLkup => on (CUST_SVC_TASK_STTUS_RSN_CD_SK)
df_Lookup_533_16 = df_Lookup_533_15.join(
    df_lnk_CustSvctskSttusRsnCdLkup.alias("lnk_CustSvctskSttusRsnCdLkup"),
    on=[F.col("lnk_Extract.CUST_SVC_TASK_STTUS_RSN_CD_SK") == F.col("lnk_CustSvctskSttusRsnCdLkup.CD_MPPNG_SK")],
    how="left"
)

# left join lnk_CustSvcTskPrtyCdLkup => on (CUST_SVC_TASK_PRTY_CD_SK)
df_Lookup_533_17 = df_Lookup_533_16.join(
    df_lnk_CustSvcTskPrtyCdLkup.alias("lnk_CustSvcTskPrtyCdLkup"),
    on=[F.col("lnk_Extract.CUST_SVC_TASK_PRTY_CD_SK") == F.col("lnk_CustSvcTskPrtyCdLkup.CD_MPPNG_SK")],
    how="left"
)

# left join lnk_CustSvcTskPgCdLkup => on (CUST_SVC_TASK_PG_TYP_CD_SK)
df_Lookup_533_18 = df_Lookup_533_17.join(
    df_lnk_CustSvcTskPgCdLkup.alias("lnk_CustSvcTskPgCdLkup"),
    on=[F.col("lnk_Extract.CUST_SVC_TASK_PG_TYP_CD_SK") == F.col("lnk_CustSvcTskPgCdLkup.CD_MPPNG_SK")],
    how="left"
)

# left join lnk_CsTskLtrRecpntCdLkup => on (CS_TASK_LTR_RECPNT_TYP_CD_SK)
df_Lookup_533_19 = df_Lookup_533_18.join(
    df_lnk_CsTskLtrRecpntCdLkup.alias("lnk_CsTskLtrRecpntCdLkup"),
    on=[F.col("lnk_Extract.CS_TASK_LTR_RECPNT_TYP_CD_SK") == F.col("lnk_CsTskLtrRecpntCdLkup.CD_MPPNG_SK")],
    how="left"
)

# left join db2_ProvLkp => "lnk_ProvLkp" on (PROV_SK)
df_Lookup_533_20 = df_Lookup_533_19.join(
    df_db2_ProvLkp.alias("lnk_ProvLkp"),
    on=[F.col("lnk_Extract.PROV_SK") == F.col("lnk_ProvLkp.PROV_SK")],
    how="left"
)

# left join db2_ProdLkp => "lnk_ProdLkp" on (PROD_SK)
df_Lookup_533_21 = df_Lookup_533_20.join(
    df_db2_ProdLkp.alias("lnk_ProdLkp"),
    on=[F.col("lnk_Extract.PROD_SK") == F.col("lnk_ProdLkp.PROD_SK")],
    how="left"
)

# left join db2_GrpLkp => "lnk_GrpLkp" on (GRP_SK)
df_Lookup_533_22 = df_Lookup_533_21.join(
    df_db2_GrpLkp.alias("lnk_GrpLkp"),
    on=[F.col("lnk_Extract.GRP_SK") == F.col("lnk_GrpLkp.GRP_SK")],
    how="left"
)

# left join lnk_LastUpdtUserLookup => on (LAST_UPDT_USER_SK)
df_Lookup_533_23 = df_Lookup_533_22.join(
    df_lnk_LastUpdtUserLookup.alias("lnk_LastUpdtUserLookup"),
    on=[F.col("lnk_Extract.LAST_UPDT_USER_SK") == F.col("lnk_LastUpdtUserLookup.USER_SK")],
    how="left"
)

# left join lnk_CustSvcTskItsTypCdLkup => on (CUST_SVC_TASK_ITS_TYP_CD_SK)
df_Lookup_533_24 = df_Lookup_533_23.join(
    df_lnk_CustSvcTskItsTypCdLkup.alias("lnk_CustSvcTskItsTypCdLkup"),
    on=[F.col("lnk_Extract.CUST_SVC_TASK_ITS_TYP_CD_SK") == F.col("lnk_CustSvcTskItsTypCdLkup.CD_MPPNG_SK")],
    how="left"
)

# left join lnk_CustSvcTskFinlActnCdLkup => on (CUST_SVC_TASK_FINL_ACTN_CD_SK)
df_Lookup_533_25 = df_lnk_CustSvcTskFinlActnCdLkup.alias("lnk_CustSvcTskFinlActnCdLkup")
df_Lookup_533_25 = df_Lookup_533_24.join(
    df_Lookup_533_25,
    on=[F.col("lnk_Extract.CUST_SVC_TASK_FINL_ACTN_CD_SK") == F.col("lnk_CustSvcTskFinlActnCdLkup.CD_MPPNG_SK")],
    how="left"
)

# left join lnk_CustSvcTskCustTypCdLkup => on (CUST_SVC_TASK_CUST_TYP_CD_SK)
df_Lookup_533_26 = df_lnk_CustSvcTskCustTypCdLkup.alias("lnk_CustSvcTskCustTypCdLkup")
df_Lookup_533_26 = df_Lookup_533_25.join(
    df_Lookup_533_26,
    on=[F.col("lnk_Extract.CUST_SVC_TASK_CUST_TYP_CD_SK") == F.col("lnk_CustSvcTskCustTypCdLkup.CD_MPPNG_SK")],
    how="left"
)

# left join lnk_CustSvcTskClsrPrfCdLkup => on (CUST_SVC_TASK_CLSR_PRF_CD_SK)
df_Lookup_533_27 = df_lnk_CustSvcTskClsrPrfCdLkup.alias("lnk_CustSvcTskClsrPrfCdLkup")
df_Lookup_533_27 = df_Lookup_533_26.join(
    df_Lookup_533_27,
    on=[F.col("lnk_Extract.CUST_SVC_TASK_CLSR_PRF_CD_SK") == F.col("lnk_CustSvcTskClsrPrfCdLkup.CD_MPPNG_SK")],
    how="left"
)

# left join CustSvcTskCatCdLkup => on (CUST_SVC_TASK_CAT_CD_SK)
df_Lookup_533_28 = df_Lookup_533_27.join(
    df_CustSvcTskCatCdLkup.alias("CustSvcTskCatCdLkup"),
    on=[F.col("lnk_Extract.CUST_SVC_TASK_CAT_CD_SK") == F.col("CustSvcTskCatCdLkup.CD_MPPNG_SK")],
    how="left"
)

# left join lnk_CrtByUserLookup => on (INPT_USER_SK)
df_Lookup_533_final = df_Lookup_533_28.join(
    df_lnk_CrtByUserLookup.alias("lnk_CrtByUserLookup"),
    on=[F.col("lnk_Extract.INPT_USER_SK") == F.col("lnk_CrtByUserLookup.USER_SK")],
    how="left"
)

df_Lookup_533 = df_Lookup_533_final.select(
    F.col("lnk_Extract.CUST_SVC_TASK_SK").alias("CUST_SVC_TASK_SK"),
    F.col("lnk_Extract.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnk_Extract.CUST_SVC_ID").alias("CUST_SVC_ID"),
    F.col("lnk_Extract.TASK_SEQ_NO").alias("TASK_SEQ_NO"),
    F.col("lnk_Extract.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("lnk_Extract.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("lnk_Extract.CUST_SVC_SK").alias("CUST_SVC_SK"),
    F.col("lnk_Extract.GRP_SK").alias("GRP_SK"),
    F.col("lnk_Extract.INPT_USER_SK").alias("INPT_USER_SK"),
    F.col("lnk_Extract.LAST_UPDT_USER_SK").alias("LAST_UPDT_USER_SK"),
    F.col("lnk_Extract.MBR_SK").alias("MBR_SK"),
    F.col("lnk_Extract.PROD_SK").alias("PROD_SK"),
    F.col("lnk_Extract.PROV_SK").alias("PROV_SK"),
    F.col("lnk_Extract.SUBGRP_SK").alias("SUBGRP_SK"),
    F.col("lnk_Extract.SUB_SK").alias("SUB_SK"),
    F.col("lnk_Extract.CUST_SVC_TASK_CAT_CD_SK").alias("CUST_SVC_TASK_CAT_CD_SK"),
    F.col("lnk_Extract.CS_TASK_CLS_PLN_PROD_CAT_CD_SK").alias("CS_TASK_CLS_PLN_PROD_CAT_CD_SK"),
    F.col("lnk_Extract.CUST_SVC_TASK_CLSR_PRF_CD_SK").alias("CUST_SVC_TASK_CLSR_PRF_CD_SK"),
    F.col("lnk_Extract.CUST_SVC_TASK_CUST_TYP_CD_SK").alias("CUST_SVC_TASK_CUST_TYP_CD_SK"),
    F.col("lnk_Extract.CUST_SVC_TASK_FINL_ACTN_CD_SK").alias("CUST_SVC_TASK_FINL_ACTN_CD_SK"),
    F.col("lnk_Extract.CUST_SVC_TASK_ITS_TYP_CD_SK").alias("CUST_SVC_TASK_ITS_TYP_CD_SK"),
    F.col("lnk_Extract.CS_TASK_LTR_RECPNT_TYP_CD_SK").alias("CS_TASK_LTR_RECPNT_TYP_CD_SK"),
    F.col("lnk_Extract.CUST_SVC_TASK_PG_TYP_CD_SK").alias("CUST_SVC_TASK_PG_TYP_CD_SK"),
    F.col("lnk_Extract.CUST_SVC_TASK_PRTY_CD_SK").alias("CUST_SVC_TASK_PRTY_CD_SK"),
    F.col("lnk_Extract.CUST_SVC_TASK_STTUS_CD_SK").alias("CUST_SVC_TASK_STTUS_CD_SK"),
    F.col("lnk_Extract.CUST_SVC_TASK_STTUS_RSN_CD_SK").alias("CUST_SVC_TASK_STTUS_RSN_CD_SK"),
    F.col("lnk_Extract.CUST_SVC_TASK_SUBJ_CD_SK").alias("CUST_SVC_TASK_SUBJ_CD_SK"),
    F.rpad(F.col("lnk_Extract.CMPLNT_IN"),1," ").alias("CMPLNT_IN"),
    F.rpad(F.col("lnk_Extract.CLSD_DT_SK"),10," ").alias("CLSD_DT_SK"),
    F.col("lnk_Extract.INPT_DTM").alias("INPT_DTM"),
    F.col("lnk_Extract.LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
    F.rpad(F.col("lnk_Extract.NEXT_RVW_DT_SK"),10," ").alias("NEXT_RVW_DT_SK"),
    F.rpad(F.col("lnk_Extract.RCVD_DT_SK"),10," ").alias("RCVD_DT_SK"),
    F.col("lnk_Extract.NEXT_RVW_INTRVL_DAYS").alias("NEXT_RVW_INTRVL_DAYS"),
    F.col("lnk_Extract.CUST_ID").alias("CUST_ID"),
    F.col("lnk_Extract.CUST_SVC_TASK_USER_SITE_ID").alias("CUST_SVC_TASK_USER_SITE_ID"),
    F.col("lnk_Extract.INPT_USER_CC_ID").alias("INPT_USER_CC_ID"),
    F.col("lnk_Extract.SUM_DESC").alias("SUM_DESC"),
    F.col("lnk_CustSvcTskFinlActnCdLkup.TRGT_CD").alias("TRGT_CD_CustSvcTskFinlActnCdLkup"),
    F.col("lnk_CustSvcTskFinlActnCdLkup.TRGT_CD_NM").alias("TRGT_CD_NM_CustSvcTskFinlActnCdLkup"),
    F.col("lnk_CustSvcTskCustTypCdLkup.TRGT_CD").alias("TRGT_CD_CustSvcTskCustTypCdLkup"),
    F.col("lnk_CustSvcTskCustTypCdLkup.TRGT_CD_NM").alias("TRGT_CD_NM_CustSvcTskCustTypCdLkup"),
    F.col("lnk_CustSvcTskClsrPrfCdLkup.TRGT_CD").alias("TRGT_CD_CustSvcTskClsrPrfCdLkup"),
    F.col("lnk_CustSvcTskClsrPrfCdLkup.TRGT_CD_NM").alias("TRGT_CD_NM_CustSvcTskClsrPrfCdLkup"),
    F.rpad(F.col("lnk_CrtByUserLookup.USER_ID"),10," ").alias("USER_ID_CrtByUserLookup"),
    F.col("lnk_CustSvcTskItsTypCdLkup.TRGT_CD").alias("TRGT_CD_CustSvcTskItsTypCdLkup"),
    F.col("lnk_CustSvcTskItsTypCdLkup.TRGT_CD_NM").alias("TRGT_CD_NM_CustSvcTskItsTypCdLkup"),
    F.rpad(F.col("lnk_LastUpdtUserLookup.USER_ID"),10," ").alias("USER_ID_LastUpdtUserLookup"),
    F.col("lnk_GrpLkp.GRP_ID").alias("GRP_ID_GrpLkp"),
    F.rpad(F.col("lnk_ProdLkp.PROD_ID"),8," ").alias("PROD_ID_ProdLkp"),
    F.col("lnk_ProvLkp.PROV_ID").alias("PROV_ID_ProvLkp"),
    F.col("lnk_CsTskLtrRecpntCdLkup.TRGT_CD").alias("TRGT_CD_CsTskLtrRecpntCdLkup"),
    F.col("lnk_CsTskLtrRecpntCdLkup.TRGT_CD_NM").alias("TRGT_CD_NM_CsTskLtrRecpntCdLkup"),
    F.col("lnk_CustSvcTskPrtyCdLkup.TRGT_CD").alias("TRGT_CD_CustSvcTskPrtyCdLkup"),
    F.col("lnk_CustSvcTskPrtyCdLkup.TRGT_CD_NM").alias("TRGT_CD_NM_CustSvcTskPrtyCdLkup"),
    F.col("lnk_CustSvcTskPgCdLkup.TRGT_CD").alias("TRGT_CD_CustSvcTskPgCdLkup"),
    F.col("lnk_CustSvcTskPgCdLkup.TRGT_CD_NM").alias("TRGT_CD_NM_CustSvcTskPgCdLkup"),
    F.col("lnk_CustSvctskSttusRsnCdLkup.TRGT_CD").alias("TRGT_CD_CustSvcTskSttusRsnCdLkup"),
    F.col("lnk_CustSvctskSttusRsnCdLkup.TRGT_CD_NM").alias("TRGT_CD_NM_CustSvcTskSttusRsnCdLkup"),
    F.col("lnk_CustSvcTskSubjCdLkup.TRGT_CD").alias("TRGT_CD_CustSvcTskSubjCdlkp"),
    F.col("lnk_CustSvcTskSubjCdLkup.TRGT_CD_NM").alias("TRGT_CD_NM_CustSvcTskSubjCdlkp"),
    F.col("lnk_CustSvcTskLtr.CUST_SVC_TASK_SK_1").alias("CUST_SVC_TASK_SK_1_CutSvcTskLtr"),
    F.col("lnk_CustSvc.CNTCT_SRC_CD").alias("CNTCT_SRC_CD_CustSvc"),
    F.col("lnk_CustSvc.METH_SRC_CD").alias("METH_SRC_CD_CustSvc"),
    F.col("lnk_CustSvcTskSttusLkp1.STTUS_DTM").alias("STTUS_DTM_CustSvcTskSttusLkp1"),
    F.col("lnk_CustSvcTskSttusLkp3.STTUS_DTM").alias("STTUS_DTM_CustSvcTskSttusLkp3"),
    F.col("lnk_CustSvcTskCstmDtl.TRGT_CD").alias("TRGT_CD_CustSvcTskCstmDtl"),
    F.col("lnk_CustSvcTskNoteLkp1.CUST_SVC_TASK_SK1").alias("CUST_SVC_TASK_SK1_CustSvcTskNoteLkp1"),
    F.col("lnk_CsTskClsPlnProdCatCdLkup.TRGT_CD").alias("TRGT_CD_CsTskClsPlnProdCatCdLkup"),
    F.col("lnk_CsTskClsPlnProdCatCdLkup.TRGT_CD_NM").alias("TRGT_CD_NM_CsTskClsPlnProdCatCdLkup"),
    F.col("lnk_CustSvcTskCstmDtlLkp3.TRGT_CD").alias("TRGT_CD_CustSvcTskCstmDtlLkp3"),
    F.col("lnk_CustSvcTskCstmDtlLkp5.TRGT_CD").alias("TRGT_CD_CustSvcTskCstmDtlLkp5"),
    F.col("lnk_CustSvcTskSrccdlkup1.SRC_CD").alias("SRC_CD_CustSvcTskSrccdlkup1"),
    F.rpad(F.col("lnk_CustTaskReOpenIN.REOPEN_IN"),1," ").alias("REOPEN_IN_CustTaskReOpenIN"),
    F.rpad(F.col("lnk_CustTaskReOpenIN.STTUS_DTM"),10," ").alias("STTUS_DTM_CustTaskReOpenIN"),
    F.col("lnk_CustTaskReOpenIN.CUST_SVC_TASK_SK_1").alias("CUST_SVC_TASK_SK_CustTaskReOpenIN"),
    F.col("lnk_ProdShNm.PROD_SH_NM").alias("PROD_SH_NM_ProdShNm"),
    F.col("CustSvcTskCatCdLkup.TRGT_CD").alias("TRGT_CD_CustSvcTskCatCdLkp"),
    F.col("CustSvcTskCatCdLkup.TRGT_CD_NM").alias("TRGT_CD_NM_CustSvcTskCatCdLkp"),
    F.col("CdmppngLkp.SRC_CD").alias("SRC_CD_CdMppngLkp"),
    F.col("lnk_CustSvcStttusCdLkp.TRGT_CD").alias("TRGT_CD_CustSvcSttusCdLkp"),
    F.col("lnk_CustSvcStttusCdLkp.TRGT_CD_NM").alias("TRGT_CD_NM_CustSvcSttusCdLkp")
)

# 29) Transformer_587
#    It produces 3 outputs: lnk_UNK, lnk_CustSvcTask, lnk_NA
#    Constraints:
#      lnk_UNK: "((@INROWNUM - 1) * @NUMPARTITIONS + @PARTITIONNUM + 1) = 1"
#      lnk_CustSvcTask: "CUST_SVC_TASK_SK <> 0 AND CUST_SVC_TASK_SK <> 1"
#      lnk_NA: "((@INROWNUM - 1) * @NUMPARTITIONS + @PARTITIONNUM + 1) = 1"
#
#    lnk_UNK and lnk_NA each produce constant or setNull columns. We replicate them exactly.
#    We'll interpret the “@INROWNUM ...=1" logic as a condition that is true for exactly one row,
#    so it yields a single dummy row. We'll do that with a simple approach: pick the first row of df_Lookup_533, or generate a row with filter, limiting to 1. 
#    Then fill columns with the specified values. 
#    We'll do two single-row DataFrames: df_lnk_UNK and df_lnk_NA.
#    The rest goes to df_lnk_CustSvcTask. We filter on "CUST_SVC_TASK_SK <> 0 AND CUST_SVC_TASK_SK <> 1".

# Create main data for lnk_CustSvcTask
df_lnk_CustSvcTask = df_Lookup_533.filter(
    (F.col("CUST_SVC_TASK_SK") != 0) & (F.col("CUST_SVC_TASK_SK") != 1)
)

# We produce single rows for lnk_UNK and lnk_NA with the specified columns/values:
# The job has more than 70 columns, each with defaults for lnk_UNK and lnk_NA.
# We will create them as literal-based DataFrames.

colNames_unk = [
 "CUST_SVC_TASK_SK","SRC_SYS_CD","CUST_SVC_ID","CUST_SVC_TASK_SEQ_NO",
 "CRT_RUN_CYC_EXCTN_DT_SK","LAST_UPDT_RUN_CYC_EXCTN_DT_SK","CUST_SVC_SK","GRP_SK","INPT_USER_SK","LAST_UPDT_USER_SK","MBR_SK","PROD_SK","PROV_SK",
 "SUBGRP_SK","SUB_SK","BCKDT_ATCHD_IN","CUST_SVC_TASK_CAT_CD","CUST_SVC_TASK_CAT_NM","CS_TASK_CLS_PLN_PROD_CAT_CD","CS_TASK_CLS_PLN_PROD_CAT_NM",
 "CUST_SVC_TASK_CLSD_DT_SK","CUST_SVC_TASK_CLSR_PRF_CD","CUST_SVC_TASK_CLSR_PRF_NM","CUST_SVC_TASK_CMPLNT_IN","CUST_SVC_TASK_CUST_ID",
 "CUST_SVC_TASK_CUST_TYP_CD","CUST_SVC_TASK_CUST_TYP_NM","CUST_SVC_TASK_FINL_ACTN_CD","CUST_SVC_TASK_FINL_ACTN_NM","CUST_SVC_TASK_INPT_DTM","CUST_SVC_TASK_INPT_USER_CC_ID",
 "CUST_SVC_TASK_INPT_USER_ID","CUST_SVC_TASK_ITS_TYP_CD","CUST_SVC_TASK_ITS_TYP_NM","CUST_SVC_TASK_LAST_UPDT_DTM","CS_TASK_LTR_RECPNT_TYP_CD","CS_TASK_LTR_RECPNT_TYP_NM",
 "CUST_SVC_TASK_PG_TYP_CD","CUST_SVC_TASK_PG_TYP_NM","CUST_SVC_TASK_PRTY_CD","CUST_SVC_TASK_PRTY_NM","CS_TASK_NEXT_RVW_INTRVL_DAYS","CUST_SVC_TASK_NEXT_RVW_DT_SK","CUST_SVC_TASK_RCVD_DT_SK",
 "CUST_SVC_TASK_STTUS_CD","CUST_SVC_TASK_STTUS_NM","CUST_SVC_TASK_STTUS_RSN_CD","CUST_SVC_TASK_STTUS_RSN_NM","CUST_SVC_TASK_SUBJ_CD","CUST_SVC_TASK_SUBJ_NM","CUST_SVC_TASK_SUM_DESC",
 "CUST_SVC_TASK_USER_SITE_ID","DOC_ATCHMT_IN","FUND_ATCHD_IN","GRP_ID","LAST_UPDT_USER_ID","LTR_ATCHD_IN","MTM_IN","MULT_CLOSING_IN","NOTE_ATCHD_IN",
 "PROD_ID","PROD_SH_NM","PROV_ID","REOPEN_DT_SK","REOPEN_IN","SAME_DAY_IN","CRT_RUN_CYC_EXCTN_SK","LAST_UPDT_RUN_CYC_EXCTN_SK","CUST_SVC_TASK_CAT_CD_SK","CS_TASK_CLS_PLN_PROD_CAT_CD_SK",
 "CUST_SVC_TASK_CLSR_PRF_CD_SK","CUST_SVC_TASK_CUST_TYP_CD_SK","CUST_SVC_TASK_FINL_ACTN_CD_SK","CUST_SVC_TASK_ITS_TYP_CD_SK","CS_TASK_LTR_RECPNT_TYP_CD_SK","CUST_SVC_TASK_PG_TYP_CD_SK",
 "CUST_SVC_TASK_PRTY_CD_SK","CUST_SVC_TASK_STTUS_CD_SK","CUST_SVC_TASK_STTUS_RSN_CD_SK","CUST_SVC_TASK_SUBJ_CD_SK"
]

vals_unk = [
  0,"UNK","UNK",0,"1753-01-01","1753-01-01",0,0,0,0,0,0,0,0,0,"N","UNK","UNK","UNK","UNK","1753-01-01","UNK","UNK","N","UNK","UNK","UNK","UNK","UNK",
  "1753-01-01 00:00:00","UNK","UNK","UNK","UNK","1753-01-01 00:00:00","UNK","UNK","UNK","UNK","UNK",0,"1753-01-01","1753-01-01","UNK","UNK","UNK","UNK","UNK","UNK",None,
  "UNK","N","N","UNK","UNK","N","N","N","N","UNK","UNK","UNK","1753-01-01","N","N",100,100,0,0,0,0,0,0,0,0,0,0,0
]

df_lnk_UNK = spark.createDataFrame([vals_unk], schema= [ (c, StringType()) for c in colNames_unk ])
# Fix numeric columns after creation (cast them properly)
df_lnk_UNK = df_lnk_UNK.select([
    (F.col(c).cast(IntegerType()) if isinstance(vals_unk[i], int) and c not in ["CUST_SVC_TASK_SUM_DESC",None] else
     F.col(c).cast(StringType())).alias(c)
    for i,c in enumerate(colNames_unk)
])

colNames_na = colNames_unk
vals_na = [
  1,"NA","NA",0,"1753-01-01","1753-01-01",1,1,1,1,1,1,1,1,1,"N","NA","NA","NA","NA","1753-01-01","NA","NA","N","NA","NA","NA","NA","NA",
  "1753-01-01 00:00:00","NA","NA","NA","NA","1753-01-01 00:00:00","NA","NA","NA","NA","NA",0,"1753-01-01","1753-01-01","NA","NA","NA","NA","NA","NA",None,
  "NA","N","N","NA","NA","N","N","N","N","NA","NA","NA","1753-01-01","N","N",100,100,1,1,1,1,1,1,1,1,1,1,1
]
df_lnk_NA = spark.createDataFrame([vals_na], schema= [ (c, StringType()) for c in colNames_na ])
df_lnk_NA = df_lnk_NA.select([
    (F.col(c).cast(IntegerType()) if isinstance(vals_na[i], int) else F.col(c).cast(StringType())).alias(c)
    for i,c in enumerate(colNames_na)
])

# 30) Funnel_588 => union of df_lnk_UNK, df_lnk_CustSvcTask, df_lnk_NA
commonCols = df_lnk_CustSvcTask.columns  # all have same column set
df_lnk_UNK2 = df_lnk_UNK.select(commonCols)
df_lnk_NA2 = df_lnk_NA.select(commonCols)
df_funnel_588 = df_lnk_UNK2.unionByName(df_lnk_CustSvcTask.select(commonCols)).unionByName(df_lnk_NA2.select(commonCols))

# 31) seq_CUST_SVC_TASK_D => write to .dat with the specified delimiter, quote, etc.
# We keep the column order exactly as funnel_588 output.
final_cols = df_funnel_588.columns

# Ensure we apply rpad for char columns as indicated by the final stage JSON (some already done, we must re-check lengths).
# The stage JSON shows many columns with "SqlType": "char" and "Length": ...
# We will apply rpad on each such column that had "Length" in the final stage.

char_len_map = {
    "CRT_RUN_CYC_EXCTN_DT_SK":10,
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK":10,
    "BCKDT_ATCHD_IN":1,
    "CUST_SVC_TASK_CLSD_DT_SK":10,
    "CUST_SVC_TASK_CMPLNT_IN":1,
    "CUST_SVC_TASK_CUST_TYP_CD":None,  # no length given => skip
    "CUST_SVC_TASK_FINL_ACTN_CD":None,
    "CUST_SVC_TASK_INPT_DTM":None,
    "CUST_SVC_TASK_LAST_UPDT_DTM":None,
    "CUST_SVC_TASK_ITS_TYP_CD":None,
    "CS_TASK_LTR_RECPNT_TYP_CD":None,
    "CUST_SVC_TASK_PG_TYP_CD":None,
    "CUST_SVC_TASK_PRTY_CD":None,
    "CUST_SVC_TASK_NEXT_RVW_DT_SK":10,
    "CUST_SVC_TASK_RCVD_DT_SK":10,
    "DOC_ATCHMT_IN":1,
    "FUND_ATCHD_IN":1,
    "LTR_ATCHD_IN":1,
    "MTM_IN":1,
    "MULT_CLOSING_IN":1,
    "NOTE_ATCHD_IN":1,
    "REOPEN_DT_SK":10,
    "REOPEN_IN":1,
    "SAME_DAY_IN":1,
}

df_final = df_funnel_588
for coln, lengthN in char_len_map.items():
    if lengthN is not None and coln in df_final.columns:
        df_final = df_final.withColumn(coln, F.rpad(F.col(coln), lengthN, " "))

# Now write the file
# "FileSchema": { "columnDelimiter": ",", "quoteChar": "^", "nullValue": null }, containsHeader=false
# Overwrite mode
write_files(
    df_final.select(final_cols),
    f"{adls_path}/load/CUST_SVC_TASK_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)