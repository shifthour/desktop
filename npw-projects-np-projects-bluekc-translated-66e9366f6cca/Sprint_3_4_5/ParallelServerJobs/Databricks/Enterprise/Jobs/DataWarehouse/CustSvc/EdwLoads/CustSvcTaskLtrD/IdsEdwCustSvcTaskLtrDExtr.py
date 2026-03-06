# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:
# MAGIC 
# MAGIC PROCESSING:
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date               Project/Altiris #               Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------    ---------------------------------------------------------   ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Bhoomi Dasari                2/14/2007           Cust Svc/3028           Originally Programmed                             devlEDW10              Steph Goddard             02/21/2007
# MAGIC                                                                                                       Changed extract to use driver table.
# MAGIC 
# MAGIC Raj mangalampally         12/23/2013           5114                 Original Programming(Server to Parallel conv)  EnterpriseWrhsdevl      Jag Yelavarthi             2014-01-29

# MAGIC Code SK lookups for Denormalization
# MAGIC 
# MAGIC Lookup Keys
# MAGIC 1.CRT_BY_USER_SK
# MAGIC 2.LAST_UPDT_USER_SK
# MAGIC 3.CUST_SVC_TASK_LTR_STYLE_CD_SK
# MAGIC 4.CUST_SVC_TASK_LTR_REPRT_STTUS_
# MAGIC 5.CUST_SVC_TASK_LTR_TYP_CD_SK
# MAGIC 6.CS_TASK_LTR_RECPNT_ST_CD_SK
# MAGIC 7.CUST_SVC_TASK_LTR_REF_ST_CD_SK
# MAGIC 8.CUST_SVC_TASK_LTR_SEND_ST_CD_S
# MAGIC Write CUST_SVC_TASK_LTR_D.dat Data into a Sequential file for Load Job IdsEdwComsnMnlAdjLoad.
# MAGIC Read all the Data from IDS CUST SVC Table CUST_SVC_TASK_LTR; 
# MAGIC Join on W_CUST_SVC_DRVR
# MAGIC Add Defaults and Null Handling.
# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Job Name: IdsEdwCustSvcTaskLtrDExtr
# MAGIC Read all the Data from IDS APP_USER table
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

df_db2_CUST_SVC_TASK_LTR_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""
SELECT 
CST_LTR.CUST_SVC_TASK_LTR_SK,
COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,
CST_LTR.CUST_SVC_ID,
CST_LTR.TASK_SEQ_NO,
CST_LTR.CUST_SVC_TASK_LTR_STYLE_CD_SK,
CST_LTR.LTR_SEQ_NO,
CST_LTR.LTR_DEST_ID,
CST_LTR.CRT_BY_USER_SK,
CST_LTR.CUST_SVC_TASK_SK,
CST_LTR.LAST_UPDT_USER_SK,
CST_LTR.CUST_SVC_TASK_LTR_REPRT_STTUS_,
CST_LTR.CUST_SVC_TASK_LTR_TYP_CD_SK,
CST_LTR.CRT_DT_SK,
CST_LTR.LAST_UPDT_DT_SK,
CST_LTR.MAILED_DT_SK,
CST_LTR.PRTED_DT_SK,
CST_LTR.RQST_DT_SK,
CST_LTR.SUBMT_DT_SK,
CST_LTR.MAILED_IN,
CST_LTR.PRTED_IN,
CST_LTR.RQST_IN,
CST_LTR.SUBMT_IN,
CST_LTR.RECPNT_NM,
CST_LTR.RECPNT_ADDR_LN_1,
CST_LTR.RECPNT_ADDR_LN_2,
CST_LTR.RECPNT_ADDR_LN_3,
CST_LTR.RECPNT_CITY_NM,
CST_LTR.CS_TASK_LTR_RECPNT_ST_CD_SK,
CST_LTR.RECPNT_POSTAL_CD,
CST_LTR.RECPNT_CNTY_NM,
CST_LTR.RECPNT_PHN_NO,
CST_LTR.RECPNT_PHN_NO_EXT,
CST_LTR.RECPNT_FAX_NO,
CST_LTR.REF_NM,
CST_LTR.REF_ADDR_LN_1,
CST_LTR.REF_ADDR_LN_2,
CST_LTR.REF_ADDR_LN_3,
CST_LTR.REF_CITY_NM,
CST_LTR.CUST_SVC_TASK_LTR_REF_ST_CD_SK,
CST_LTR.REF_POSTAL_CD,
CST_LTR.REF_CNTY_NM,
CST_LTR.REF_PHN_NO,
CST_LTR.REF_PHN_NO_EXT,
CST_LTR.REF_FAX_NO,
CST_LTR.SEND_NM,
CST_LTR.SEND_ADDR_LN_1,
CST_LTR.SEND_ADDR_LN_2,
CST_LTR.SEND_ADDR_LN_3,
CST_LTR.SEND_CITY_NM,
CST_LTR.CUST_SVC_TASK_LTR_SEND_ST_CD_S,
CST_LTR.SEND_POSTAL_CD,
CST_LTR.SEND_CNTY_NM,
CST_LTR.SEND_PHN_NO,
CST_LTR.SEND_PHN_NO_EXT,
CST_LTR.SEND_FAX_NO,
CST_LTR.EXPL_TX,
CST_LTR.LTR_TX_1,
CST_LTR.LTR_TX_2,
CST_LTR.LTR_TX_3,
CST_LTR.LTR_TX_4,
CST_LTR.MSG_TX 
FROM {IDSOwner}.CUST_SVC_TASK_LTR CST_LTR
LEFT JOIN {IDSOwner}.CD_MPPNG CD
ON CST_LTR.SRC_SYS_CD_SK = CD.CD_MPPNG_SK,
{IDSOwner}.W_CUST_SVC_DRVR DRVR 
WHERE 
CST_LTR.SRC_SYS_CD_SK = DRVR.SRC_SYS_CD_SK
AND CST_LTR.CUST_SVC_ID       = DRVR.CUST_SVC_ID
"""
    )
    .load()
)

df_db2_CD_MPPNG_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""
SELECT CD_MPPNG_SK,
COALESCE(TRGT_CD,'UNK') TRGT_CD,
COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM 
FROM {IDSOwner}.CD_MPPNG
"""
    )
    .load()
)

df_db2_APP_USER_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""
SELECT 
APP_USER.USER_SK,
APP_USER.USER_ID
FROM {IDSOwner}.APP_USER APP_USER
"""
    )
    .load()
)

df_Ref_CustSvcTskltrReprtCd_Lkup = df_db2_CD_MPPNG_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Ref_CustSvcTskltrTypCd_Lkup = df_db2_CD_MPPNG_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Ref_CustSvcTskltrRefCd_Lkup = df_db2_CD_MPPNG_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Ref_CustSvcTskltrRcpntCd_Lkup = df_db2_CD_MPPNG_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Ref_CustSvcTskltrSendCd_Lkup = df_db2_CD_MPPNG_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Ref_CustSvcTskltrstyleCd_Lkup = df_db2_CD_MPPNG_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Ref_CrtByUserLkup = df_db2_APP_USER_in.select(
    F.col("USER_SK").alias("USER_SK"),
    F.col("USER_ID").alias("USER_ID")
)

df_Ref_LastUpdtUserLkup = df_db2_APP_USER_in.select(
    F.col("USER_SK").alias("USER_SK"),
    F.col("USER_ID").alias("USER_ID")
)

df_lkp_Codes_alias = df_db2_CUST_SVC_TASK_LTR_in.alias("lnk_IdsEdwCustSvcTaskLtrExtr_InABC")

df_lkp_Codes = (
    df_lkp_Codes_alias
    .join(
        df_Ref_CustSvcTskltrReprtCd_Lkup.alias("Ref_CustSvcTskltrReprtCd_Lkup"),
        F.col("lnk_IdsEdwCustSvcTaskLtrExtr_InABC.CUST_SVC_TASK_LTR_REPRT_STTUS_")
        == F.col("Ref_CustSvcTskltrReprtCd_Lkup.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_Ref_CustSvcTskltrTypCd_Lkup.alias("Ref_CustSvcTskltrTypCd_Lkup"),
        F.col("lnk_IdsEdwCustSvcTaskLtrExtr_InABC.CUST_SVC_TASK_LTR_TYP_CD_SK")
        == F.col("Ref_CustSvcTskltrTypCd_Lkup.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_Ref_CustSvcTskltrRefCd_Lkup.alias("Ref_CustSvcTskltrRefCd_Lkup"),
        F.col("lnk_IdsEdwCustSvcTaskLtrExtr_InABC.CUST_SVC_TASK_LTR_REF_ST_CD_SK")
        == F.col("Ref_CustSvcTskltrRefCd_Lkup.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_Ref_CustSvcTskltrRcpntCd_Lkup.alias("Ref_CustSvcTskltrRcpntCd_Lkup"),
        F.col("lnk_IdsEdwCustSvcTaskLtrExtr_InABC.CS_TASK_LTR_RECPNT_ST_CD_SK")
        == F.col("Ref_CustSvcTskltrRcpntCd_Lkup.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_Ref_LastUpdtUserLkup.alias("Ref_LastUpdtUserLkup"),
        F.col("lnk_IdsEdwCustSvcTaskLtrExtr_InABC.LAST_UPDT_USER_SK")
        == F.col("Ref_LastUpdtUserLkup.USER_SK"),
        "left"
    )
    .join(
        df_Ref_CrtByUserLkup.alias("Ref_CrtByUserLkup"),
        F.col("lnk_IdsEdwCustSvcTaskLtrExtr_InABC.CRT_BY_USER_SK")
        == F.col("Ref_CrtByUserLkup.USER_SK"),
        "left"
    )
    .join(
        df_Ref_CustSvcTskltrSendCd_Lkup.alias("Ref_CustSvcTskltrSendCd_Lkup"),
        F.col("lnk_IdsEdwCustSvcTaskLtrExtr_InABC.CUST_SVC_TASK_LTR_SEND_ST_CD_S")
        == F.col("Ref_CustSvcTskltrSendCd_Lkup.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_Ref_CustSvcTskltrstyleCd_Lkup.alias("Ref_CustSvcTskltrstyleCd_Lkup"),
        F.col("lnk_IdsEdwCustSvcTaskLtrExtr_InABC.CUST_SVC_TASK_LTR_STYLE_CD_SK")
        == F.col("Ref_CustSvcTskltrstyleCd_Lkup.CD_MPPNG_SK"),
        "left"
    )
    .select(
        F.col("lnk_IdsEdwCustSvcTaskLtrExtr_InABC.CUST_SVC_TASK_LTR_SK").alias("CUST_SVC_TASK_LTR_SK"),
        F.col("lnk_IdsEdwCustSvcTaskLtrExtr_InABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("lnk_IdsEdwCustSvcTaskLtrExtr_InABC.CUST_SVC_ID").alias("CUST_SVC_ID"),
        F.col("lnk_IdsEdwCustSvcTaskLtrExtr_InABC.TASK_SEQ_NO").alias("CUST_SVC_TASK_SEQ_NO"),
        F.col("Ref_CustSvcTskltrstyleCd_Lkup.TRGT_CD").alias("CUST_SVC_TASK_LTR_STYLE_CD"),
        F.col("lnk_IdsEdwCustSvcTaskLtrExtr_InABC.LTR_SEQ_NO").alias("CUST_SVC_TASK_LTR_SEQ_NO"),
        F.col("lnk_IdsEdwCustSvcTaskLtrExtr_InABC.LTR_DEST_ID").alias("CUST_SVC_TASK_LTR_DEST_ID"),
        F.col("lnk_IdsEdwCustSvcTaskLtrExtr_InABC.CRT_BY_USER_SK").alias("CRT_BY_USER_SK"),
        F.col("lnk_IdsEdwCustSvcTaskLtrExtr_InABC.CUST_SVC_TASK_SK").alias("CUST_SVC_TASK_SK"),
        F.col("lnk_IdsEdwCustSvcTaskLtrExtr_InABC.LAST_UPDT_USER_SK").alias("LAST_UPDT_USER_SK"),
        F.col("Ref_CrtByUserLkup.USER_ID").alias("CRT_BY_USER_ID"),
        F.col("lnk_IdsEdwCustSvcTaskLtrExtr_InABC.CRT_DT_SK").alias("CUST_SVC_TASK_LTR_CRT_DT_SK"),
        F.col("lnk_IdsEdwCustSvcTaskLtrExtr_InABC.EXPL_TX").alias("CUST_SVC_TASK_LTR_EXPL_TX"),
        F.col("lnk_IdsEdwCustSvcTaskLtrExtr_InABC.LAST_UPDT_DT_SK").alias("CS_TASK_LTR_LAST_UPDT_DT_SK"),
        F.col("lnk_IdsEdwCustSvcTaskLtrExtr_InABC.MAILED_DT_SK").alias("CUST_SVC_TASK_LTR_MAILED_DT_SK"),
        F.col("lnk_IdsEdwCustSvcTaskLtrExtr_InABC.MAILED_IN").alias("CUST_SVC_TASK_LTR_MAILED_IN"),
        F.col("lnk_IdsEdwCustSvcTaskLtrExtr_InABC.MSG_TX").alias("CUST_SVC_TASK_LTR_MSG_TX"),
        F.col("lnk_IdsEdwCustSvcTaskLtrExtr_InABC.PRTED_DT_SK").alias("CUST_SVC_TASK_LTR_PRTED_DT_SK"),
        F.col("lnk_IdsEdwCustSvcTaskLtrExtr_InABC.PRTED_IN").alias("CUST_SVC_TASK_LTR_PRTED_IN"),
        F.col("lnk_IdsEdwCustSvcTaskLtrExtr_InABC.RECPNT_NM").alias("CUST_SVC_TASK_LTR_RECPNT_NM"),
        F.col("lnk_IdsEdwCustSvcTaskLtrExtr_InABC.RECPNT_ADDR_LN_1").alias("CS_TASK_LTR_RECPNT_ADDR_LN_1"),
        F.col("lnk_IdsEdwCustSvcTaskLtrExtr_InABC.RECPNT_ADDR_LN_2").alias("CS_TASK_LTR_RECPNT_ADDR_LN_2"),
        F.col("lnk_IdsEdwCustSvcTaskLtrExtr_InABC.RECPNT_ADDR_LN_3").alias("CS_TASK_LTR_RECPNT_ADDR_LN_3"),
        F.col("lnk_IdsEdwCustSvcTaskLtrExtr_InABC.RECPNT_CITY_NM").alias("CS_TASK_LTR_RECPNT_CITY_NM"),
        F.col("Ref_CustSvcTskltrRcpntCd_Lkup.TRGT_CD").alias("CUST_SVC_TASK_LTR_RECPNT_ST_CD"),
        F.col("Ref_CustSvcTskltrRcpntCd_Lkup.TRGT_CD_NM").alias("CUST_SVC_TASK_LTR_RECPNT_ST_NM"),
        F.col("lnk_IdsEdwCustSvcTaskLtrExtr_InABC.RECPNT_POSTAL_CD").alias("CS_TASK_LTR_RECPNT_POSTAL_CD"),
        F.col("lnk_IdsEdwCustSvcTaskLtrExtr_InABC.RECPNT_CNTY_NM").alias("CS_TASK_LTR_RECPNT_CNTY_NM"),
        F.col("lnk_IdsEdwCustSvcTaskLtrExtr_InABC.RECPNT_PHN_NO").alias("CS_TASK_LTR_RECPNT_PHN_NO"),
        F.col("lnk_IdsEdwCustSvcTaskLtrExtr_InABC.RECPNT_PHN_NO_EXT").alias("CS_TASK_LTR_RECPNT_PHN_NO_EXT"),
        F.col("lnk_IdsEdwCustSvcTaskLtrExtr_InABC.RECPNT_FAX_NO").alias("CS_TASK_LTR_RECPNT_FAX_NO"),
        F.col("lnk_IdsEdwCustSvcTaskLtrExtr_InABC.REF_NM").alias("CUST_SVC_TASK_LTR_REF_NM"),
        F.col("lnk_IdsEdwCustSvcTaskLtrExtr_InABC.REF_ADDR_LN_1").alias("CS_TASK_LTR_REF_ADDR_LN_1"),
        F.col("lnk_IdsEdwCustSvcTaskLtrExtr_InABC.REF_ADDR_LN_2").alias("CS_TASK_LTR_REF_ADDR_LN_2"),
        F.col("lnk_IdsEdwCustSvcTaskLtrExtr_InABC.REF_ADDR_LN_3").alias("CS_TASK_LTR_REF_ADDR_LN_3"),
        F.col("lnk_IdsEdwCustSvcTaskLtrExtr_InABC.REF_CITY_NM").alias("CUST_SVC_TASK_LTR_REF_CITY_NM"),
        F.col("Ref_CustSvcTskltrRefCd_Lkup.TRGT_CD").alias("CUST_SVC_TASK_LTR_REF_ST_CD"),
        F.col("Ref_CustSvcTskltrRefCd_Lkup.TRGT_CD_NM").alias("CUST_SVC_TASK_LTR_REF_ST_NM"),
        F.col("lnk_IdsEdwCustSvcTaskLtrExtr_InABC.REF_POSTAL_CD").alias("CS_TASK_LTR_REF_POSTAL_CD"),
        F.col("lnk_IdsEdwCustSvcTaskLtrExtr_InABC.REF_CNTY_NM").alias("CUST_SVC_TASK_LTR_REF_CNTY_NM"),
        F.col("lnk_IdsEdwCustSvcTaskLtrExtr_InABC.REF_PHN_NO").alias("CUST_SVC_TASK_LTR_REF_PHN_NO"),
        F.col("lnk_IdsEdwCustSvcTaskLtrExtr_InABC.REF_PHN_NO_EXT").alias("CS_TASK_LTR_REF_PHN_NO_EXT"),
        F.col("lnk_IdsEdwCustSvcTaskLtrExtr_InABC.REF_FAX_NO").alias("CUST_SVC_TASK_LTR_REF_FAX_NO"),
        F.col("Ref_CustSvcTskltrReprtCd_Lkup.TRGT_CD").alias("CS_TASK_LTR_REPRT_STTUS_CD"),
        F.col("Ref_CustSvcTskltrReprtCd_Lkup.TRGT_CD_NM").alias("CS_TASK_LTR_REPRT_STTUS_NM"),
        F.col("lnk_IdsEdwCustSvcTaskLtrExtr_InABC.RQST_DT_SK").alias("CUST_SVC_TASK_LTR_RQST_DT_SK"),
        F.col("lnk_IdsEdwCustSvcTaskLtrExtr_InABC.RQST_IN").alias("CUST_SVC_TASK_LTR_RQST_IN"),
        F.col("lnk_IdsEdwCustSvcTaskLtrExtr_InABC.SEND_NM").alias("CUST_SVC_TASK_LTR_SEND_NM"),
        F.col("lnk_IdsEdwCustSvcTaskLtrExtr_InABC.SEND_ADDR_LN_1").alias("CS_TASK_LTR_SEND_ADDR_LN_1"),
        F.col("lnk_IdsEdwCustSvcTaskLtrExtr_InABC.SEND_ADDR_LN_2").alias("CS_TASK_LTR_SEND_ADDR_LN_2"),
        F.col("lnk_IdsEdwCustSvcTaskLtrExtr_InABC.SEND_ADDR_LN_3").alias("CS_TASK_LTR_SEND_ADDR_LN_3"),
        F.col("lnk_IdsEdwCustSvcTaskLtrExtr_InABC.SEND_CITY_NM").alias("CUST_SVC_TASK_LTR_SEND_CITY_NM"),
        F.col("Ref_CustSvcTskltrSendCd_Lkup.TRGT_CD").alias("CUST_SVC_TASK_LTR_SEND_ST_CD"),
        F.col("Ref_CustSvcTskltrSendCd_Lkup.TRGT_CD_NM").alias("CUST_SVC_TASK_LTR_SEND_ST_NM"),
        F.col("lnk_IdsEdwCustSvcTaskLtrExtr_InABC.SEND_POSTAL_CD").alias("CS_TASK_LTR_SEND_POSTAL_CD"),
        F.col("lnk_IdsEdwCustSvcTaskLtrExtr_InABC.SEND_CNTY_NM").alias("CUST_SVC_TASK_LTR_SEND_CNTY_NM"),
        F.col("lnk_IdsEdwCustSvcTaskLtrExtr_InABC.SEND_PHN_NO").alias("CUST_SVC_TASK_LTR_SEND_PHN_NO"),
        F.col("lnk_IdsEdwCustSvcTaskLtrExtr_InABC.SEND_PHN_NO_EXT").alias("CS_TASK_LTR_SEND_PHN_NO_EXT"),
        F.col("lnk_IdsEdwCustSvcTaskLtrExtr_InABC.SEND_FAX_NO").alias("CUST_SVC_TASK_LTR_SEND_FAX_NO"),
        F.col("lnk_IdsEdwCustSvcTaskLtrExtr_InABC.SUBMT_DT_SK").alias("CUST_SVC_TASK_LTR_SUBMT_DT_SK"),
        F.col("lnk_IdsEdwCustSvcTaskLtrExtr_InABC.SUBMT_IN").alias("CUST_SVC_TASK_LTR_SUBMT_IN"),
        F.col("Ref_CustSvcTskltrstyleCd_Lkup.TRGT_CD_NM").alias("CUST_SVC_TASK_LTR_STYLE_NM"),
        F.col("lnk_IdsEdwCustSvcTaskLtrExtr_InABC.LTR_TX_1").alias("CUST_SVC_TASK_LTR_TX_1"),
        F.col("lnk_IdsEdwCustSvcTaskLtrExtr_InABC.LTR_TX_2").alias("CUST_SVC_TASK_LTR_TX_2"),
        F.col("lnk_IdsEdwCustSvcTaskLtrExtr_InABC.LTR_TX_3").alias("CUST_SVC_TASK_LTR_TX_3"),
        F.col("lnk_IdsEdwCustSvcTaskLtrExtr_InABC.LTR_TX_4").alias("CUST_SVC_TASK_LTR_TX_4"),
        F.col("Ref_CustSvcTskltrTypCd_Lkup.TRGT_CD").alias("CUST_SVC_TASK_LTR_TYP_CD"),
        F.col("Ref_CustSvcTskltrTypCd_Lkup.TRGT_CD_NM").alias("CUST_SVC_TASK_LTR_TYP_NM"),
        F.col("Ref_LastUpdtUserLkup.USER_ID").alias("LAST_UPDT_USER_ID"),
        F.col("lnk_IdsEdwCustSvcTaskLtrExtr_InABC.CS_TASK_LTR_RECPNT_ST_CD_SK").alias("CS_TASK_LTR_RECPNT_ST_CD_SK"),
        F.col("lnk_IdsEdwCustSvcTaskLtrExtr_InABC.CUST_SVC_TASK_LTR_REF_ST_CD_SK").alias("CUST_SVC_TASK_LTR_REF_ST_CD_SK"),
        F.col("lnk_IdsEdwCustSvcTaskLtrExtr_InABC.CUST_SVC_TASK_LTR_REPRT_STTUS_").alias("CS_TASK_LTR_REPRT_STTUS_CD_SK"),
        F.col("lnk_IdsEdwCustSvcTaskLtrExtr_InABC.CUST_SVC_TASK_LTR_SEND_ST_CD_S").alias("CS_TASK_LTR_SEND_ST_CD_SK"),
        F.col("lnk_IdsEdwCustSvcTaskLtrExtr_InABC.CUST_SVC_TASK_LTR_STYLE_CD_SK").alias("CUST_SVC_TASK_LTR_STYLE_CD_SK"),
        F.col("lnk_IdsEdwCustSvcTaskLtrExtr_InABC.CUST_SVC_TASK_LTR_TYP_CD_SK").alias("CUST_SVC_TASK_LTR_TYP_CD_SK")
    )
)

df_xfrm_BusinessLogic = df_lkp_Codes

df_xfrm_BusinessLogic = df_xfrm_BusinessLogic.withColumn(
    "CUST_SVC_TASK_LTR_STYLE_CD",
    F.when(F.trim(F.col("CUST_SVC_TASK_LTR_STYLE_CD")) == "", F.lit("NA")).otherwise(F.col("CUST_SVC_TASK_LTR_STYLE_CD"))
).withColumn(
    "CRT_BY_USER_ID",
    F.when(F.col("CRT_BY_USER_ID").isNull() | (F.trim(F.col("CRT_BY_USER_ID")) == ""), F.lit("NA")).otherwise(F.col("CRT_BY_USER_ID"))
).withColumn(
    "CUST_SVC_TASK_LTR_RECPNT_ST_CD",
    F.when(F.trim(F.col("CUST_SVC_TASK_LTR_RECPNT_ST_CD")) == "", F.lit("NA")).otherwise(F.col("CUST_SVC_TASK_LTR_RECPNT_ST_CD"))
).withColumn(
    "CUST_SVC_TASK_LTR_REF_ST_CD",
    F.when(F.trim(F.col("CUST_SVC_TASK_LTR_REF_ST_CD")) == "", F.lit("NA")).otherwise(F.col("CUST_SVC_TASK_LTR_REF_ST_CD"))
).withColumn(
    "CUST_SVC_TASK_LTR_REF_ST_NM",
    F.when(F.trim(F.col("CUST_SVC_TASK_LTR_REF_ST_NM")) == "", F.lit("NA")).otherwise(F.col("CUST_SVC_TASK_LTR_REF_ST_NM"))
).withColumn(
    "CS_TASK_LTR_REPRT_STTUS_CD",
    F.when(F.trim(F.col("CS_TASK_LTR_REPRT_STTUS_CD")) == "", F.lit("NA")).otherwise(F.col("CS_TASK_LTR_REPRT_STTUS_CD"))
).withColumn(
    "CS_TASK_LTR_REPRT_STTUS_NM",
    F.when(F.trim(F.col("CS_TASK_LTR_REPRT_STTUS_NM")) == "", F.lit("NA")).otherwise(F.col("CS_TASK_LTR_REPRT_STTUS_NM"))
).withColumn(
    "CUST_SVC_TASK_LTR_SEND_ST_CD",
    F.when(F.trim(F.col("CUST_SVC_TASK_LTR_SEND_ST_CD")) == "", F.lit("NA")).otherwise(F.col("CUST_SVC_TASK_LTR_SEND_ST_CD"))
).withColumn(
    "CUST_SVC_TASK_LTR_SEND_ST_NM",
    F.when(F.trim(F.col("CUST_SVC_TASK_LTR_SEND_ST_NM")) == "", F.lit("NA")).otherwise(F.col("CUST_SVC_TASK_LTR_SEND_ST_NM"))
).withColumn(
    "CUST_SVC_TASK_LTR_STYLE_NM",
    F.when(F.trim(F.col("CUST_SVC_TASK_LTR_STYLE_NM")) == "", F.lit("NA")).otherwise(F.col("CUST_SVC_TASK_LTR_STYLE_NM"))
).withColumn(
    "CUST_SVC_TASK_LTR_TYP_CD",
    F.when(F.trim(F.col("CUST_SVC_TASK_LTR_TYP_CD")) == "", F.lit("NA")).otherwise(F.col("CUST_SVC_TASK_LTR_TYP_CD"))
).withColumn(
    "CUST_SVC_TASK_LTR_TYP_NM",
    F.when(F.trim(F.col("CUST_SVC_TASK_LTR_TYP_NM")) == "", F.lit("NA")).otherwise(F.col("CUST_SVC_TASK_LTR_TYP_NM"))
).withColumn(
    "LAST_UPDT_USER_ID",
    F.when(F.col("LAST_UPDT_USER_ID").isNull() | (F.trim(F.col("LAST_UPDT_USER_ID")) == ""), F.lit("NA")).otherwise(F.col("LAST_UPDT_USER_ID"))
)

df_main = df_xfrm_BusinessLogic.filter(
    (F.col("CUST_SVC_TASK_LTR_SK") != 0) & (F.col("CUST_SVC_TASK_LTR_SK") != 1)
)

NA_values = {
    "CUST_SVC_TASK_LTR_SK": "1",
    "SRC_SYS_CD": "NA",
    "CUST_SVC_ID": "NA",
    "CUST_SVC_TASK_SEQ_NO": "0",
    "CUST_SVC_TASK_LTR_STYLE_CD": "NA",
    "CUST_SVC_TASK_LTR_SEQ_NO": "0",
    "CUST_SVC_TASK_LTR_DEST_ID": "NA",
    "CRT_RUN_CYC_EXCTN_DT_SK": "1753-01-01",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK": "1753-01-01",
    "CRT_BY_USER_SK": "1",
    "CUST_SVC_TASK_SK": "1",
    "LAST_UPDT_USER_SK": "1",
    "CRT_BY_USER_ID": "NA",
    "CUST_SVC_TASK_LTR_CRT_DT_SK": "1753-01-01",
    "CUST_SVC_TASK_LTR_EXPL_TX": "",
    "CS_TASK_LTR_LAST_UPDT_DT_SK": "1753-01-01",
    "CUST_SVC_TASK_LTR_MAILED_DT_SK": "1753-01-01",
    "CUST_SVC_TASK_LTR_MAILED_IN": "N",
    "CUST_SVC_TASK_LTR_MSG_TX": "",
    "CUST_SVC_TASK_LTR_PRTED_DT_SK": "1753-01-01",
    "CUST_SVC_TASK_LTR_PRTED_IN": "N",
    "CUST_SVC_TASK_LTR_RECPNT_NM": "NA",
    "CS_TASK_LTR_RECPNT_ADDR_LN_1": "",
    "CS_TASK_LTR_RECPNT_ADDR_LN_2": "",
    "CS_TASK_LTR_RECPNT_ADDR_LN_3": "",
    "CS_TASK_LTR_RECPNT_CITY_NM": "NA",
    "CUST_SVC_TASK_LTR_RECPNT_ST_CD": "NA",
    "CUST_SVC_TASK_LTR_RECPNT_ST_NM": "NA",
    "CS_TASK_LTR_RECPNT_POSTAL_CD": "NA",
    "CS_TASK_LTR_RECPNT_CNTY_NM": "NA",
    "CS_TASK_LTR_RECPNT_PHN_NO": "",
    "CS_TASK_LTR_RECPNT_PHN_NO_EXT": "",
    "CS_TASK_LTR_RECPNT_FAX_NO": "",
    "CUST_SVC_TASK_LTR_REF_NM": "NA",
    "CS_TASK_LTR_REF_ADDR_LN_1": "",
    "CS_TASK_LTR_REF_ADDR_LN_2": "",
    "CS_TASK_LTR_REF_ADDR_LN_3": "",
    "CUST_SVC_TASK_LTR_REF_CITY_NM": "NA",
    "CUST_SVC_TASK_LTR_REF_ST_CD": "NA",
    "CUST_SVC_TASK_LTR_REF_ST_NM": "NA",
    "CS_TASK_LTR_REF_POSTAL_CD": "NA",
    "CUST_SVC_TASK_LTR_REF_CNTY_NM": "NA",
    "CUST_SVC_TASK_LTR_REF_PHN_NO": "",
    "CS_TASK_LTR_REF_PHN_NO_EXT": "",
    "CUST_SVC_TASK_LTR_REF_FAX_NO": "",
    "CS_TASK_LTR_REPRT_STTUS_CD": "NA",
    "CS_TASK_LTR_REPRT_STTUS_NM": "NA",
    "CUST_SVC_TASK_LTR_RQST_DT_SK": "1753-01-01",
    "CUST_SVC_TASK_LTR_RQST_IN": "N",
    "CUST_SVC_TASK_LTR_SEND_NM": "NA",
    "CS_TASK_LTR_SEND_ADDR_LN_1": "",
    "CS_TASK_LTR_SEND_ADDR_LN_2": "",
    "CS_TASK_LTR_SEND_ADDR_LN_3": "",
    "CUST_SVC_TASK_LTR_SEND_CITY_NM": "NA",
    "CUST_SVC_TASK_LTR_SEND_ST_CD": "NA",
    "CUST_SVC_TASK_LTR_SEND_ST_NM": "NA",
    "CS_TASK_LTR_SEND_POSTAL_CD": "NA",
    "CUST_SVC_TASK_LTR_SEND_CNTY_NM": "NA",
    "CUST_SVC_TASK_LTR_SEND_PHN_NO": "",
    "CS_TASK_LTR_SEND_PHN_NO_EXT": "",
    "CUST_SVC_TASK_LTR_SEND_FAX_NO": "",
    "CUST_SVC_TASK_LTR_SUBMT_DT_SK": "1753-01-01",
    "CUST_SVC_TASK_LTR_SUBMT_IN": "N",
    "CUST_SVC_TASK_LTR_STYLE_NM": "NA",
    "CUST_SVC_TASK_LTR_TX_1": "",
    "CUST_SVC_TASK_LTR_TX_2": "",
    "CUST_SVC_TASK_LTR_TX_3": "",
    "CUST_SVC_TASK_LTR_TX_4": "",
    "CUST_SVC_TASK_LTR_TYP_CD": "NA",
    "CUST_SVC_TASK_LTR_TYP_NM": "NA",
    "LAST_UPDT_USER_ID": "NA",
    "CRT_RUN_CYC_EXCTN_SK": "100",
    "LAST_UPDT_RUN_CYC_EXCTN_SK": "100",
    "CS_TASK_LTR_RECPNT_ST_CD_SK": "1",
    "CUST_SVC_TASK_LTR_REF_ST_CD_SK": "1",
    "CS_TASK_LTR_REPRT_STTUS_CD_SK": "1",
    "CS_TASK_LTR_SEND_ST_CD_SK": "1",
    "CUST_SVC_TASK_LTR_STYLE_CD_SK": "1",
    "CUST_SVC_TASK_LTR_TYP_CD_SK": "1"
}

UNK_values = {
    "CUST_SVC_TASK_LTR_SK": "0",
    "SRC_SYS_CD": "UNK",
    "CUST_SVC_ID": "UNK",
    "CUST_SVC_TASK_SEQ_NO": "0",
    "CUST_SVC_TASK_LTR_STYLE_CD": "UNK",
    "CUST_SVC_TASK_LTR_SEQ_NO": "0",
    "CUST_SVC_TASK_LTR_DEST_ID": "UNK",
    "CRT_RUN_CYC_EXCTN_DT_SK": "1753-01-01",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK": "1753-01-01",
    "CRT_BY_USER_SK": "0",
    "CUST_SVC_TASK_SK": "0",
    "LAST_UPDT_USER_SK": "0",
    "CRT_BY_USER_ID": "UNK",
    "CUST_SVC_TASK_LTR_CRT_DT_SK": "1753-01-01",
    "CUST_SVC_TASK_LTR_EXPL_TX": "",
    "CS_TASK_LTR_LAST_UPDT_DT_SK": "1753-01-01",
    "CUST_SVC_TASK_LTR_MAILED_DT_SK": "1753-01-01",
    "CUST_SVC_TASK_LTR_MAILED_IN": "N",
    "CUST_SVC_TASK_LTR_MSG_TX": "",
    "CUST_SVC_TASK_LTR_PRTED_DT_SK": "1753-01-01",
    "CUST_SVC_TASK_LTR_PRTED_IN": "N",
    "CUST_SVC_TASK_LTR_RECPNT_NM": "UNK",
    "CS_TASK_LTR_RECPNT_ADDR_LN_1": "",
    "CS_TASK_LTR_RECPNT_ADDR_LN_2": "",
    "CS_TASK_LTR_RECPNT_ADDR_LN_3": "",
    "CS_TASK_LTR_RECPNT_CITY_NM": "UNK",
    "CUST_SVC_TASK_LTR_RECPNT_ST_CD": "UNK",
    "CUST_SVC_TASK_LTR_RECPNT_ST_NM": "UNK",
    "CS_TASK_LTR_RECPNT_POSTAL_CD": "UNK",
    "CS_TASK_LTR_RECPNT_CNTY_NM": "UNK",
    "CS_TASK_LTR_RECPNT_PHN_NO": "",
    "CS_TASK_LTR_RECPNT_PHN_NO_EXT": "",
    "CS_TASK_LTR_RECPNT_FAX_NO": "",
    "CUST_SVC_TASK_LTR_REF_NM": "UNK",
    "CS_TASK_LTR_REF_ADDR_LN_1": "",
    "CS_TASK_LTR_REF_ADDR_LN_2": "",
    "CS_TASK_LTR_REF_ADDR_LN_3": "",
    "CUST_SVC_TASK_LTR_REF_CITY_NM": "UNK",
    "CUST_SVC_TASK_LTR_REF_ST_CD": "UNK",
    "CUST_SVC_TASK_LTR_REF_ST_NM": "UNK",
    "CS_TASK_LTR_REF_POSTAL_CD": "UNK",
    "CUST_SVC_TASK_LTR_REF_CNTY_NM": "UNK",
    "CUST_SVC_TASK_LTR_REF_PHN_NO": "",
    "CS_TASK_LTR_REF_PHN_NO_EXT": "",
    "CUST_SVC_TASK_LTR_REF_FAX_NO": "",
    "CS_TASK_LTR_REPRT_STTUS_CD": "UNK",
    "CS_TASK_LTR_REPRT_STTUS_NM": "UNK",
    "CUST_SVC_TASK_LTR_RQST_DT_SK": "1753-01-01",
    "CUST_SVC_TASK_LTR_RQST_IN": "N",
    "CUST_SVC_TASK_LTR_SEND_NM": "UNK",
    "CS_TASK_LTR_SEND_ADDR_LN_1": "",
    "CS_TASK_LTR_SEND_ADDR_LN_2": "",
    "CS_TASK_LTR_SEND_ADDR_LN_3": "",
    "CUST_SVC_TASK_LTR_SEND_CITY_NM": "UNK",
    "CUST_SVC_TASK_LTR_SEND_ST_CD": "UNK",
    "CUST_SVC_TASK_LTR_SEND_ST_NM": "UNK",
    "CS_TASK_LTR_SEND_POSTAL_CD": "UNK",
    "CUST_SVC_TASK_LTR_SEND_CNTY_NM": "UNK",
    "CUST_SVC_TASK_LTR_SEND_PHN_NO": "",
    "CS_TASK_LTR_SEND_PHN_NO_EXT": "",
    "CUST_SVC_TASK_LTR_SEND_FAX_NO": "",
    "CUST_SVC_TASK_LTR_SUBMT_DT_SK": "1753-01-01",
    "CUST_SVC_TASK_LTR_SUBMT_IN": "N",
    "CUST_SVC_TASK_LTR_STYLE_NM": "UNK",
    "CUST_SVC_TASK_LTR_TX_1": "",
    "CUST_SVC_TASK_LTR_TX_2": "",
    "CUST_SVC_TASK_LTR_TX_3": "",
    "CUST_SVC_TASK_LTR_TX_4": "",
    "CUST_SVC_TASK_LTR_TYP_CD": "UNK",
    "CUST_SVC_TASK_LTR_TYP_NM": "UNK",
    "LAST_UPDT_USER_ID": "UNK",
    "CRT_RUN_CYC_EXCTN_SK": "100",
    "LAST_UPDT_RUN_CYC_EXCTN_SK": "100",
    "CS_TASK_LTR_RECPNT_ST_CD_SK": "0",
    "CUST_SVC_TASK_LTR_REF_ST_CD_SK": "0",
    "CS_TASK_LTR_REPRT_STTUS_CD_SK": "0",
    "CS_TASK_LTR_SEND_ST_CD_SK": "0",
    "CUST_SVC_TASK_LTR_STYLE_CD_SK": "0",
    "CUST_SVC_TASK_LTR_TYP_CD_SK": "0"
}

output_columns_order = [
    "CUST_SVC_TASK_LTR_SK","SRC_SYS_CD","CUST_SVC_ID","CUST_SVC_TASK_SEQ_NO","CUST_SVC_TASK_LTR_STYLE_CD",
    "CUST_SVC_TASK_LTR_SEQ_NO","CUST_SVC_TASK_LTR_DEST_ID","CRT_RUN_CYC_EXCTN_DT_SK","LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "CRT_BY_USER_SK","CUST_SVC_TASK_SK","LAST_UPDT_USER_SK","CRT_BY_USER_ID","CUST_SVC_TASK_LTR_CRT_DT_SK",
    "CUST_SVC_TASK_LTR_EXPL_TX","CS_TASK_LTR_LAST_UPDT_DT_SK","CUST_SVC_TASK_LTR_MAILED_DT_SK","CUST_SVC_TASK_LTR_MAILED_IN",
    "CUST_SVC_TASK_LTR_MSG_TX","CUST_SVC_TASK_LTR_PRTED_DT_SK","CUST_SVC_TASK_LTR_PRTED_IN","CUST_SVC_TASK_LTR_RECPNT_NM",
    "CS_TASK_LTR_RECPNT_ADDR_LN_1","CS_TASK_LTR_RECPNT_ADDR_LN_2","CS_TASK_LTR_RECPNT_ADDR_LN_3","CS_TASK_LTR_RECPNT_CITY_NM",
    "CUST_SVC_TASK_LTR_RECPNT_ST_CD","CUST_SVC_TASK_LTR_RECPNT_ST_NM","CS_TASK_LTR_RECPNT_POSTAL_CD","CS_TASK_LTR_RECPNT_CNTY_NM",
    "CS_TASK_LTR_RECPNT_PHN_NO","CS_TASK_LTR_RECPNT_PHN_NO_EXT","CS_TASK_LTR_RECPNT_FAX_NO","CUST_SVC_TASK_LTR_REF_NM",
    "CS_TASK_LTR_REF_ADDR_LN_1","CS_TASK_LTR_REF_ADDR_LN_2","CS_TASK_LTR_REF_ADDR_LN_3","CUST_SVC_TASK_LTR_REF_CITY_NM",
    "CUST_SVC_TASK_LTR_REF_ST_CD","CUST_SVC_TASK_LTR_REF_ST_NM","CS_TASK_LTR_REF_POSTAL_CD","CUST_SVC_TASK_LTR_REF_CNTY_NM",
    "CUST_SVC_TASK_LTR_REF_PHN_NO","CS_TASK_LTR_REF_PHN_NO_EXT","CUST_SVC_TASK_LTR_REF_FAX_NO","CS_TASK_LTR_REPRT_STTUS_CD",
    "CS_TASK_LTR_REPRT_STTUS_NM","CUST_SVC_TASK_LTR_RQST_DT_SK","CUST_SVC_TASK_LTR_RQST_IN","CUST_SVC_TASK_LTR_SEND_NM",
    "CS_TASK_LTR_SEND_ADDR_LN_1","CS_TASK_LTR_SEND_ADDR_LN_2","CS_TASK_LTR_SEND_ADDR_LN_3","CUST_SVC_TASK_LTR_SEND_CITY_NM",
    "CUST_SVC_TASK_LTR_SEND_ST_CD","CUST_SVC_TASK_LTR_SEND_ST_NM","CS_TASK_LTR_SEND_POSTAL_CD","CUST_SVC_TASK_LTR_SEND_CNTY_NM",
    "CUST_SVC_TASK_LTR_SEND_PHN_NO","CS_TASK_LTR_SEND_PHN_NO_EXT","CUST_SVC_TASK_LTR_SEND_FAX_NO","CUST_SVC_TASK_LTR_SUBMT_DT_SK",
    "CUST_SVC_TASK_LTR_SUBMT_IN","CUST_SVC_TASK_LTR_STYLE_NM","CUST_SVC_TASK_LTR_TX_1","CUST_SVC_TASK_LTR_TX_2","CUST_SVC_TASK_LTR_TX_3",
    "CUST_SVC_TASK_LTR_TX_4","CUST_SVC_TASK_LTR_TYP_CD","CUST_SVC_TASK_LTR_TYP_NM","LAST_UPDT_USER_ID","CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK","CS_TASK_LTR_RECPNT_ST_CD_SK","CUST_SVC_TASK_LTR_REF_ST_CD_SK","CS_TASK_LTR_REPRT_STTUS_CD_SK",
    "CS_TASK_LTR_SEND_ST_CD_SK","CUST_SVC_TASK_LTR_STYLE_CD_SK","CUST_SVC_TASK_LTR_TYP_CD_SK"
]

NA_schema = []
for c in output_columns_order:
    NA_schema.append(StructField(c, StringType(), True))
df_NA = spark.createDataFrame([[NA_values.get(col, "") for col in output_columns_order]], StructType(NA_schema))

UNK_schema = []
for c in output_columns_order:
    UNK_schema.append(StructField(c, StringType(), True))
df_UNK = spark.createDataFrame([[UNK_values.get(col, "") for col in output_columns_order]], StructType(UNK_schema))

df_fnl_NA_UNK = df_main.select(output_columns_order).unionByName(df_NA.select(output_columns_order)).unionByName(df_UNK.select(output_columns_order))

df_final = df_fnl_NA_UNK

char_columns_and_lengths = {
    "CRT_RUN_CYC_EXCTN_DT_SK": 10,
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK": 10,
    "CUST_SVC_TASK_LTR_CRT_DT_SK": 10,
    "CS_TASK_LTR_LAST_UPDT_DT_SK": 10,
    "CUST_SVC_TASK_LTR_MAILED_DT_SK": 10,
    "CUST_SVC_TASK_LTR_PRTED_DT_SK": 10,
    "CS_TASK_LTR_RECPNT_PHN_NO_EXT": 5,
    "CS_TASK_LTR_REF_PHN_NO_EXT": 5,
    "CUST_SVC_TASK_LTR_RQST_DT_SK": 10,
    "CUST_SVC_TASK_LTR_RQST_IN": 1,
    "CUST_SVC_TASK_LTR_SEND_ST_CD":  None,  # Not actually labeled as char with length in the final stage
    "CS_TASK_LTR_SEND_PHN_NO_EXT": 5,
    "CUST_SVC_TASK_LTR_SUBMT_DT_SK": 10,
    "CUST_SVC_TASK_LTR_SUBMT_IN": 1,
    "CUST_SVC_TASK_LTR_STYLE_NM": None,  # No explicit length
    "CUST_SVC_TASK_LTR_TYP_CD": None,    # No explicit length
    "CRT_RUN_CYC_EXCTN_SK": None,
    "LAST_UPDT_RUN_CYC_EXCTN_SK": None
}

# The funnel metadata specifically identifies some columns as char with length=10 or 5 or 1.
# Ensure we rpad only those columns with a numeric length.
char_columns_and_lengths_explicit = {
    "CRT_RUN_CYC_EXCTN_DT_SK": 10,
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK": 10,
    "CUST_SVC_TASK_LTR_CRT_DT_SK": 10,
    "CS_TASK_LTR_LAST_UPDT_DT_SK": 10,
    "CUST_SVC_TASK_LTR_MAILED_DT_SK": 10,
    "CUST_SVC_TASK_LTR_MAILED_IN": 1,
    "CUST_SVC_TASK_LTR_PRTED_DT_SK": 10,
    "CUST_SVC_TASK_LTR_PRTED_IN": 1,
    "CS_TASK_LTR_RECPNT_PHN_NO_EXT": 5,
    "CS_TASK_LTR_REF_PHN_NO_EXT": 5,
    "CUST_SVC_TASK_LTR_RQST_DT_SK": 10,
    "CUST_SVC_TASK_LTR_RQST_IN": 1,
    "CS_TASK_LTR_SEND_PHN_NO_EXT": 5,
    "CUST_SVC_TASK_LTR_SUBMT_DT_SK": 10,
    "CUST_SVC_TASK_LTR_SUBMT_IN": 1
}

for col_name, length in char_columns_and_lengths_explicit.items():
    df_final = df_final.withColumn(col_name, F.rpad(F.col(col_name), length, " "))

df_final = df_final.select(output_columns_order)

write_files(
    df_final,
    f"{adls_path}/load/CUST_SVC_TASK_LTR_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)