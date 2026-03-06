# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date              Project/Altiris #               Change Description                                                                                                                   Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------     -----------------------------------    ---------------------------------------------------------                                                                                        ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Bhoomi Dasari                2/20/2007        Cust Svc/3028               Originally Programmed                                                                                                                devlEDW10
# MAGIC Bhoomi Dasari                01/27/2009      IAD Prod Supp/15         Added new Cd_mppng lkup  to add new logic to TASK_AGE                                                   devlEDW                       Steph Goddard              02/02/2009
# MAGIC SAndrew                        2009-05-26        TTR524                         Added MBR to the sql in extract called Age_0_Tasks to get the                                               devlEDW                       Steph Goddard              05/28/2009
# MAGIC                                                                                                         those Customer Service tasks that are tied to type MBR. 
# MAGIC                                                                                                        Condensed last two transformers into one - all were deriving the MTM_TASK_Age field.  
# MAGIC                                                                                                        Now using stage variables to do so
# MAGIC 
# MAGIC  Bhupinder Kaur          11/25/2013        5114                               Create Load File for EDW Table CUST_SVC_TASK_F                                                            EnterpriseWhseDevl           Jag Yelavarthi            2014-01-29

# MAGIC INPT_DTM  is converted from timestamp to date
# MAGIC Data extracted from IDS table: CD_MPPNG
# MAGIC Write CUST_SVC_TASK_F Data into a Sequential file for Load Job IdsEdwCustSvcTaskFLoad.
# MAGIC Code SK lookups for Denormalization
# MAGIC Lookup Keys:
# MAGIC CUST_SVC_TASK_SK
# MAGIC SRC_SYS_CD_SK
# MAGIC GRP_SK
# MAGIC PROD_SK
# MAGIC CUST_SVC_SK
# MAGIC CUST_SVC_TASK_CAT_CD_SK
# MAGIC CS_TASK_CLS_PLN_PROD_CAT_CD_SK
# MAGIC Job: IdsEdwCustSvcTaskFExtr
# MAGIC Add Defaults and Null Handling.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


# MAGIC %run ../../../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

# Parameter retrieval
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')

# db2_GRP_in: DB2ConnectorPX
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query_db2_GRP_in = f"""
SELECT 
GRP.GRP_SK,
GRP.GRP_ID
FROM {IDSOwner}.GRP GRP
"""
df_db2_GRP_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_GRP_in)
    .load()
)

# db2_PROD_in: DB2ConnectorPX
extract_query_db2_PROD_in = f"""
SELECT DISTINCT
PROD.PROD_SK,
PROD.PROD_ID
FROM 
{IDSOwner}.CUST_SVC_TASK TASK,
{IDSOwner}.PROD PROD,
{IDSOwner}.W_CUST_SVC_DRVR DRVR
WHERE 
TASK.PROD_SK = PROD.PROD_SK
AND TASK.SRC_SYS_CD_SK = DRVR.SRC_SYS_CD_SK 
AND TASK.CUST_SVC_ID       = DRVR.CUST_SVC_ID
"""
df_db2_PROD_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_PROD_in)
    .load()
)

# db2_CustSvcTask_in
extract_query_db2_CustSvcTask_in = f"""
SELECT
TASK.CUST_SVC_TASK_SK,
TASK.CUST_SVC_TASK_PG_TYP_CD_SK,
TASK.RCVD_DT_SK,
MPPNG.TRGT_CD
FROM
{IDSOwner}.CUST_SVC_TASK TASK,
{IDSOwner}.CD_MPPNG MPPNG
WHERE
TASK.CUST_SVC_TASK_PG_TYP_CD_SK = MPPNG.CD_MPPNG_SK 
AND (MPPNG.TRGT_CD = 'IDCARD' OR MPPNG.TRGT_CD = 'MBR')
AND TASK.RCVD_DT_SK = '1753-01-01'
"""
df_db2_CustSvcTask_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CustSvcTask_in)
    .load()
)

# db2_CUST_SVC_in
extract_query_db2_CUST_SVC_in = f"""
SELECT 
SVC.CUST_SVC_SK,
CD_MPPNG.SRC_CD as CNTCT_SRC_CD,
CD_MPPNG2.SRC_CD as METH_SRC_CD
FROM 
{IDSOwner}.CUST_SVC SVC, 
{IDSOwner}.CD_MPPNG CD_MPPNG,
{IDSOwner}.CD_MPPNG CD_MPPNG2,
{IDSOwner}.W_CUST_SVC_DRVR DRVR
WHERE
SVC.CUST_SVC_CNTCT_RELSHP_CD_SK = CD_MPPNG.CD_MPPNG_SK
AND SVC.CUST_SVC_METH_CD_SK = CD_MPPNG2.CD_MPPNG_SK 
AND SVC.SRC_SYS_CD_SK = DRVR.SRC_SYS_CD_SK 
AND SVC.CUST_SVC_ID       = DRVR.CUST_SVC_ID
"""
df_db2_CUST_SVC_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CUST_SVC_in)
    .load()
)

# db2_CDMA_Codes_in
extract_query_db2_CDMA_Codes_in = f"""
SELECT
CD_MPPNG_SK,
COALESCE(TRGT_CD,'UNK') TRGT_CD,
COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM,
SRC_CD
FROM {IDSOwner}.CD_MPPNG
"""
df_db2_CDMA_Codes_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CDMA_Codes_in)
    .load()
)

# cpy_Cd_Mppng (PxCopy)
df_cpy_Cd_Mppng_ref_SrcSysCd = df_db2_CDMA_Codes_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_cpy_Cd_Mppng_ref_CustSvcTaskCatCdLkup = df_db2_CDMA_Codes_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("SRC_CD").alias("SRC_CD")
)

df_cpy_Cd_Mppng_ref_CsTaskClsPlnProdCatCdLkup = df_db2_CDMA_Codes_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("SRC_CD").alias("SRC_CD")
)

# db2_CUST_SVC_TASK_in
extract_query_db2_CUST_SVC_TASK_in = f"""
SELECT 
TASK.CUST_SVC_TASK_SK,
TASK.SRC_SYS_CD_SK,
TASK.CUST_SVC_ID,
TASK.TASK_SEQ_NO,
TASK.CRT_RUN_CYC_EXCTN_SK,
TASK.GRP_SK,
TASK.SUB_SK,
TASK.CLSD_DT_SK,
TASK.INPT_DTM,
TASK.RCVD_DT_SK,
TASK.CUST_SVC_SK,
TASK.PROD_SK,
TASK.CUST_SVC_TASK_CAT_CD_SK,
TASK.CS_TASK_CLS_PLN_PROD_CAT_CD_SK
FROM 
{IDSOwner}.CUST_SVC_TASK TASK,
{IDSOwner}.W_CUST_SVC_DRVR DRVR
WHERE
TASK.SRC_SYS_CD_SK = DRVR.SRC_SYS_CD_SK
AND TASK.CUST_SVC_ID = DRVR.CUST_SVC_ID
"""
df_db2_CUST_SVC_TASK_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CUST_SVC_TASK_in)
    .load()
)

# xfrm_AppUserBusinessLogic (CTransformerStage)
df_xfrm_AppUserBusinessLogic = df_db2_CUST_SVC_TASK_in.select(
    F.col("CUST_SVC_TASK_SK").alias("CUST_SVC_TASK_SK"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("CUST_SVC_ID").alias("CUST_SVC_ID"),
    F.col("TASK_SEQ_NO").alias("TASK_SEQ_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("GRP_SK").alias("GRP_SK"),
    F.col("SUB_SK").alias("SUB_SK"),
    F.col("CLSD_DT_SK").alias("CLSD_DT_SK"),
    TimestampToDate(F.col("INPT_DTM")).alias("INPT_DTM"),
    F.col("RCVD_DT_SK").alias("RCVD_DT_SK"),
    F.col("CUST_SVC_SK").alias("CUST_SVC_SK"),
    F.col("PROD_SK").alias("PROD_SK"),
    F.col("CUST_SVC_TASK_CAT_CD_SK").alias("CUST_SVC_TASK_CAT_CD_SK"),
    F.col("CS_TASK_CLS_PLN_PROD_CAT_CD_SK").alias("CS_TASK_CLS_PLN_PROD_CAT_CD_SK")
)

# Add_lkp_Codes (PxLookup)
df_Add_lkp_Codes = (
    df_xfrm_AppUserBusinessLogic.alias("lnk_CustSvcTaskData_In")
    .join(df_db2_GRP_in.alias("ref_GrpLookup"),
          F.col("lnk_CustSvcTaskData_In.GRP_SK") == F.col("ref_GrpLookup.GRP_SK"),
          "left")
    .join(df_db2_PROD_in.alias("ref_Prodlkup"),
          F.col("lnk_CustSvcTaskData_In.PROD_SK") == F.col("ref_Prodlkup.PROD_SK"),
          "left")
    .join(df_db2_CUST_SVC_in.alias("ref_CustSvcTskSrccdlkup"),
          F.col("lnk_CustSvcTaskData_In.CUST_SVC_SK") == F.col("ref_CustSvcTskSrccdlkup.CUST_SVC_SK"),
          "left")
    .join(df_db2_CustSvcTask_in.alias("ref_Age_0_Tasks"),
          F.col("lnk_CustSvcTaskData_In.CUST_SVC_TASK_SK") == F.col("ref_Age_0_Tasks.CUST_SVC_TASK_SK"),
          "left")
    .join(df_cpy_Cd_Mppng_ref_SrcSysCd.alias("ref_SrcSysCd"),
          F.col("lnk_CustSvcTaskData_In.SRC_SYS_CD_SK") == F.col("ref_SrcSysCd.CD_MPPNG_SK"),
          "left")
    .join(df_cpy_Cd_Mppng_ref_CustSvcTaskCatCdLkup.alias("ref_CustSvcTaskCatCdLkup"),
          F.col("lnk_CustSvcTaskData_In.CUST_SVC_TASK_CAT_CD_SK") == F.col("ref_CustSvcTaskCatCdLkup.CD_MPPNG_SK"),
          "left")
    .join(df_cpy_Cd_Mppng_ref_CsTaskClsPlnProdCatCdLkup.alias("ref_CsTaskClsPlnProdCatCdLkup"),
          F.col("lnk_CustSvcTaskData_In.CS_TASK_CLS_PLN_PROD_CAT_CD_SK") == F.col("ref_CsTaskClsPlnProdCatCdLkup.CD_MPPNG_SK"),
          "left")
)

df_Add_lkp_Codes = df_Add_lkp_Codes.select(
    F.col("lnk_CustSvcTaskData_In.CUST_SVC_TASK_SK").alias("CUST_SVC_TASK_SK"),
    F.col("lnk_CustSvcTaskData_In.CUST_SVC_ID").alias("CUST_SVC_ID"),
    F.col("lnk_CustSvcTaskData_In.TASK_SEQ_NO").alias("TASK_SEQ_NO"),
    F.col("lnk_CustSvcTaskData_In.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("lnk_CustSvcTaskData_In.GRP_SK").alias("GRP_SK"),
    F.col("lnk_CustSvcTaskData_In.SUB_SK").alias("SUB_SK"),
    F.col("lnk_CustSvcTaskData_In.CLSD_DT_SK").alias("CLSD_DT_SK"),
    F.col("lnk_CustSvcTaskData_In.INPT_DTM").alias("INPT_DTM"),
    F.col("lnk_CustSvcTaskData_In.RCVD_DT_SK").alias("RCVD_DT_SK"),
    F.col("ref_GrpLookup.GRP_ID").alias("GRP_ID"),
    F.col("ref_Prodlkup.PROD_ID").alias("PROD_ID"),
    F.col("ref_CustSvcTskSrccdlkup.CNTCT_SRC_CD").alias("CNTCT_SRC_CD"),
    F.col("ref_CustSvcTskSrccdlkup.METH_SRC_CD").alias("METH_SRC_CD"),
    F.col("ref_Age_0_Tasks.CUST_SVC_TASK_SK").alias("CUST_SVC_TASK_SK_AGE_0"),
    F.col("ref_SrcSysCd.TRGT_CD").alias("SRC_SYS_CD"),
    F.col("ref_CustSvcTaskCatCdLkup.SRC_CD").alias("SRC_CD_TASK_CAT_CD"),
    F.col("ref_CsTaskClsPlnProdCatCdLkup.SRC_CD").alias("SRC_CD_PROD_CAT_CD")
)

# xfrm_BusinessLogicCust (CTransformerStage)
# We emulate stage variables with when/otherwise logic or additional columns
df_xfrm_BusinessLogicCust_vars = (
    df_Add_lkp_Codes
    .withColumn(
        "svPreTaskage",
        F.when(
            (F.col("CLSD_DT_SK") != F.lit("1753-01-01")) & (DaysSinceFromDate(F.col("INPT_DTM"), F.col("RCVD_DT_SK")) + F.lit(1) < 0),
            DaysSinceFromDate(F.col("CLSD_DT_SK"), F.col("INPT_DTM")) + F.lit(1)
        ).when(
            (F.col("CLSD_DT_SK") != F.lit("1753-01-01")) & (DaysSinceFromDate(F.col("INPT_DTM"), F.col("RCVD_DT_SK")) + F.lit(1) > -1),
            DaysSinceFromDate(F.col("CLSD_DT_SK"), F.col("RCVD_DT_SK")) + F.lit(1)
        ).when(
            (F.col("CLSD_DT_SK") == F.lit("1753-01-01")) & (DaysSinceFromDate(F.col("INPT_DTM"), F.col("RCVD_DT_SK")) + F.lit(1) < 0),
            DaysSinceFromDate(F.lit(EDWRunCycleDate), F.col("INPT_DTM")) + F.lit(1)
        ).when(
            (F.col("CLSD_DT_SK") == F.lit("1753-01-01")) & (DaysSinceFromDate(F.col("INPT_DTM"), F.col("RCVD_DT_SK")) + F.lit(1) > -1),
            DaysSinceFromDate(F.lit(EDWRunCycleDate), F.col("RCVD_DT_SK")) + F.lit(1)
        ).otherwise(F.lit("NA"))
    )
    .withColumn(
        "svTaskage",
        F.when(
            F.isnull(F.col("CUST_SVC_TASK_SK_AGE_0")),
            F.col("svPreTaskage")
        ).otherwise(F.lit(0))
    )
)

df_xfrm_BusinessLogicCust = df_xfrm_BusinessLogicCust_vars.select(
    F.col("CUST_SVC_TASK_SK").alias("CUST_SVC_TASK_SK"),
    F.when(
        (F.col("SRC_SYS_CD").isNull()) | (F.length(trim("SRC_SYS_CD")) == 0),
        F.lit("NA")
    ).otherwise(F.col("SRC_SYS_CD")).alias("SRC_SYS_CD"),
    F.col("CUST_SVC_ID").alias("CUST_SVC_ID"),
    F.col("TASK_SEQ_NO").alias("TASK_SEQ_NO"),
    F.lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("GRP_SK").alias("GRP_SK"),
    F.col("SUB_SK").alias("SUB_SK"),
    F.lit(0).alias("MTM_TASK_AGE"),          # WhereExpression=0
    F.col("svTaskage").alias("TASK_AGE"),
    F.lit("1").alias("TASK_CT"),             # WhereExpression=1
    F.when(
        (F.length(F.col("GRP_ID")) == 0) | (F.col("GRP_ID").isNull()),
        F.lit("NA")
    ).otherwise(F.col("GRP_ID")).alias("GRP_ID"),
    F.lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.when(
        (F.col("SRC_SYS_CD") == F.lit("NPS")) & (F.col("CNTCT_SRC_CD") != F.lit("NX")),
        F.lit("Y")
    ).when(
        (F.col("SRC_SYS_CD") == F.lit("FACETS")) & (F.col("SRC_CD_PROD_CAT_CD") != F.lit("M")),
        F.lit("N")
    ).when(
        (F.col("SRC_SYS_CD") == F.lit("FACETS")) &
        (F.col("SRC_CD_PROD_CAT_CD") == F.lit("M")) &
        (F.col("CNTCT_SRC_CD") != F.lit("CALI")) &
        (F.col("METH_SRC_CD") != F.lit("8")) &
        (F.col("SRC_CD_TASK_CAT_CD") != F.lit("CXEL")) &
        (F.col("SRC_CD_TASK_CAT_CD") != F.lit("CXM5")) &
        (F.col("SRC_CD_TASK_CAT_CD") != F.lit("CXEK")) &
        (F.col("SRC_CD_TASK_CAT_CD") != F.lit("CXM4")) &
        (F.col("SRC_CD_TASK_CAT_CD") != F.lit("CXGB")) &
        (F.col("SRC_CD_TASK_CAT_CD") != F.lit("CXG5")) &
        (F.col("SRC_CD_TASK_CAT_CD") != F.lit("CXG7")) &
        (F.col("SRC_CD_TASK_CAT_CD") != F.lit("CXG8")) &
        (F.col("SRC_CD_TASK_CAT_CD") != F.lit("CXG9")) &
        (F.col("SRC_CD_TASK_CAT_CD") != F.lit("CXM6")) &
        (F.col("SRC_CD_TASK_CAT_CD") != F.lit("CXU2")) &
        (F.col("SRC_CD_TASK_CAT_CD") != F.lit("CXD9")) &
        (F.col("SRC_CD_TASK_CAT_CD") != F.lit("CXN1")) &
        (F.col("SRC_CD_TASK_CAT_CD") != F.lit("CXV1")) &
        (F.col("SRC_CD_TASK_CAT_CD") != F.lit("CXE2")) &
        (F.col("SRC_CD_TASK_CAT_CD") != F.lit("CXD2")) &
        (F.col("SRC_CD_TASK_CAT_CD") != F.lit("CXD5")) &
        (F.col("SRC_CD_TASK_CAT_CD") != F.lit("CXD6")) &
        (F.col("SRC_CD_TASK_CAT_CD") != F.lit("CXD7")) &
        (F.col("SRC_CD_TASK_CAT_CD") != F.lit("CXD8")) &
        (F.col("SRC_CD_TASK_CAT_CD") != F.lit("CXD9")) &
        (F.col("SRC_CD_TASK_CAT_CD") != F.lit("CXI4")) &
        (F.col("SRC_CD_TASK_CAT_CD") != F.lit("CXM7")) &
        (F.col("SRC_CD_TASK_CAT_CD") != F.lit("CXM9")) &
        (F.col("SRC_CD_TASK_CAT_CD") != F.lit("CXMA")) &
        (F.col("SRC_CD_TASK_CAT_CD") != F.lit("TRAN")) &
        (F.col("SRC_CD_TASK_CAT_CD") != F.lit("CXDA")) &
        (F.col("SRC_CD_TASK_CAT_CD") != F.lit("CXEM")) &
        (F.col("SRC_CD_TASK_CAT_CD") != F.lit("CXI8")) &
        (F.col("SRC_CD_TASK_CAT_CD") != F.lit("CXIA")) &
        (F.col("SRC_CD_TASK_CAT_CD") != F.lit("CXIJ")) &
        (F.col("SRC_CD_TASK_CAT_CD") != F.lit("CXM8")) &
        (F.col("SRC_CD_TASK_CAT_CD") != F.lit("CXMB")) &
        (F.col("SRC_CD_TASK_CAT_CD") != F.lit("CXMD")) &
        (F.col("SRC_CD_TASK_CAT_CD") != F.lit("CXME")) &
        (F.col("SRC_CD_TASK_CAT_CD") != F.lit("CXUI")) &
        (F.col("SRC_CD_TASK_CAT_CD") != F.lit("CXU3")) &
        (F.col("SRC_CD_TASK_CAT_CD") != F.lit("CXU4")) &
        (F.col("SRC_CD_TASK_CAT_CD") != F.lit("CXIH")) &
        (F.col("PROD_ID") != F.lit("PV")) &
        (F.col("PROD_ID") != F.lit("MV")) &
        (F.col("PROD_ID") != F.lit("DV")) &
        (F.col("PROD_ID") != F.lit("DW")) &
        (F.col("PROD_ID") != F.lit("EE")) &
        (F.col("PROD_ID") != F.lit("EH")) &
        (F.col("PROD_ID") != F.lit("PF")) &
        (F.col("PROD_ID") != F.lit("DF")),
        F.lit("Y")
    ).otherwise(F.lit("N")).alias("MTM_IN"),
    F.col("CLSD_DT_SK").alias("CLSD_DT_SK"),
    F.col("INPT_DTM").alias("INPT_DTM")
)

# xfrm_Business_rule (CTransformerStage)
df_xfrm_Business_rule_vars = (
    df_xfrm_BusinessLogicCust
    .withColumn(
        "svMTMTaskAge",
        F.when(
            (F.col("MTM_IN") == F.lit("Y")) & (F.col("CLSD_DT_SK") != F.lit("1753-01-01")),
            DaysSinceFromDate(F.col("CLSD_DT_SK"), F.col("INPT_DTM")) - F.lit(1)
        ).when(
            (F.col("MTM_IN") == F.lit("Y")) & (F.col("CLSD_DT_SK") == F.lit("1753-01-01")),
            F.lit("9999")
        ).when(
            (F.col("MTM_IN") == F.lit("N")),
            F.lit("9999")
        ).otherwise(F.lit("9999"))
    )
)

df_xfrm_Business_rule = df_xfrm_Business_rule_vars

# lnk_Full_Data_Out: filter constraint => CUST_SVC_TASK_SK <> 0 AND <> 1
df_lnk_Full_Data_Out = df_xfrm_Business_rule.filter(
    (F.col("CUST_SVC_TASK_SK") != 0) & (F.col("CUST_SVC_TASK_SK") != 1)
).select(
    F.col("CUST_SVC_TASK_SK").alias("CUST_SVC_TASK_SK"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CUST_SVC_ID").alias("CUST_SVC_ID"),
    F.col("TASK_SEQ_NO").alias("CUST_SVC_TASK_SEQ_NO"),
    F.col("CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("GRP_SK").alias("GRP_SK"),
    F.col("SUB_SK").alias("SUB_SK"),
    F.when(
        (F.col("svMTMTaskAge") == 0) | (F.col("svMTMTaskAge") < 0),
        F.lit(1)
    ).when(
        F.col("svMTMTaskAge") == F.lit("9999"),
        F.lit(None).cast(StringType())
    ).otherwise(F.col("svMTMTaskAge")).alias("MTM_TASK_AGE"),
    F.col("TASK_AGE").alias("TASK_AGE"),
    F.lit("1").alias("TASK_CT"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

# lnk_UNK_Out: single-row injection
# Constraint => ((@INROWNUM - 1)*@NUMPARTITIONS + @PARTITIONNUM +1)=1
# This yields a single row with the specified column "WhereExpression" values
df_lnk_UNK_Out = spark.createDataFrame(
    [
        (
            0,          # CUST_SVC_TASK_SK
            "UNK",      # SRC_SYS_CD
            "UNK",      # CUST_SVC_ID
            0,          # CUST_SVC_TASK_SEQ_NO
            "1753-01-01",  # CRT_RUN_CYC_EXCTN_DT_SK
            "1753-01-01",  # LAST_UPDT_RUN_CYC_EXCTN_DT_SK
            0,          # GRP_SK
            0,          # SUB_SK
            None,       # MTM_TASK_AGE (SetNull())
            0,          # TASK_AGE
            0,          # TASK_CT
            "UNK",      # GRP_ID
            100,        # CRT_RUN_CYC_EXCTN_SK
            100         # LAST_UPDT_RUN_CYC_EXCTN_SK
        )
    ],
    [
        "CUST_SVC_TASK_SK","SRC_SYS_CD","CUST_SVC_ID","CUST_SVC_TASK_SEQ_NO",
        "CRT_RUN_CYC_EXCTN_DT_SK","LAST_UPDT_RUN_CYC_EXCTN_DT_SK","GRP_SK","SUB_SK",
        "MTM_TASK_AGE","TASK_AGE","TASK_CT","GRP_ID","CRT_RUN_CYC_EXCTN_SK","LAST_UPDT_RUN_CYC_EXCTN_SK"
    ]
)

# lnk_NA_Out: single-row injection
df_lnk_NA_Out = spark.createDataFrame(
    [
        (
            1,          # CUST_SVC_TASK_SK
            "NA",       # SRC_SYS_CD
            "NA",       # CUST_SVC_ID
            0,          # CUST_SVC_TASK_SEQ_NO
            "1753-01-01",  # CRT_RUN_CYC_EXCTN_DT_SK
            "1753-01-01",  # LAST_UPDT_RUN_CYC_EXCTN_DT_SK
            1,          # GRP_SK
            1,          # SUB_SK
            None,       # MTM_TASK_AGE
            0,          # TASK_AGE
            0,          # TASK_CT
            "NA",       # GRP_ID
            100,        # CRT_RUN_CYC_EXCTN_SK
            100         # LAST_UPDT_RUN_CYC_EXCTN_SK
        )
    ],
    [
        "CUST_SVC_TASK_SK","SRC_SYS_CD","CUST_SVC_ID","CUST_SVC_TASK_SEQ_NO",
        "CRT_RUN_CYC_EXCTN_DT_SK","LAST_UPDT_RUN_CYC_EXCTN_DT_SK","GRP_SK","SUB_SK",
        "MTM_TASK_AGE","TASK_AGE","TASK_CT","GRP_ID","CRT_RUN_CYC_EXCTN_SK","LAST_UPDT_RUN_CYC_EXCTN_SK"
    ]
)

# Fnl_UNK_NA_data (PxFunnel)
df_funnel = df_lnk_NA_Out.unionByName(df_lnk_UNK_Out).unionByName(df_lnk_Full_Data_Out)

# Ensure final column order is exactly as funnel output
# Stated order in funnel's columns:
final_select = [
    "CUST_SVC_TASK_SK",
    "SRC_SYS_CD",
    "CUST_SVC_ID",
    "CUST_SVC_TASK_SEQ_NO",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "GRP_SK",
    "SUB_SK",
    "MTM_TASK_AGE",
    "TASK_AGE",
    "TASK_CT",
    "GRP_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK"
]
df_final = df_funnel.select(final_select)

# Apply rpad for char columns
df_final = (
    df_final
    .withColumn(
        "CRT_RUN_CYC_EXCTN_DT_SK",
        rpad("CRT_RUN_CYC_EXCTN_DT_SK", 10, " ")
    )
    .withColumn(
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        rpad("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", 10, " ")
    )
)

# seq_CUST_SVC_TASK_F_csv_load (PxSequentialFile)
# Write to "CUST_SVC_TASK_F.dat" in the "load" folder => f"{adls_path}/load/CUST_SVC_TASK_F.dat"
write_files(
    df_final,
    f"{adls_path}/load/CUST_SVC_TASK_F.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)