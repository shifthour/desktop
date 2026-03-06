# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date               Project/Altiris #               Change Description                                                     Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------    ---------------------------------------------------------                            ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Naren Garapaty                2/14/2007           Cust Svc/3028           Originally Programmed                                               devlEDW10              
# MAGIC 
# MAGIC 
# MAGIC Bhupinder Kaur               12/13/2013        5114                          Create Load File for EDW Table CUST_SVC_D          EnterpriseWhseDevl         Jagadesh Yelavarthi      2014-01-29

# MAGIC Job: IdsEdwCustSvcDExtr
# MAGIC Apply Business Rules
# MAGIC Code SK lookups for Denormalization
# MAGIC EDW Cust Svc D extract from IDS based on Driver Table - W_CUST_SVC_DRVR
# MAGIC Write CUST_SVC_D Data into a Sequential file for Load Job IdsEdwCustSvcDLoad.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


# MAGIC %run ../../../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

df_db2_CUST_SVC_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""
SELECT 
SVC.CUST_SVC_SK,
SVC.SRC_SYS_CD_SK,
SVC.CUST_SVC_ID,
SVC.CRT_RUN_CYC_EXCTN_SK,
SVC.LAST_UPDT_RUN_CYC_EXCTN_SK,
SVC.CUST_SVC_CNTCT_RELSHP_CD_SK,
SVC.CUST_SVC_EXCL_CD_SK,
SVC.CUST_SVC_METH_CD_SK,
SVC.CUST_SVC_SATSFCTN_LVL_CD_SK,
SVC.DISCLMR_IN,
SVC.CNTCT_INFO_TX,
SVC.CNTCT_RQST_DESC

FROM 
{IDSOwner}.CUST_SVC  SVC,
{IDSOwner}.W_CUST_SVC_DRVR DRVR

WHERE SVC.SRC_SYS_CD_SK = DRVR.SRC_SYS_CD_SK 
      AND SVC.CUST_SVC_ID       = DRVR.CUST_SVC_ID
"""
    )
    .load()
)

df_db2_MaxClsdDt_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""
SELECT
TASK.CUST_SVC_SK,
max(TASK.CLSD_DT_SK) as CLSD_DT_SK
FROM {IDSOwner}.CUST_SVC  SVC,
     {IDSOwner}.CUST_SVC_TASK TASK,
     {IDSOwner}.W_CUST_SVC_DRVR DRVR
WHERE  SVC.CUST_SVC_SK=TASK.CUST_SVC_SK 
       AND  SVC.SRC_SYS_CD_SK = DRVR.SRC_SYS_CD_SK 
       AND  SVC.CUST_SVC_ID       = DRVR.CUST_SVC_ID
GROUP BY TASK.CUST_SVC_SK
"""
    )
    .load()
)

df_db2_MinClsdDt_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""
SELECT
TASK.CUST_SVC_SK,
min(TASK.CLSD_DT_SK) as CLSD_DT_SK
FROM {IDSOwner}.CUST_SVC  SVC,
     {IDSOwner}.CUST_SVC_TASK TASK,
     {IDSOwner}.W_CUST_SVC_DRVR DRVR
WHERE SVC.CUST_SVC_SK = TASK.CUST_SVC_SK 
      AND SVC.SRC_SYS_CD_SK = DRVR.SRC_SYS_CD_SK 
      AND SVC.CUST_SVC_ID       = DRVR.CUST_SVC_ID
GROUP BY TASK.CUST_SVC_SK
"""
    )
    .load()
)

df_db2_MinRcvdDt_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""
SELECT 
TASK.CUST_SVC_SK,
min(TASK.RCVD_DT_SK)  RCVD_DT_SK
FROM {IDSOwner}.CUST_SVC  SVC,
     {IDSOwner}.CUST_SVC_TASK TASK,
     {IDSOwner}.W_CUST_SVC_DRVR DRVR
WHERE SVC.CUST_SVC_SK = TASK.CUST_SVC_SK 
      AND SVC.SRC_SYS_CD_SK = DRVR.SRC_SYS_CD_SK 
      AND SVC.CUST_SVC_ID       = DRVR.CUST_SVC_ID
GROUP BY TASK.CUST_SVC_SK
"""
    )
    .load()
)

df_db2_CustSvc_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""
SELECT 
SVC.CUST_SVC_SK
FROM  {IDSOwner}.CUST_SVC  SVC,
      {IDSOwner}.CUST_SVC_TASK TASK,
      {IDSOwner}.CUST_SVC_TASK_STTUS STTUS,
      {IDSOwner}.CD_MPPNG CD_MPPNG,
      {IDSOwner}.W_CUST_SVC_DRVR DRVR
WHERE  SVC.CUST_SVC_SK = TASK.CUST_SVC_SK 
       AND  TASK.CUST_SVC_TASK_SK = STTUS.CUST_SVC_TASK_SK 
       AND  STTUS.CUST_SVC_TASK_STTUS_CD_SK = CD_MPPNG.CD_MPPNG_SK 
       AND  CD_MPPNG.TRGT_CD = 'RTE'
       AND  SVC.SRC_SYS_CD_SK = DRVR.SRC_SYS_CD_SK 
       AND  SVC.CUST_SVC_ID       = DRVR.CUST_SVC_ID
"""
    )
    .load()
)

df_dedup_CustSvc = dedup_sort(
    df_db2_CustSvc_in,
    partition_cols=["CUST_SVC_SK"],
    sort_cols=[("CUST_SVC_SK","A")]
)
df_rmdup_CustSvc = df_dedup_CustSvc.select(F.col("CUST_SVC_SK"))

df_db2_CD_MPPNG_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""
SELECT
CD_MPPNG_SK,
COALESCE(TRGT_CD,'UNK') TRGT_CD,
COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM
from {IDSOwner}.CD_MPPNG
"""
    )
    .load()
)

df_cpy_CdMppng = df_db2_CD_MPPNG_Extr.select(
    F.col("CD_MPPNG_SK"),
    F.col("TRGT_CD"),
    F.col("TRGT_CD_NM")
)

df_ref_SatsfctnLvlCdLkup = df_cpy_CdMppng
df_ref_ExclCdCdLkup = df_cpy_CdMppng
df_ref_SrcSysCd = df_cpy_CdMppng
df_ref_MethCdLkup = df_cpy_CdMppng
df_ref_CnctRelCdLkup = df_cpy_CdMppng

df_Add_lkp_Codes = (
    df_db2_CUST_SVC_in.alias("lnk_IdsEdwCustSvcExtr_In")
    .join(df_db2_MinRcvdDt_in.alias("ref_MinRcvdDt"),
          F.col("lnk_IdsEdwCustSvcExtr_In.CUST_SVC_SK") == F.col("ref_MinRcvdDt.CUST_SVC_SK"), 
          how="left")
    .join(df_db2_MinClsdDt_in.alias("ref_MinClsdDt"),
          F.col("lnk_IdsEdwCustSvcExtr_In.CUST_SVC_SK") == F.col("ref_MinClsdDt.CUST_SVC_SK"), 
          how="left")
    .join(df_db2_MaxClsdDt_in.alias("ref_MaxClsdDt"),
          F.col("lnk_IdsEdwCustSvcExtr_In.CUST_SVC_SK") == F.col("ref_MaxClsdDt.CUST_SVC_SK"), 
          how="left")
    .join(df_ref_SrcSysCd.alias("ref_SrcSysCd"),
          F.col("lnk_IdsEdwCustSvcExtr_In.SRC_SYS_CD_SK") == F.col("ref_SrcSysCd.CD_MPPNG_SK"),
          how="left")
    .join(df_ref_ExclCdCdLkup.alias("ref_ExclCdCdLkup"),
          F.col("lnk_IdsEdwCustSvcExtr_In.CUST_SVC_EXCL_CD_SK") == F.col("ref_ExclCdCdLkup.CD_MPPNG_SK"),
          how="left")
    .join(df_ref_MethCdLkup.alias("ref_MethCdLkup"),
          F.col("lnk_IdsEdwCustSvcExtr_In.CUST_SVC_METH_CD_SK") == F.col("ref_MethCdLkup.CD_MPPNG_SK"),
          how="left")
    .join(df_ref_CnctRelCdLkup.alias("ref_CnctRelCdLkup"),
          F.col("lnk_IdsEdwCustSvcExtr_In.CUST_SVC_CNTCT_RELSHP_CD_SK") == F.col("ref_CnctRelCdLkup.CD_MPPNG_SK"),
          how="left")
    .join(df_ref_SatsfctnLvlCdLkup.alias("ref_SatsfctnLvlCdLkup"),
          F.col("lnk_IdsEdwCustSvcExtr_In.CUST_SVC_SATSFCTN_LVL_CD_SK") == F.col("ref_SatsfctnLvlCdLkup.CD_MPPNG_SK"),
          how="left")
    .join(df_rmdup_CustSvc.alias("ref_CustSvcSk"),
          F.col("lnk_IdsEdwCustSvcExtr_In.CUST_SVC_SK") == F.col("ref_CustSvcSk.CUST_SVC_SK"),
          how="left")
    .select(
        F.col("lnk_IdsEdwCustSvcExtr_In.CUST_SVC_SK").alias("CUST_SVC_SK"),
        F.col("lnk_IdsEdwCustSvcExtr_In.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("lnk_IdsEdwCustSvcExtr_In.CUST_SVC_ID").alias("CUST_SVC_ID"),
        F.col("lnk_IdsEdwCustSvcExtr_In.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("lnk_IdsEdwCustSvcExtr_In.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("lnk_IdsEdwCustSvcExtr_In.CUST_SVC_CNTCT_RELSHP_CD_SK").alias("CUST_SVC_CNTCT_RELSHP_CD_SK"),
        F.col("lnk_IdsEdwCustSvcExtr_In.CUST_SVC_EXCL_CD_SK").alias("CUST_SVC_EXCL_CD_SK"),
        F.col("lnk_IdsEdwCustSvcExtr_In.CUST_SVC_METH_CD_SK").alias("CUST_SVC_METH_CD_SK"),
        F.col("lnk_IdsEdwCustSvcExtr_In.CUST_SVC_SATSFCTN_LVL_CD_SK").alias("CUST_SVC_SATSFCTN_LVL_CD_SK"),
        F.col("lnk_IdsEdwCustSvcExtr_In.DISCLMR_IN").alias("DISCLMR_IN"),
        F.col("lnk_IdsEdwCustSvcExtr_In.CNTCT_INFO_TX").alias("CNTCT_INFO_TX"),
        F.col("lnk_IdsEdwCustSvcExtr_In.CNTCT_RQST_DESC").alias("CNTCT_RQST_DESC"),
        F.col("ref_MinRcvdDt.RCVD_DT_SK").alias("RCVD_DT_SK"),
        F.col("ref_MinClsdDt.CLSD_DT_SK").alias("CLSD_DT_SK_MIN"),
        F.col("ref_MaxClsdDt.CLSD_DT_SK").alias("CLSD_DT_SK_MAX"),
        F.col("ref_CustSvcSk.CUST_SVC_SK").alias("CUST_SVC_SK_1"),
        F.col("ref_SrcSysCd.TRGT_CD").alias("SRC_SYS_CD"),
        F.col("ref_ExclCdCdLkup.TRGT_CD").alias("EXCL_CD"),
        F.col("ref_ExclCdCdLkup.TRGT_CD_NM").alias("EXCL_CD_NM"),
        F.col("ref_MethCdLkup.TRGT_CD").alias("METH_CD"),
        F.col("ref_MethCdLkup.TRGT_CD_NM").alias("METH_CD_NM"),
        F.col("ref_CnctRelCdLkup.TRGT_CD").alias("CNTCT_CD"),
        F.col("ref_CnctRelCdLkup.TRGT_CD_NM").alias("CNTCT_CD_NM"),
        F.col("ref_SatsfctnLvlCdLkup.TRGT_CD").alias("STSFCTN_CD"),
        F.col("ref_SatsfctnLvlCdLkup.TRGT_CD_NM").alias("STSFCTN_CD_NM")
    )
)

# xfrm_Business_rule: Three output links from df_Add_lkp_Codes

df_xfrm_Business_rule_Full_Data_Out = (
    df_Add_lkp_Codes
    .filter( (F.col("CUST_SVC_SK") != 0) & (F.col("CUST_SVC_SK") != 1) )
    .select(
        F.col("CUST_SVC_SK").alias("CUST_SVC_SK"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("CUST_SVC_ID").alias("CUST_SVC_ID"),
        F.lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.expr(
            """
CASE 
 WHEN CLSD_DT_SK_MIN = '1753-01-01' THEN '1753-01-01' 
 ELSE CASE 
        WHEN CLSD_DT_SK_MAX IS NULL OR length(CLSD_DT_SK_MAX)=0 THEN '    ' 
        ELSE CLSD_DT_SK_MAX 
      END
END
"""
        ).alias("CUST_SVC_CLSD_DT_SK"),
        F.expr(
            """
CASE 
 WHEN CLSD_DT_SK_MIN = '1753-01-01' THEN 'N'
 ELSE 'Y'
END
"""
        ).alias("CUST_SVC_CLSD_STTUS_IN"),
        F.col("CNTCT_INFO_TX").alias("CUST_SVC_CNTCT_INFO_TX"),
        F.col("CNTCT_CD").alias("CUST_SVC_CNTCT_RELSHP_CD"),
        F.col("CNTCT_CD_NM").alias("CUST_SVC_CNTCT_RELSHP_NM"),
        F.expr(
            """
CASE 
 WHEN CNTCT_RQST_DESC IS NULL OR length(trim(CNTCT_RQST_DESC))=0 THEN '    '
 ELSE trim(CNTCT_RQST_DESC)
END
"""
        ).alias("CUST_SVC_CNTCT_RQST_DESC"),
        F.col("DISCLMR_IN").alias("CUST_SVC_DISCLMR_IN"),
        F.col("EXCL_CD").alias("CUST_SVC_EXCL_CD"),
        F.col("EXCL_CD_NM").alias("CUST_SVC_EXCL_NM"),
        F.col("METH_CD").alias("CUST_SVC_METH_CD"),
        F.col("METH_CD_NM").alias("CUST_SVC_METH_NM"),
        F.col("STSFCTN_CD").alias("CUST_SVC_SATSFCTN_LVL_CD"),
        F.col("STSFCTN_CD_NM").alias("CUST_SVC_SATSFCTN_LVL_NM"),
        F.expr(
            """
CASE
 WHEN CLSD_DT_SK_MIN <> '1753-01-01'
   AND CUST_SVC_SK_1 IS NULL
   AND trim(RCVD_DT_SK)=trim(CLSD_DT_SK_MAX)
 THEN 'Y'
 ELSE 'N'
END
"""
        ).alias("SAME_DAY_SAME_PRSN_IN"),
        F.lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("CUST_SVC_CNTCT_RELSHP_CD_SK").alias("CUST_SVC_CNTCT_RELSHP_CD_SK"),
        F.col("CUST_SVC_EXCL_CD_SK").alias("CUST_SVC_EXCL_CD_SK"),
        F.col("CUST_SVC_METH_CD_SK").alias("CUST_SVC_METH_CD_SK"),
        F.col("CUST_SVC_SATSFCTN_LVL_CD_SK").alias("CUST_SVC_SATSFCTN_LVL_CD_SK")
    )
)

# lnk_UNK_Out => Single row with fixed values
schema_UNK = StructType([
    StructField("CUST_SVC_SK", IntegerType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("CUST_SVC_ID", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("CUST_SVC_CLSD_DT_SK", StringType(), True),
    StructField("CUST_SVC_CLSD_STTUS_IN", StringType(), True),
    StructField("CUST_SVC_CNTCT_INFO_TX", StringType(), True),
    StructField("CUST_SVC_CNTCT_RELSHP_CD", StringType(), True),
    StructField("CUST_SVC_CNTCT_RELSHP_NM", StringType(), True),
    StructField("CUST_SVC_CNTCT_RQST_DESC", StringType(), True),
    StructField("CUST_SVC_DISCLMR_IN", StringType(), True),
    StructField("CUST_SVC_EXCL_CD", StringType(), True),
    StructField("CUST_SVC_EXCL_NM", StringType(), True),
    StructField("CUST_SVC_METH_CD", StringType(), True),
    StructField("CUST_SVC_METH_NM", StringType(), True),
    StructField("CUST_SVC_SATSFCTN_LVL_CD", StringType(), True),
    StructField("CUST_SVC_SATSFCTN_LVL_NM", StringType(), True),
    StructField("SAME_DAY_SAME_PRSN_IN", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("CUST_SVC_CNTCT_RELSHP_CD_SK", IntegerType(), True),
    StructField("CUST_SVC_EXCL_CD_SK", IntegerType(), True),
    StructField("CUST_SVC_METH_CD_SK", IntegerType(), True),
    StructField("CUST_SVC_SATSFCTN_LVL_CD_SK", IntegerType(), True)
])
df_xfrm_Business_rule_UNK_Out = spark.createDataFrame(
    [
        (
            0, "UNK", "UNK", "1753-01-01", "1753-01-01", "1753-01-01", "N", None,
            "UNK", "UNK", "",
            "N", "UNK", "UNK", "UNK", "UNK", "UNK", "UNK",
            "N", 100, 100, 0, 0, 0, 0
        )
    ],
    schema_UNK
)

# lnk_NA_Out => Single row with fixed values
schema_NA = StructType([
    StructField("CUST_SVC_SK", IntegerType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("CUST_SVC_ID", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("CUST_SVC_CLSD_DT_SK", StringType(), True),
    StructField("CUST_SVC_CLSD_STTUS_IN", StringType(), True),
    StructField("CUST_SVC_CNTCT_INFO_TX", StringType(), True),
    StructField("CUST_SVC_CNTCT_RELSHP_CD", StringType(), True),
    StructField("CUST_SVC_CNTCT_RELSHP_NM", StringType(), True),
    StructField("CUST_SVC_CNTCT_RQST_DESC", StringType(), True),
    StructField("CUST_SVC_DISCLMR_IN", StringType(), True),
    StructField("CUST_SVC_EXCL_CD", StringType(), True),
    StructField("CUST_SVC_EXCL_NM", StringType(), True),
    StructField("CUST_SVC_METH_CD", StringType(), True),
    StructField("CUST_SVC_METH_NM", StringType(), True),
    StructField("CUST_SVC_SATSFCTN_LVL_CD", StringType(), True),
    StructField("CUST_SVC_SATSFCTN_LVL_NM", StringType(), True),
    StructField("SAME_DAY_SAME_PRSN_IN", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("CUST_SVC_CNTCT_RELSHP_CD_SK", IntegerType(), True),
    StructField("CUST_SVC_EXCL_CD_SK", IntegerType(), True),
    StructField("CUST_SVC_METH_CD_SK", IntegerType(), True),
    StructField("CUST_SVC_SATSFCTN_LVL_CD_SK", IntegerType(), True)
])
df_xfrm_Business_rule_NA_Out = spark.createDataFrame(
    [
        (
            1, "NA", "NA", "1753-01-01", "1753-01-01", "1753-01-01", "N", None,
            "NA", "NA", "",
            "N", "NA", "NA", "NA", "NA", "NA", "NA",
            "N", 100, 100, 1, 1, 1, 1
        )
    ],
    schema_NA
)

df_Fnl_UNK_NA_data = (
    df_xfrm_Business_rule_NA_Out
    .union(df_xfrm_Business_rule_UNK_Out)
    .union(df_xfrm_Business_rule_Full_Data_Out)
)

df_funnel = df_Fnl_UNK_NA_data.select(
    F.col("CUST_SVC_SK"),
    F.col("SRC_SYS_CD"),
    F.col("CUST_SVC_ID"),
    F.col("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("CUST_SVC_CLSD_DT_SK"),
    F.col("CUST_SVC_CLSD_STTUS_IN"),
    F.col("CUST_SVC_CNTCT_INFO_TX"),
    F.col("CUST_SVC_CNTCT_RELSHP_CD"),
    F.col("CUST_SVC_CNTCT_RELSHP_NM"),
    F.col("CUST_SVC_CNTCT_RQST_DESC"),
    F.col("CUST_SVC_DISCLMR_IN"),
    F.col("CUST_SVC_EXCL_CD"),
    F.col("CUST_SVC_EXCL_NM"),
    F.col("CUST_SVC_METH_CD"),
    F.col("CUST_SVC_METH_NM"),
    F.col("CUST_SVC_SATSFCTN_LVL_CD"),
    F.col("CUST_SVC_SATSFCTN_LVL_NM"),
    F.col("SAME_DAY_SAME_PRSN_IN"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CUST_SVC_CNTCT_RELSHP_CD_SK"),
    F.col("CUST_SVC_EXCL_CD_SK"),
    F.col("CUST_SVC_METH_CD_SK"),
    F.col("CUST_SVC_SATSFCTN_LVL_CD_SK")
)

df_final = (
    df_funnel
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("CUST_SVC_CLSD_DT_SK", rpad(F.col("CUST_SVC_CLSD_DT_SK"), 10, " "))
    .withColumn("CUST_SVC_CLSD_STTUS_IN", rpad(F.col("CUST_SVC_CLSD_STTUS_IN"), 1, " "))
    .withColumn("CUST_SVC_DISCLMR_IN", rpad(F.col("CUST_SVC_DISCLMR_IN"), 1, " "))
    .withColumn("SAME_DAY_SAME_PRSN_IN", rpad(F.col("SAME_DAY_SAME_PRSN_IN"), 1, " "))
    .select(
        "CUST_SVC_SK",
        "SRC_SYS_CD",
        "CUST_SVC_ID",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "CUST_SVC_CLSD_DT_SK",
        "CUST_SVC_CLSD_STTUS_IN",
        "CUST_SVC_CNTCT_INFO_TX",
        "CUST_SVC_CNTCT_RELSHP_CD",
        "CUST_SVC_CNTCT_RELSHP_NM",
        "CUST_SVC_CNTCT_RQST_DESC",
        "CUST_SVC_DISCLMR_IN",
        "CUST_SVC_EXCL_CD",
        "CUST_SVC_EXCL_NM",
        "CUST_SVC_METH_CD",
        "CUST_SVC_METH_NM",
        "CUST_SVC_SATSFCTN_LVL_CD",
        "CUST_SVC_SATSFCTN_LVL_NM",
        "SAME_DAY_SAME_PRSN_IN",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CUST_SVC_CNTCT_RELSHP_CD_SK",
        "CUST_SVC_EXCL_CD_SK",
        "CUST_SVC_METH_CD_SK",
        "CUST_SVC_SATSFCTN_LVL_CD_SK"
    )
)

write_files(
    df_final,
    f"{adls_path}/load/CUST_SVC_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)

params = {
  "EnvProjectPath": f"dap/<...>/Jobs/DataWarehouse/CustSvc/EdwLoads/CustSvcD",
  "File_Path": "load",
  "File_Name": "CUST_SVC_D.dat"
}
dbutils.notebook.run("../../../../../../sequencer_routines/Move_File", timeout_seconds=3600, arguments=params)