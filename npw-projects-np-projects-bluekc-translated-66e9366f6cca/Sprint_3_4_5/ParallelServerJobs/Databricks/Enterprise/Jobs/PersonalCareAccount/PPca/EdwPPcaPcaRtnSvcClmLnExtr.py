# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     EdwPPcaPcaRtnSvcClmLnExtr
# MAGIC 
# MAGIC DESCRIPTION:      Pulls claim lines and amount fields from EDW tables using the P_PCA filter criteria to load the W_PCA_RTN_SVC_CLM_LN.  
# MAGIC                                This table is used to optimize efficiency for EdwPPcaExtr.  EdwPPcaExtr will use the data multiple times through its processing.
# MAGIC 
# MAGIC 
# MAGIC INPUTS:
# MAGIC                               CLM_LN_F
# MAGIC                               CLM_F
# MAGIC                               P_PCA_RTN_SVC
# MAGIC                               PROD_D 
# MAGIC                               SUBGRP_D 
# MAGIC                               CLS_D 
# MAGIC                               CLS_PLN_D 
# MAGIC                               PROD_D 
# MAGIC   
# MAGIC HASH FILES:
# MAGIC                               hf_ppca_subgrp
# MAGIC                               hf_ppca_cls
# MAGIC                               hf_ppca_cls_pln
# MAGIC                               hf_ppca_prod
# MAGIC                               hf_ppca_pca_rtn_svc_sttus
# MAGIC 
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                                IsNull
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                                Identify if each record's status (INCLD_IN) is a duplicate (D), excluded (N), included (Y), or not applicable (X).
# MAGIC                                Mark and pass through every record because there must be an record that a claim line was marked duplicate or excluded.
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC 		W_PCA_RTN_SVC_CLM_LN.dat
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC                                 Tao Luo                  Initial Development           06/14/2006
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer	Date                 	Project/Ticket #	   Change Description                                                                 Development Project	Code Reviewer	Date Reviewed       
# MAGIC ------------------	--------------------	---------------------------	   ---------------------------------------------------------------------------                     --------------------------------	-----------------------------	----------------------------       
# MAGIC Shanmugam A.         03/08/2017             Project #5321           Make the SQL in EDW stage as user defined so when           EnterpriseDev2                    Jag Yelavarthi          2017-03-08    
# MAGIC                                                                                                    it goes through CCMigration, it will not run into SQL issues

# MAGIC All the claim lines will be passed through with a corresponding indicator.  This is done because there must be an record that a claim line was marked duplicate or excluded.
# MAGIC A working table containing all the claim line records and their status
# MAGIC This transform checks to see if the records in the PCA_RTN_SVC table matches any claim line records in CLM_LN_F.  The matching CLM_LN_SKs are outputted along with their respective indicators (Y or N).
# MAGIC Extracts the CLM_LN_SKs marked as duplicates (D)
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, DecimalType, StringType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


BeginDt = get_widget_value("BeginDt", "2006-04-13")
EndDt = get_widget_value("EndDt", "2006-04-20")
EDWOwner = get_widget_value("EDWOwner", "")
edw_secret_name = get_widget_value("edw_secret_name", "")

jdbc_url, jdbc_props = get_db_config(edw_secret_name)

df_EDW_Pca_Rtn_Svc = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
SELECT 
CLM_F.SUBGRP_SK,
P_PCA_RTN_SVC.SUBGRP_ID,
CLM_F.CLS_SK,
P_PCA_RTN_SVC.CLS_ID,
CLM_F.CLS_PLN_SK,
P_PCA_RTN_SVC.CLS_PLN_ID,
CLM_F.PROD_SK,
P_PCA_RTN_SVC.PROD_ID,
CLM_LN_F.CLM_LN_SK,
P_PCA_RTN_SVC.INCLD_IN
FROM {EDWOwner}.CLM_LN_F CLM_LN_F, 
{EDWOwner}.CLM_F CLM_F, 
{EDWOwner}.CLM_F2 CLM_F2, 
{EDWOwner}.P_PCA_RTN_SVC P_PCA_RTN_SVC, 
{EDWOwner}.PROD_D PROD_D
WHERE
  CLM_F.SRC_SYS_CD = 'FACETS'
  AND CLM_F.CLM_TYP_CD = 'MED'
  AND CLM_F.CLM_SUBTYP_CD IN ('PR', 'IP', 'OP')
  AND CLM_F.CLM_STTUS_CD IN ('A02', 'A08', 'A09')
  AND (
    (CLM_F.CLM_PD_DT_SK BETWEEN '{BeginDt}' AND '{EndDt}'
     AND CLM_F2.PCA_EXISTS_IN = 'N')
    OR
    (CLM_F.LAST_UPDT_RUN_CYC_EXCTN_DT_SK BETWEEN '{BeginDt}' AND '{EndDt}'
     AND CLM_F2.PCA_EXISTS_IN = 'Y')
  )
  and CLM_F.CLM_PD_DT_SK > '2006-12-11'
  AND CLM_F.CLM_SVC_STRT_DT_SK < '2007-01-01'
  AND CLM_F.CLM_SK = CLM_F2.CLM_SK
  AND CLM_F2.PCA_EXISTS_IN = 'N'
  AND CLM_F.PROD_SK = PROD_D.PROD_SK
  AND PROD_D.PROD_SUBPROD_CD = 'PB'
  AND CLM_LN_F.CLM_SK = CLM_F.CLM_SK
  AND CLM_LN_F.SRC_SYS_CD = CLM_F.SRC_SYS_CD
  AND P_PCA_RTN_SVC.SRC_SYS_CD = CLM_F.SRC_SYS_CD
  AND P_PCA_RTN_SVC.GRP_ID = CLM_F.GRP_ID
  AND P_PCA_RTN_SVC.PCA_SVC_TYP_CD = CLM_LN_F.CLM_LN_SVC_PRICE_RULE_CD
  AND CLM_LN_F.CLM_LN_SVC_STRT_DT_SK BETWEEN P_PCA_RTN_SVC.PCA_SVC_TYP_CD_EFF_DT AND P_PCA_RTN_SVC.PCA_SVC_TYP_CD_TERM_DT
"""
    )
    .load()
)

df_EDW_Subgrp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
SELECT SUBGRP_D.SUBGRP_SK as SUBGRP_SK
FROM {EDWOwner}.P_PCA_RTN_SVC P_PCA_RTN_SVC, {EDWOwner}.SUBGRP_D SUBGRP_D
WHERE
  P_PCA_RTN_SVC.SRC_SYS_CD = 'FACETS'
  AND P_PCA_RTN_SVC.SUBGRP_ID = SUBGRP_D.SUBGRP_ID
"""
    )
    .load()
)

df_EDW_Cls = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
SELECT CLS_D.CLS_SK as CLS_SK
FROM {EDWOwner}.P_PCA_RTN_SVC P_PCA_RTN_SVC, {EDWOwner}.CLS_D CLS_D
WHERE
  P_PCA_RTN_SVC.SRC_SYS_CD = 'FACETS'
  AND P_PCA_RTN_SVC.CLS_ID = CLS_D.CLS_ID
"""
    )
    .load()
)

df_EDW_Cls_Pln = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
SELECT CLS_PLN_D.CLS_PLN_SK as CLS_PLN_SK
FROM {EDWOwner}.P_PCA_RTN_SVC P_PCA_RTN_SVC, {EDWOwner}.CLS_PLN_D CLS_PLN_D
WHERE
  P_PCA_RTN_SVC.SRC_SYS_CD = 'FACETS'
  AND P_PCA_RTN_SVC.CLS_PLN_ID = CLS_PLN_D.CLS_PLN_ID
"""
    )
    .load()
)

df_EDW_Prod = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
SELECT PROD_D.PROD_SK as PROD_SK
FROM {EDWOwner}.P_PCA_RTN_SVC P_PCA_RTN_SVC, {EDWOwner}.PROD_D PROD_D
WHERE
  P_PCA_RTN_SVC.SRC_SYS_CD = 'FACETS'
  AND P_PCA_RTN_SVC.PROD_ID = PROD_D.PROD_ID
"""
    )
    .load()
)

df_EDW_Duplicate = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
SELECT CLM_LN_F.CLM_LN_SK,'D' AS INCLD_IN
FROM {EDWOwner}.CLM_F CLM_F, {EDWOwner}.CLM_LN_F CLM_LN_F, {EDWOwner}.EXCD_D EXCD_D, {EDWOwner}.PROD_D PROD_D
WHERE
  CLM_F.SRC_SYS_CD = 'FACETS'
  AND CLM_F.CLM_TYP_CD = 'MED'
  AND CLM_F.CLM_SUBTYP_CD IN ('PR', 'IP', 'OP')
  AND CLM_F.CLM_STTUS_CD IN ('A02', 'A08', 'A09')
  AND CLM_F.CLM_PD_DT_SK BETWEEN '{BeginDt}' AND '{EndDt}'
  AND CLM_F.PROD_SK = PROD_D.PROD_SK
  AND PROD_D.PROD_SUBPROD_CD = 'PB'
  AND CLM_LN_F.CLM_SK = CLM_F.CLM_SK
  AND CLM_LN_F.SRC_SYS_CD = CLM_F.SRC_SYS_CD
  AND CLM_LN_F.CLM_LN_DSALW_EXCD_SK = EXCD_D.EXCD_SK
  AND EXCD_D.EXCD_ID IN ('SHD', 'X02')
"""
    )
    .load()
)

df_EDW_MAIN_EXTRACTION = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
SELECT 
CLM_LN_F.CLM_LN_SK,
CLM_LN_F.CLM_SK,
CLM_LN_F.CLM_LN_CHRG_AMT,
CLM_LN_F.CLM_LN_COINS_AMT,
CLM_LN_F.CLM_LN_CNSD_CHRG_AMT,
CLM_LN_F.CLM_LN_COPAY_AMT,
CLM_LN_F.CLM_LN_DEDCT_AMT,
CLM_LN_F.CLM_LN_MBR_OBLGTN_DSALW_AMT,
CLM_LN_F.CLM_LN_NO_RESP_DSALW_AMT,
CLM_LN_F.CLM_LN_PAYBL_AMT,
CLM_LN_F.CLM_LN_PROV_WRT_OFF_AMT
FROM
{EDWOwner}.CLM_F CLM_F,
{EDWOwner}.CLM_F2 CLM_F2,
{EDWOwner}.CLM_LN_F CLM_LN_F,
{EDWOwner}.PROD_D PROD_D
WHERE
  CLM_F.SRC_SYS_CD = 'FACETS'
  AND CLM_F.CLM_TYP_CD = 'MED'
  AND CLM_F.CLM_SUBTYP_CD IN ('PR', 'IP', 'OP')
  AND CLM_F.CLM_STTUS_CD IN ('A02', 'A08', 'A09')
  AND CLM_F.CLM_SK = CLM_F2.CLM_SK
  AND (
    (
      (CLM_F.CLM_PD_DT_SK BETWEEN '{BeginDt}' AND '{EndDt}')
      AND (CLM_F.CLM_PD_DT_SK > '2006-12-11')
      AND (CLM_F2.PCA_EXISTS_IN = 'N')
    )
    OR
    (
      CLM_F.LAST_UPDT_RUN_CYC_EXCTN_DT_SK BETWEEN '{BeginDt}' AND '{EndDt}'
      AND (CLM_F2.PCA_EXISTS_IN = 'Y')
    )
  )
  AND CLM_F.CLM_SVC_STRT_DT_SK < '2007-01-01'
  AND CLM_F.PROD_SK = PROD_D.PROD_SK
  AND PROD_D.PROD_SUBPROD_CD = 'PB'
  AND CLM_F.CLM_SK = CLM_LN_F.CLM_SK
  AND CLM_F.SRC_SYS_CD = CLM_LN_F.SRC_SYS_CD
"""
    )
    .load()
)

df_lookup_subgrp = df_EDW_Subgrp.dropDuplicates(["SUBGRP_SK"])
df_lookup_cls = df_EDW_Cls.dropDuplicates(["CLS_SK"])
df_lookup_cls_pln = df_EDW_Cls_Pln.dropDuplicates(["CLS_PLN_SK"])
df_lookup_prod = df_EDW_Prod.dropDuplicates(["PROD_SK"])

df_BusinessRules_Join = (
    df_EDW_Pca_Rtn_Svc.alias("Pca_Rtn_Svc")
    .join(df_lookup_subgrp.alias("Subgrp_Lookup"), F.col("Pca_Rtn_Svc.SUBGRP_SK") == F.col("Subgrp_Lookup.SUBGRP_SK"), "left")
    .join(df_lookup_cls.alias("Cls_Lookup"), F.col("Pca_Rtn_Svc.CLS_SK") == F.col("Cls_Lookup.CLS_SK"), "left")
    .join(df_lookup_cls_pln.alias("Cls_Pln_Lookup"), F.col("Pca_Rtn_Svc.CLS_PLN_SK") == F.col("Cls_Pln_Lookup.CLS_PLN_SK"), "left")
    .join(df_lookup_prod.alias("Prod_Lookup"), F.col("Pca_Rtn_Svc.PROD_SK") == F.col("Prod_Lookup.PROD_SK"), "left")
)

df_BusinessRules_StageVars = (
    df_BusinessRules_Join.withColumn(
        "svSubgrpFound",
        (
            (F.col("Subgrp_Lookup.SUBGRP_SK").isNotNull())
            | (F.col("Pca_Rtn_Svc.SUBGRP_ID") == F.lit("ALL"))
        ),
    )
    .withColumn(
        "svClsFound",
        (
            (F.col("Cls_Lookup.CLS_SK").isNotNull())
            | (F.col("Pca_Rtn_Svc.CLS_ID") == F.lit("ALL"))
        ),
    )
    .withColumn(
        "svClsPlnFound",
        (
            (F.col("Cls_Pln_Lookup.CLS_PLN_SK").isNotNull())
            | (F.col("Pca_Rtn_Svc.CLS_PLN_ID") == F.lit("ALL"))
        ),
    )
    .withColumn(
        "svProdFound",
        (
            (F.col("Prod_Lookup.PROD_SK").isNotNull())
            | (F.col("Pca_Rtn_Svc.PROD_ID") == F.lit("ALL"))
        ),
    )
    .withColumn(
        "svFilter",
        F.col("svSubgrpFound")
        & F.col("svClsFound")
        & F.col("svClsPlnFound")
        & F.col("svProdFound"),
    )
)

df_BusinessRules_Processed = df_BusinessRules_StageVars.filter(F.col("svFilter")).select(
    F.col("Pca_Rtn_Svc.CLM_LN_SK").alias("CLM_LN_SK"),
    F.rpad(F.col("Pca_Rtn_Svc.INCLD_IN"), 1, " ").alias("INCLD_IN"),
)

df_Collector = df_BusinessRules_Processed.unionByName(
    df_EDW_Duplicate.select(
        F.col("CLM_LN_SK"),
        F.rpad(F.col("INCLD_IN"), 1, " ").alias("INCLD_IN"),
    )
)

write_files(
    df_Collector.select("CLM_LN_SK", "INCLD_IN"),
    "hf_ppca_pca_rtn_svc_sttus.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

df_hf_ppca_pca_rtn_svc_sttus = spark.read.parquet("hf_ppca_pca_rtn_svc_sttus.parquet")

df_Filter_Join = (
    df_EDW_MAIN_EXTRACTION.alias("MAIN_EXTRACTION")
    .join(
        df_hf_ppca_pca_rtn_svc_sttus.alias("Pca_Rtn_Svc_Exclude_Lookup"),
        (
            (F.col("MAIN_EXTRACTION.CLM_LN_SK") == F.col("Pca_Rtn_Svc_Exclude_Lookup.CLM_LN_SK"))
            & (F.col("Pca_Rtn_Svc_Exclude_Lookup.INCLD_IN") == F.lit("N"))
        ),
        "left",
    )
    .join(
        df_hf_ppca_pca_rtn_svc_sttus.alias("Pca_Rtn_Svc_Dup_Lookup"),
        (
            (F.col("MAIN_EXTRACTION.CLM_LN_SK") == F.col("Pca_Rtn_Svc_Dup_Lookup.CLM_LN_SK"))
            & (F.col("Pca_Rtn_Svc_Dup_Lookup.INCLD_IN") == F.lit("D"))
        ),
        "left",
    )
    .join(
        df_hf_ppca_pca_rtn_svc_sttus.alias("Pca_Rtn_Svc_Include_Lookup"),
        (
            (F.col("MAIN_EXTRACTION.CLM_LN_SK") == F.col("Pca_Rtn_Svc_Include_Lookup.CLM_LN_SK"))
            & (F.col("Pca_Rtn_Svc_Include_Lookup.INCLD_IN") == F.lit("Y"))
        ),
        "left",
    )
)

df_Filter_StageVars = df_Filter_Join.withColumn(
    "svIncldIn",
    F.when(
        F.col("Pca_Rtn_Svc_Exclude_Lookup.INCLD_IN").isNull(),
        F.when(
            F.col("Pca_Rtn_Svc_Dup_Lookup.INCLD_IN").isNull(),
            F.when(
                F.col("Pca_Rtn_Svc_Include_Lookup.INCLD_IN").isNull(),
                F.lit("X"),
            ).otherwise(F.col("Pca_Rtn_Svc_Include_Lookup.INCLD_IN")),
        ).otherwise(F.col("Pca_Rtn_Svc_Dup_Lookup.INCLD_IN")),
    ).otherwise(F.col("Pca_Rtn_Svc_Exclude_Lookup.INCLD_IN")),
)

df_Load = df_Filter_StageVars.select(
    F.col("MAIN_EXTRACTION.CLM_LN_SK").alias("CLM_LN_SK"),
    F.col("MAIN_EXTRACTION.CLM_SK").alias("CLM_SK"),
    F.col("MAIN_EXTRACTION.CLM_LN_CHRG_AMT").alias("CLM_LN_CHRG_AMT"),
    F.col("MAIN_EXTRACTION.CLM_LN_COINS_AMT").alias("CLM_LN_COINS_AMT"),
    F.col("MAIN_EXTRACTION.CLM_LN_CNSD_CHRG_AMT").alias("CLM_LN_CNSD_CHRG_AMT"),
    F.col("MAIN_EXTRACTION.CLM_LN_COPAY_AMT").alias("CLM_LN_COPAY_AMT"),
    F.col("MAIN_EXTRACTION.CLM_LN_DEDCT_AMT").alias("CLM_LN_DEDCT_AMT"),
    F.col("MAIN_EXTRACTION.CLM_LN_MBR_OBLGTN_DSALW_AMT").alias("CLM_LN_MBR_OBLGTN_DSALW_AMT"),
    F.col("MAIN_EXTRACTION.CLM_LN_NO_RESP_DSALW_AMT").alias("CLM_LN_NO_RESP_DSALW_AMT"),
    F.col("MAIN_EXTRACTION.CLM_LN_PAYBL_AMT").alias("CLM_LN_PAYBL_AMT"),
    F.col("MAIN_EXTRACTION.CLM_LN_PROV_WRT_OFF_AMT").alias("CLM_LN_PROV_WRT_OFF_AMT"),
    F.rpad(F.col("svIncldIn"), 1, " ").alias("INCLD_IN"),
)

write_files(
    df_Load.select(
        "CLM_LN_SK",
        "CLM_SK",
        "CLM_LN_CHRG_AMT",
        "CLM_LN_COINS_AMT",
        "CLM_LN_CNSD_CHRG_AMT",
        "CLM_LN_COPAY_AMT",
        "CLM_LN_DEDCT_AMT",
        "CLM_LN_MBR_OBLGTN_DSALW_AMT",
        "CLM_LN_NO_RESP_DSALW_AMT",
        "CLM_LN_PAYBL_AMT",
        "CLM_LN_PROV_WRT_OFF_AMT",
        "INCLD_IN",
    ),
    f"{adls_path}/load/W_PCA_RTN_SVC_CLM_LN.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)