# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2013 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC JOB NAME:  FctsIdsWorkCompnClmDiagExtr
# MAGIC CALLED BY:  FctsIdsWorkCompClmSeq
# MAGIC 
# MAGIC PROCESSING:     Workers Compensation claims extract from FacetsDbo and load to IDS WORK_COMPNSTN_CLM_DIAG table
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                    Date                 \(9)Project                                                          Change Description                                                   Development Project\(9)          Code Reviewer          Date Reviewed       
# MAGIC ------------------                --------------------     \(9)------------------------                                            -----------------------------------------------------------------------                  ------------------------------\(9)         -------------------------------   ----------------------------       
# MAGIC Akhila Manickavelu\(9)  2017-01-04\(9)5628 WORK_COMPNSTN_CLM_DIAG \(9)    Original Programming\(9)\(9)\(9)          Integratedev2                           Kalyan Neelam           2017-02-27     
# MAGIC                                                                 (Facets to IDS) ETL Report 
# MAGIC 
# MAGIC Akhila Manickavelu\(9)  2017-05-09\(9)5628 WORK_COMPNSTN_CLM \(9)    Reversal Logic updated\(9)\(9)\(9)          Integratedev2                          Kalyan Neelam           2017-05-22         
# MAGIC                                                                 (Facets to IDS) ETL Report 
# MAGIC Prabhu ES                 2022-03-02            S2S Remediation                                        MSSQL ODBC conn added and other param changes  IntegrateDev5\(9)Ken Bradmon\(9)2022-06-03

# MAGIC This job Extracts the Claims data from Facets tables to generate the load file WORK_COMPNSTN_CLM_DIAG.dat to be send to WORK_COMPNSTN_CLM_DIAG table
# MAGIC Join CMC_CLMD_DIAG, CMC_GRGR_GROUP, CMC_MEME_MEMBER for master extract and Look up on P_SEL_PRCS_CRITR for GRGR_ID
# MAGIC Seq. file to load into the DB2 table B_WORK_COMPNSTN_CLM_DIAG for Balancing Report
# MAGIC Seq. file to load into the DB2 table WORK_COMPNSTN_CLM_DIAG
# MAGIC get the field CRT_RUN_CYC_EXCTN_SK from 
# MAGIC IDS DB2 table  WORK_COMPNSTN_CLM_DIAG
# MAGIC Reversal Logic
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
BCBSOwner = get_widget_value('BCBSOwner','')
bcbs_secret_name = get_widget_value('bcbs_secret_name','')
WorkCompOwner = get_widget_value('WorkCompOwner','')
workcomp_secret_name = get_widget_value('workcomp_secret_name','')
RunCycle = get_widget_value('RunCycle','')
DrivTable = get_widget_value('DrivTable','')
SrcSysCd = get_widget_value('SrcSysCd','')

# WorkCompnstnClmDiag (DB2ConnectorPX, Database=IDS)
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"""
SELECT
    CLM_ID,
    CLM_DIAG_ORDNL_CD,
    CRT_RUN_CYC_EXCTN_SK
FROM
    {WorkCompOwner}.WORK_COMPNSTN_CLM_DIAG
WHERE
    SRC_SYS_CD = 'FACETS'
"""
df_WorkCompnstnClmDiag = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# Ds_ReversalClaim (PxDataSet) reading Clm_Reversals.ds → Clm_Reversals.parquet
schema_Reversal = StructType([
    StructField("CLCL_ID", StringType(), True),
    StructField("CLCL_CUR_STS", StringType(), True),
    StructField("CLCL_PAID_DT", StringType(), True)
])
df_Ds_ReversalClaim = spark.read.schema(schema_Reversal).parquet(f"{adls_path}/ds/Clm_Reversals.parquet")

# DiagCD (DB2ConnectorPX, Database=IDS)
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"""
SELECT DISTINCT
    DIAG_CD.DIAG_CD,
    DIAG_CD.DIAG_CD_SK
FROM
    {IDSOwner}.DIAG_CD DIAG_CD
WHERE
    DIAG_CD.DIAG_CD_TYP_CD='ICD10'
"""
df_DiagCD = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# FacetsDB_Input (ODBCConnectorPX)
jdbc_url, jdbc_props = get_db_config(facets_secret_name)
extract_query = f"""
SELECT DISTINCT
    CMC_CLMD_DIAG.CLCL_ID,
    CASE 
        WHEN CMC_CLMD_DIAG.CLMD_TYPE = '01' THEN '1'
        WHEN CMC_CLMD_DIAG.CLMD_TYPE = '02' THEN '2'
        WHEN CMC_CLMD_DIAG.CLMD_TYPE = '03' THEN '3'
        WHEN CMC_CLMD_DIAG.CLMD_TYPE = '04' THEN '4'
        WHEN CMC_CLMD_DIAG.CLMD_TYPE = '05' THEN '5'
        WHEN CMC_CLMD_DIAG.CLMD_TYPE = '06' THEN '6'
        WHEN CMC_CLMD_DIAG.CLMD_TYPE = '07' THEN '7'
        WHEN CMC_CLMD_DIAG.CLMD_TYPE = '08' THEN '8'
        WHEN CMC_CLMD_DIAG.CLMD_TYPE = '09' THEN '9'
        ELSE CMC_CLMD_DIAG.CLMD_TYPE
    END AS CLMD_TYPE,
    CMC_CLMD_DIAG.IDCD_ID,
    CMC_CLMD_DIAG.CLMD_POA_IND
FROM {FacetsOwner}.CMC_CLMD_DIAG CMC_CLMD_DIAG
INNER JOIN tempdb..{DrivTable} Drvr
    ON Drvr.CLM_ID = CMC_CLMD_DIAG.CLCL_ID
"""
df_FacetsDB_Input = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# CD_MPPNG (DB2ConnectorPX, Database=IDS)
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"""
SELECT DISTINCT
    SRC_CD,
    CD_MPPNG_SK,
    SRC_DOMAIN_NM,
    TRGT_DOMAIN_NM,
    TRGT_CD
FROM
    {IDSOwner}.CD_MPPNG
WHERE
    SRC_SYS_CD = 'FACETS'
    AND SRC_CLCTN_CD = 'FACETS DBO'
    AND SRC_DOMAIN_NM IN ('DIAGNOSIS ORDINAL','CLAIM DIAGNOSIS PRESENT ON ADMISSION')
    AND TRGT_CLCTN_CD = 'IDS'
    AND TRGT_DOMAIN_NM IN ('DIAGNOSIS ORDINAL','CLAIM DIAGNOSIS PRESENT ON ADMISSION')
"""
df_CD_MPPNG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# Flt_CdMppng (PxFilter)
df_Flt_CdMppng_Ordinal = df_CD_MPPNG.filter(
    "(SRC_DOMAIN_NM = 'DIAGNOSIS ORDINAL' AND TRGT_DOMAIN_NM = 'DIAGNOSIS ORDINAL')"
)
df_Flt_CdMppng_Admission = df_CD_MPPNG.filter(
    "(SRC_DOMAIN_NM = 'CLAIM DIAGNOSIS PRESENT ON ADMISSION' AND TRGT_DOMAIN_NM = 'CLAIM DIAGNOSIS PRESENT ON ADMISSION')"
)

df_Ordinal = df_Flt_CdMppng_Ordinal.selectExpr(
    "SRC_CD as SRC_CD",
    "CD_MPPNG_SK as CD_MPPNG_SK",
    "TRGT_CD as TRGT_CD"
)
df_Admission = df_Flt_CdMppng_Admission.selectExpr(
    "SRC_CD as SRC_CD",
    "CD_MPPNG_SK as CD_MPPNG_SK"
)

# Lookup_DBs (PxLookup)
df_Lookup_DBs = (
    df_FacetsDB_Input.alias("Lnk_FctsInp")
    .join(df_Ordinal.alias("Ordinal"), F.col("Lnk_FctsInp.CLMD_TYPE") == F.col("Ordinal.SRC_CD"), "inner")
    .join(df_Admission.alias("Admission"), F.col("Lnk_FctsInp.CLMD_POA_IND") == F.col("Admission.SRC_CD"), "left")
    .join(df_DiagCD.alias("Lnk_DiagCD"), F.col("Lnk_FctsInp.IDCD_ID") == F.col("Lnk_DiagCD.DIAG_CD"), "left")
    .select(
        F.col("Lnk_FctsInp.CLCL_ID").alias("CLCL_ID"),
        F.col("Lnk_DiagCD.DIAG_CD_SK").alias("DIAG_CD_SK"),
        F.col("Ordinal.TRGT_CD").alias("CLM_DIAG_ORDNL_CD"),
        F.col("Ordinal.CD_MPPNG_SK").alias("CLM_DIAG_ORDNL_CD_SK"),
        F.col("Admission.CD_MPPNG_SK").alias("CLM_DIAG_POA_CD_SK")
    )
)

# Lkp_IdsDB2 (PxLookup)
df_Lkp_IdsDB2 = (
    df_Lookup_DBs.alias("Lnk_lkpIn")
    .join(
        df_WorkCompnstnClmDiag.alias("Lnk_WorkCompClmDiag"),
        (
            (F.col("Lnk_lkpIn.CLCL_ID") == F.col("Lnk_WorkCompClmDiag.CLM_ID")) &
            (F.col("Lnk_lkpIn.CLM_DIAG_ORDNL_CD") == F.col("Lnk_WorkCompClmDiag.CLM_DIAG_ORDNL_CD"))
        ),
        "left"
    )
    .join(
        df_Ds_ReversalClaim.alias("Lnk_ReversalDs"),
        F.col("Lnk_lkpIn.CLCL_ID") == F.col("Lnk_ReversalDs.CLCL_ID"),
        "left"
    )
    .select(
        F.col("Lnk_lkpIn.CLCL_ID").alias("CLCL_ID"),
        F.col("Lnk_lkpIn.DIAG_CD_SK").alias("DIAG_CD_SK"),
        F.col("Lnk_lkpIn.CLM_DIAG_ORDNL_CD").alias("CLM_DIAG_ORDNL_CD"),
        F.col("Lnk_lkpIn.CLM_DIAG_ORDNL_CD_SK").alias("CLM_DIAG_ORDNL_CD_SK"),
        F.col("Lnk_lkpIn.CLM_DIAG_POA_CD_SK").alias("CLM_DIAG_POA_CD_SK"),
        F.col("Lnk_WorkCompClmDiag.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("Lnk_ReversalDs.CLCL_ID").alias("CLCL_ID_Reverse")
    )
)

# Xfm_Reversal (CTransformerStage)
df_Xfm_Reversal_Lnk_All = df_Lkp_IdsDB2.select(
    F.col("CLCL_ID").alias("CLCL_ID"),
    F.col("DIAG_CD_SK").alias("DIAG_CD_SK"),
    F.col("CLM_DIAG_ORDNL_CD").alias("CLM_DIAG_ORDNL_CD"),
    F.col("CLM_DIAG_ORDNL_CD_SK").alias("CLM_DIAG_ORDNL_CD_SK"),
    F.col("CLM_DIAG_POA_CD_SK").alias("CLM_DIAG_POA_CD_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK")
)

df_Xfm_Reversal_Lnk_Reversal = (
    df_Lkp_IdsDB2
    .filter("trim(coalesce(CLCL_ID_Reverse,'')) <> ''")
    .select(
        F.expr("trim(CLCL_ID) || 'R'").alias("CLCL_ID"),
        F.col("DIAG_CD_SK").alias("DIAG_CD_SK"),
        F.col("CLM_DIAG_ORDNL_CD").alias("CLM_DIAG_ORDNL_CD"),
        F.col("CLM_DIAG_ORDNL_CD_SK").alias("CLM_DIAG_ORDNL_CD_SK"),
        F.col("CLM_DIAG_POA_CD_SK").alias("CLM_DIAG_POA_CD_SK"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK")
    )
)

# Fnl_All_Reverse (PxFunnel)
df_Fnl_All_Reverse = df_Xfm_Reversal_Lnk_All.unionByName(df_Xfm_Reversal_Lnk_Reversal)

# Xfm_BusinessLogic (CTransformerStage)
df_Xfm_BusinessLogic_Lnk_B_WorkCompnstnClmDiag = df_Fnl_All_Reverse.select(
    F.col("CLCL_ID").alias("CLM_ID"),
    F.when(F.col("CLM_DIAG_ORDNL_CD").isNull(), F.lit("")).otherwise(F.col("CLM_DIAG_ORDNL_CD")).alias("CLM_DIAG_ORDNL_CD"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD")
)

df_Xfm_BusinessLogic_Lnk_WorkCompnstnClmDiag = df_Fnl_All_Reverse.select(
    F.col("CLCL_ID").alias("CLM_ID"),
    F.when(F.col("CLM_DIAG_ORDNL_CD").isNull(), F.lit("")).otherwise(F.col("CLM_DIAG_ORDNL_CD")).alias("CLM_DIAG_ORDNL_CD"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.when(
        (F.col("CRT_RUN_CYC_EXCTN_SK").isNull()) | (F.col("CRT_RUN_CYC_EXCTN_SK") == 0),
        F.lit(RunCycle)
    ).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK")).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(RunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.when(F.col("DIAG_CD_SK").isNull(), F.lit(0)).otherwise(F.col("DIAG_CD_SK")).alias("DIAG_CD_SK"),
    F.when(F.col("CLM_DIAG_ORDNL_CD_SK").isNull(), F.lit(0)).otherwise(F.col("CLM_DIAG_ORDNL_CD_SK")).alias("CLM_DIAG_ORDNL_CD_SK"),
    F.when(
        F.col("CLM_DIAG_POA_CD_SK").isNull(),
        F.lit(1)
    ).when(
        F.trim(F.col("CLM_DIAG_POA_CD_SK")) == "",
        F.lit(1)
    ).otherwise(
        F.col("CLM_DIAG_POA_CD_SK")
    ).alias("CLM_DIAG_POA_CD_SK")
)

# WORK_COMPNSTN_CLM_DIAG (PxSequentialFile)
df_WORK_COMPNSTN_CLM_DIAG = df_Xfm_BusinessLogic_Lnk_WorkCompnstnClmDiag.select(
    "CLM_ID",
    "CLM_DIAG_ORDNL_CD",
    "SRC_SYS_CD",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "DIAG_CD_SK",
    "CLM_DIAG_ORDNL_CD_SK",
    "CLM_DIAG_POA_CD_SK"
)
df_WORK_COMPNSTN_CLM_DIAG = df_WORK_COMPNSTN_CLM_DIAG\
    .withColumn("CLM_ID", F.rpad("CLM_ID", 12, " "))\
    .withColumn("CLM_DIAG_ORDNL_CD", F.rpad("CLM_DIAG_ORDNL_CD", <...>, " "))\
    .withColumn("SRC_SYS_CD", F.rpad("SRC_SYS_CD", <...>, " "))

write_files(
    df_WORK_COMPNSTN_CLM_DIAG,
    f"{adls_path}/load/WORK_COMPNSTN_CLM_DIAG.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# B_WORK_COMPNSTN_CLM_DIAG (PxSequentialFile)
df_B_WORK_COMPNSTN_CLM_DIAG = df_Xfm_BusinessLogic_Lnk_B_WorkCompnstnClmDiag.select(
    "CLM_ID",
    "CLM_DIAG_ORDNL_CD",
    "SRC_SYS_CD"
)
df_B_WORK_COMPNSTN_CLM_DIAG = df_B_WORK_COMPNSTN_CLM_DIAG\
    .withColumn("CLM_ID", F.rpad("CLM_ID", 12, " "))\
    .withColumn("CLM_DIAG_ORDNL_CD", F.rpad("CLM_DIAG_ORDNL_CD", <...>, " "))\
    .withColumn("SRC_SYS_CD", F.rpad("SRC_SYS_CD", <...>, " "))

write_files(
    df_B_WORK_COMPNSTN_CLM_DIAG,
    f"{adls_path}/load/B_WORK_COMPNSTN_CLM_DIAG.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)