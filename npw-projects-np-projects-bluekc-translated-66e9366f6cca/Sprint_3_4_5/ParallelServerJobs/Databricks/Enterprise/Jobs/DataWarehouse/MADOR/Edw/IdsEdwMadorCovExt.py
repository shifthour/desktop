# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2018 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY : IdsEdwMadorSeq
# MAGIC 
# MAGIC PROCESSING : Extracts coverage data from IDS and writes to the file MBR_MA_DOR_COV_F.dat
# MAGIC 
# MAGIC MODIFICATIONS :
# MAGIC 
# MAGIC Developer                                 Date                 \(9)Project                                                Change Description                                               Development Project\(9)          Code Reviewer          Date Reviewed       
# MAGIC ------------------                               --------------------     \(9)------------------------                             -----------------------------------------------------------------------             ------------------------------\(9)         -------------------------------   ----------------------------       
# MAGIC Sethuraman Rajendran           \(9) 2018-04-12\(9)5839\(9)                                          Original Programming\(9)\(9)\(9)          EnterpriseDev2                        Kalyan Neelam           2018-06-14

# MAGIC Extracts coverage data from IDS and writes to the file MBR_MA_DOR_COV_F.dat
# MAGIC Extract Coverage Data from IDS
# MAGIC Edit data and format into table format
# MAGIC Create the Load file MBR_MA_DOR_COV_F.dat
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

# Parameters
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
SrcSysCd = get_widget_value('SrcSysCd','')
RunID = get_widget_value('RunID','')
FctsRunCycle = get_widget_value('FctsRunCycle','')
BcbsScRunCycle = get_widget_value('BcbsScRunCycle','')
UwsRunCycle = get_widget_value('UwsRunCycle','')
EdwRunCycleDate = get_widget_value('EdwRunCycleDate','')
EdwRunCycle = get_widget_value('EdwRunCycle','')

# STAGE: MBR_MA_DOR_COV (DB2ConnectorPX)
query_MBR_MA_DOR_COV = f"""SELECT 
MBR_MA_DOR_COV_SK, 
MBR_ID, 
SUB_ID, 
GRP_ID, 
PROD_ID, 
MBR_COV_EFF_DT, 
TAX_YR, 
SRC_SYS_CD, 
CRT_RUN_CYC_EXCTN_SK, 
LAST_UPDT_RUN_CYC_EXCTN_SK, 
AS_OF_DTM, 
GRP_SK, 
MBR_MA_DOR_SK, 
MBR_SK, 
PROD_SK, 
SUB_SK, 
NON_CRBL_COV_RSN_CD_SK, 
PROD_CRBL_COV_CD_SK, 
HSA_ACCT_IN, 
ON_EXCL_LIST_IN, 
MBR_COV_TERM_DT, 
FMLY_IN_NTWK_DEDCT_AMT, 
FMLY_IN_NTWK_OOP_AMT, 
FMLY_PDX_DEDCT_AMT, 
INDV_IN_NTWK_DEDCT_AMT, 
INDV_IN_NTWK_OOP_AMT, 
INDV_PDX_DEDCT_AMT
FROM {IDSOwner}.MBR_MA_DOR_COV
WHERE 
(SRC_SYS_CD = 'FACETS' AND LAST_UPDT_RUN_CYC_EXCTN_SK >= {FctsRunCycle}
 OR SRC_SYS_CD = 'BCBSSC' AND LAST_UPDT_RUN_CYC_EXCTN_SK >= {BcbsScRunCycle}
 OR SRC_SYS_CD = 'UWS' AND LAST_UPDT_RUN_CYC_EXCTN_SK >= {UwsRunCycle})"""

jdbc_url_MBR_MA_DOR_COV, jdbc_props_MBR_MA_DOR_COV = get_db_config(ids_secret_name)
df_MBR_MA_DOR_COV = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_MBR_MA_DOR_COV)
    .options(**jdbc_props_MBR_MA_DOR_COV)
    .option("query", query_MBR_MA_DOR_COV)
    .load()
)

# STAGE: Cd_Mppng (DB2ConnectorPX)
query_Cd_Mppng = f"""SELECT 
CD_MPPNG_SK, 
SRC_DRVD_LKUP_VAL, 
TRGT_CD, 
TRGT_CD_NM 
FROM {IDSOwner}.CD_MPPNG CD_MPPNG
WHERE SRC_SYS_CD = 'FACETS'
  AND SRC_CLCTN_CD = 'FACETS DBO'
  AND SRC_DOMAIN_NM = 'NON CREDITABLE COVERAGE REASON'
  AND TRGT_CLCTN_CD = 'IDS'
  AND TRGT_DOMAIN_NM = 'NON CREDITABLE COVERAGE REASON'
"""

jdbc_url_Cd_Mppng, jdbc_props_Cd_Mppng = get_db_config(ids_secret_name)
df_Cd_Mppng = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_Cd_Mppng)
    .options(**jdbc_props_Cd_Mppng)
    .option("query", query_Cd_Mppng)
    .load()
)

# STAGE: Cd_Mppng_2 (DB2ConnectorPX)
query_Cd_Mppng_2 = f"""SELECT 
CD_MPPNG_SK, 
SRC_DRVD_LKUP_VAL, 
TRGT_CD AS TRGT_CD2, 
TRGT_CD_NM AS TRGT_CD_NM2
FROM {IDSOwner}.CD_MPPNG CD_MPPNG
WHERE TRGT_CLCTN_CD = 'IDS'
  AND TRGT_SRC_SYS_CD = 'IDS'
  AND SRC_DOMAIN_NM = 'PRODUCT CREDITABLE COVERAGE'
"""

jdbc_url_Cd_Mppng_2, jdbc_props_Cd_Mppng_2 = get_db_config(ids_secret_name)
df_Cd_Mppng_2 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_Cd_Mppng_2)
    .options(**jdbc_props_Cd_Mppng_2)
    .option("query", query_Cd_Mppng_2)
    .load()
)

# STAGE: Lkp_Cov (PxLookup)
df_Lkp_Cov = (
    df_MBR_MA_DOR_COV.alias("MBR_MA_DOR_COV_Out")
    .join(
        df_Cd_Mppng.alias("Lnk_Cd_Mppng"),
        F.col("MBR_MA_DOR_COV_Out.NON_CRBL_COV_RSN_CD_SK") == F.col("Lnk_Cd_Mppng.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_Cd_Mppng_2.alias("Lnk_Cd_Mppng2"),
        F.col("MBR_MA_DOR_COV_Out.PROD_CRBL_COV_CD_SK") == F.col("Lnk_Cd_Mppng2.CD_MPPNG_SK"),
        "left"
    )
    .select(
        F.col("MBR_MA_DOR_COV_Out.MBR_MA_DOR_COV_SK").alias("MBR_MA_DOR_COV_SK"),
        F.col("MBR_MA_DOR_COV_Out.MBR_ID").alias("MBR_ID"),
        F.col("MBR_MA_DOR_COV_Out.SUB_ID").alias("SUB_ID"),
        F.col("MBR_MA_DOR_COV_Out.GRP_ID").alias("GRP_ID"),
        F.col("MBR_MA_DOR_COV_Out.PROD_ID").alias("PROD_ID"),
        F.col("MBR_MA_DOR_COV_Out.MBR_COV_EFF_DT").alias("MBR_COV_EFF_DT"),
        F.col("MBR_MA_DOR_COV_Out.TAX_YR").alias("TAX_YR"),
        F.col("MBR_MA_DOR_COV_Out.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("MBR_MA_DOR_COV_Out.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("MBR_MA_DOR_COV_Out.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("MBR_MA_DOR_COV_Out.AS_OF_DTM").alias("AS_OF_DTM"),
        F.col("MBR_MA_DOR_COV_Out.GRP_SK").alias("GRP_SK"),
        F.col("MBR_MA_DOR_COV_Out.MBR_MA_DOR_SK").alias("MBR_MA_DOR_SK"),
        F.col("MBR_MA_DOR_COV_Out.MBR_SK").alias("MBR_SK"),
        F.col("MBR_MA_DOR_COV_Out.PROD_SK").alias("PROD_SK"),
        F.col("MBR_MA_DOR_COV_Out.SUB_SK").alias("SUB_SK"),
        F.col("MBR_MA_DOR_COV_Out.NON_CRBL_COV_RSN_CD_SK").alias("NON_CRBL_COV_RSN_CD_SK"),
        F.col("MBR_MA_DOR_COV_Out.PROD_CRBL_COV_CD_SK").alias("PROD_CRBL_COV_CD_SK"),
        F.col("MBR_MA_DOR_COV_Out.HSA_ACCT_IN").alias("HSA_ACCT_IN"),
        F.col("MBR_MA_DOR_COV_Out.ON_EXCL_LIST_IN").alias("ON_EXCL_LIST_IN"),
        F.col("MBR_MA_DOR_COV_Out.MBR_COV_TERM_DT").alias("MBR_COV_TERM_DT"),
        F.col("MBR_MA_DOR_COV_Out.FMLY_IN_NTWK_DEDCT_AMT").alias("FMLY_IN_NTWK_DEDCT_AMT"),
        F.col("MBR_MA_DOR_COV_Out.FMLY_IN_NTWK_OOP_AMT").alias("FMLY_IN_NTWK_OOP_AMT"),
        F.col("MBR_MA_DOR_COV_Out.FMLY_PDX_DEDCT_AMT").alias("FMLY_PDX_DEDCT_AMT"),
        F.col("MBR_MA_DOR_COV_Out.INDV_IN_NTWK_DEDCT_AMT").alias("INDV_IN_NTWK_DEDCT_AMT"),
        F.col("MBR_MA_DOR_COV_Out.INDV_IN_NTWK_OOP_AMT").alias("INDV_IN_NTWK_OOP_AMT"),
        F.col("MBR_MA_DOR_COV_Out.INDV_PDX_DEDCT_AMT").alias("INDV_PDX_DEDCT_AMT"),
        F.col("Lnk_Cd_Mppng.TRGT_CD").alias("TRGT_CD"),
        F.col("Lnk_Cd_Mppng.TRGT_CD_NM").alias("TRGT_CD_NM"),
        F.col("Lnk_Cd_Mppng2.TRGT_CD2").alias("TRGT_CD2"),
        F.col("Lnk_Cd_Mppng2.TRGT_CD_NM2").alias("TRGT_CD_NM2")
    )
)

# STAGE: Trf (CTransformerStage)
df_Trf = df_Lkp_Cov
df_Trf = df_Trf.withColumn(
    "MBR_COV_EFF_DT",
    TimestampToDate(F.col("MBR_COV_EFF_DT"))
)

df_Trf = df_Trf.withColumn(
    "TAX_YR",
    F.col("TAX_YR")
)

df_Trf = df_Trf.withColumn(
    "SRC_SYS_CD",
    F.col("SRC_SYS_CD")
)

df_Trf = df_Trf.withColumn(
    "CRT_RUN_CYC_EXCTN_DT_SK",
    F.lit(EdwRunCycleDate)
)

df_Trf = df_Trf.withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    F.lit(EdwRunCycleDate)
)

df_Trf = df_Trf.withColumn(
    "NON_CRBL_COV_RSN_CD",
    F.when(
        trim(F.when(F.col("TRGT_CD").isNotNull(), F.col("TRGT_CD")).otherwise("")) == "",
        F.lit("NA")
    ).otherwise(F.col("TRGT_CD"))
)

df_Trf = df_Trf.withColumn(
    "NON_CRBL_COV_RSN_NM",
    F.when(
        trim(F.when(F.col("TRGT_CD_NM").isNotNull(), F.col("TRGT_CD_NM")).otherwise("")) == "",
        F.lit("NA")
    ).otherwise(trim(F.when(F.col("TRGT_CD_NM").isNotNull(), F.col("TRGT_CD_NM")).otherwise("")))
)

df_Trf = df_Trf.withColumn(
    "PROD_CRBL_COV_CD",
    F.when(
        trim(F.when(F.col("TRGT_CD2").isNotNull(), F.col("TRGT_CD2")).otherwise("")) == "",
        F.lit("U")
    ).otherwise(F.col("TRGT_CD2"))
)

df_Trf = df_Trf.withColumn(
    "PROD_CRBL_COV_NM",
    F.when(
        trim(F.when(F.col("TRGT_CD_NM2").isNotNull(), F.col("TRGT_CD_NM2")).otherwise("")) == "",
        F.lit("UNKNOWN")
    ).otherwise(trim(F.when(F.col("TRGT_CD_NM2").isNotNull(), F.col("TRGT_CD_NM2")).otherwise("")))
)

df_Trf = df_Trf.withColumn(
    "MBR_COV_TERM_DT",
    TimestampToDate(F.col("MBR_COV_TERM_DT"))
)

df_Trf = df_Trf.withColumn(
    "CRT_RUN_CYC_EXCTN_SK",
    F.lit(EdwRunCycle)
)

df_Trf = df_Trf.withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    F.lit(EdwRunCycle)
)

df_Trf = df_Trf.withColumn(
    "IDS_LAST_UPDT_RUN_CYC_EXCTN_SK",
    F.when(F.col("SRC_SYS_CD") == F.lit("FACETS"), F.lit(FctsRunCycle))
    .when(F.col("SRC_SYS_CD") == F.lit("BCBSSC"), F.lit(BcbsScRunCycle))
    .when(F.col("SRC_SYS_CD") == F.lit("UWS"), F.lit(UwsRunCycle))
    .otherwise(F.lit(1))
)

# Final select with correct order and rpad for char columns
df_final = df_Trf.select(
    F.col("MBR_MA_DOR_COV_SK"),
    F.col("MBR_ID"),
    F.col("SUB_ID"),
    F.col("GRP_ID"),
    F.col("PROD_ID"),
    F.col("MBR_COV_EFF_DT"),
    F.rpad(F.col("TAX_YR"), 4, " ").alias("TAX_YR"),
    F.col("SRC_SYS_CD"),
    F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("AS_OF_DTM"),
    F.col("GRP_SK"),
    F.col("MBR_MA_DOR_SK"),
    F.col("MBR_SK"),
    F.col("PROD_SK"),
    F.col("SUB_SK"),
    F.col("NON_CRBL_COV_RSN_CD"),
    F.col("NON_CRBL_COV_RSN_NM"),
    F.col("PROD_CRBL_COV_CD"),
    F.col("PROD_CRBL_COV_NM"),
    F.rpad(F.col("HSA_ACCT_IN"), 1, " ").alias("HSA_ACCT_IN"),
    F.rpad(F.col("ON_EXCL_LIST_IN"), 1, " ").alias("ON_EXCL_LIST_IN"),
    F.col("MBR_COV_TERM_DT"),
    F.col("FMLY_IN_NTWK_DEDCT_AMT"),
    F.col("FMLY_IN_NTWK_OOP_AMT"),
    F.col("FMLY_PDX_DEDCT_AMT"),
    F.col("INDV_IN_NTWK_DEDCT_AMT"),
    F.col("INDV_IN_NTWK_OOP_AMT"),
    F.col("INDV_PDX_DEDCT_AMT"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("NON_CRBL_COV_RSN_CD_SK"),
    F.col("PROD_CRBL_COV_CD_SK")
)

# STAGE: MBR_MA_DOR_COV_F_File (PxSequentialFile)
output_file_path = f"{adls_path}/load/MBR_MA_DOR_COV_F.{SrcSysCd}.{RunID}.dat"
write_files(
    df_final,
    output_file_path,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)