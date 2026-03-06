# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC PROCESSING:  Extracting data from EPSD_TREAT_GRP
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date               Project/Altiris #               Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------    ---------------------------------------------------------   ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Bhoomi Dasari                1/03/2008         CLINICALS/3044            Originally Programmed                        devlEDW10                  Steph Goddard             01/07/2008
# MAGIC 
# MAGIC Raj Mangalampally         10/30/2013        5114                              Original Programming                         EnterpriseWrhsDevl        Jag Yelavarthi              2013-12-09
# MAGIC                                                                                                         (Server to parallel Conv)

# MAGIC Create a Load ready file EPSD_TREAT_GRP_D table.
# MAGIC JobName: IdsEdwEpsdTreatGrpDExtr
# MAGIC Data extracted from IDS table EPSD_TREAT_GRP and Join on Code mapping table and bring SRC_SYS_CD  for Denormalization
# MAGIC Code SK lookups for Denormalization
# MAGIC 
# MAGIC Lookup Keys:
# MAGIC CD_SK
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
ExtractRunCycle = get_widget_value('ExtractRunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query_db2_EPSD_TREAT_GRP_D_in = f"""
SELECT 
EPSD.EPSD_TREAT_GRP_SK,
COALESCE(CD.TRGT_CD,'UNK') as SRC_SYS_CD,
EPSD.EPSD_TREAT_GRP_CD,
EPSD.CRT_RUN_CYC_EXCTN_SK,
EPSD.LAST_UPDT_RUN_CYC_EXCTN_SK,
EPSD.MAJ_PRCTC_CAT_CD_SK,
EPSD.EPSD_TREAT_GRP_DESC,
EPSD.EPSD_TREAT_GRP_LABEL
FROM {IDSOwner}.EPSD_TREAT_GRP EPSD
LEFT JOIN {IDSOwner}.CD_MPPNG CD
ON EPSD.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
WHERE EPSD.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtractRunCycle}
"""

df_db2_EPSD_TREAT_GRP_D_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_EPSD_TREAT_GRP_D_in)
    .load()
)

df_lnk_IdsEdwEpsdTreatGrpDExtr_in_ABC = df_db2_EPSD_TREAT_GRP_D_in

extract_query_db2_CD_MPPNG_in = f"""
SELECT  
CD_MPPNG_SK,
COALESCE(TRGT_CD,'UNK') as TRGT_CD,
COALESCE(TRGT_CD_NM,'UNK') as TRGT_CD_NM
FROM {IDSOwner}.CD_MPPNG
"""

df_db2_CD_MPPNG_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CD_MPPNG_in)
    .load()
)

df_Ref_MajPrctCatCdLkup = df_db2_CD_MPPNG_in

df_lkp_CdMppng_joined = df_lnk_IdsEdwEpsdTreatGrpDExtr_in_ABC.alias("lnk_IdsEdwEpsdTreatGrpDExtr_in_ABC").join(
    df_Ref_MajPrctCatCdLkup.alias("Ref_MajPrctCatCdLkup"),
    F.col("lnk_IdsEdwEpsdTreatGrpDExtr_in_ABC.MAJ_PRCTC_CAT_CD_SK") == F.col("Ref_MajPrctCatCdLkup.CD_MPPNG_SK"),
    "left"
)

df_lnk_CodesData_out = df_lkp_CdMppng_joined.select(
    F.col("lnk_IdsEdwEpsdTreatGrpDExtr_in_ABC.EPSD_TREAT_GRP_SK").alias("EPSD_TREAT_GRP_SK"),
    F.col("lnk_IdsEdwEpsdTreatGrpDExtr_in_ABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnk_IdsEdwEpsdTreatGrpDExtr_in_ABC.EPSD_TREAT_GRP_CD").alias("EPSD_TREAT_GRP_CD"),
    F.col("Ref_MajPrctCatCdLkup.TRGT_CD").alias("TRGT_CD"),
    F.col("Ref_MajPrctCatCdLkup.TRGT_CD_NM").alias("TRGT_CD_NM"),
    F.col("lnk_IdsEdwEpsdTreatGrpDExtr_in_ABC.EPSD_TREAT_GRP_DESC").alias("EPSD_TREAT_GRP_DESC"),
    F.col("lnk_IdsEdwEpsdTreatGrpDExtr_in_ABC.EPSD_TREAT_GRP_LABEL").alias("EPSD_TREAT_GRP_LABEL")
)

df_main_filtered = df_lnk_CodesData_out.filter(
    (F.col("EPSD_TREAT_GRP_SK") != 0) & (F.col("EPSD_TREAT_GRP_SK") != 1)
).select(
    F.col("EPSD_TREAT_GRP_SK").alias("EPSD_TREAT_GRP_SK"),
    F.when(trim(F.col("SRC_SYS_CD")) == '', 'UNK').otherwise(F.col("SRC_SYS_CD")).alias("SRC_SYS_CD"),
    F.col("EPSD_TREAT_GRP_CD").alias("EPSD_TREAT_GRP_CD"),
    F.lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.when(trim(F.col("TRGT_CD")) == '', 'UNK').otherwise(F.col("TRGT_CD")).alias("MAJ_PRCTC_CAT_CD"),
    F.when(trim(F.col("TRGT_CD_NM")) == '', 'UNK').otherwise(F.col("TRGT_CD_NM")).alias("MAJ_PRCTC_CAT_DESC"),
    F.col("EPSD_TREAT_GRP_DESC").alias("EPSD_TREAT_GRP_DESC"),
    F.col("EPSD_TREAT_GRP_LABEL").alias("EPSD_TREAT_GRP_LABEL"),
    F.lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

df_lnk_UNK_Out = df_lnk_CodesData_out.limit(1).select(
    F.lit(0).alias("EPSD_TREAT_GRP_SK"),
    F.lit("UNK").alias("SRC_SYS_CD"),
    F.lit("UNK").alias("EPSD_TREAT_GRP_CD"),
    F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.lit("UNK").alias("MAJ_PRCTC_CAT_CD"),
    F.lit("").alias("MAJ_PRCTC_CAT_DESC"),
    F.lit("").alias("EPSD_TREAT_GRP_DESC"),
    F.lit("").alias("EPSD_TREAT_GRP_LABEL"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

df_lnk_NA_Out = df_lnk_CodesData_out.limit(1).select(
    F.lit(1).alias("EPSD_TREAT_GRP_SK"),
    F.lit("NA").alias("SRC_SYS_CD"),
    F.lit("NA").alias("EPSD_TREAT_GRP_CD"),
    F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.lit("NA").alias("MAJ_PRCTC_CAT_CD"),
    F.lit("").alias("MAJ_PRCTC_CAT_DESC"),
    F.lit("").alias("EPSD_TREAT_GRP_DESC"),
    F.lit("").alias("EPSD_TREAT_GRP_LABEL"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

df_fnl_UNK_NA_data = df_lnk_NA_Out.unionByName(df_lnk_UNK_Out).unionByName(df_main_filtered)

df_lnk_IdsEdwEpsdTreatGrpDExtr_outABC = df_fnl_UNK_NA_data

df_out = df_lnk_IdsEdwEpsdTreatGrpDExtr_outABC.select(
    F.col("EPSD_TREAT_GRP_SK"),
    F.col("SRC_SYS_CD"),
    F.col("EPSD_TREAT_GRP_CD"),
    rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("MAJ_PRCTC_CAT_CD"),
    F.col("MAJ_PRCTC_CAT_DESC"),
    F.col("EPSD_TREAT_GRP_DESC"),
    F.col("EPSD_TREAT_GRP_LABEL"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

write_files(
    df_out,
    f"{adls_path}/load/EPSD_TREAT_GRP_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)