# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2006,2007,2008,2009 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:    Pulls data from IDS Facets table PCA_ALLOC into EDW table PCA_ALLOC_F
# MAGIC    
# MAGIC              
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date               Project/Altiris #                     Change Description                                                                           Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------             --------------------------------------------------------                                                  ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Bhoomi Dasari                 11/22/2006                                                   Originally Programmed
# MAGIC 
# MAGIC Judy Reynolds/Bhoomi   06/04/09       #3745 Defined Contribution     Added additional new fields to output file                                          devlEDWnew                 
# MAGIC 
# MAGIC Archana Palivela     05/23/2013        5114                                           Originally Programmed  (In Parallel)                                                  EnterpriseWhseDevl      Bhoomi Dasari             8/11/2013

# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC WritePCA_ALLOC_F Data into a Sequential file for Load Ready Job.
# MAGIC Job name: IdsEdwPcaAllocFExtr
# MAGIC 
# MAGIC 
# MAGIC Job Extract data from IDS table and apply Fkey and Code denormalization, create a load ready file.
# MAGIC Read data from source table 
# MAGIC PCA_ALLOC
# MAGIC Code SK lookups for Denormalization
# MAGIC 
# MAGIC Lookup Keys:
# MAGIC CD_MPPNG_SK
# MAGIC PCA_ALLOC_DEDCT_ALT_TYP_CD_SK
# MAGIC PCA_ALLOC_DEDCT_STD_TYP_CD_SK
# MAGIC PCA_ALLOC_METH_CD_SK
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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


# Retrieve parameter values
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')

# DB2ConnectorPX stage: db2_CD_MPPNG_Extr
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_db2_CD_MPPNG_Extr = f"""
SELECT 
CD_MPPNG_SK,
COALESCE(TRGT_CD,'UNK') TRGT_CD,
COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM
from {IDSOwner}.CD_MPPNG
"""
df_db2_CD_MPPNG_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_CD_MPPNG_Extr)
    .load()
)

# DB2ConnectorPX stage: db2_PCA_ALLOC_Extr
extract_query_db2_PCA_ALLOC_Extr = f"""
SELECT 
PCA_ALLOC_SK,
COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD, 
PCA_ALLOC_PFX_ID,
EFF_DT_SK,
TERM_DT_SK,
FMLY_ALLOC_AMT,
INDV_ALLOC_AMT,
PCA_ALLOC_DEDCT_ALT_TYP_CD_SK,
PCA_ALLOC_DEDCT_STD_TYP_CD_SK,
PCA_ALLOC_METH_CD_SK,
FMLY_ALT_DEDCT_AMT,
FMLY_MAX_CAROVR_AMT,
FMLY_STD_DEDCT_AMT,
INDV_ALT_DEDCT_AMT,
INDV_MAX_CAROVR_AMT,
INDV_STD_DEDCT_AMT,
SUB_DPNDT_ALLOC_AMT,
SUB_DPNDT_ALT_DEDCT_AMT,
SUB_DPNDT_MAX_CAROVR_AMT,
SUB_DPNDT_STD_DEDCT_AMT,
SUB_1_DPNDT_ALLOC_AMT,
SUB_1_DPNDT_ALT_DEDCT_AMT,
SUB_1_DPNDT_MAX_CAROVR_AMT,
SUB_1_DPNDT_STD_DEDCT_AMT 
FROM {IDSOwner}.PCA_ALLOC PCA_ALLOC
LEFT JOIN {IDSOwner}.CD_MPPNG CD
ON PCA_ALLOC.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
"""
df_db2_PCA_ALLOC_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_PCA_ALLOC_Extr)
    .load()
)

# PxCopy stage: cpy_cd_mppng
df_RefPcaAllocDedctAltTyp = df_db2_CD_MPPNG_Extr.selectExpr(
    "CD_MPPNG_SK as CD_MPPNG_SK",
    "TRGT_CD as TRGT_CD",
    "TRGT_CD_NM as TRGT_CD_NM"
)
df_RefPcaAllocDedctStdTyp = df_db2_CD_MPPNG_Extr.selectExpr(
    "CD_MPPNG_SK as CD_MPPNG_SK",
    "TRGT_CD as TRGT_CD",
    "TRGT_CD_NM as TRGT_CD_NM"
)
df_RefPcaAllocMeth = df_db2_CD_MPPNG_Extr.selectExpr(
    "CD_MPPNG_SK as CD_MPPNG_SK",
    "TRGT_CD as TRGT_CD",
    "TRGT_CD_NM as TRGT_CD_NM"
)

# PxLookup stage: lkp_FKeys
df_lkp_FKeys = (
    df_db2_PCA_ALLOC_Extr.alias("Lnk_IdsEdwPcaAllocFExtr_InABC")
    .join(
        df_RefPcaAllocDedctAltTyp.alias("RefPcaAllocDedctAltTyp"),
        F.col("Lnk_IdsEdwPcaAllocFExtr_InABC.PCA_ALLOC_DEDCT_ALT_TYP_CD_SK")
        == F.col("RefPcaAllocDedctAltTyp.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_RefPcaAllocDedctStdTyp.alias("RefPcaAllocDedctStdTyp"),
        F.col("Lnk_IdsEdwPcaAllocFExtr_InABC.PCA_ALLOC_DEDCT_STD_TYP_CD_SK")
        == F.col("RefPcaAllocDedctStdTyp.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_RefPcaAllocMeth.alias("RefPcaAllocMeth"),
        F.col("Lnk_IdsEdwPcaAllocFExtr_InABC.PCA_ALLOC_METH_CD_SK")
        == F.col("RefPcaAllocMeth.CD_MPPNG_SK"),
        "left"
    )
    .select(
        F.col("Lnk_IdsEdwPcaAllocFExtr_InABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("RefPcaAllocDedctAltTyp.TRGT_CD").alias("PCA_ALLOC_DEDCT_ALT_TYP_CD"),
        F.col("RefPcaAllocDedctAltTyp.TRGT_CD_NM").alias("PCA_ALLOC_DEDCT_ALT_TYP_NM"),
        F.col("RefPcaAllocDedctStdTyp.TRGT_CD").alias("PCA_ALLOC_DEDCT_STD_TYP_CD"),
        F.col("RefPcaAllocDedctStdTyp.TRGT_CD_NM").alias("PCA_ALLOC_DEDCT_STD_TYP_NM"),
        F.col("RefPcaAllocMeth.TRGT_CD").alias("PCA_ALLOC_METH_CD"),
        F.col("RefPcaAllocMeth.TRGT_CD_NM").alias("PCA_ALLOC_METH_NM"),
        F.col("Lnk_IdsEdwPcaAllocFExtr_InABC.PCA_ALLOC_SK").alias("PCA_ALLOC_SK"),
        F.col("Lnk_IdsEdwPcaAllocFExtr_InABC.PCA_ALLOC_PFX_ID").alias("PCA_ALLOC_PFX_ID"),
        F.col("Lnk_IdsEdwPcaAllocFExtr_InABC.EFF_DT_SK").alias("EFF_DT_SK"),
        F.col("Lnk_IdsEdwPcaAllocFExtr_InABC.TERM_DT_SK").alias("PCA_ALLOC_TERM_DT_SK"),
        F.col("Lnk_IdsEdwPcaAllocFExtr_InABC.FMLY_ALLOC_AMT").alias("PCA_ALLOC_FMLY_ALLOC_AMT"),
        F.col("Lnk_IdsEdwPcaAllocFExtr_InABC.FMLY_ALT_DEDCT_AMT").alias("PCA_ALLOC_FMLY_ALT_DEDCT_AMT"),
        F.col("Lnk_IdsEdwPcaAllocFExtr_InABC.FMLY_MAX_CAROVR_AMT").alias("PCA_ALLOC_FMLY_MAX_CAROVR_AMT"),
        F.col("Lnk_IdsEdwPcaAllocFExtr_InABC.FMLY_STD_DEDCT_AMT").alias("PCA_ALLOC_FMLY_STD_DEDCT_AMT"),
        F.col("Lnk_IdsEdwPcaAllocFExtr_InABC.INDV_ALLOC_AMT").alias("PCA_ALLOC_INDV_ALLOC_AMT"),
        F.col("Lnk_IdsEdwPcaAllocFExtr_InABC.INDV_ALT_DEDCT_AMT").alias("PCA_ALLOC_INDV_ALT_DEDCT_AMT"),
        F.col("Lnk_IdsEdwPcaAllocFExtr_InABC.INDV_MAX_CAROVR_AMT").alias("PCA_ALLOC_INDV_MAX_CAROVR_AMT"),
        F.col("Lnk_IdsEdwPcaAllocFExtr_InABC.INDV_STD_DEDCT_AMT").alias("PCA_ALLOC_INDV_STD_DEDCT_AMT"),
        F.col("Lnk_IdsEdwPcaAllocFExtr_InABC.SUB_DPNDT_ALLOC_AMT").alias("PCA_ALLOC_SUB_DPNDT_ALLOC_AMT"),
        F.col("Lnk_IdsEdwPcaAllocFExtr_InABC.SUB_DPNDT_ALT_DEDCT_AMT").alias("PCA_ALC_SUB_DPNDT_ALT_DED_AMT"),
        F.col("Lnk_IdsEdwPcaAllocFExtr_InABC.SUB_DPNDT_MAX_CAROVR_AMT").alias("PCA_ALC_SUB_DPT_MAXCAROVER_AMT"),
        F.col("Lnk_IdsEdwPcaAllocFExtr_InABC.SUB_DPNDT_STD_DEDCT_AMT").alias("PCA_ALC_SUB_DPNDT_STD_DED_AMT"),
        F.col("Lnk_IdsEdwPcaAllocFExtr_InABC.SUB_1_DPNDT_STD_DEDCT_AMT").alias("PCA_ALC_SUB_1DPNDT_STD_DED_AMT"),
        F.col("Lnk_IdsEdwPcaAllocFExtr_InABC.SUB_1_DPNDT_ALLOC_AMT").alias("PCA_ALLOC_SUB_1DPNDT_ALLOC_AMT"),
        F.col("Lnk_IdsEdwPcaAllocFExtr_InABC.SUB_1_DPNDT_ALT_DEDCT_AMT").alias("PCA_ALC_SUB_1DPNDT_ALT_DED_AMT"),
        F.col("Lnk_IdsEdwPcaAllocFExtr_InABC.SUB_1_DPNDT_MAX_CAROVR_AMT").alias("PCA_ALC_SUB_1DPT_MAXCAROVR_AMT"),
        F.col("Lnk_IdsEdwPcaAllocFExtr_InABC.PCA_ALLOC_DEDCT_ALT_TYP_CD_SK").alias("PCA_ALLOC_DEDCT_ALT_TYP_CD_SK"),
        F.col("Lnk_IdsEdwPcaAllocFExtr_InABC.PCA_ALLOC_DEDCT_STD_TYP_CD_SK").alias("PCA_ALLOC_DEDCT_STD_TYP_CD_SK"),
        F.col("Lnk_IdsEdwPcaAllocFExtr_InABC.PCA_ALLOC_METH_CD_SK").alias("PCA_ALLOC_METH_CD_SK")
    )
)

# CTransformerStage: xmf_businessLogic
df_xmf_businessLogic = df_lkp_FKeys.select(
    F.col("PCA_ALLOC_SK").alias("PCA_ALLOC_SK"),
    F.when(trim(F.col("SRC_SYS_CD")) == "", "UNK").otherwise(trim(F.col("SRC_SYS_CD"))).alias("SRC_SYS_CD"),
    F.col("PCA_ALLOC_PFX_ID").alias("PCA_ALLOC_PFX_ID"),
    F.col("EFF_DT_SK").alias("EFF_DT_SK"),
    F.lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.when(trim(F.col("PCA_ALLOC_DEDCT_ALT_TYP_CD")) == "", "UNK").otherwise(trim(F.col("PCA_ALLOC_DEDCT_ALT_TYP_CD"))).alias("PCA_ALLOC_DEDCT_ALT_TYP_CD"),
    F.when(trim(F.col("PCA_ALLOC_DEDCT_ALT_TYP_NM")) == "", "UNK").otherwise(trim(F.col("PCA_ALLOC_DEDCT_ALT_TYP_NM"))).alias("PCA_ALLOC_DEDCT_ALT_TYP_NM"),
    F.when(trim(F.col("PCA_ALLOC_DEDCT_STD_TYP_CD")) == "", "UNK").otherwise(trim(F.col("PCA_ALLOC_DEDCT_STD_TYP_CD"))).alias("PCA_ALLOC_DEDCT_STD_TYP_CD"),
    F.when(trim(F.col("PCA_ALLOC_DEDCT_STD_TYP_NM")) == "", "UNK").otherwise(trim(F.col("PCA_ALLOC_DEDCT_STD_TYP_NM"))).alias("PCA_ALLOC_DEDCT_STD_TYP_NM"),
    F.when(trim(F.col("PCA_ALLOC_METH_CD")) == "", "UNK").otherwise(trim(F.col("PCA_ALLOC_METH_CD"))).alias("PCA_ALLOC_METH_CD"),
    F.when(trim(F.col("PCA_ALLOC_METH_NM")) == "", "UNK").otherwise(trim(F.col("PCA_ALLOC_METH_NM"))).alias("PCA_ALLOC_METH_NM"),
    F.col("PCA_ALLOC_TERM_DT_SK").alias("PCA_ALLOC_TERM_DT_SK"),
    F.col("PCA_ALLOC_FMLY_ALLOC_AMT").alias("PCA_ALLOC_FMLY_ALLOC_AMT"),
    F.col("PCA_ALLOC_FMLY_ALT_DEDCT_AMT").alias("PCA_ALLOC_FMLY_ALT_DEDCT_AMT"),
    F.col("PCA_ALLOC_FMLY_MAX_CAROVR_AMT").alias("PCA_ALLOC_FMLY_MAX_CAROVR_AMT"),
    F.col("PCA_ALLOC_FMLY_STD_DEDCT_AMT").alias("PCA_ALLOC_FMLY_STD_DEDCT_AMT"),
    F.col("PCA_ALLOC_INDV_ALLOC_AMT").alias("PCA_ALLOC_INDV_ALLOC_AMT"),
    F.col("PCA_ALLOC_INDV_ALT_DEDCT_AMT").alias("PCA_ALLOC_INDV_ALT_DEDCT_AMT"),
    F.col("PCA_ALLOC_INDV_MAX_CAROVR_AMT").alias("PCA_ALLOC_INDV_MAX_CAROVR_AMT"),
    F.col("PCA_ALLOC_INDV_STD_DEDCT_AMT").alias("PCA_ALLOC_INDV_STD_DEDCT_AMT"),
    F.col("PCA_ALLOC_SUB_DPNDT_ALLOC_AMT").alias("PCA_ALLOC_SUB_DPNDT_ALLOC_AMT"),
    F.col("PCA_ALC_SUB_DPNDT_ALT_DED_AMT").alias("PCA_ALC_SUB_DPNDT_ALT_DED_AMT"),
    F.col("PCA_ALC_SUB_DPT_MAXCAROVER_AMT").alias("PCA_ALC_SUB_DPT_MAXCAROVER_AMT"),
    F.col("PCA_ALC_SUB_DPNDT_STD_DED_AMT").alias("PCA_ALC_SUB_DPNDT_STD_DED_AMT"),
    F.col("PCA_ALLOC_SUB_1DPNDT_ALLOC_AMT").alias("PCA_ALLOC_SUB_1DPNDT_ALLOC_AMT"),
    F.col("PCA_ALC_SUB_1DPNDT_ALT_DED_AMT").alias("PCA_ALC_SUB_1DPNDT_ALT_DED_AMT"),
    F.col("PCA_ALC_SUB_1DPT_MAXCAROVR_AMT").alias("PCA_ALC_SUB_1DPT_MAXCAROVR_AMT"),
    F.col("PCA_ALC_SUB_1DPNDT_STD_DED_AMT").alias("PCA_ALC_SUB_1DPNDT_STD_DED_AMT"),
    F.lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("PCA_ALLOC_DEDCT_ALT_TYP_CD_SK").alias("PCA_ALLOC_DEDCT_ALT_TYP_CD_SK"),
    F.col("PCA_ALLOC_DEDCT_STD_TYP_CD_SK").alias("PCA_ALLOC_DEDCT_STD_TYP_CD_SK"),
    F.col("PCA_ALLOC_METH_CD_SK").alias("PCA_ALLOC_METH_CD_SK")
)

# PxSequentialFile stage: Seq_PCA_ALLOC_F_Load
# Apply rpad for char columns before writing
df_seq_pca_alloc_f_load = df_xmf_businessLogic.select(
    F.col("PCA_ALLOC_SK"),
    F.col("SRC_SYS_CD"),
    F.col("PCA_ALLOC_PFX_ID"),
    F.rpad(F.col("EFF_DT_SK"), 10, " ").alias("EFF_DT_SK"),
    F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.when(F.col("PCA_ALLOC_DEDCT_ALT_TYP_CD").isNotNull(), F.col("PCA_ALLOC_DEDCT_ALT_TYP_CD")).otherwise("UNK").alias("PCA_ALLOC_DEDCT_ALT_TYP_CD"),
    F.when(F.col("PCA_ALLOC_DEDCT_ALT_TYP_NM").isNotNull(), F.col("PCA_ALLOC_DEDCT_ALT_TYP_NM")).otherwise("UNK").alias("PCA_ALLOC_DEDCT_ALT_TYP_NM"),
    F.when(F.col("PCA_ALLOC_DEDCT_STD_TYP_CD").isNotNull(), F.col("PCA_ALLOC_DEDCT_STD_TYP_CD")).otherwise("UNK").alias("PCA_ALLOC_DEDCT_STD_TYP_CD"),
    F.when(F.col("PCA_ALLOC_DEDCT_STD_TYP_NM").isNotNull(), F.col("PCA_ALLOC_DEDCT_STD_TYP_NM")).otherwise("UNK").alias("PCA_ALLOC_DEDCT_STD_TYP_NM"),
    F.when(F.col("PCA_ALLOC_METH_CD").isNotNull(), F.col("PCA_ALLOC_METH_CD")).otherwise("UNK").alias("PCA_ALLOC_METH_CD"),
    F.when(F.col("PCA_ALLOC_METH_NM").isNotNull(), F.col("PCA_ALLOC_METH_NM")).otherwise("UNK").alias("PCA_ALLOC_METH_NM"),
    F.rpad(F.col("PCA_ALLOC_TERM_DT_SK"), 10, " ").alias("PCA_ALLOC_TERM_DT_SK"),
    F.col("PCA_ALLOC_FMLY_ALLOC_AMT"),
    F.col("PCA_ALLOC_FMLY_ALT_DEDCT_AMT"),
    F.col("PCA_ALLOC_FMLY_MAX_CAROVR_AMT"),
    F.col("PCA_ALLOC_FMLY_STD_DEDCT_AMT"),
    F.col("PCA_ALLOC_INDV_ALLOC_AMT"),
    F.col("PCA_ALLOC_INDV_ALT_DEDCT_AMT"),
    F.col("PCA_ALLOC_INDV_MAX_CAROVR_AMT"),
    F.col("PCA_ALLOC_INDV_STD_DEDCT_AMT"),
    F.col("PCA_ALLOC_SUB_DPNDT_ALLOC_AMT"),
    F.col("PCA_ALC_SUB_DPNDT_ALT_DED_AMT"),
    F.col("PCA_ALC_SUB_DPT_MAXCAROVER_AMT"),
    F.col("PCA_ALC_SUB_DPNDT_STD_DED_AMT"),
    F.col("PCA_ALLOC_SUB_1DPNDT_ALLOC_AMT"),
    F.col("PCA_ALC_SUB_1DPNDT_ALT_DED_AMT"),
    F.col("PCA_ALC_SUB_1DPT_MAXCAROVR_AMT"),
    F.col("PCA_ALC_SUB_1DPNDT_STD_DED_AMT"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("PCA_ALLOC_DEDCT_ALT_TYP_CD_SK"),
    F.col("PCA_ALLOC_DEDCT_STD_TYP_CD_SK"),
    F.col("PCA_ALLOC_METH_CD_SK")
)

write_files(
    df_seq_pca_alloc_f_load,
    f"{adls_path}/load/PCA_ALLOC_F.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)