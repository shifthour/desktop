# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     EdwDedctCmpntD
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC 
# MAGIC   Pulls data from IDS Facets table DEDCT_CMPNT into EDW table DEDCT_CMPNT_D
# MAGIC 
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC 
# MAGIC                   IDS - read from source, lookup all code SK values from CodeMapping hash file and get the natural codes
# MAGIC   
# MAGIC OUTPUTS: 
# MAGIC                   Loading into a sequential file
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC                Bhoomi Dasari    11/27/2006  ---    Originally Programmed
# MAGIC                
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                    
# MAGIC DEVELOPER                         DATE                 PROJECT                                                DESCRIPTION                                            DATASTAGE                                            CODE                            REVIEW
# MAGIC                                                                                                                                                                                                               ENVIRONMENT                                       REVIEWER                   DATE                                              
# MAGIC ------------------------------------------   ----------------------      ------------------------------------------------                ---------------------------------------------------------------     ---------------------------------------------------              ------------------------------          --------------------
# MAGIC Rajasekhar Mangalampally    06/03/2013       5114                                                         Original Programming                                   EnterpriseWrhsDevl                                  Pete Marshall               8/8/2013
# MAGIC                                                                                                                                           (Server to Parallel Conversion)

# MAGIC This is Source extract data from an IDS table and apply Code denormalization, create a load ready file.
# MAGIC 
# MAGIC Job Name:
# MAGIC IdsEdwDeductCmpntDExtr
# MAGIC 
# MAGIC Table:
# MAGIC DEDCT_CMPNT_D
# MAGIC Read from source table DEDCT_CMPNT from IDS: 
# MAGIC Pull records based on LAST_UPDT_RUN_CYC_EXCTN_SK
# MAGIC This is a load ready file conforms to the target table structure.
# MAGIC 
# MAGIC Write DEDCT_CMPNT_D Data into a Sequential file for Load Job.
# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Add Defaults and Null Handling
# MAGIC Code SK lookups for Denormalization
# MAGIC 
# MAGIC Lookup Keys:
# MAGIC 
# MAGIC 1) DEDCT_CMPNT_ACCUM_PERD_CD_SK
# MAGIC 2) DEDCT_CMPNT_OOP_USAGE_CD_SK
# MAGIC 3) DEDCT_CMPNT_TYP_CD_SK
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame, functions as F
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query_db2_DEDCT_CMPNT_D_in = f"""SELECT 
D.DEDCT_CMPNT_SK,
COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,
D.DEDCT_CMPNT_ID,
D.ACCUM_NO,
D.DEDCT_CMPNT_DESC,
D.FMLY_DEDCT_AMT,
D.FMLY_DEDCT_CAROVR_AMT,
D.FMLY_DEDCT_CAROVR_PRSN_CT,
D.FMLY_DEDCT_PRSN_CT,
D.MBR_DEDCT_AMT,
D.MBR_DEDCT_CAROVR_AMT,
D.REL_ACCUM_NO,
D.STOP_LOSS_IN,
D.DEDCT_CMPNT_ACCUM_PERD_CD_SK,
D.DEDCT_CMPNT_OOP_USAGE_CD_SK,
D.DEDCT_CMPNT_TYP_CD_SK
FROM {IDSOwner}.DEDCT_CMPNT D
LEFT OUTER JOIN {IDSOwner}.CD_MPPNG CD ON D.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
"""
df_db2_DEDCT_CMPNT_D_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_DEDCT_CMPNT_D_in)
    .load()
)

extract_query_db2_CD_MPPNG_Extr = f"""SELECT 
CD_MPPNG_SK,
COALESCE(TRGT_CD,'UNK') TRGT_CD,
COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM 
from {IDSOwner}.CD_MPPNG
"""
df_db2_CD_MPPNG_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CD_MPPNG_Extr)
    .load()
)

df_cpy_cd_mppnig = df_db2_CD_MPPNG_Extr.select(
    F.col("CD_MPPNG_SK"),
    F.col("TRGT_CD"),
    F.col("TRGT_CD_NM")
)

df_RefTyp_CDLkp = df_cpy_cd_mppnig
df_RefAccumPerd_CDLkp = df_cpy_cd_mppnig
df_RefOopUsage_CDLkp = df_cpy_cd_mppnig

df_lkp_Codes = (
    df_db2_DEDCT_CMPNT_D_in.alias("lnk_IdsEdwDeductCmpntDExtr_InABC")
    .join(
        df_RefAccumPerd_CDLkp.alias("RefAccumPerd_CDLkp"),
        F.col("lnk_IdsEdwDeductCmpntDExtr_InABC.DEDCT_CMPNT_ACCUM_PERD_CD_SK")
        == F.col("RefAccumPerd_CDLkp.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_RefOopUsage_CDLkp.alias("RefOopUsage_CDLkp"),
        F.col("lnk_IdsEdwDeductCmpntDExtr_InABC.DEDCT_CMPNT_OOP_USAGE_CD_SK")
        == F.col("RefOopUsage_CDLkp.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_RefTyp_CDLkp.alias("RefTyp_CDLkp"),
        F.col("lnk_IdsEdwDeductCmpntDExtr_InABC.DEDCT_CMPNT_TYP_CD_SK")
        == F.col("RefTyp_CDLkp.CD_MPPNG_SK"),
        "left",
    )
    .select(
        F.col("lnk_IdsEdwDeductCmpntDExtr_InABC.DEDCT_CMPNT_SK").alias("DEDCT_CMPNT_SK"),
        F.col("lnk_IdsEdwDeductCmpntDExtr_InABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("lnk_IdsEdwDeductCmpntDExtr_InABC.DEDCT_CMPNT_ID").alias("DEDCT_CMPNT_ID"),
        F.col("lnk_IdsEdwDeductCmpntDExtr_InABC.ACCUM_NO").alias("ACCUM_NO"),
        F.col("RefAccumPerd_CDLkp.TRGT_CD").alias("DEDCT_CMPNT_ACCUM_PERD_CD"),
        F.col("RefAccumPerd_CDLkp.TRGT_CD_NM").alias("DEDCT_CMPNT_ACCUM_PERD_NM"),
        F.col("lnk_IdsEdwDeductCmpntDExtr_InABC.DEDCT_CMPNT_DESC").alias("DEDCT_CMPNT_DESC"),
        F.col("lnk_IdsEdwDeductCmpntDExtr_InABC.FMLY_DEDCT_AMT").alias("DEDCT_CMPNT_FMLY_AMT"),
        F.col("lnk_IdsEdwDeductCmpntDExtr_InABC.FMLY_DEDCT_CAROVR_AMT").alias("DEDCT_CMPNT_FMLY_CAROVR_AMT"),
        F.col("lnk_IdsEdwDeductCmpntDExtr_InABC.FMLY_DEDCT_CAROVR_PRSN_CT").alias("DEDCT_CMPNT_FMLY_CROVR_PRSN_CT"),
        F.col("lnk_IdsEdwDeductCmpntDExtr_InABC.FMLY_DEDCT_PRSN_CT").alias("DEDCT_CMPNT_FMLY_PRSN_CT"),
        F.col("lnk_IdsEdwDeductCmpntDExtr_InABC.MBR_DEDCT_AMT").alias("DEDCT_CMPNT_MBR_AMT"),
        F.col("lnk_IdsEdwDeductCmpntDExtr_InABC.MBR_DEDCT_CAROVR_AMT").alias("DEDCT_CMPNT_MBR_CAROVR_AMT"),
        F.col("RefOopUsage_CDLkp.TRGT_CD").alias("DEDCT_CMPNT_OOP_USAGE_CD"),
        F.col("RefOopUsage_CDLkp.TRGT_CD_NM").alias("DEDCT_CMPNT_OOP_USAGE_NM"),
        F.col("lnk_IdsEdwDeductCmpntDExtr_InABC.REL_ACCUM_NO").alias("DEDCT_CMPNT_REL_ACCUM_NO"),
        F.col("lnk_IdsEdwDeductCmpntDExtr_InABC.STOP_LOSS_IN").alias("DEDCT_CMPNT_STOP_LOSS_IN"),
        F.col("RefTyp_CDLkp.TRGT_CD").alias("DEDCT_CMPNT_TYP_CD"),
        F.col("RefTyp_CDLkp.TRGT_CD_NM").alias("DEDCT_CMPNT_TYP_NM"),
        F.col("lnk_IdsEdwDeductCmpntDExtr_InABC.DEDCT_CMPNT_ACCUM_PERD_CD_SK").alias("DEDCT_CMPNT_ACCUM_PERD_CD_SK"),
        F.col("lnk_IdsEdwDeductCmpntDExtr_InABC.DEDCT_CMPNT_OOP_USAGE_CD_SK").alias("DEDCT_CMPNT_OOP_USAGE_CD_SK"),
        F.col("lnk_IdsEdwDeductCmpntDExtr_InABC.DEDCT_CMPNT_TYP_CD_SK").alias("DEDCT_CMPNT_TYP_CD_SK"),
    )
)

df_xfm_BusinessLogic_intermediate = (
    df_lkp_Codes
    .withColumn(
        "SRC_SYS_CD",
        F.when(trim(F.col("SRC_SYS_CD")) == "", F.lit("UNK")).otherwise(F.col("SRC_SYS_CD"))
    )
    .withColumn(
        "DEDCT_CMPNT_ACCUM_PERD_CD",
        F.when(trim(F.col("DEDCT_CMPNT_ACCUM_PERD_CD")) == "", F.lit("UNK")).otherwise(F.col("DEDCT_CMPNT_ACCUM_PERD_CD"))
    )
    .withColumn(
        "DEDCT_CMPNT_ACCUM_PERD_NM",
        F.when(trim(F.col("DEDCT_CMPNT_ACCUM_PERD_NM")) == "", F.lit("UNK")).otherwise(F.col("DEDCT_CMPNT_ACCUM_PERD_NM"))
    )
    .withColumn(
        "DEDCT_CMPNT_OOP_USAGE_CD",
        F.when(trim(F.col("DEDCT_CMPNT_OOP_USAGE_CD")) == "", F.lit("UNK")).otherwise(F.col("DEDCT_CMPNT_OOP_USAGE_CD"))
    )
    .withColumn(
        "DEDCT_CMPNT_OOP_USAGE_NM",
        F.when(trim(F.col("DEDCT_CMPNT_OOP_USAGE_NM")) == "", F.lit("UNK")).otherwise(F.col("DEDCT_CMPNT_OOP_USAGE_NM"))
    )
    .withColumn(
        "DEDCT_CMPNT_TYP_CD",
        F.when(trim(F.col("DEDCT_CMPNT_TYP_CD")) == "", F.lit("UNK")).otherwise(F.col("DEDCT_CMPNT_TYP_CD"))
    )
    .withColumn(
        "DEDCT_CMPNT_TYP_NM",
        F.when(trim(F.col("DEDCT_CMPNT_TYP_NM")) == "", F.lit("UNK")).otherwise(F.col("DEDCT_CMPNT_TYP_NM"))
    )
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.lit(EDWRunCycleDate))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.lit(EDWRunCycleDate))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(EDWRunCycle))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(EDWRunCycle))
)

df_xfm_BusinessLogic = df_xfm_BusinessLogic_intermediate.select(
    F.col("DEDCT_CMPNT_SK"),
    F.col("SRC_SYS_CD"),
    F.col("DEDCT_CMPNT_ID"),
    F.col("ACCUM_NO").alias("DEDCT_CMPNT_ACCUM_NO"),
    F.col("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("DEDCT_CMPNT_ACCUM_PERD_CD"),
    F.col("DEDCT_CMPNT_ACCUM_PERD_NM"),
    F.col("DEDCT_CMPNT_DESC"),
    F.col("DEDCT_CMPNT_FMLY_AMT"),
    F.col("DEDCT_CMPNT_FMLY_CAROVR_AMT"),
    F.col("DEDCT_CMPNT_FMLY_CROVR_PRSN_CT"),
    F.col("DEDCT_CMPNT_FMLY_PRSN_CT"),
    F.col("DEDCT_CMPNT_MBR_AMT"),
    F.col("DEDCT_CMPNT_MBR_CAROVR_AMT"),
    F.col("DEDCT_CMPNT_OOP_USAGE_CD"),
    F.col("DEDCT_CMPNT_OOP_USAGE_NM"),
    F.col("DEDCT_CMPNT_REL_ACCUM_NO"),
    F.col("DEDCT_CMPNT_STOP_LOSS_IN"),
    F.col("DEDCT_CMPNT_TYP_CD"),
    F.col("DEDCT_CMPNT_TYP_NM"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("DEDCT_CMPNT_ACCUM_PERD_CD_SK"),
    F.col("DEDCT_CMPNT_OOP_USAGE_CD_SK"),
    F.col("DEDCT_CMPNT_TYP_CD_SK"),
).withColumn(
    "CRT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
).withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
).withColumn(
    "DEDCT_CMPNT_STOP_LOSS_IN", F.rpad(F.col("DEDCT_CMPNT_STOP_LOSS_IN"), 1, " ")
)

write_files(
    df_xfm_BusinessLogic,
    f"{adls_path}/load/DEDCT_CMPNT_D.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)