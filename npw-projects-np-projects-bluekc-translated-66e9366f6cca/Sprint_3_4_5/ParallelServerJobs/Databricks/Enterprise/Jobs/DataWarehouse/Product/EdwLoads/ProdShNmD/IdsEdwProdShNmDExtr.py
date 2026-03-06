# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     EdwProdShNmExtr
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC       
# MAGIC 
# MAGIC INPUTS:
# MAGIC 	IDS:  PROD_SH_NM
# MAGIC                 EDW:  PROD_SH_NM_D                        
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC                     Each of the stream output hash files
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC               Oliver Nielsen 08/19/2005-   Originally Programmed
# MAGIC                                                                                                                                                                                    DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                        ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --
# MAGIC Archana Palivela     05/24/2013        5114                              Originally Programmed  (In Parallel)                         EnterpriseWhseDevl   Bhoomi Dasari        8/11/2013

# MAGIC Job name: IdsEdwProdShNmDExtr
# MAGIC 
# MAGIC 
# MAGIC Job Extract data from IDS table and apply Fkey and Code denormalization, create a load ready file.
# MAGIC Write PROD_SH_NM_D Data into a Sequential file for Load Ready Job.
# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Code SK lookups for Denormalization
# MAGIC 
# MAGIC Lookup Keys:
# MAGIC CD_MPPNG_SK
# MAGIC MCARE_BNF_CD_SK
# MAGIC PROD_SH_NM_SK
# MAGIC PROD_SH_NM_DLVRY_METH_CD_SK
# MAGIC Read data from source table PROD_SH_NM
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DecimalType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


# Retrieve all job parameters
EDWRunCycle = get_widget_value('EDWRunCycle','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
IDSOwner = get_widget_value('IDSOwner','')
IDSRunCycle = get_widget_value('IDSRunCycle','')
ids_secret_name = get_widget_value('ids_secret_name','')

# ----------------------------------------------------------------------------
# db2_PROD_SH_NM_D_Extr (DB2ConnectorPX)
# ----------------------------------------------------------------------------
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query_db2_PROD_SH_NM_D_Extr = (
    f"SELECT "
    f"SH_NM.PROD_SH_NM_SK, "
    f"COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD, "
    f"SH_NM.PROD_SH_NM, "
    f"SH_NM.MCARE_BNF_CD_SK, "
    f"SH_NM.PROD_SH_NM_DLVRY_METH_CD_SK, "
    f"SH_NM.PROD_SH_NM_CAT_CD_SK, "
    f"SH_NM.MCARE_SUPLMT_COV_IN, "
    f"SH_NM.PROD_SH_NM_DESC, "
    f"SH_NM.USER_ID, "
    f"SH_NM.LAST_UPDT_DT_SK "
    f"FROM {IDSOwner}.PROD_SH_NM SH_NM "
    f"LEFT JOIN {IDSOwner}.CD_MPPNG CD "
    f"ON SH_NM.SRC_SYS_CD_SK = CD.CD_MPPNG_SK "
    f"WHERE SH_NM.LAST_UPDT_RUN_CYC_EXCTN_SK >= {IDSRunCycle}"
)
df_db2_PROD_SH_NM_D_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_PROD_SH_NM_D_Extr)
    .load()
)

# ----------------------------------------------------------------------------
# db2_CD_MPPNG_Extr (DB2ConnectorPX)
# ----------------------------------------------------------------------------
extract_query_db2_CD_MPPNG_Extr = (
    f"SELECT "
    f"CD_MPPNG_SK, "
    f"COALESCE(TRGT_CD,'UNK') TRGT_CD, "
    f"COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM "
    f"FROM {IDSOwner}.CD_MPPNG"
)
df_db2_CD_MPPNG_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CD_MPPNG_Extr)
    .load()
)

# ----------------------------------------------------------------------------
# cpy_cd_mppng (PxCopy)
# ----------------------------------------------------------------------------
df_cpy_cd_mppng_RefMcareBnf = df_db2_CD_MPPNG_Extr.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_cpy_cd_mppng_RefProdShNmCat = df_db2_CD_MPPNG_Extr.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_cpy_cd_mppng_RefProdShNmDlvryMeth = df_db2_CD_MPPNG_Extr.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

# ----------------------------------------------------------------------------
# lkp_Codes (PxLookup)
# ----------------------------------------------------------------------------
df_lkp_joined = (
    df_db2_PROD_SH_NM_D_Extr.alias("Lnk_IdsEdwProdShNmD_InABC")
    .join(
        df_cpy_cd_mppng_RefMcareBnf.alias("RefMcareBnf"),
        F.col("Lnk_IdsEdwProdShNmD_InABC.MCARE_BNF_CD_SK") == F.col("RefMcareBnf.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_cpy_cd_mppng_RefProdShNmCat.alias("RefProdShNmCat"),
        F.col("Lnk_IdsEdwProdShNmD_InABC.PROD_SH_NM_CAT_CD_SK") == F.col("RefProdShNmCat.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_cpy_cd_mppng_RefProdShNmDlvryMeth.alias("RefProdShNmDlvryMeth"),
        F.col("Lnk_IdsEdwProdShNmD_InABC.PROD_SH_NM_DLVRY_METH_CD_SK") == F.col("RefProdShNmDlvryMeth.CD_MPPNG_SK"),
        "left"
    )
)

df_lkp_Codes = df_lkp_joined.select(
    F.col("Lnk_IdsEdwProdShNmD_InABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("RefProdShNmCat.TRGT_CD").alias("PROD_SH_NM_CAT_CD"),
    F.col("RefProdShNmCat.TRGT_CD_NM").alias("PROD_SH_NM_CAT_NM"),
    F.col("RefProdShNmDlvryMeth.TRGT_CD").alias("PROD_SH_NM_DLVRY_METH_CD"),
    F.col("RefProdShNmDlvryMeth.TRGT_CD_NM").alias("PROD_SH_NM_DLVRY_METH_NM"),
    F.col("RefMcareBnf.TRGT_CD").alias("MCARE_BNF_CD"),
    F.col("RefMcareBnf.TRGT_CD_NM").alias("MCARE_BNF_NM"),
    F.col("Lnk_IdsEdwProdShNmD_InABC.PROD_SH_NM_SK").alias("PROD_SH_NM_SK"),
    F.col("Lnk_IdsEdwProdShNmD_InABC.PROD_SH_NM").alias("PROD_SH_NM"),
    F.col("Lnk_IdsEdwProdShNmD_InABC.PROD_SH_NM_DESC").alias("PROD_SH_NM_DESC"),
    F.col("Lnk_IdsEdwProdShNmD_InABC.USER_ID").alias("USER_ID"),
    F.col("Lnk_IdsEdwProdShNmD_InABC.LAST_UPDT_DT_SK").alias("LAST_UPDT_DT_SK"),
    F.col("Lnk_IdsEdwProdShNmD_InABC.MCARE_SUPLMT_COV_IN").alias("PROD_SHNM_MCARE_SUPLMT_COV_IN"),
    F.col("Lnk_IdsEdwProdShNmD_InABC.PROD_SH_NM_CAT_CD_SK").alias("PROD_SH_NM_CAT_CD_SK"),
    F.col("Lnk_IdsEdwProdShNmD_InABC.PROD_SH_NM_DLVRY_METH_CD_SK").alias("PROD_SH_NM_DLVRY_METH_CD_SK")
)

# ----------------------------------------------------------------------------
# xmf_businessLogic (CTransformerStage)
# ----------------------------------------------------------------------------
df_xmf_businessLogic = df_lkp_Codes.select(
    F.col("PROD_SH_NM_SK").alias("PROD_SH_NM_SK"),
    F.when(trim(F.col("SRC_SYS_CD")) == "", "UNK").otherwise(F.col("SRC_SYS_CD")).alias("SRC_SYS_CD"),
    F.col("PROD_SH_NM").alias("PROD_SH_NM"),
    F.lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.when(trim(F.col("MCARE_BNF_CD")) == "", "UNK").otherwise(F.col("MCARE_BNF_CD")).alias("MCARE_BNF_CD"),
    F.when(trim(F.col("MCARE_BNF_NM")) == "", "UNK").otherwise(F.col("MCARE_BNF_NM")).alias("MCARE_BNF_NM"),
    F.when(trim(F.col("PROD_SH_NM_CAT_CD")) == "", "UNK").otherwise(F.col("PROD_SH_NM_CAT_CD")).alias("PROD_SH_NM_CAT_CD"),
    F.when(trim(F.col("PROD_SH_NM_CAT_NM")) == "", "UNK").otherwise(F.col("PROD_SH_NM_CAT_NM")).alias("PROD_SH_NM_CAT_NM"),
    F.when(trim(F.col("PROD_SH_NM_DLVRY_METH_CD")) == "", "UNK").otherwise(F.col("PROD_SH_NM_DLVRY_METH_CD")).alias("PROD_SH_NM_DLVRY_METH_CD"),
    F.when(trim(F.col("PROD_SH_NM_DLVRY_METH_NM")) == "", "UNK").otherwise(F.col("PROD_SH_NM_DLVRY_METH_NM")).alias("PROD_SH_NM_DLVRY_METH_NM"),
    F.col("PROD_SH_NM_DESC").alias("PROD_SH_NM_DESC"),
    F.col("PROD_SHNM_MCARE_SUPLMT_COV_IN").alias("PROD_SHNM_MCARE_SUPLMT_COV_IN"),
    F.col("USER_ID").alias("USER_ID"),
    F.col("LAST_UPDT_DT_SK").alias("LAST_UPDT_DT_SK"),
    F.lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("PROD_SH_NM_CAT_CD_SK").alias("PROD_SH_NM_CAT_CD_SK"),
    F.col("PROD_SH_NM_DLVRY_METH_CD_SK").alias("PROD_SH_NM_DLVRY_METH_CD_SK")
)

# ----------------------------------------------------------------------------
# Seq_PROD_SH_NM_D_Load (PxSequentialFile)
# ----------------------------------------------------------------------------
df_final = (
    df_xmf_businessLogic
    .select(
        "PROD_SH_NM_SK",
        "SRC_SYS_CD",
        "PROD_SH_NM",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "MCARE_BNF_CD",
        "MCARE_BNF_NM",
        "PROD_SH_NM_CAT_CD",
        "PROD_SH_NM_CAT_NM",
        "PROD_SH_NM_DLVRY_METH_CD",
        "PROD_SH_NM_DLVRY_METH_NM",
        "PROD_SH_NM_DESC",
        "PROD_SHNM_MCARE_SUPLMT_COV_IN",
        "USER_ID",
        "LAST_UPDT_DT_SK",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "PROD_SH_NM_CAT_CD_SK",
        "PROD_SH_NM_DLVRY_METH_CD_SK"
    )
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("PROD_SHNM_MCARE_SUPLMT_COV_IN", F.rpad(F.col("PROD_SHNM_MCARE_SUPLMT_COV_IN"), 1, " "))
    .withColumn("LAST_UPDT_DT_SK", F.rpad(F.col("LAST_UPDT_DT_SK"), 10, " "))
)

write_files(
    df_final,
    f"{adls_path}/load/PROD_SH_NM_D.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)