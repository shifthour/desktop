# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Copyright 2009 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  FctsPcaClmPKExtrSeq
# MAGIC 
# MAGIC Description: This processes the Claim Id's coming from "hf_clm_pca_clms" from "FctsClmExtr" in to the "hf_clm" to get the respective PCA claims in to it.
# MAGIC                
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 \(9)Project/Altiris #\(9)Change Description\(9)\(9)\(9)\(9)\(9)\(9)Development Project\(9)Code Reviewer\(9)Date Reviewed       
# MAGIC ------------------              --------------------     \(9)------------------------\(9)-----------------------------------------------------------------------\(9)\(9)\(9)--------------------------------\(9)-------------------------------\(9)----------------------------       
# MAGIC Bhoomi Dasari\(9)2009-06-29\(9)4202                        Original Programming                                             \(9)\(9) \(9) devlIDSnew                              Brent Leland           06-30-2009     
# MAGIC 
# MAGIC Jaideep Mankala     2017-05-31              5321                       Removed Data elements in stage ClmLoadPK2
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)input and output links in order to match with Shared container\(9)IntegrateDev2                            Jag Yelavarthi         2017-06-05

# MAGIC This job loads primary key hash file with all keys used by tables with natural keys of source system code and claim ID
# MAGIC Get SK for records with out keys
# MAGIC This hash file is created from PCA claims in the FctsClmExtr job in the \"contHashFiles\" container, where we have the PCA claims ID's in the hash file. And load these particular claims Id's in to hf_clm, inorder to get a Fkey lookup for the respective base claims.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# Parameter definitions
SrcSysCd = get_widget_value("SrcSysCd","FACETS")
SrcSysCdSK = get_widget_value("SrcSysCdSK","69560")
CurrRunCycle = get_widget_value("CurrRunCycle","100")
CommitPoint = get_widget_value("CommitPoint","10000")
IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")

# MAGIC %run ../../../../shared_containers/PrimaryKey/ClmLoadPK
# COMMAND ----------

# Read from hashed file hf_clm_pca_clms (Scenario C => read parquet)
df_hf_clm_pca_clms = spark.read.parquet("hf_clm_pca_clms.parquet")

# Trans4: Transformer logic
df_trans4 = (
    df_hf_clm_pca_clms
    .withColumn("SRC_SYS_CD_SK", lit(SrcSysCdSK))
    .withColumn("CLM_ID", col("REL_PCA_CLM_ID"))
    .select("SRC_SYS_CD_SK", "CLM_ID")
)

# Call shared container ClmLoadPK2 (which references ClmLoadPK)
params = {
    "SrcSysCd": SrcSysCd,
    "SrcSysCdSK": SrcSysCdSK,
    "CurrRunCycle": CurrRunCycle,
    "IDSOwner": IDSOwner
}
df_clmloadpk2 = ClmLoadPK(df_trans4, params)

# Final write to hashed file hf_clm_pk_lkup (Scenario C => write parquet)
df_clmloadpk2 = df_clmloadpk2.select(
    rpad("SRC_SYS_CD", 12, " ").alias("SRC_SYS_CD"),
    rpad("CLM_ID", <...>, " ").alias("CLM_ID"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("CLM_SK")
)

write_files(
    df_clmloadpk2,
    "hf_clm_pk_lkup.parquet",
    ",",
    "overwrite",
    True,
    True,
    '"',
    None
)