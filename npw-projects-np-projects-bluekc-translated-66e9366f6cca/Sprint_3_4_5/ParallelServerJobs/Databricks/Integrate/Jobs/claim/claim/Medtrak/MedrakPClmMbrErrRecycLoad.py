# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2012 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC CALLED BY : BCBSSCClmLandSeq
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION :  Medtrak Claims Member Matching Error File Load.
# MAGIC                                
# MAGIC 
# MAGIC MODIFICATIONS :
# MAGIC 
# MAGIC Developer                                  Date                 Project/Altiris #          Change Description                                                           Development Project         Code Reviewer          Date Reviewed       
# MAGIC ------------------                            --------------------         ------------------------          -----------------------------------------------------------------------                   --------------------------------         -------------------------------   ----------------------------      
# MAGIC Jaideep Mankala                   2018-02-26              5828                         Original Programming                                                            IntegrateDev2               Kalyan Neelam           2018-02-28  
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType
import pyspark.sql.functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


SrcSysCd = get_widget_value("SrcSysCd","")

schema_Medtrak_Error_File = StructType([
    StructField("CLM_ID", StringType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("CLM_TYP_CD", StringType(), True),
    StructField("CLM_SUBTYP_CD", StringType(), True),
    StructField("CLM_SVC_STRT_DT_SK", StringType(), True),
    StructField("SRC_SYS_GRP_PFX", StringType(), True),
    StructField("SRC_SYS_GRP_ID", StringType(), True),
    StructField("SRC_SYS_GRP_SFX", StringType(), True),
    StructField("SUB_SSN", StringType(), True),
    StructField("PATN_LAST_NM", StringType(), True),
    StructField("PATN_FIRST_NM", StringType(), True),
    StructField("PATN_GNDR_CD", StringType(), True),
    StructField("PATN_BRTH_DT_SK", StringType(), True),
    StructField("ERR_CD", StringType(), True),
    StructField("ERR_DESC", StringType(), True),
    StructField("FEP_MBR_ID", StringType(), True),
    StructField("SUB_FIRST_NM", StringType(), True),
    StructField("SUB_LAST_NM", StringType(), True),
    StructField("SRC_SYS_SUB_ID", StringType(), True),
    StructField("SRC_SYS_MBR_SFX_NO", StringType(), True),
    StructField("GRP_ID", StringType(), True),
    StructField("FILE_DT_SK", StringType(), True),
    StructField("PATN_SSN", StringType(), True)
])

df_Medtrak_Error_File = (
    spark.read
    .option("header", True)
    .option("sep", ",")
    .option("quote", '"')
    .schema(schema_Medtrak_Error_File)
    .csv(f"{adls_path_publish}/external/MEDTRAK_MbrMatch_Rx_ErrorFile.dat")
)

df_hf_medtrak_errclm_rm_dupe = dedup_sort(df_Medtrak_Error_File, ["CLM_ID"], [])

df_Trn_Clm_Sk_Gen = df_hf_medtrak_errclm_rm_dupe.select(
    GetFkeyClm(SrcSysCd, F.lit(1), trim(F.col("CLM_ID")), F.lit("N")).alias("CLM_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CLM_TYP_CD").alias("CLM_TYP_CD"),
    F.col("CLM_SUBTYP_CD").alias("CLM_SUBTYP_CD"),
    F.col("CLM_SVC_STRT_DT_SK").alias("CLM_SVC_STRT_DT_SK"),
    F.col("SRC_SYS_GRP_PFX").alias("SRC_SYS_GRP_PFX"),
    F.when(F.col("SRC_SYS_GRP_ID").isNull(), F.lit(" ")).otherwise(F.col("SRC_SYS_GRP_ID")).alias("SRC_SYS_GRP_ID"),
    F.col("SRC_SYS_GRP_SFX").alias("SRC_SYS_GRP_SFX"),
    F.col("SUB_SSN").alias("SUB_SSN"),
    F.col("PATN_LAST_NM").alias("PATN_LAST_NM"),
    F.col("PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    F.col("PATN_GNDR_CD").alias("PATN_GNDR_CD"),
    F.col("PATN_BRTH_DT_SK").alias("PATN_BRTH_DT_SK"),
    F.col("ERR_CD").alias("ERR_CD"),
    F.col("ERR_DESC").alias("ERR_DESC"),
    F.col("FEP_MBR_ID").alias("FEP_MBR_ID"),
    F.col("SUB_FIRST_NM").alias("SUB_FIRST_NM"),
    F.col("SUB_LAST_NM").alias("SUB_LAST_NM"),
    F.col("SRC_SYS_SUB_ID").alias("SRC_SYS_SUB_ID"),
    F.col("SRC_SYS_MBR_SFX_NO").alias("SRC_SYS_MBR_SFX_NO"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("FILE_DT_SK").alias("FILE_DT_SK"),
    F.col("PATN_SSN").alias("PATN_SSN")
)

df_final = df_Trn_Clm_Sk_Gen.select(
    F.rpad(F.col("CLM_SK"), 255, " ").alias("CLM_SK"),
    F.rpad(F.col("CLM_ID"), 255, " ").alias("CLM_ID"),
    F.rpad(F.col("SRC_SYS_CD"), 255, " ").alias("SRC_SYS_CD"),
    F.rpad(F.col("CLM_TYP_CD"), 255, " ").alias("CLM_TYP_CD"),
    F.rpad(F.col("CLM_SUBTYP_CD"), 255, " ").alias("CLM_SUBTYP_CD"),
    F.rpad(F.col("CLM_SVC_STRT_DT_SK"), 10, " ").alias("CLM_SVC_STRT_DT_SK"),
    F.rpad(F.col("SRC_SYS_GRP_PFX"), 255, " ").alias("SRC_SYS_GRP_PFX"),
    F.rpad(F.col("SRC_SYS_GRP_ID"), 255, " ").alias("SRC_SYS_GRP_ID"),
    F.rpad(F.col("SRC_SYS_GRP_SFX"), 255, " ").alias("SRC_SYS_GRP_SFX"),
    F.rpad(F.col("SUB_SSN"), 255, " ").alias("SUB_SSN"),
    F.rpad(F.col("PATN_LAST_NM"), 255, " ").alias("PATN_LAST_NM"),
    F.rpad(F.col("PATN_FIRST_NM"), 255, " ").alias("PATN_FIRST_NM"),
    F.rpad(F.col("PATN_GNDR_CD"), 255, " ").alias("PATN_GNDR_CD"),
    F.rpad(F.col("PATN_BRTH_DT_SK"), 10, " ").alias("PATN_BRTH_DT_SK"),
    F.rpad(F.col("ERR_CD"), 255, " ").alias("ERR_CD"),
    F.rpad(F.col("ERR_DESC"), 255, " ").alias("ERR_DESC"),
    F.rpad(F.col("FEP_MBR_ID"), 255, " ").alias("FEP_MBR_ID"),
    F.rpad(F.col("SUB_FIRST_NM"), 255, " ").alias("SUB_FIRST_NM"),
    F.rpad(F.col("SUB_LAST_NM"), 255, " ").alias("SUB_LAST_NM"),
    F.rpad(F.col("SRC_SYS_SUB_ID"), 255, " ").alias("SRC_SYS_SUB_ID"),
    F.rpad(F.col("SRC_SYS_MBR_SFX_NO"), 3, " ").alias("SRC_SYS_MBR_SFX_NO"),
    F.rpad(F.col("GRP_ID"), 255, " ").alias("GRP_ID"),
    F.rpad(F.col("FILE_DT_SK"), 10, " ").alias("FILE_DT_SK"),
    F.rpad(F.col("PATN_SSN"), 255, " ").alias("PATN_SSN")
)

write_files(
    df_final,
    f"{adls_path}/load/P_CLM_MBRSH_ERR_RECYC_RX.dat",
    ",",
    "overwrite",
    False,
    False,
    '"',
    None
)