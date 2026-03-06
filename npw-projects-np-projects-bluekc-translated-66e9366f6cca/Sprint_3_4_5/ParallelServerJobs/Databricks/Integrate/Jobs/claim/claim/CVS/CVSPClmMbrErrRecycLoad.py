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
# MAGIC DESCRIPTION :  CVS Claims Member Matching Error File Load.
# MAGIC                                
# MAGIC 
# MAGIC MODIFICATIONS :
# MAGIC 
# MAGIC Developer                                  Date                 Project/Altiris #          Change Description                                                           Development Project         Code Reviewer          Date Reviewed       
# MAGIC ------------------                            --------------------         ------------------------          -----------------------------------------------------------------------                   --------------------------------         -------------------------------   ----------------------------      
# MAGIC Kaushik Kapoor                    2018-09-18              5828                         Original Programming                                                            IntegrateDev2                Kalyan Neelam          2018-10-01


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


SrcSysCd = get_widget_value('SrcSysCd','')

schema_CVS_Error_File = StructType([
    StructField("CLM_ID", StringType(), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("CLM_TYP_CD", StringType(), False),
    StructField("CLM_SUBTYP_CD", StringType(), False),
    StructField("CLM_SVC_STRT_DT_SK", StringType(), False),
    StructField("SRC_SYS_GRP_PFX", StringType(), False),
    StructField("SRC_SYS_GRP_ID", StringType(), False),
    StructField("SRC_SYS_GRP_SFX", StringType(), False),
    StructField("SUB_SSN", StringType(), False),
    StructField("PATN_LAST_NM", StringType(), False),
    StructField("PATN_FIRST_NM", StringType(), False),
    StructField("PATN_GNDR_CD", StringType(), False),
    StructField("PATN_BRTH_DT_SK", StringType(), False),
    StructField("ERR_CD", StringType(), False),
    StructField("ERR_DESC", StringType(), False),
    StructField("FEP_MBR_ID", StringType(), False),
    StructField("SUB_FIRST_NM", StringType(), True),
    StructField("SUB_LAST_NM", StringType(), True),
    StructField("SRC_SYS_SUB_ID", StringType(), True),
    StructField("SRC_SYS_MBR_SFX_NO", StringType(), True),
    StructField("GRP_ID", StringType(), True),
    StructField("FILE_DT_SK", StringType(), True),
    StructField("PATN_SSN", StringType(), True)
])

df_CVS_Error_File = (
    spark.read
    .option("header", True)
    .option("quote", '"')
    .schema(schema_CVS_Error_File)
    .csv(f"{adls_path_publish}/external/CVS_MbrMatch_Rx_ErrorFile.dat")
)

df_hf_CVS_errclm_rm_dupe = df_CVS_Error_File.dropDuplicates(["CLM_ID"])

df_Transform = (
    df_hf_CVS_errclm_rm_dupe
    .withColumn(
        "CLM_SK",
        GetFkeyClm(
            F.lit(SrcSysCd),
            F.lit(1),
            trim(F.col("CLM_ID")),
            F.lit("N")
        )
    )
    .withColumn("CLM_ID", F.col("CLM_ID"))
    .withColumn("SRC_SYS_CD", F.col("SRC_SYS_CD"))
    .withColumn("CLM_TYP_CD", F.col("CLM_TYP_CD"))
    .withColumn("CLM_SUBTYP_CD", F.col("CLM_SUBTYP_CD"))
    .withColumn("CLM_SVC_STRT_DT_SK", F.col("CLM_SVC_STRT_DT_SK"))
    .withColumn("SRC_SYS_GRP_PFX", F.col("SRC_SYS_GRP_PFX"))
    .withColumn(
        "SRC_SYS_GRP_ID",
        F.when(F.col("SRC_SYS_GRP_ID").isNull(), F.lit(" "))
         .otherwise(F.col("SRC_SYS_GRP_ID"))
    )
    .withColumn("SRC_SYS_GRP_SFX", F.col("SRC_SYS_GRP_SFX"))
    .withColumn("SUB_SSN", F.col("SUB_SSN"))
    .withColumn("PATN_LAST_NM", F.col("PATN_LAST_NM"))
    .withColumn("PATN_FIRST_NM", F.col("PATN_FIRST_NM"))
    .withColumn("PATN_GNDR_CD", F.col("PATN_GNDR_CD"))
    .withColumn("PATN_BRTH_DT_SK", F.col("PATN_BRTH_DT_SK"))
    .withColumn("ERR_CD", F.col("ERR_CD"))
    .withColumn("ERR_DESC", F.col("ERR_DESC"))
    .withColumn("FEP_MBR_ID", F.col("FEP_MBR_ID"))
    .withColumn("SUB_FIRST_NM", F.col("SUB_FIRST_NM"))
    .withColumn("SUB_LAST_NM", F.col("SUB_LAST_NM"))
    .withColumn("SRC_SYS_SUB_ID", F.col("SRC_SYS_SUB_ID"))
    .withColumn("SRC_SYS_MBR_SFX_NO", F.col("SRC_SYS_MBR_SFX_NO"))
    .withColumn("GRP_ID", F.col("GRP_ID"))
    .withColumn("FILE_DT_SK", F.col("FILE_DT_SK"))
    .withColumn("PATN_SSN", F.col("PATN_SSN"))
)

df_final = (
    df_Transform
    .select(
        "CLM_SK",
        "CLM_ID",
        "SRC_SYS_CD",
        "CLM_TYP_CD",
        "CLM_SUBTYP_CD",
        "CLM_SVC_STRT_DT_SK",
        "SRC_SYS_GRP_PFX",
        "SRC_SYS_GRP_ID",
        "SRC_SYS_GRP_SFX",
        "SUB_SSN",
        "PATN_LAST_NM",
        "PATN_FIRST_NM",
        "PATN_GNDR_CD",
        "PATN_BRTH_DT_SK",
        "ERR_CD",
        "ERR_DESC",
        "FEP_MBR_ID",
        "SUB_FIRST_NM",
        "SUB_LAST_NM",
        "SRC_SYS_SUB_ID",
        "SRC_SYS_MBR_SFX_NO",
        "GRP_ID",
        "FILE_DT_SK",
        "PATN_SSN"
    )
    .withColumn("CLM_ID", F.rpad(F.col("CLM_ID"), 255, " "))
    .withColumn("SRC_SYS_CD", F.rpad(F.col("SRC_SYS_CD"), 255, " "))
    .withColumn("CLM_TYP_CD", F.rpad(F.col("CLM_TYP_CD"), 255, " "))
    .withColumn("CLM_SUBTYP_CD", F.rpad(F.col("CLM_SUBTYP_CD"), 255, " "))
    .withColumn("CLM_SVC_STRT_DT_SK", F.rpad(F.col("CLM_SVC_STRT_DT_SK"), 10, " "))
    .withColumn("SRC_SYS_GRP_PFX", F.rpad(F.col("SRC_SYS_GRP_PFX"), 255, " "))
    .withColumn("SRC_SYS_GRP_ID", F.rpad(F.col("SRC_SYS_GRP_ID"), 255, " "))
    .withColumn("SRC_SYS_GRP_SFX", F.rpad(F.col("SRC_SYS_GRP_SFX"), 255, " "))
    .withColumn("SUB_SSN", F.rpad(F.col("SUB_SSN"), 255, " "))
    .withColumn("PATN_LAST_NM", F.rpad(F.col("PATN_LAST_NM"), 255, " "))
    .withColumn("PATN_FIRST_NM", F.rpad(F.col("PATN_FIRST_NM"), 255, " "))
    .withColumn("PATN_GNDR_CD", F.rpad(F.col("PATN_GNDR_CD"), 255, " "))
    .withColumn("PATN_BRTH_DT_SK", F.rpad(F.col("PATN_BRTH_DT_SK"), 10, " "))
    .withColumn("ERR_CD", F.rpad(F.col("ERR_CD"), 255, " "))
    .withColumn("ERR_DESC", F.rpad(F.col("ERR_DESC"), 255, " "))
    .withColumn("FEP_MBR_ID", F.rpad(F.col("FEP_MBR_ID"), 255, " "))
    .withColumn("SUB_FIRST_NM", F.rpad(F.col("SUB_FIRST_NM"), 255, " "))
    .withColumn("SUB_LAST_NM", F.rpad(F.col("SUB_LAST_NM"), 255, " "))
    .withColumn("SRC_SYS_SUB_ID", F.rpad(F.col("SRC_SYS_SUB_ID"), 255, " "))
    .withColumn("SRC_SYS_MBR_SFX_NO", F.rpad(F.col("SRC_SYS_MBR_SFX_NO"), 3, " "))
    .withColumn("GRP_ID", F.rpad(F.col("GRP_ID"), 255, " "))
    .withColumn("FILE_DT_SK", F.rpad(F.col("FILE_DT_SK"), 10, " "))
    .withColumn("PATN_SSN", F.rpad(F.col("PATN_SSN"), 255, " "))
)

write_files(
    df_final,
    f"{adls_path}/load/P_CLM_MBRSH_ERR_RECYC_RX.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)